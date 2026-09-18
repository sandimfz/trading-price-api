package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/alen/trading-price-api/internal/apikey"
	"github.com/alen/trading-price-api/internal/broadcast"
	"github.com/alen/trading-price-api/internal/candle"
	"github.com/alen/trading-price-api/internal/config"
	"github.com/alen/trading-price-api/internal/httpapi"
	"github.com/alen/trading-price-api/internal/ratelimit"
	"github.com/alen/trading-price-api/internal/scanner"
	"github.com/alen/trading-price-api/internal/storage/postgres"
	"github.com/alen/trading-price-api/internal/storage/redis"
	"github.com/alen/trading-price-api/internal/subscription"
	"github.com/alen/trading-price-api/internal/tvsocket"
	"github.com/alen/trading-price-api/internal/wsapi"
	"github.com/alen/trading-price-api/migrations"
	"github.com/alen/trading-price-api/pkg/types"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("server exited with error", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// --- storage opsional: gagal connect tidak mematikan server (dev mode) ---
	pgPool, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Warn("postgres unavailable, lanjut tanpa DB", "err", err)
		pgPool = nil
	}
	if pgPool != nil {
		defer pgPool.Close()
	}

	// --- auto-migration: jalankan schema saat startup (serverless friendly) ---
	if pgPool != nil && cfg.RunMigrations {
		if err := migrations.New(pgPool, log).Up(ctx); err != nil {
			return err
		}
	}

	// --- persister: tulis tick & candle ke DB secara async (batch) ---
	var persister *postgres.Persister
	if pgPool != nil {
		persister = postgres.NewPersister(pgPool, log, cfg.TickRetention, cfg.CandleRetention)
		go persister.Run(ctx)
	}

	rdb, err := redis.Connect(ctx, cfg.RedisAddr, cfg.RedisPassword)
	if err != nil {
		log.Warn("redis unavailable, lanjut tanpa redis", "err", err)
		rdb = nil
	}
	if rdb != nil {
		defer rdb.Close()
	}

	// --- shared state ---
	hub := broadcast.NewHub()
	store := candle.NewStore()
	tickCh := make(chan types.Tick, 4096)

	// --- upstream tvsocket ---
	upstream := tvsocket.NewClient(cfg.UpstreamWSURL, cfg.UpstreamWSOrigin, tickCh)

	// --- subscription manager: ref-count, hubung ke upstream ---
	subManager := subscription.NewManager(
		func(symbol string) error { return upstream.SubscribeSymbol(symbol) },
		func(symbol string) error { return upstream.UnsubscribeSymbol(symbol) },
	)

	// --- candle builder: tick -> OHLC, broadcast + store + persist ---
	builder := candle.NewBuilder(func(c types.Candle) {
		store.AddCandle(c)
		hub.PublishCandle(c)
		if persister != nil {
			persister.PushCandle(c)
		}
	})

	// --- tick fan-out: builder + store + hub + persister ---
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		builder.Run(tickCh)
	}()
	go func() {
		defer wg.Done()
		for tick := range tickCh {
			store.PushTick(tick)
			hub.PublishTick(tick)
			if persister != nil {
				persister.PushTick(tick)
			}
		}
	}()

	// subscribe simbol default
	for _, sym := range cfg.DefaultSymbols {
		_ = subManager.Subscribe(sym)
	}

	// --- API key & rate limit ---
	var keyRepo apikey.Repository
	if pgPool != nil {
		keyRepo = apikey.NewPostgresRepository(pgPool)
	}
	keyService := apikey.NewService(keyRepo, log)
	var limiter ratelimit.Limiter = ratelimit.NewMemLimiter()
	if rdb != nil {
		limiter = ratelimit.NewRedisLimiter(rdb.Raw())
		log.Info("rate limiter: redis")
	}
	scannerClient := scanner.NewClient(cfg.UpstreamScannerURL)

	// --- HTTP API ---
	httpDeps := httpapi.Deps{
		KeyService: keyService,
		Limiter:    limiter,
		Scanner:    scannerClient,
		Upstream:   upstream,
		DB:         pgPool,
		LatestTick: func(symbol string) (float64, bool) {
			ticks := store.RecentTicks(symbol, 1)
			if len(ticks) == 0 {
				return 0, false
			}
			return ticks[0].LastPrice, true
		},
		Subscriber:  subManager,
		CandleStore: store,
	}
	httpRouter := httpapi.Router(httpDeps)

	// --- WS API (downstream) ---
	wsHandler := wsapi.NewHandler(hub, subManager, keyService, log)
	wsRouter := chi.NewRouter()
	wsRouter.Get("/ws", wsHandler.ServeHTTP)

	if cfg.WSPort == cfg.HTTPPort {
		// Satu listener: HTTP API + WS di port yang sama (Cloud Run PORT).
		httpRouter.Get("/ws", wsHandler.ServeHTTP)
		httpServer := &http.Server{
			Addr:              ":" + cfg.HTTPPort,
			Handler:           httpRouter,
			ReadHeaderTimeout: 5 * time.Second,
		}
		log.Info("single listener (http+ws)", "port", cfg.HTTPPort)
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("http server error", "err", err)
			}
		}()
		<-ctx.Done()
		log.Info("shutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		_ = upstream.Close()
		close(tickCh)
		wg.Wait()
		if persister != nil {
			persister.Flush(shutdownCtx)
		}
		return nil
	}

	httpServer := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           httpRouter,
		ReadHeaderTimeout: 5 * time.Second,
	}
	wsServer := &http.Server{
		Addr:              ":" + cfg.WSPort,
		Handler:           wsRouter,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// --- start upstream + servers ---
	go upstream.RunWithReconnect(ctx)

	go func() {
		log.Info("http server listening", "port", cfg.HTTPPort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", "err", err)
		}
	}()
	go func() {
		log.Info("ws server listening", "port", cfg.WSPort)
		if err := wsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("ws server error", "err", err)
		}
	}()

	// --- graceful shutdown ---
	<-ctx.Done()
	log.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = wsServer.Shutdown(shutdownCtx)
	_ = upstream.Close()
	close(tickCh)
	wg.Wait()
	if persister != nil {
		persister.Flush(shutdownCtx)
	}

	return nil
}
