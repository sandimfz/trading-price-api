package postgres

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alen/trading-price-api/pkg/types"
)

// Persister menulis tick & candle ke PostgreSQL secara async (batch insert)
// agar hot path tvsocket tidak pernah diblokir oleh I/O DB.
//
// Tick di-buffer lalu di-flush tiap 2 detik atau 200 baris; candle tiap
// 2 detik atau 50 baris. Jika buffer penuh, tick dibuang (drop) dan dihitung.
// Retention otomatis: ticks 7 hari, candles 30 hari.
type Persister struct {
	pool *pgxpool.Pool
	log  *slog.Logger

	symMu   sync.RWMutex
	symbols map[string]int32

	tickCh   chan types.Tick
	candleCh chan types.Candle

	droppedTicks    atomic.Int64
	droppedCandles  atomic.Int64
	insertedTicks   atomic.Int64
	insertedCandles atomic.Int64

	tickRetention   string
	candleRetention string

	flushHook func() // dipakai test; nil di production
}

// NewPersister membuat persister dengan buffer tick 8192 dan candle 256,
// lalu memuat peta code -> id dari tabel symbols. tickRetention/candleRetention
// berupa interval Postgres (contoh "24 hours", "720 hours").
func NewPersister(pool *pgxpool.Pool, log *slog.Logger, tickRetention, candleRetention string) *Persister {
	p := &Persister{
		pool:            pool,
		log:             log,
		symbols:         make(map[string]int32),
		tickCh:          make(chan types.Tick, 8192),
		candleCh:        make(chan types.Candle, 256),
		tickRetention:   tickRetention,
		candleRetention: candleRetention,
	}
	p.preloadSymbols(context.Background())
	return p
}

func (p *Persister) preloadSymbols(ctx context.Context) {
	rows, err := p.pool.Query(ctx, "SELECT id, code FROM symbols")
	if err != nil {
		p.log.Warn("persister: gagal preload simbol", "err", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id int32
		var code string
		if err := rows.Scan(&id, &code); err != nil {
			continue
		}
		p.symbols[code] = id
	}
	p.log.Info("persister: simbol dimuat", "count", len(p.symbols))
}

// resolveSymbolID mengembalikan id tabel symbols untuk sebuah code;
// lookup langsung ke DB jika belum ada di cache.
func (p *Persister) resolveSymbolID(ctx context.Context, code string) (int32, bool) {
	p.symMu.RLock()
	id, ok := p.symbols[code]
	p.symMu.RUnlock()
	if ok {
		return id, true
	}

	err := p.pool.QueryRow(ctx, "SELECT id FROM symbols WHERE code = $1", code).Scan(&id)
	if err != nil {
		return 0, false
	}
	p.symMu.Lock()
	p.symbols[code] = id
	p.symMu.Unlock()
	return id, true
}

// PushTick mengantrekan tick untuk ditulis; non-blocking, drop jika penuh.
func (p *Persister) PushTick(t types.Tick) {
	select {
	case p.tickCh <- t:
	default:
		p.droppedTicks.Add(1)
	}
}

// PushCandle mengantrekan candle untuk ditulis; non-blocking, drop jika penuh.
func (p *Persister) PushCandle(c types.Candle) {
	select {
	case p.candleCh <- c:
	default:
		p.droppedCandles.Add(1)
	}
}

// Run menjalankan dua writer (tick & candle) plus retention loop sampai ctx selesai.
func (p *Persister) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(3)

	go func() { defer wg.Done(); p.tickWriter(ctx) }()
	go func() { defer wg.Done(); p.candleWriter(ctx) }()
	go func() { defer wg.Done(); p.retentionLoop(ctx) }()

	<-ctx.Done()
	wg.Wait()
	p.log.Info("persister: berhenti",
		"inserted_ticks", p.insertedTicks.Load(),
		"inserted_candles", p.insertedCandles.Load(),
		"dropped_ticks", p.droppedTicks.Load(),
		"dropped_candles", p.droppedCandles.Load())
}

func (p *Persister) tickWriter(ctx context.Context) {
	const maxBatch = 200
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var batch []types.Tick
	flush := func() {
		if len(batch) == 0 {
			return
		}
		p.insertTicks(ctx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case t := <-p.tickCh:
			batch = append(batch, t)
			if len(batch) >= maxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (p *Persister) candleWriter(ctx context.Context) {
	const maxBatch = 50
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var batch []types.Candle
	flush := func() {
		if len(batch) == 0 {
			return
		}
		p.upsertCandles(ctx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case c := <-p.candleCh:
			batch = append(batch, c)
			if len(batch) >= maxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (p *Persister) insertTicks(ctx context.Context, ticks []types.Tick) {
	rows := make([][]any, 0, len(ticks))
	for _, t := range ticks {
		id, ok := p.resolveSymbolID(ctx, t.Symbol)
		if !ok {
			continue
		}
		rows = append(rows, []any{
			id, t.LastPrice, t.Change, t.ChangePct, t.High, t.Low,
			time.Unix(t.Timestamp, 0).UTC(),
		})
	}
	if len(rows) == 0 {
		return
	}

	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(`
			INSERT INTO ticks (symbol_id, last_price, change, change_pct, high_price, low_price, ts)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, r...)
	}
	if err := p.pool.SendBatch(ctx, batch).Close(); err != nil {
		p.log.Warn("persister: gagal insert ticks", "rows", len(rows), "err", err)
		return
	}
	p.insertedTicks.Add(int64(len(rows)))
}

func (p *Persister) upsertCandles(ctx context.Context, candles []types.Candle) {
	rows := make([][]any, 0, len(candles))
	for _, c := range candles {
		id, ok := p.resolveSymbolID(ctx, c.Symbol)
		if !ok {
			continue
		}
		rows = append(rows, []any{
			id, c.Interval, c.Open, c.High, c.Low, c.Close, c.Volume,
			time.Unix(c.Timestamp, 0).UTC(),
		})
	}
	if len(rows) == 0 {
		return
	}

	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(`
			INSERT INTO candles (symbol_id, interval, open, high, low, close, volume, bucket_start)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (symbol_id, interval, bucket_start)
			DO UPDATE SET
				high = GREATEST(candles.high, EXCLUDED.high),
				low  = LEAST(candles.low, EXCLUDED.low),
				close = EXCLUDED.close,
				volume = EXCLUDED.volume`, r...)
	}
	if err := p.pool.SendBatch(ctx, batch).Close(); err != nil {
		p.log.Warn("persister: gagal upsert candles", "rows", len(rows), "err", err)
		return
	}
	p.insertedCandles.Add(int64(len(rows)))
}

func (p *Persister) retentionLoop(ctx context.Context) {
	p.retain(ctx)
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.retain(ctx)
		}
	}
}

func (p *Persister) retain(ctx context.Context) {
	if _, err := p.pool.Exec(ctx,
		`DELETE FROM ticks WHERE ts < now() - $1::interval`, p.tickRetention); err != nil {
		p.log.Warn("persister: retention ticks gagal", "err", err)
	}
	if _, err := p.pool.Exec(ctx,
		`DELETE FROM candles WHERE bucket_start < now() - $1::interval`, p.candleRetention); err != nil {
		p.log.Warn("persister: retention candles gagal", "err", err)
	}
}

// Flush mengosongkan semua buffer ke DB secara sinkron (dipakai saat shutdown).
func (p *Persister) Flush(ctx context.Context) {
	p.flushTicksNow(ctx)
	p.flushCandlesNow(ctx)
}

func (p *Persister) flushTicksNow(ctx context.Context) {
	for {
		select {
		case t := <-p.tickCh:
			p.insertTicks(ctx, []types.Tick{t})
		default:
			return
		}
	}
}

func (p *Persister) flushCandlesNow(ctx context.Context) {
	for {
		select {
		case c := <-p.candleCh:
			p.upsertCandles(ctx, []types.Candle{c})
		default:
			return
		}
	}
}
