package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config adalah seluruh konfigurasi aplikasi, di-load dari environment variables.
type Config struct {
	AppEnv   string
	HTTPPort string
	WSPort   string

	DatabaseURL string

	RedisAddr     string
	RedisPassword string

	UpstreamWSURL      string
	UpstreamWSOrigin   string
	UpstreamScannerURL string
	DefaultSymbols     []string

	RateLimitFree int
	RateLimitPro  int

	// RunMigrations: jalankan migration otomatis saat startup.
	RunMigrations bool
	// Retention window data di DB (format interval Postgres).
	TickRetention   string
	CandleRetention string
}

// Load membaca dan memvalidasi env config.
func Load() (*Config, error) {
	c := &Config{
		AppEnv:             getEnv("APP_ENV", "development"),
		HTTPPort:           getEnv("HTTP_PORT", "8080"),
		WSPort:             getEnv("WS_PORT", "8081"),
		DatabaseURL:        getEnv("DATABASE_URL", "postgres://user:password@localhost:5432/trading_price_api?sslmode=disable"),
		RedisAddr:          getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:      getEnv("REDIS_PASSWORD", ""),
		UpstreamWSURL:      getEnv("UPSTREAM_WS_URL", "wss://data.tradingview.com/socket.io/websocket"),
		UpstreamWSOrigin:   getEnv("UPSTREAM_WS_ORIGIN", "https://data.tradingview.com"),
		UpstreamScannerURL: getEnv("UPSTREAM_SCANNER_URL", "https://scanner.tradingview.com"),
		RunMigrations:      getEnvBool("RUN_MIGRATIONS", true),
		TickRetention:      getEnv("TICK_RETENTION", "168 hours"),
		CandleRetention:    getEnv("CANDLE_RETENTION", "720 hours"),
	}

	// Cloud Run menginjeksi PORT untuk satu listener: HTTP dan WS di port yang sama.
	if port, ok := os.LookupEnv("PORT"); ok && !envSet("HTTP_PORT") && !envSet("WS_PORT") {
		c.HTTPPort = port
		c.WSPort = port
	}

	symbols := getEnv("DEFAULT_SYMBOLS", "FOREXCOM:XAUUSD,FOREXCOM:XAGUSD,FOREXCOM:EURUSD")
	for _, s := range strings.Split(symbols, ",") {
		if s = strings.TrimSpace(s); s != "" {
			c.DefaultSymbols = append(c.DefaultSymbols, s)
		}
	}

	var err error
	if c.RateLimitFree, err = getEnvInt("RATE_LIMIT_FREE", 60); err != nil {
		return nil, fmt.Errorf("invalid RATE_LIMIT_FREE: %w", err)
	}
	if c.RateLimitPro, err = getEnvInt("RATE_LIMIT_PRO", 1000); err != nil {
		return nil, fmt.Errorf("invalid RATE_LIMIT_PRO: %w", err)
	}

	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// Validate memastikan konfigurasi penting terisi.
func (c *Config) Validate() error {
	if c.UpstreamWSURL == "" {
		return fmt.Errorf("UPSTREAM_WS_URL wajib diisi")
	}
	if len(c.DefaultSymbols) == 0 {
		return fmt.Errorf("DEFAULT_SYMBOLS tidak boleh kosong")
	}
	return nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func envSet(key string) bool {
	_, ok := os.LookupEnv(key)
	return ok
}

func getEnvBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}
