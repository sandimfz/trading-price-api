package httpapi

import (
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alen/trading-price-api/internal/apikey"
	"github.com/alen/trading-price-api/internal/ratelimit"
	"github.com/alen/trading-price-api/internal/scanner"
	"github.com/alen/trading-price-api/internal/tvsocket"
)

// Deps adalah dependensi yang dibutuhkan handler HTTP.
type Deps struct {
	KeyService  *apikey.KeyService
	Limiter     ratelimit.Limiter
	Scanner     *scanner.Client
	Upstream    *tvsocket.Client
	DB          *pgxpool.Pool
	LatestTick  func(symbol string) (float64, bool)
	Subscriber  Subscriber
	CandleStore candleProvider
}

// Subscriber adalah interface untuk menambah/menghapus subscription simbol
// ke upstream, dipenuhi oleh wiring di main.
type Subscriber interface {
	Subscribe(symbol string) error
	Unsubscribe(symbol string) error
}

// Router membuat HTTP router dengan semua route.
func Router(d Deps) *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", handleHealth(d))
	r.Get("/openapi.yaml", handleOpenAPI())
	r.Get("/docs/openapi.yaml", handleOpenAPI())

	r.Route("/v1", func(r chi.Router) {
		r.Use(d.KeyService.AuthMiddleware(""))
		r.Use(rateLimitMiddleware(d.Limiter))

		r.Get("/quote", handleQuote(d))
		r.Get("/candle", handleCandle(d))
		r.Get("/indicator", handleIndicator(d))
	})

	return r
}

func handleOpenAPI() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		for _, p := range []string{"openapi.yaml", "./openapi.yaml", "../openapi.yaml", "../../openapi.yaml"} {
			if b, err := os.ReadFile(p); err == nil {
				w.Header().Set("Content-Type", "application/yaml")
				w.Header().Set("Cache-Control", "public, max-age=60")
				_, _ = w.Write(b)
				return
			}
		}
		http.Error(w, `{"error":"openapi spec not found"}`, http.StatusNotFound)
	}
}

func rateLimitMiddleware(l ratelimit.Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			k, ok := apikey.FromContext(r.Context())
			if !ok {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}

			res, err := l.Allow(r.Context(), k.ID, k.RequestsPerMinute)
			if err != nil {
				http.Error(w, `{"error":"rate limiter unavailable"}`, http.StatusInternalServerError)
				return
			}

			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(res.Limit))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(res.ResetAt.Unix(), 10))

			if !res.Allowed {
				retry := int(res.ResetAt.Sub(time.Now()).Seconds()) + 1
				w.Header().Set("Retry-After", strconv.Itoa(retry))
				http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
