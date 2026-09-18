package httpapi

import (
	"net/http"
	"strconv"

	"github.com/alen/trading-price-api/pkg/types"
)

// candleProvider mengambil candle dari penyimpanan, di-set dari wiring.
type candleProvider interface {
	RecentCandles(symbol, interval string, n int) []types.Candle
}

// handleCandle mengembalikan histori candle OHLC.
func handleCandle(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		symbol := r.URL.Query().Get("symbol")
		interval := r.URL.Query().Get("interval")
		if symbol == "" || interval == "" {
			http.Error(w, `{"error":"parameter symbol dan interval wajib diisi"}`, http.StatusBadRequest)
			return
		}

		limit := 100
		if v := r.URL.Query().Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > 1000 {
				http.Error(w, `{"error":"limit harus antara 1-1000"}`, http.StatusBadRequest)
				return
			}
			limit = n
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"symbol":   symbol,
			"interval": interval,
			"candles":  d.CandleStore.RecentCandles(symbol, interval, limit),
		})
	}
}
