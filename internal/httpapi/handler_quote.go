package httpapi

import (
	"encoding/json"
	"net/http"
)

// handleHealth melaporkan status service.
func handleHealth(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"symbols": 0,
		})
	}
}

// handleQuote mengembalikan harga terakhir untuk satu simbol.
func handleQuote(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		symbol := r.URL.Query().Get("symbol")
		if symbol == "" {
			http.Error(w, `{"error":"parameter symbol wajib diisi"}`, http.StatusBadRequest)
			return
		}

		price, ok := d.LatestTick(symbol)
		if !ok {
			http.Error(w, `{"error":"simbol tidak ditemukan atau belum ada data"}`, http.StatusNotFound)
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"symbol": symbol,
			"price":  price,
		})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
