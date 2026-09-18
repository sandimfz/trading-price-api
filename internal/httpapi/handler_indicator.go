package httpapi

import (
	"net/http"

	"github.com/alen/trading-price-api/internal/scanner"
)

// handleIndicator mengembalikan indikator teknikal dari scanner (RSI, MACD, dst).
// Kelas pasar diambil dari tabel symbols untuk memilih endpoint scanner
// (forex/crypto/global); jika tidak dikenal, default /global/scan.
func handleIndicator(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		symbol := r.URL.Query().Get("symbol")
		if symbol == "" {
			http.Error(w, `{"error":"parameter symbol wajib diisi"}`, http.StatusBadRequest)
			return
		}

		class := ""
		if d.DB != nil {
			_ = d.DB.QueryRow(r.Context(),
				"SELECT asset_class FROM symbols WHERE code = $1", symbol).Scan(&class)
		}

		result, err := d.Scanner.Scan(r.Context(), []scanner.SymbolQuery{{Code: symbol, Class: class}})
		if err != nil {
			http.Error(w, `{"error":"scanner tidak tersedia"}`, http.StatusBadGateway)
			return
		}

		ind, ok := result[symbol]
		if !ok {
			http.Error(w, `{"error":"simbol tidak ditemukan di scanner"}`, http.StatusNotFound)
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"symbol":     symbol,
			"indicators": ind,
		})
	}
}
