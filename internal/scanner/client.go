package scanner

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Indicator adalah hasil scan teknikal untuk satu simbol.
// Field diurutkan sesuai posisi kolom di var columns (index 0..29).
type Indicator struct {
	Name         *string  `json:"name,omitempty"`
	Description  *string  `json:"description,omitempty"`
	Price        *float64 `json:"close,omitempty"`
	ChangePct    *float64 `json:"change,omitempty"`
	ChangeAbs    *float64 `json:"change_abs,omitempty"`
	RecommendAll *float64 `json:"Recommend.All,omitempty"`
	RSI          *float64 `json:"RSI,omitempty"`
	RSIPrev      *float64 `json:"RSI[1],omitempty"`
	StochK       *float64 `json:"Stoch.K,omitempty"`
	StochD       *float64 `json:"Stoch.D,omitempty"`
	Mom          *float64 `json:"Mom,omitempty"`
	MomPrev      *float64 `json:"Mom[1],omitempty"`
	MACD         *float64 `json:"MACD.macd,omitempty"`
	MACDSignal   *float64 `json:"MACD.signal,omitempty"`
	EMA5         *float64 `json:"EMA5,omitempty"`
	EMA10        *float64 `json:"EMA10,omitempty"`
	EMA20        *float64 `json:"EMA20,omitempty"`
	SMA20        *float64 `json:"SMA20,omitempty"`
	SMA50        *float64 `json:"SMA50,omitempty"`
	SMA200       *float64 `json:"SMA200,omitempty"`
	BBUpper      *float64 `json:"BB.upper,omitempty"`
	BBLower      *float64 `json:"BB.lower,omitempty"`
	ADX          *float64 `json:"ADX,omitempty"`
	ADXPlusDI    *float64 `json:"ADX+DI,omitempty"`
	ADXMinusDI   *float64 `json:"ADX-DI,omitempty"`
	AO           *float64 `json:"AO,omitempty"`
	AOPrev       *float64 `json:"AO[1],omitempty"`
	ATR          *float64 `json:"ATR,omitempty"`
	High         *float64 `json:"high,omitempty"`
	Low          *float64 `json:"low,omitempty"`
}

// columns adalah daftar kolom yang diminta ke scanner TradingView.
// URUTAN INI TIDAK BOLEH BERUBAH — parseRow memetakan berdasarkan posisi.
var columns = []string{
	"name", "description", "close", "change", "change_abs",
	"Recommend.All", "RSI", "RSI[1]", "Stoch.K", "Stoch.D",
	"Mom", "Mom[1]", "MACD.macd", "MACD.signal",
	"EMA5", "EMA10", "EMA20", "SMA20", "SMA50", "SMA200",
	"BB.upper", "BB.lower", "ADX", "ADX+DI", "ADX-DI",
	"AO", "AO[1]", "ATR", "high", "low",
}

// scannerRequest adalah payload POST ke endpoint scanner TradingView.
type scannerRequest struct {
	Symbols struct {
		Tickers   []string `json:"tickers"`
		QueryType string   `json:"query_type"`
	} `json:"symbols"`
	Columns []string `json:"columns"`
	Options struct {
		Lang string `json:"lang"`
	} `json:"options"`
	Range []int `json:"range"`
	Sort  struct {
		SortBy    string `json:"sortBy"`
		SortOrder string `json:"sortOrder"`
	} `json:"sort"`
	Filter []struct {
		Left      string `json:"left"`
		Operation string `json:"operation"`
	} `json:"filter"`
}

// scannerResponse adalah bentuk respon scanner: { "data": [ { "d": [...], "s": "FX:XAUUSD" } ] }.
type scannerResponse struct {
	Data []struct {
		D []any  `json:"d"`
		S string `json:"s"`
	} `json:"data"`
}

// Client memanggil scanner HTTP TradingView dengan cache TTL.
type Client struct {
	httpClient *http.Client
	url        string
	cache      *Cache
}

// NewClient membuat scanner client dengan cache TTL 10 detik.
func NewClient(url string) *Client {
	return &Client{
		httpClient: &http.Client{Timeout: 10 * time.Second},
		url:        url,
		cache:      NewCache(10 * time.Second),
	}
}

// SymbolQuery adalah simbol beserta kelas pasarnya (dari tabel symbols).
type SymbolQuery struct {
	Code  string
	Class string
}

// marketPath memilih endpoint scanner sesuai kelas pasar (docs TradingViewScannerService):
// forex -> /forex/scan, crypto -> /crypto/scan, sisanya (stock/index/metal) -> /global/scan.
func marketPath(class string) string {
	switch class {
	case "forex":
		return "/forex/scan"
	case "crypto":
		return "/crypto/scan"
	default:
		return "/global/scan"
	}
}

// Scan mengambil indikator untuk daftar simbol (format internal, misal FOREXCOM:XAUUSD).
// Hasil di-cache per simbol dengan TTL. Simbol yang tidak ditemukan di endpoint
// marketnya akan dicoba ulang via /global/scan sebagai fallback.
func (c *Client) Scan(ctx context.Context, queries []SymbolQuery) (map[string]Indicator, error) {
	out := make(map[string]Indicator, len(queries))
	byMarket := make(map[string][]string)

	for _, q := range queries {
		if v, ok := c.cache.Get(q.Code); ok {
			out[q.Code] = v
			continue
		}
		path := marketPath(q.Class)
		byMarket[path] = append(byMarket[path], q.Code)
	}

	if len(byMarket) == 0 {
		return out, nil
	}

	for path, codes := range byMarket {
		found, missing, err := c.scanMarket(ctx, path, codes)
		if err != nil {
			return nil, err
		}
		for k, v := range found {
			out[k] = v
		}
		if len(missing) > 0 && path != "/global/scan" {
			found, _, err := c.scanMarket(ctx, "/global/scan", missing)
			if err != nil {
				return nil, err
			}
			for k, v := range found {
				out[k] = v
			}
		}
	}

	return out, nil
}

// scanMarket melakukan satu POST ke endpoint scanner tertentu.
func (c *Client) scanMarket(ctx context.Context, path string, symbols []string) (map[string]Indicator, []string, error) {
	out := make(map[string]Indicator, len(symbols))

	scannerSymbols := make([]string, 0, len(symbols))
	for _, sym := range symbols {
		scannerSymbols = append(scannerSymbols, ToScannerSymbol(sym))
	}

	var req scannerRequest
	req.Symbols.Tickers = scannerSymbols
	req.Symbols.QueryType = "scan"
	req.Columns = columns
	req.Options.Lang = "en"
	req.Range = []int{0, len(scannerSymbols)}
	req.Sort.SortBy = "close"
	req.Sort.SortOrder = "desc"
	req.Filter = []struct {
		Left      string `json:"left"`
		Operation string `json:"operation"`
	}{{Left: "close", Operation: "nempty"}}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal scanner request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+path, strings.NewReader(string(body)))
	if err != nil {
		return nil, nil, fmt.Errorf("new scanner request: %w", err)
	}
	// Header meniru browser untuk menghindari anti-bot (lihat docs TradingViewScannerService).
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	httpReq.Header.Set("Accept", "application/json, text/plain, */*")
	httpReq.Header.Set("Accept-Language", "en-US,en;q=0.9")
	httpReq.Header.Set("Accept-Encoding", "gzip, deflate")
	httpReq.Header.Set("Origin", "https://www.tradingview.com")
	httpReq.Header.Set("Referer", "https://www.tradingview.com/")
	httpReq.Header.Set("Connection", "keep-alive")
	httpReq.Header.Set("Sec-Fetch-Dest", "empty")
	httpReq.Header.Set("Sec-Fetch-Mode", "cors")
	httpReq.Header.Set("Sec-Fetch-Site", "same-site")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, nil, fmt.Errorf("scanner request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("scanner returned status %d", resp.StatusCode)
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("read scanner response: %w", err)
	}
	raw, err = decodeCompressed(resp.Header.Get("Content-Encoding"), raw)
	if err != nil {
		return nil, nil, err
	}

	var parsed scannerResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, nil, fmt.Errorf("parse scanner response: %w", err)
	}

	found := make(map[string]bool, len(parsed.Data))
	for _, row := range parsed.Data {
		ind := parseRow(row.D)
		key := FromScannerSymbol(row.S)
		if key == "" {
			key = row.S
		}
		found[key] = true
		c.cache.Set(key, ind)
		out[key] = ind
	}

	var missing []string
	for _, sym := range symbols {
		if !found[sym] {
			missing = append(missing, sym)
		}
	}

	return out, missing, nil
}

// parseRow memetakan array nilai columns ke struct Indicator.
// Posisi index harus sinkron dengan var columns (lihat docs TradingViewScannerService).
func parseRow(values []any) Indicator {
	var ind Indicator
	setF := func(dst **float64, v any) {
		switch n := v.(type) {
		case float64:
			*dst = &n
		}
	}
	setS := func(dst **string, v any) {
		switch s := v.(type) {
		case string:
			*dst = &s
		}
	}
	if len(values) > 0 {
		setS(&ind.Name, values[0])
	}
	if len(values) > 1 {
		setS(&ind.Description, values[1])
	}
	if len(values) > 2 {
		setF(&ind.Price, values[2])
	}
	if len(values) > 3 {
		setF(&ind.ChangePct, values[3])
	}
	if len(values) > 4 {
		setF(&ind.ChangeAbs, values[4])
	}
	if len(values) > 5 {
		setF(&ind.RecommendAll, values[5])
	}
	if len(values) > 6 {
		setF(&ind.RSI, values[6])
	}
	if len(values) > 7 {
		setF(&ind.RSIPrev, values[7])
	}
	if len(values) > 8 {
		setF(&ind.StochK, values[8])
	}
	if len(values) > 9 {
		setF(&ind.StochD, values[9])
	}
	if len(values) > 10 {
		setF(&ind.Mom, values[10])
	}
	if len(values) > 11 {
		setF(&ind.MomPrev, values[11])
	}
	if len(values) > 12 {
		setF(&ind.MACD, values[12])
	}
	if len(values) > 13 {
		setF(&ind.MACDSignal, values[13])
	}
	if len(values) > 14 {
		setF(&ind.EMA5, values[14])
	}
	if len(values) > 15 {
		setF(&ind.EMA10, values[15])
	}
	if len(values) > 16 {
		setF(&ind.EMA20, values[16])
	}
	if len(values) > 17 {
		setF(&ind.SMA20, values[17])
	}
	if len(values) > 18 {
		setF(&ind.SMA50, values[18])
	}
	if len(values) > 19 {
		setF(&ind.SMA200, values[19])
	}
	if len(values) > 20 {
		setF(&ind.BBUpper, values[20])
	}
	if len(values) > 21 {
		setF(&ind.BBLower, values[21])
	}
	if len(values) > 22 {
		setF(&ind.ADX, values[22])
	}
	if len(values) > 23 {
		setF(&ind.ADXPlusDI, values[23])
	}
	if len(values) > 24 {
		setF(&ind.ADXMinusDI, values[24])
	}
	if len(values) > 25 {
		setF(&ind.AO, values[25])
	}
	if len(values) > 26 {
		setF(&ind.AOPrev, values[26])
	}
	if len(values) > 27 {
		setF(&ind.ATR, values[27])
	}
	if len(values) > 28 {
		setF(&ind.High, values[28])
	}
	if len(values) > 29 {
		setF(&ind.Low, values[29])
	}
	return ind
}

// decodeCompressed menangani respons gzip/deflate dari scanner
// (Accept-Encoding di-set manual, jadi Go tidak auto-decompress).
func decodeCompressed(encoding string, raw []byte) ([]byte, error) {
	switch encoding {
	case "", "identity":
		return raw, nil
	case "gzip":
		r, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		defer r.Close()
		return io.ReadAll(r)
	case "deflate":
		r, err := zlib.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("zlib reader: %w", err)
		}
		defer r.Close()
		return io.ReadAll(r)
	default:
		return nil, fmt.Errorf("unsupported content-encoding %q", encoding)
	}
}
