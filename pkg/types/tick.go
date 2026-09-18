package types

// Tick adalah data harga real-time per simbol dari upstream tvsocket.
type Tick struct {
	Symbol    string  `json:"symbol"`
	LastPrice float64 `json:"lp"`
	Change    float64 `json:"ch"`
	ChangePct float64 `json:"chp"`
	High      float64 `json:"high_price"`
	Low       float64 `json:"low_price"`
	Timestamp int64   `json:"ts"`
}
