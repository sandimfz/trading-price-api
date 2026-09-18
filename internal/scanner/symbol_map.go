package scanner

import "strings"

// ToScannerSymbol mengubah format internal (FOREXCOM:XAUUSD) ke format
// scanner TradingView (FX:XAUUSD).
func ToScannerSymbol(symbol string) string {
	exchange, rest, ok := strings.Cut(symbol, ":")
	if !ok {
		return symbol
	}
	switch exchange {
	case "FOREXCOM":
		return "FX:" + rest
	case "CRYPTO":
		return "CRYPTO:" + rest
	case "NASDAQ":
		return "NASDAQ:" + rest
	case "NYSE":
		return "NYSE:" + rest
	case "AMEX":
		return "AMEX:" + rest
	case "OANDA":
		return "OANDA:" + rest
	case "BITSTAMP":
		return "BITSTAMP:" + rest
	default:
		return symbol
	}
}

// FromScannerSymbol mengubah format scanner (FX:XAUUSD) kembali ke format
// internal (FOREXCOM:XAUUSD).
func FromScannerSymbol(symbol string) string {
	exchange, rest, ok := strings.Cut(symbol, ":")
	if !ok {
		return symbol
	}
	switch exchange {
	case "FX":
		return "FOREXCOM:" + rest
	case "CRYPTO":
		return "CRYPTO:" + rest
	case "NASDAQ":
		return "NASDAQ:" + rest
	case "NYSE":
		return "NYSE:" + rest
	case "AMEX":
		return "AMEX:" + rest
	case "OANDA":
		return "OANDA:" + rest
	case "BITSTAMP":
		return "BITSTAMP:" + rest
	default:
		return symbol
	}
}
