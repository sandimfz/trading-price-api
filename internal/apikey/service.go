package apikey

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrInvalidKey dipakai saat key tidak dikenal/revoked/expired.
var ErrInvalidKey = errors.New("invalid api key")

// Scopes yang tersedia.
const (
	ScopeQuoteRead     = "quote:read"
	ScopeCandleRead    = "candle:read"
	ScopeIndicatorRead = "indicator:read"
	ScopeAdmin         = "admin"
)

// Key adalah metadata API key hasil validasi.
type Key struct {
	ID                string
	UserID            string
	Prefix            string
	Scopes            []string
	Tier              string
	RequestsPerMinute int
	MaxSymbolsPerKey  int
	MaxConcurrentWS   int
	ExpiresAt         time.Time
	RevokedAt         time.Time
}

// GenerateAPIKey membuat plain key + SHA-256 hash.
// Plain key hanya boleh ditampilkan sekali; yang disimpan hanya hash.
func GenerateAPIKey() (plainKey string, hashedKey string, err error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", "", err
	}

	prefix := make([]byte, 6)
	if _, err := rand.Read(prefix); err != nil {
		return "", "", err
	}
	prefixStr := base64.RawURLEncoding.EncodeToString(prefix)[:8]

	plainKey = fmt.Sprintf("tpa_live_%s.%s", prefixStr, base64.RawURLEncoding.EncodeToString(secret))

	hash := sha256.Sum256([]byte(plainKey))
	hashedKey = hex.EncodeToString(hash[:])
	return plainKey, hashedKey, nil
}

// PrefixOf mengambil bagian prefix yang aman ditampilkan dari plain key.
func PrefixOf(plainKey string) string {
	if i := strings.Index(plainKey, "."); i != -1 {
		return plainKey[:i]
	}
	return plainKey
}

// HashKey menghitung SHA-256 hex dari key masuk untuk lookup DB.
func HashKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

// HasScope mengecek apakah key punya scope tertentu.
func (k *Key) HasScope(scope string) bool {
	for _, s := range k.Scopes {
		if s == scope || s == ScopeAdmin {
			return true
		}
	}
	return false
}

// IsValid mengecek status key terhadap waktu sekarang.
func (k *Key) IsValid(now time.Time) bool {
	if k == nil || k.ID == "" {
		return false
	}
	if !k.RevokedAt.IsZero() {
		return false
	}
	if !k.ExpiresAt.IsZero() && now.After(k.ExpiresAt) {
		return false
	}
	return true
}
