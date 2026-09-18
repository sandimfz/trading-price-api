package apikey

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// KeyResolver mengambil key dari cache/DB; hasil validasi di-cache TTL pendek.
type KeyResolver interface {
	// Resolve memvalidasi key dan mengembalikan metadata-nya.
	Resolve(ctx context.Context, key string) (*Key, error)
}

// KeyService menggabungkan repository + cache hasil validasi.
type KeyService struct {
	repo      Repository
	cache     *keyCache
	log       *slog.Logger
	cacheTTL  time.Duration
	auditHook func(action, prefix string)
}

// NewService membuat service API key dengan cache validasi TTL 60 detik.
func NewService(repo Repository, log *slog.Logger) *KeyService {
	return &KeyService{
		repo:     repo,
		cache:    newKeyCache(60 * time.Second),
		log:      log,
		cacheTTL: 60 * time.Second,
	}
}

// Resolve mengambil key dari cache atau DB.
func (s *KeyService) Resolve(ctx context.Context, key string) (*Key, error) {
	hashed := HashKey(key)

	if k, ok := s.cache.Get(hashed); ok {
		if !k.IsValid(time.Now()) {
			s.cache.Delete(hashed)
			return nil, ErrInvalidKey
		}
		return k, nil
	}

	if s.repo == nil {
		// DB tidak tersedia (dev mode): semua key ditolak dengan aman.
		s.log.Warn("apikey: repo nil, key ditolak")
		return nil, ErrInvalidKey
	}

	k, err := s.repo.FindByHash(ctx, hashed)
	if err != nil {
		return nil, err
	}
	if !k.IsValid(time.Now()) {
		return nil, ErrInvalidKey
	}
	s.cache.Set(hashed, k, s.cacheTTL)
	return k, nil
}

// Generate membuat key baru dan menyimpannya.
func (s *KeyService) Generate(ctx context.Context, userID string, tierID int, name string, scopes []string) (plainKey string, prefix string, err error) {
	if s.repo == nil {
		return "", "", ErrInvalidKey
	}

	plainKey, hashedKey, err := GenerateAPIKey()
	if err != nil {
		return "", "", fmt.Errorf("generate api key: %w", err)
	}
	prefix = PrefixOf(plainKey)

	if _, err := s.repo.Create(ctx, userID, tierID, prefix, hashedKey, name, scopes); err != nil {
		return "", "", err
	}
	if s.auditHook != nil {
		s.auditHook("api_key.created", prefix)
	}
	return plainKey, prefix, nil
}

// Revoke menonaktifkan key.
func (s *KeyService) Revoke(ctx context.Context, id string) error {
	if s.repo == nil {
		return ErrInvalidKey
	}
	if err := s.repo.Revoke(ctx, id); err != nil {
		return err
	}
	if s.auditHook != nil {
		s.auditHook("api_key.revoked", id)
	}
	return nil
}

// AuthMiddleware memvalidasi Authorization: Bearer <key> atau X-API-Key.
// Scope tidak kosong berarti handler butuh scope tertentu.
func (s *KeyService) AuthMiddleware(requiredScope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := extractKey(r)
			if key == "" {
				http.Error(w, `{"error":"missing api key"}`, http.StatusUnauthorized)
				return
			}

			k, err := s.Resolve(r.Context(), key)
			if err != nil {
				http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
				return
			}

			if requiredScope != "" && !k.HasScope(requiredScope) {
				http.Error(w, `{"error":"insufficient scope"}`, http.StatusForbidden)
				return
			}

			// key di-embed ke context untuk handler & ratelimit
			ctx := WithKey(r.Context(), k)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

type ctxKey struct{}

// WithKey menyimpan *Key di context.
func WithKey(ctx context.Context, k *Key) context.Context {
	return context.WithValue(ctx, ctxKey{}, k)
}

// FromContext mengambil *Key dari context (set oleh AuthMiddleware).
func FromContext(ctx context.Context) (*Key, bool) {
	k, ok := ctx.Value(ctxKey{}).(*Key)
	return k, ok
}

func extractKey(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}
