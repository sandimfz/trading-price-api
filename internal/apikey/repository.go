package apikey

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository adalah interface akses DB untuk API key.
type Repository interface {
	// FindByHash mengambil key aktif (belum revoked) berdasarkan SHA-256 hash.
	FindByHash(ctx context.Context, hashedKey string) (*Key, error)
	// Create menyimpan key baru; mengembalikan id key.
	Create(ctx context.Context, userID string, tierID int, prefix, hashedKey, name string, scopes []string) (string, error)
	// Revoke menandai key sebagai revoked.
	Revoke(ctx context.Context, id string) error
}

// PostgresRepository implementasi Repository dengan pgxpool.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresRepository membuat repository PostgreSQL.
func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

// FindByHash mengambil key aktif berdasarkan hash, join tier untuk limit.
func (r *PostgresRepository) FindByHash(ctx context.Context, hashedKey string) (*Key, error) {
	query := `
		SELECT ak.id, ak.user_id, ak.key_prefix, ak.scopes,
		       rt.name, rt.requests_per_minute, rt.max_symbols_per_key, rt.max_concurrent_ws_conn,
		       ak.expires_at, ak.revoked_at
		FROM api_keys ak
		JOIN rate_limit_tiers rt ON rt.id = ak.tier_id
		WHERE ak.hashed_key = $1 AND ak.revoked_at IS NULL`

	var k Key
	var expiresAt, revokedAt *time.Time
	err := r.pool.QueryRow(ctx, query, hashedKey).Scan(
		&k.ID, &k.UserID, &k.Prefix, &k.Scopes,
		&k.Tier, &k.RequestsPerMinute, &k.MaxSymbolsPerKey, &k.MaxConcurrentWS,
		&expiresAt, &revokedAt,
	)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, ErrInvalidKey
		}
		return nil, fmt.Errorf("find api key by hash: %w", err)
	}
	if expiresAt != nil {
		k.ExpiresAt = *expiresAt
	}
	if revokedAt != nil {
		k.RevokedAt = *revokedAt
	}
	return &k, nil
}

// Create menyimpan key baru ke DB.
func (r *PostgresRepository) Create(ctx context.Context, userID string, tierID int, prefix, hashedKey, name string, scopes []string) (string, error) {
	var id string
	err := r.pool.QueryRow(ctx, `
		INSERT INTO api_keys (user_id, tier_id, key_prefix, hashed_key, name, scopes)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id`,
		userID, tierID, prefix, hashedKey, name, scopes,
	).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("insert api key: %w", err)
	}
	return id, nil
}

// Revoke menandai key sebagai revoked.
func (r *PostgresRepository) Revoke(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `UPDATE api_keys SET revoked_at = now() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("revoke api key: %w", err)
	}
	return nil
}
