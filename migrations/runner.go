package migrations

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var FS embed.FS

// Runner menerapkan migration .up.sql yang belum pernah dijalankan,
// dicatat di tabel schema_migrations (dibuat otomatis).
type Runner struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func New(pool *pgxpool.Pool, log *slog.Logger) *Runner {
	return &Runner{pool: pool, log: log}
}

// Up menjalankan semua migration yang belum diterapkan, berurutan per versi.
func (r *Runner) Up(ctx context.Context) error {
	if _, err := r.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("buat schema_migrations: %w", err)
	}

	entries, err := FS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("baca embedded migrations: %w", err)
	}

	var versions []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".up.sql") {
			versions = append(versions, strings.TrimSuffix(name, ".up.sql"))
		}
	}
	sort.Strings(versions)

	applied := 0
	for _, v := range versions {
		var exists bool
		if err := r.pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", v).
			Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}

		sql, err := FS.ReadFile(v + ".up.sql")
		if err != nil {
			return err
		}

		tx, err := r.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %s gagal: %w", v, err)
		}
		if _, err := tx.Exec(ctx,
			"INSERT INTO schema_migrations (version) VALUES ($1)", v); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		applied++
		r.log.Info("migration diterapkan", "version", v)
	}

	if applied == 0 {
		r.log.Info("migrate: tidak ada migration baru")
	} else {
		r.log.Info("migrate: selesai", "applied", applied)
	}
	return nil
}
