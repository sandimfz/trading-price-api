// gen-apikey adalah helper CLI untuk generate API key manual
// (misal untuk development). Jalankan: go run ./scripts/gen-apikey
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alen/trading-price-api/internal/apikey"
)

func main() {
	var (
		userID      = flag.String("user", "", "user_id (UUID) pemilik key")
		tierID      = flag.Int("tier", 1, "tier_id (1=free, 2=pro, 3=enterprise)")
		name        = flag.String("name", "default", "label key")
		scopes      = flag.String("scopes", "quote:read,candle:read", "daftar scope, pisahkan koma")
		databaseURL = flag.String("database-url", os.Getenv("DATABASE_URL"), "DATABASE_URL")
	)
	flag.Parse()

	if *userID == "" || *databaseURL == "" {
		log.Fatal("wajib isi -user dan -database-url (atau env DATABASE_URL)")
	}

	pool, err := pgxpool.New(context.Background(), *databaseURL)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer pool.Close()

	repo := apikey.NewPostgresRepository(pool)
	svc := apikey.NewService(repo, nil)

	scopeList := splitScopes(*scopes)
	plainKey, prefix, err := svc.Generate(context.Background(), *userID, *tierID, *name, scopeList)
	if err != nil {
		log.Fatalf("generate key: %v", err)
	}

	fmt.Println("=== API Key Baru ===")
	fmt.Println("prefix:", prefix)
	fmt.Println("plain key (TAMPILKAN SEKALI SAJA, tidak bisa dilihat lagi):")
	fmt.Println(plainKey)
}

func splitScopes(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if v := s[start:i]; v != "" {
				out = append(out, v)
			}
			start = i + 1
		}
	}
	return out
}
