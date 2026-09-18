.PHONY: dev build test lint migrate-up migrate-down gen-apikey

dev:
	air

build:
	go build -o bin/server ./cmd/server

test:
	go test -race -cover ./...

lint:
	golangci-lint run ./...

migrate-up:
	migrate -path migrations -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path migrations -database "$(DATABASE_URL)" down 1

gen-apikey:
	go run ./scripts/gen-apikey.go