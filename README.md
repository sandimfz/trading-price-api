# Trading Price API

Backend service penyedia harga real-time (tick, candle OHLC, indikator teknikal) yang diambil dari upstream WebSocket, dibangun dengan Go.
## Arsitektur

```
tvsocket (WS upstream) ──> tickCh ──> candle.Builder ──> store + broadcast.Hub
                                        │                      │
                                   (OHLC per interval)    (push ke client WS)
scanner (HTTP) ──> cache TTL 10s ──> /v1/indicator
apikey + ratelimit ──> middleware ──> /v1/* (HTTP REST)
```

## Struktur Folder

```
cmd/server/            # entrypoint
internal/
  config/              # load & validasi env
  tvsocket/            # client upstream WS + protocol ~m~{len}~m~ + reconnect + ping
  subscription/        # ref counting simbol
  candle/              # builder OHLC + store ring buffer
  scanner/             # scanner HTTP + cache + symbol map
  broadcast/           # pub/sub ke client downstream
  apikey/              # generate, validasi, middleware auth
  ratelimit/           # token bucket
  httpapi/             # REST API (chi)
  wsapi/               # WebSocket endpoint client
  storage/             # postgres + redis
pkg/types/             # struct data murni (Tick, Candle)
migrations/            # SQL migration (golang-migrate)
scripts/               # helper CLI
```

## Prasyarat

- Go 1.22+
- PostgreSQL + TimescaleDB (opsional untuk dev — server tetap jalan tanpa DB)
- Redis (opsional untuk dev)
- Air (hot reload): `go install github.com/air-verse/air@latest`

## Setup

```bash
cp .env.example .env        # sesuaikan value
make build
make migrate-up             # butuh DATABASE_URL terisi
make dev                    # air, hot reload
```

Tanpa PostgreSQL/Redis, server tetap jalan (log warning) — API key auth memakai repository in-memory kosong, jadi buat key langsung via SQL atau nyalakan DB.

## Endpoint

| Method | Path              | Auth | Keterangan                        |
| ------ | ----------------- | ---- | --------------------------------- |
| GET    | `/healthz`        | -    | health check                      |
| GET    | `/v1/quote`       | key  | `?symbol=FOREXCOM:XAUUSD` harga terakhir |
| GET    | `/v1/candle`      | key  | `?symbol=..&interval=1m&limit=100` |
| GET    | `/v1/indicator`   | key  | `?symbol=..` RSI, MACD, dst       |
| WS     | `:8081/ws?api_key=..` | key | subscribe/unsubscribe simbol real-time |

Auth header: `Authorization: Bearer <key>` atau `X-API-Key: <key>`.

Rate limit headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, status `429`.

## Generate API Key

```bash
make gen-apikey -- -user <UUID> -tier 2 -database-url "$DATABASE_URL"
```

Plain key hanya tampil sekali; yang tersimpan di DB hanya SHA-256 hash.

## Testing

```bash
make test        # go test -race -cover ./...
```

## Catatan

- Docker/deploy (`deploy/`) sengaja belum dibuat.
- Tabel `ticks` (raw tick history) opsional — ring buffer in-memory 3600 tick/simbol sudah cukup untuk dev.
- Rate limiter saat ini in-memory (per instance); ganti implementasi Redis untuk scale horizontal.
