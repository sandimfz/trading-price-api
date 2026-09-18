# ============================================================
# Build stage
# ============================================================
FROM golang:1.26-alpine AS build
WORKDIR /src

# cache dependencies
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

# ============================================================
# Runtime stage
# ============================================================
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 app

COPY --from=build /out/server /usr/local/bin/server
USER app

# Cloud Run hanya mengekspos satu port (PORT). HTTP + WS berbagi port yang sama.
ENV HTTP_PORT=8080 WS_PORT=8080
EXPOSE 8080

ENTRYPOINT ["server"]