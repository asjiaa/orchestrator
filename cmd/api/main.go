package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/asjiaa/orchestrator/internal/api"
	iqueue "github.com/asjiaa/orchestrator/internal/queue"
	"github.com/asjiaa/orchestrator/internal/storage"
	"github.com/asjiaa/orchestrator/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx := context.Background()

	redisClient, err := iqueue.NewRedisClient(mustEnv("REDIS_URL"))
	if err != nil {
		slog.Error("connect redis", "error", err)
		os.Exit(1)
	}
	if err := redisClient.Ping(ctx).Err(); err != nil {
		slog.Error("ping redis", "error", err)
		os.Exit(1)
	}

	cc := iqueue.NewConcurrencyChecker(redisClient)
	idem := iqueue.NewIdempotencyStore(redisClient)
	rl := iqueue.NewRateLimiter(redisClient)

	q := iqueue.NewRedisQueue(redisClient, cc)

	s, err := store.NewPostgresStore(ctx, mustEnv("DATABASE_URL"))
	if err != nil {
		slog.Error("connect postgres", "error", err)
		os.Exit(1)
	}
	defer s.Close()

	storageCfg := storage.Config{
		Endpoint:        os.Getenv("S3_ENDPOINT"),
		Bucket:          mustEnv("S3_BUCKET"),
		Region:          mustEnv("AWS_REGION"),
		AccessKeyID:     mustEnv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: mustEnv("AWS_SECRET_ACCESS_KEY"),
	}
	storageClient, err := storage.NewClient(ctx, storageCfg)
	if err != nil {
		slog.Error("connect storage", "error", err)
		os.Exit(1)
	}

	h := api.NewHandler(q, s, storageClient, idem)

	router := api.NewRouter(h, s, rl)

	addr := ":8080"
	slog.Info("api listening", "addr", addr)
	if err := http.ListenAndServe(addr, router); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("missing required env var", "key", key)
		os.Exit(1)
	}
	return v
}
