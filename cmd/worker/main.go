package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/asjiaa/orchestrator/internal/processor"
	iqueue "github.com/asjiaa/orchestrator/internal/queue"
	"github.com/asjiaa/orchestrator/internal/storage"
	"github.com/asjiaa/orchestrator/internal/store"
	"github.com/asjiaa/orchestrator/internal/worker"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM,
		syscall.SIGINT,
	)
	defer cancel()

	redisClient, err := iqueue.NewRedisClient(mustEnv("REDIS_URL"))
	if err != nil {
		slog.Error("connect redis", "error", err)
		os.Exit(1)
	}
	if err := redisClient.Ping(ctx).Err(); err != nil {
		slog.Error("ping redis", "error", err)
		os.Exit(1)
	}

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

	s, err := store.NewPostgresStore(ctx, mustEnv("DATABASE_URL"))
	if err != nil {
		slog.Error("connect postgres", "error", err)
		os.Exit(1)
	}
	defer s.Close()

	vipsWorkers := vipsWorkerCount()
	slog.Info("starting vips processor", "max_concurrent", vipsWorkers)
	p, err := processor.NewGovipsProcessor(vipsWorkers)

	if err != nil {
		slog.Error("initialise govips processor", "error", err)
		os.Exit(1)
	}
	defer p.Shutdown()

	n := workerConcurrency()
	workerID := buildWorkerID()
	slog.Info("starting workers", "count", n)

	go func() {
		if err := http.ListenAndServe(":6060", nil); err != nil {
			slog.Error("pprof server failed", "error", err)
		}
	}()

	cc := iqueue.NewConcurrencyChecker(redisClient)
	q := iqueue.NewRedisQueue(redisClient, cc)
	d := worker.NewDispatcher(s, q, cc)

	go d.Run(ctx)

	r := worker.NewReaper(s, q)
	go r.Run(ctx)

	w := worker.New(workerID, q, s, storageClient, p)

	// Draining pattern on worker shutdown
	channel := make(chan struct{})
	go func() {
		w.RunN(ctx, n)
		close(channel)
	}()

	<-ctx.Done()
	slog.Info("shutdown: signal received, draining workers")

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer drainCancel()

	select {
	case <-channel:
		slog.Info("shutdown: all workers drained cleanly")
	case <-drainCtx.Done():
		slog.Warn("shutdown: drain timeout exceeded, forcing exit")
	}

	slog.Info("shutdown: exiting")

	slog.Info("all workers stopped, exiting")
}

func workerConcurrency() int {
	v := os.Getenv("WORKER_CONCURRENCY")
	if v == "" {
		return 2
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		slog.Warn("invalid WORKER_CONCURRENCY, using default", "value", v)
		return 2
	}
	return n
}

func vipsWorkerCount() int {
	if v := os.Getenv("VIPS_WORKERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil && n > 0 {
			return n
		}
		slog.Warn("invalid VIPS_WORKERS, falling back to GOMAXPROCS", "value", v)
	}
	return runtime.GOMAXPROCS(0)
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("missing required env var", "key", key)
		os.Exit(1)
	}
	return v
}

func buildWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "worker"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}
