package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

type tenant struct {
	name           string
	plan           string
	rateLimitRPS   int
	maxConcurrent  int
	dispatchWeight int
	apiKey         string // raw bearer key from client
}

var fixtures = []tenant{
	{
		name:           "Free Tenant",
		plan:           "free",
		rateLimitRPS:   5,
		maxConcurrent:  2,
		dispatchWeight: 1,
		apiKey:         "free-tenant-api-key-plaintext",
	},
	{
		name:           "Pro Tenant",
		plan:           "pro",
		rateLimitRPS:   30,
		maxConcurrent:  8,
		dispatchWeight: 2,
		apiKey:         "pro-tenant-api-key-plaintext",
	},
	{
		name:           "Enterprise Tenant",
		plan:           "enterprise",
		rateLimitRPS:   200,
		maxConcurrent:  20,
		dispatchWeight: 3,
		apiKey:         "enterprise-tenant-api-key-plaintext",
	},
}

func main() {
	ctx := context.Background()

	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		slog.Error("DATABASE_URL is not set")
		os.Exit(1)
	}

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		slog.Error("failed to open pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("failed to reach database", "error", err)
		os.Exit(1)
	}

	for _, f := range fixtures {
		var tenantID string
		err := pool.QueryRow(ctx, `
			INSERT INTO tenants (name, plan, rate_limit_rps, max_concurrent, dispatch_weight)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT DO NOTHING
			RETURNING id
		`, f.name, f.plan, f.rateLimitRPS, f.maxConcurrent, f.dispatchWeight).Scan(&tenantID)

		if err != nil {
			slog.Info("tenant already exists, skipping", "name", f.name)
			continue
		}

		sum := sha256.Sum256([]byte(f.apiKey))
		keyHash := hex.EncodeToString(sum[:]) // store hashed key

		_, err = pool.Exec(ctx, `
			INSERT INTO keys (hash, tenant_id)
			VALUES ($1, $2)
			ON CONFLICT DO NOTHING
		`, keyHash, tenantID)
		if err != nil {
			slog.Error("failed to insert api key", "tenant", f.name, "error", err)
			os.Exit(1)
		}

		slog.Info("seeded tenant",
			"name", f.name,
			"plan", f.plan,
			"tenant_id", tenantID,
			"key", f.apiKey,
		)

		fmt.Printf("\nTenant: %s\n  ID:      %s\n  API key: %s\n  Hash:    %s\n",
			f.name, tenantID, f.apiKey, keyHash)
	}

	fmt.Println("\nseed complete")
}
