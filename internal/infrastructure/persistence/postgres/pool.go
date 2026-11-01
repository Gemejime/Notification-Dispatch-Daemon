// Package postgres provides cold-path PostgreSQL 16+ (PgBouncer) persistence adapters
// for hydrating tenant notification subscriptions, webhook targets, and cold event journals.
// CRITICAL INVARIANT (ADR-004): Never invoked synchronously on the hot CDC ingestion path.
package postgres

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

// Rows abstracts SQL result-set iteration for cold-path repository queries.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// Querier abstracts SQL query and command execution over a PgBouncer connection pool.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	Exec(ctx context.Context, sql string, args ...any) error
	Ping(ctx context.Context) error
	Close() error
}

// PoolConfig holds PgBouncer connection pool settings with automatic DSN redaction.
type PoolConfig struct {
	DSN             observability.SecretString
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	ConnectTimeout  time.Duration
}

// LogValue implements slog.LogValuer so PoolConfig never leaks the database DSN in logs.
func (c PoolConfig) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("postgres_dsn", observability.RedactedPlaceholder),
		slog.Int64("max_conns", int64(c.MaxConns)),
		slog.Int64("min_conns", int64(c.MinConns)),
		slog.Duration("max_conn_lifetime", c.MaxConnLifetime),
		slog.Duration("connect_timeout", c.ConnectTimeout),
	)
}

// Pool manages cold-path PostgreSQL / PgBouncer access and tracks total SQL query count
// to allow unit/integration tests to prove 0 SQL reads occur on the hot CDC ingestion path.
type Pool struct {
	cfg        PoolConfig
	querier    Querier
	queryCount atomic.Uint64
	closed     atomic.Bool
}

// NewPool initializes a cold-path Pool backed by the provided Querier.
func NewPool(cfg PoolConfig, querier Querier) (*Pool, error) {
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 10
	}
	if cfg.MaxConnLifetime <= 0 {
		cfg.MaxConnLifetime = 30 * time.Minute
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	return &Pool{
		cfg:     cfg,
		querier: querier,
	}, nil
}

// Query executes a cold-path read query and increments the diagnostic SQL query counter.
func (p *Pool) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	if p.closed.Load() {
		return nil, errors.New("postgres pool is closed")
	}
	p.queryCount.Add(1)
	if p.querier == nil {
		return &emptyRows{}, nil
	}
	return p.querier.Query(ctx, sql, args...)
}

// Exec executes a cold-path write statement and increments the diagnostic SQL query counter.
func (p *Pool) Exec(ctx context.Context, sql string, args ...any) error {
	if p.closed.Load() {
		return errors.New("postgres pool is closed")
	}
	p.queryCount.Add(1)
	if p.querier == nil {
		return nil
	}
	return p.querier.Exec(ctx, sql, args...)
}

// Ping verifies cold-path PgBouncer connectivity.
func (p *Pool) Ping(ctx context.Context) error {
	if p.closed.Load() {
		return errors.New("postgres pool is closed")
	}
	if p.querier == nil {
		return nil
	}
	return p.querier.Ping(ctx)
}

// Close releases pool connections cleanly.
func (p *Pool) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	if p.querier != nil {
		return p.querier.Close()
	}
	return nil
}

// QueryCount returns the total number of SQL operations executed against this pool.
func (p *Pool) QueryCount() uint64 {
	return p.queryCount.Load()
}

type emptyRows struct{}

func (e *emptyRows) Next() bool          { return false }
func (e *emptyRows) Scan(_ ...any) error { return nil }
func (e *emptyRows) Err() error          { return nil }
func (e *emptyRows) Close() error        { return nil }
