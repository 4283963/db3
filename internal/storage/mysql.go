// Package storage implements the MySQL persistence layer for the id
// generator. It owns the connection pool, ensures the schema exists and
// exposes a BaseRepository that allocates a per-device-type base value
// (the snowflake workerID) atomically.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/kl/db3/internal/config"
)

// schemaStatements are executed idempotently on startup.
//
//   - device_id_base: the table required by the project — one row per device
//     type recording its allocated base value (the snowflake workerID). This
//     value is stable across restarts, so a device type always reuses the
//     same worker slot.
//   - id_worker_counter: a single-row sequence used to allocate the next
//     base value atomically via the LAST_INSERT_ID trick.
var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS device_id_base (
		device_type VARCHAR(64) NOT NULL,
		base_value  BIGINT      NOT NULL,
		created_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (device_type)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	`CREATE TABLE IF NOT EXISTS id_worker_counter (
		id          TINYINT UNSIGNED NOT NULL,
		next_worker BIGINT           NOT NULL DEFAULT 0,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	`INSERT IGNORE INTO id_worker_counter (id, next_worker) VALUES (1, 0)`,
}

// MySQLStore is the concrete BaseRepository backed by MySQL.
type MySQLStore struct {
	db *sql.DB
}

// NewMySQLStore opens the pool, creates the database (when configured via
// fields) and ensures the schema exists. When cfg.DSN is set directly the
// database is assumed to be managed externally and is only connected to.
func NewMySQLStore(ctx context.Context, cfg config.MySQLConfig) (*MySQLStore, error) {
	if cfg.DSN == "" {
		if err := ensureDatabase(ctx, cfg); err != nil {
			return nil, err
		}
	}

	db, err := sql.Open("mysql", cfg.BuildDSN())
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetimeSec) * time.Second)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}

	s := &MySQLStore{db: db}
	if err := s.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// ensureDatabase connects without a database name and creates the target
// database if it does not yet exist.
func ensureDatabase(ctx context.Context, cfg config.MySQLConfig) error {
	adminDSN := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/?charset=%s&parseTime=true&loc=Local",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Charset,
	)
	admin, err := sql.Open("mysql", adminDSN)
	if err != nil {
		return fmt.Errorf("open admin mysql: %w", err)
	}
	defer admin.Close()

	if err := admin.PingContext(ctx); err != nil {
		return fmt.Errorf("ping mysql (admin): %w", err)
	}

	stmt := fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
		cfg.DBName,
	)
	if _, err := admin.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("create database %q: %w", cfg.DBName, err)
	}
	return nil
}

func (s *MySQLStore) ensureSchema(ctx context.Context) error {
	for _, stmt := range schemaStatements {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
	}
	return nil
}

// Close releases the connection pool.
func (s *MySQLStore) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// DB exposes the underlying pool (mainly for health checks / tests).
func (s *MySQLStore) DB() *sql.DB { return s.db }
