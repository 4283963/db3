package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/kl/db3/internal/idgen"
)

const (
	// baseSelectSQL reads an already-allocated base value (fast path).
	baseSelectSQL = `SELECT base_value FROM device_id_base WHERE device_type = ?`

	// counterNextSQL atomically advances the single-row worker counter and
	// stores the new value in the connection-local LAST_INSERT_ID state.
	counterNextSQL = `UPDATE id_worker_counter SET next_worker = LAST_INSERT_ID(next_worker + 1) WHERE id = 1`

	// counterLastIDSQL reads the value set by counterNextSQL. Must run on
	// the same connection (i.e. inside the same transaction).
	counterLastIDSQL = `SELECT LAST_INSERT_ID()`

	// baseUpsertSQL inserts the newly allocated base; on a concurrent insert
	// for the same device type it becomes a no-op so the existing value is
	// never overwritten (the loser re-reads the authoritative value below).
	baseUpsertSQL = `INSERT INTO device_id_base (device_type, base_value) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE base_value = base_value`
)

// Compile-time assertion that MySQLStore satisfies idgen.BaseRepository.
var _ idgen.BaseRepository = (*MySQLStore)(nil)

// GetOrAllocateBase implements idgen.BaseRepository. It first reads the
// cached base value; when the device type is new it allocates the next
// worker id atomically and persists it, then re-reads the authoritative
// value (which may differ from the one this caller allocated if another
// process raced and won — the wasted id simply creates a small gap, which
// is harmless for snowflake).
func (s *MySQLStore) GetOrAllocateBase(ctx context.Context, deviceType string) (int64, error) {
	var base int64
	err := s.db.QueryRowContext(ctx, baseSelectSQL, deviceType).Scan(&base)
	switch {
	case err == nil:
		return base, nil
	case !errors.Is(err, sql.ErrNoRows):
		return 0, fmt.Errorf("select base for %q: %w", deviceType, err)
	}

	return s.allocateBase(ctx, deviceType)
}

// allocateBase claims the next worker id and persists it for deviceType.
func (s *MySQLStore) allocateBase(ctx context.Context, deviceType string) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, counterNextSQL); err != nil {
		return 0, fmt.Errorf("advance worker counter: %w", err)
	}

	var next int64
	if err := tx.QueryRowContext(ctx, counterLastIDSQL).Scan(&next); err != nil {
		return 0, fmt.Errorf("read last_insert_id: %w", err)
	}
	workerID := next - 1

	if _, err := tx.ExecContext(ctx, baseUpsertSQL, deviceType, workerID); err != nil {
		return 0, fmt.Errorf("persist base for %q: %w", deviceType, err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit base for %q: %w", deviceType, err)
	}

	// Re-read: on a lost race the stored value belongs to the winner.
	var got int64
	if err := s.db.QueryRowContext(ctx, baseSelectSQL, deviceType).Scan(&got); err != nil {
		return 0, fmt.Errorf("reselect base for %q: %w", deviceType, err)
	}
	return got, nil
}
