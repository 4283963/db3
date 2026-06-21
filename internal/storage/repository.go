package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/kl/db3/internal/idgen"
)

const (
	// baseSelectSQL reads an already-allocated base value (fast path).
	baseSelectSQL = `SELECT base_value FROM device_id_base WHERE device_type = ?`

	// counterReadSQL reads the current counter value plus its version.
	counterReadSQL = `SELECT next_worker, version FROM id_worker_counter WHERE id = 1`

	// counterCASSQL atomically advances the counter by exactly one step
	// using optimistic locking: the update only succeeds when the version
	// column still matches what we read. "version = version + 1" bumps the
	// version on a successful write, which defeats any concurrent winner.
	counterCASSQL = `UPDATE id_worker_counter
		SET next_worker = ?, version = version + 1
		WHERE id = 1 AND version = ?`

	// baseInsertSQL persists a freshly allocated base for a device type.
	// The ON DUPLICATE KEY branch is a strict no-op: whichever goroutine
	// first inserts the row owns the base value, and any concurrent loser
	// must re-read the authoritative value afterwards instead of
	// overwriting it.
	baseInsertSQL = `INSERT INTO device_id_base (device_type, base_value, version) VALUES (?, ?, 0)
		ON DUPLICATE KEY UPDATE base_value = base_value`

	// maxAllocateAttempts is how many optimistic-CAS retries we perform on
	// the counter before giving up. 3 attempts is more than enough for the
	// expected level of contention on a single-row counter.
	maxAllocateAttempts = 3
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
//
// The counter update uses optimistic CAS with bounded retries instead of a
// long-running transaction wrapping both the counter advance and the base
// insert. Splitting the two writes into separate auto-commit statements
// avoids the cross-row InnoDB gap locks that produced the deadlocks seen
// under high-concurrency load tests.
func (s *MySQLStore) allocateBase(ctx context.Context, deviceType string) (int64, error) {
	workerID, err := s.advanceCounter(ctx)
	if err != nil {
		return 0, err
	}

	if _, err := s.db.ExecContext(ctx, baseInsertSQL, deviceType, workerID); err != nil {
		return 0, fmt.Errorf("persist base for %q: %w", deviceType, err)
	}

	// Re-read: on a lost race the stored value belongs to the winner.
	var got int64
	if err := s.db.QueryRowContext(ctx, baseSelectSQL, deviceType).Scan(&got); err != nil {
		return 0, fmt.Errorf("reselect base for %q: %w", deviceType, err)
	}
	return got, nil
}

// advanceCounter bumps id_worker_counter.next_worker by one using
// compare-and-swap on the version column. On CAS failure (RowsAffected ==
// 0) we sleep a short, increasing delay and retry up to maxAllocateAttempts
// times. This is the single-line fix for the "high-concurrency 500 / deadlock"
// bug reported by the user: the single-row counter no longer sits inside a
// transaction that also touches device_id_base, and on collision we back
// off instead of letting InnoDB escalate to a deadlock.
func (s *MySQLStore) advanceCounter(ctx context.Context) (int64, error) {
	var lastErr error
	for attempt := 0; attempt < maxAllocateAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(time.Duration(attempt*20) * time.Millisecond):
			}
		}

		var current int64
		var version int
		if err := s.db.QueryRowContext(ctx, counterReadSQL).Scan(&current, &version); err != nil {
			lastErr = fmt.Errorf("read worker counter: %w", err)
			continue
		}

		res, err := s.db.ExecContext(ctx, counterCASSQL, current+1, version)
		if err != nil {
			lastErr = fmt.Errorf("advance worker counter: %w", err)
			continue
		}
		rows, err := res.RowsAffected()
		if err != nil {
			lastErr = fmt.Errorf("rows affected: %w", err)
			continue
		}
		if rows == 1 {
			return current, nil
		}
	}
	if lastErr == nil {
		lastErr = errors.New("counter version conflicts exceeded")
	}
	return 0, fmt.Errorf("advance worker counter after %d attempts: %w", maxAllocateAttempts, lastErr)
}
