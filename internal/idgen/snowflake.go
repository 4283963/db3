// Package idgen implements the core serial-number generator.
//
// The algorithm is a classic snowflake derivative tuned to emit a 16-digit
// decimal serial number. The 64-bit id occupies at most 53 bits, so its value
// is always strictly less than 10^16 (max 2^53-1 = 9,007,199,254,740,991,
// i.e. 16 digits), which lets the generator honour the "16位流水号" contract.
//
// Bit layout (53 bits in total):
//
//	| 36 bits            | 7 bits   | 10 bits  |
//	| timestamp (ms)     | workerID | sequence |
//	  MSB                                          LSB
//
//	- timestamp: milliseconds elapsed since a configurable custom epoch
//	  (36 bits => ~2.18 years of lifetime per epoch, rotatable).
//	- workerID:  the per-device-type "base value" persisted in MySQL
//	  (7 bits => up to 128 device types).
//	- sequence:  a per-millisecond counter (10 bits => up to 1024 ids/ms,
//	  ~1,000,000 ids/s per device type).
//
// Uniqueness under high concurrency is guaranteed by:
//   - a per-device-type mutex so each generator is single-writer,
//   - the monotonic sequence counter that wraps only on a millisecond rollover,
//   - spin-waiting into the next millisecond when a millisecond's sequence is
//     exhausted, and
//   - bounded spin-waiting when the system clock moves backward.
package idgen

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Bit widths.
const (
	workerIDBits  = 7
	sequenceBits  = 10
	timestampBits = 36
)

// Derived masks and shifts.
const (
	maxWorkerID    = int64(-1) ^ (int64(-1) << workerIDBits) // 127
	maxSequence    = int64(-1) ^ (int64(-1) << sequenceBits) // 1023
	workerIDShift  = sequenceBits
	timestampShift = sequenceBits + workerIDBits

	// maxID is the largest id that still fits in 16 decimal digits
	// (2^53 - 1 = 9,007,199,254,740,991).
	maxID int64 = 1<<53 - 1

	// idDigitCount is the fixed width of the rendered serial number.
	idDigitCount = 16
)

// MaxWorkerID is the largest base value (workerID) a device type may hold.
const MaxWorkerID = maxWorkerID

// MaxDeviceTypes is the maximum number of distinct device types the
// algorithm can serve before running out of workerIDs.
const MaxDeviceTypes = maxWorkerID + 1

// ErrClockMovedBackward is returned when the system clock moves backward by
// more than the configured tolerance.
var ErrClockMovedBackward = errors.New("clock moved backward beyond tolerance")

// Snowflake is the single-device-type id generator. Each device type owns its
// own instance, so contention is confined to a single type.
type Snowflake struct {
	mu          sync.Mutex
	epoch       int64 // base epoch in unix milliseconds
	lastTs      int64 // last allocated millisecond (relative to epoch)
	workerID    int64
	seq         int64
	maxBackward int64 // tolerated backward skew in ms
}

// NewSnowflake creates a generator for the given workerID (the device-type
// base value). epoch is the custom epoch and maxBackwardMs bounds the
// tolerated clock skew before an error is returned.
func NewSnowflake(workerID int64, epoch time.Time, maxBackwardMs int64) (*Snowflake, error) {
	if workerID < 0 || workerID > maxWorkerID {
		return nil, fmt.Errorf("idgen: workerID %d out of range [0,%d]", workerID, maxWorkerID)
	}
	if maxBackwardMs < 0 {
		return nil, errors.New("idgen: maxBackwardMs must be non-negative")
	}
	return &Snowflake{
		epoch:       epoch.UnixMilli(),
		workerID:    workerID,
		maxBackward: maxBackwardMs,
	}, nil
}

// WorkerID returns the base value this generator was created with.
func (s *Snowflake) WorkerID() int64 {
	return s.workerID
}

// NextID produces the next unique id. It is safe for concurrent use.
func (s *Snowflake) NextID() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli() - s.epoch

	// Clock moved backward: try to recover by spin-waiting, but bail out
	// when the skew exceeds the configured tolerance.
	if now < s.lastTs {
		diff := s.lastTs - now
		if diff > s.maxBackward {
			return 0, fmt.Errorf("%w: skew=%dms tolerance=%dms",
				ErrClockMovedBackward, diff, s.maxBackward)
		}
		for now < s.lastTs {
			time.Sleep(time.Millisecond)
			now = time.Now().UnixMilli() - s.epoch
		}
	}

	switch {
	case now == s.lastTs:
		// Same millisecond as the previous id: advance the sequence.
		s.seq = (s.seq + 1) & maxSequence
		if s.seq == 0 {
			// Sequence exhausted in this ms: block until the next ms.
			now = tilNextMillis(s.epoch, s.lastTs)
		}
	default: // now > s.lastTs
		s.seq = 0
	}

	s.lastTs = now

	id := (now << timestampShift) | (s.workerID << workerIDShift) | s.seq
	if id < 0 || id > maxID {
		return 0, fmt.Errorf("idgen: id overflow (epoch exhausted or clock too far ahead): %d", id)
	}
	return id, nil
}

// tilNextMillis spins until the millisecond advances past lastTs.
func tilNextMillis(epoch, lastTs int64) int64 {
	ts := time.Now().UnixMilli() - epoch
	for ts <= lastTs {
		ts = time.Now().UnixMilli() - epoch
	}
	return ts
}

// FormatID renders a snowflake id as a fixed 16-digit decimal string. Every
// id is guaranteed to be < 10^16, so the result is always exactly 16 characters.
func FormatID(id int64) string {
	return fmt.Sprintf("%0*d", idDigitCount, id)
}

// IDDigitCount returns the fixed decimal width of a serial number.
func IDDigitCount() int { return idDigitCount }
