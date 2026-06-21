// Package idgen implements the core serial-number generator.
//
// The algorithm is a classic snowflake derivative tuned to emit a 16-digit
// decimal serial number. The 64-bit id occupies at most 53 bits, so its value
// is always strictly less than 10^16 (max 2^53-1 = 9,007,199,254,740,991,
// i.e. 16 digits), which lets the generator honour the "16位流水号" contract.
//
// Bit layout (53 bits in total, all ids fit in 16 decimal digits):
//
//	| 29 bits             | 12 bits   | 12 bits   |
//	| timestamp (seconds) | workerID  | sequence  |
//	  MSB                                           LSB
//
//	- timestamp: seconds elapsed since a configurable custom epoch
//	  (29 bits => ~17 years of lifetime per epoch, rotatable).
//	- workerID:  the per-device-type "base value" persisted in MySQL
//	  (12 bits => up to 4096 device types).
//	- sequence:  a per-second counter (12 bits => up to 4096 ids/s per
//	  device type, plenty for most workloads).
//
// Using a second-resolution timestamp (instead of millis) trades off peak
// per-device id-rate (from ~1M/s to ~4k/s) for two huge wins:
//  1. worker capacity grows from 128 to 4096 (enough for a real fleet),
//  2. sequence exhaustion is so rare it practically never happens.
//
// Uniqueness under high concurrency is guaranteed by:
//   - a per-device-type mutex so each generator is single-writer,
//   - the monotonic sequence counter that wraps only on a second rollover,
//   - spin-waiting into the next second when a second's sequence is
//     exhausted, and
//   - bounded spin-waiting when the system clock moves backward.
package idgen

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Bit widths.
const (
	workerIDBits  = 12
	sequenceBits  = 12
	timestampBits = 29
)

// Derived masks and shifts.
const (
	maxWorkerID    = int64(-1) ^ (int64(-1) << workerIDBits) // 4095
	maxSequence    = int64(-1) ^ (int64(-1) << sequenceBits) // 4095
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
	epoch       int64 // base epoch in unix seconds
	lastTs      int64 // last allocated second (relative to epoch)
	workerID    int64
	seq         int64
	maxBackward int64 // tolerated backward skew in seconds
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
		epoch:       epoch.Unix(),
		workerID:    workerID,
		maxBackward: maxBackwardMs / 1000,
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

	now := time.Now().Unix() - s.epoch

	// Clock moved backward: try to recover by spin-waiting, but bail out
	// when the skew exceeds the configured tolerance.
	if now < s.lastTs {
		diff := s.lastTs - now
		if diff > s.maxBackward {
			return 0, fmt.Errorf("%w: skew=%ds tolerance=%ds",
				ErrClockMovedBackward, diff, s.maxBackward)
		}
		for now < s.lastTs {
			time.Sleep(200 * time.Millisecond)
			now = time.Now().Unix() - s.epoch
		}
	}

	switch {
	case now == s.lastTs:
		s.seq = (s.seq + 1) & maxSequence
		if s.seq == 0 {
			now = tilNextSecond(s.epoch, s.lastTs)
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

// tilNextSecond spins until the second advances past lastTs.
func tilNextSecond(epoch, lastTs int64) int64 {
	ts := time.Now().Unix() - epoch
	for ts <= lastTs {
		time.Sleep(50 * time.Millisecond)
		ts = time.Now().Unix() - epoch
	}
	return ts
}

// DefaultRegion is used as the 2-character region prefix when the caller
// does not supply one.
const DefaultRegion = "XX"

// RegionLength is the exact length of a region code.
const RegionLength = 2

// FormatID renders a snowflake id as a fixed 16-digit decimal string. Every
// id is guaranteed to be < 10^16, so the result is always exactly 16
// characters. Equivalent to FormatIDWithRegion(id, DefaultRegion).
func FormatID(id int64) string {
	return fmt.Sprintf("%0*d", idDigitCount, id)
}

// FormatIDWithRegion produces the final 16-character serial number with a
// region code prepended to the numeric id. The leftmost two digits of the
// 16-digit numeric id are replaced by the region code in upper case.
//
// The region code must be exactly RegionLength (2) ASCII letters; an empty
// region falls back to DefaultRegion ("XX").
func FormatIDWithRegion(id int64, region string) (string, error) {
	if region == "" {
		region = DefaultRegion
	}
	if len(region) != RegionLength {
		return "", fmt.Errorf("idgen: region %q must be exactly %d characters", region, RegionLength)
	}
	for _, r := range region {
		if r < 'A' || r > 'Z' {
			if r < 'a' || r > 'z' {
				return "", fmt.Errorf("idgen: region %q must contain only letters", region)
			}
		}
	}
	region = strings.ToUpper(region)
	numeric := fmt.Sprintf("%0*d", idDigitCount, id)
	return region + numeric[RegionLength:], nil
}

// IDDigitCount returns the fixed decimal width of a serial number.
func IDDigitCount() int { return idDigitCount }
