package idgen

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// BaseRepository persists the per-device-type base value (the snowflake
// workerID). The generator hits it only the first time a device type is seen
// (cold start / new type); the result is then cached in memory.
type BaseRepository interface {
	// GetOrAllocateBase returns the base value for deviceType. If deviceType
	// is new the next base value is allocated atomically and persisted, so
	// the returned value is stable across restarts.
	GetOrAllocateBase(ctx context.Context, deviceType string) (int64, error)
}

// GeneratorManager owns one Snowflake generator per device type. Each device
// type therefore has an independent mutex, so contention is confined to a
// single type and different types scale without interference.
type GeneratorManager struct {
	repo        BaseRepository
	epoch       time.Time
	maxBackward int64

	gens     sync.Map // map[string]*Snowflake (fast path, lock-free)
	createMu sync.Mutex
}

// NewGeneratorManager builds a manager backed by repo.
func NewGeneratorManager(repo BaseRepository, epoch time.Time, maxBackwardMs int64) *GeneratorManager {
	return &GeneratorManager{
		repo:        repo,
		epoch:       epoch,
		maxBackward: maxBackwardMs,
	}
}

// NextID returns the next unique id for deviceType.
func (m *GeneratorManager) NextID(ctx context.Context, deviceType string) (int64, error) {
	gen, err := m.generator(ctx, deviceType)
	if err != nil {
		return 0, err
	}
	return gen.NextID()
}

// generator returns the cached generator for deviceType, creating and
// registering it on first use. Double-checked locking avoids duplicate base
// allocations (and thus wasted workerIDs) when several goroutines race on a
// brand-new device type.
func (m *GeneratorManager) generator(ctx context.Context, deviceType string) (*Snowflake, error) {
	if v, ok := m.gens.Load(deviceType); ok {
		return v.(*Snowflake), nil
	}

	m.createMu.Lock()
	defer m.createMu.Unlock()

	if v, ok := m.gens.Load(deviceType); ok {
		return v.(*Snowflake), nil
	}

	base, err := m.repo.GetOrAllocateBase(ctx, deviceType)
	if err != nil {
		return nil, fmt.Errorf("idgen: load base for %q: %w", deviceType, err)
	}
	if base < 0 || base > maxWorkerID {
		return nil, fmt.Errorf("idgen: base %d for %q exceeds worker capacity [0,%d]", base, deviceType, maxWorkerID)
	}

	gen, err := NewSnowflake(base, m.epoch, m.maxBackward)
	if err != nil {
		return nil, err
	}

	m.gens.Store(deviceType, gen)
	return gen, nil
}
