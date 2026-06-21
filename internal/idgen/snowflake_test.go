package idgen

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recentEpoch is safely within the snowflake's ~17-year window so the
// tests never overflow regardless of when they run.
func recentEpoch() time.Time { return time.Now().Add(-24 * time.Hour) }

func TestNewSnowflakeRejectsInvalidWorkerID(t *testing.T) {
	cases := []int64{-1, MaxWorkerID + 1, 10000}
	for _, w := range cases {
		if _, err := NewSnowflake(w, recentEpoch(), 1000); err == nil {
			t.Fatalf("expected error for workerID %d", w)
		}
	}
	if _, err := NewSnowflake(0, recentEpoch(), 1000); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFormatID(t *testing.T) {
	cases := []struct {
		id  int64
		out string
	}{
		{0, "0000000000000000"},
		{1, "0000000000000001"},
		{12345, "0000000000012345"},
		{maxID, "9007199254740991"},
	}
	for _, c := range cases {
		if got := FormatID(c.id); got != c.out {
			t.Errorf("FormatID(%d) = %q, want %q", c.id, got, c.out)
		}
	}
	if l := len(FormatID(0)); l != IDDigitCount() {
		t.Errorf("FormatID width = %d, want %d", l, IDDigitCount())
	}
}

func TestNextIDFormatAndRange(t *testing.T) {
	s, err := NewSnowflake(5, recentEpoch(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		id, err := s.NextID()
		if err != nil {
			t.Fatalf("NextID: %v", err)
		}
		if id < 0 || id > maxID {
			t.Fatalf("id %d out of range [0,%d]", id, maxID)
		}
		formatted := FormatID(id)
		if len(formatted) != 16 {
			t.Fatalf("formatted id %q has len %d, want 16", formatted, len(formatted))
		}
		for _, ch := range formatted {
			if ch < '0' || ch > '9' {
				t.Fatalf("formatted id %q contains non-digit", formatted)
			}
		}
	}
}

func TestNextIDMonotonic(t *testing.T) {
	s, _ := NewSnowflake(3, recentEpoch(), 1000)
	var prev int64 = -1
	for i := 0; i < 10000; i++ {
		id, err := s.NextID()
		if err != nil {
			t.Fatalf("NextID: %v", err)
		}
		if id <= prev {
			t.Fatalf("id not monotonic at i=%d: prev=%d cur=%d", i, prev, id)
		}
		prev = id
	}
}

// TestUniquenessHighConcurrency hammers a single generator from many
// goroutines and asserts that every id is distinct.
func TestUniquenessHighConcurrency(t *testing.T) {
	const workers = 64
	const perWorker = 2000
	const total = workers * perWorker

	s, _ := NewSnowflake(0, recentEpoch(), 1000)

	seen := make(map[int64]struct{}, total)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(workers)

	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			<-start
			local := make([]int64, 0, perWorker)
			for i := 0; i < perWorker; i++ {
				id, err := s.NextID()
				if err != nil {
					t.Errorf("NextID: %v", err)
					return
				}
				local = append(local, id)
			}
			mu.Lock()
			for _, id := range local {
				seen[id] = struct{}{}
			}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	if got := len(seen); got != total {
		t.Fatalf("expected %d unique ids, got %d (duplicates detected)", total, got)
	}
}

// TestMultipleWorkersDisjoint verifies that generators with different base
// values (workerIDs) never collide.
func TestMultipleWorkersDisjoint(t *testing.T) {
	const workers = 8
	const perWorker = 5000

	gens := make([]*Snowflake, workers)
	for i := range gens {
		s, err := NewSnowflake(int64(i), recentEpoch(), 1000)
		if err != nil {
			t.Fatal(err)
		}
		gens[i] = s
	}

	var dupCount int64
	seen := make(map[int64]struct{}, workers*perWorker)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(workers)

	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < perWorker; j++ {
				id, err := gens[i].NextID()
				if err != nil {
					t.Errorf("NextID: %v", err)
					return
				}
				mu.Lock()
				if _, ok := seen[id]; ok {
					atomic.AddInt64(&dupCount, 1)
				}
				seen[id] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	want := workers * perWorker
	if got := len(seen); got != want {
		t.Fatalf("expected %d unique ids across workers, got %d (duplicates=%d)",
			want, got, dupCount)
	}
}

// TestSequenceExhaustionSpinsToNextSecond exercises the path where the
// sequence overflows within a second: many ids requested back-to-back must
// still all be unique because the generator spins into the next second.
func TestSequenceExhaustionSpinsToNextSecond(t *testing.T) {
	s, _ := NewSnowflake(1, recentEpoch(), 1000)
	// Far more than maxSequence (4095) in one burst so we are guaranteed
	// to wrap at least once and force a wait into the following second.
	count := int(maxSequence)*5 + 100
	seen := make(map[int64]struct{}, count)
	for i := 0; i < count; i++ {
		id, err := s.NextID()
		if err != nil {
			t.Fatalf("NextID at %d: %v", i, err)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id %d at iteration %d", id, i)
		}
		seen[id] = struct{}{}
	}
}

func TestWorkerIDAccessor(t *testing.T) {
	s, _ := NewSnowflake(42, recentEpoch(), 1000)
	if got := s.WorkerID(); got != 42 {
		t.Fatalf("WorkerID = %d, want 42", got)
	}
}

// TestGeneratorManagerCachingAndDisjoint drives the manager with an in-memory
// fake repository: the same device type must be allocated once, distinct
// device types must get distinct bases, and all produced ids must be unique.
func TestGeneratorManagerCachingAndDisjoint(t *testing.T) {
	fake := newFakeRepo()
	mgr := NewGeneratorManager(fake, recentEpoch(), 1000)

	deviceTypes := []string{"sensor-A", "sensor-B", "gateway-1", "sensor-A"}
	for _, dt := range deviceTypes {
		if _, err := mgr.NextID(context.Background(), dt); err != nil {
			t.Fatalf("NextID(%q): %v", dt, err)
		}
	}

	// "sensor-A" appeared twice but should have been allocated exactly once.
	if got := fake.allocCount("sensor-A"); got != 1 {
		t.Fatalf("sensor-A allocated %d times, want 1", got)
	}
	// Distinct device types -> distinct base values.
	if fake.base("sensor-A") == fake.base("sensor-B") {
		t.Fatal("sensor-A and sensor-B share a base value")
	}

	// Generate a burst and confirm global uniqueness.
	const n = 5000
	seen := make(map[int64]struct{}, n)
	for i := 0; i < n; i++ {
		dt := deviceTypes[i%len(deviceTypes)]
		id, err := mgr.NextID(context.Background(), dt)
		if err != nil {
			t.Fatalf("NextID: %v", err)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = struct{}{}
	}
}

// fakeRepo is an in-memory BaseRepository for testing the manager without a
// database. It hands out ascending base values and remembers per-device-type
// allocations so the manager's caching can be asserted.
type fakeRepo struct {
	mu     sync.Mutex
	next   int64
	bases  map[string]int64
	counts map[string]int
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		bases:  make(map[string]int64),
		counts: make(map[string]int),
	}
}

func (f *fakeRepo) GetOrAllocateBase(_ context.Context, deviceType string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts[deviceType]++
	if b, ok := f.bases[deviceType]; ok {
		return b, nil
	}
	b := f.next
	f.next++
	f.bases[deviceType] = b
	return b, nil
}

func (f *fakeRepo) base(deviceType string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bases[deviceType]
}

func (f *fakeRepo) allocCount(deviceType string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[deviceType]
}

func ExampleFormatID() {
	fmt.Println(FormatID(42))
	// Output: 0000000000000042
}
