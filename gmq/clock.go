package gmq

import (
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
}

func NewWallClock() Clock {
	return &WallClock{}
}

type WallClock struct{}

func (it *WallClock) Now() time.Time {
	return time.Now()
}

// A SimulatedClock is a Clock implementation that doesn't "tick" on its own.
// Change time by explicit call the AdvanceTime() or SetTime() functions.
type SimulatedClock struct {
	mu sync.Mutex
	t  time.Time
}

func NewSimulatedClock(t time.Time) *SimulatedClock {
	return &SimulatedClock{t: t}
}

func (it *SimulatedClock) Now() time.Time {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.t
}

func (it *SimulatedClock) SetTime(t time.Time) {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.t = t
}

func (it *SimulatedClock) AdvanceTime(d time.Duration) {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.t = it.t.Add(d)
}
