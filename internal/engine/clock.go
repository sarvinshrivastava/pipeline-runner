package engine

import (
	"sync"
	"time"
)

// Clock abstracts time so the engine can be driven by a fake in tests.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer is the subset of *time.Timer the engine uses.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// RealClock is the production clock.
type RealClock struct{}

// Now returns the wall clock.
func (RealClock) Now() time.Time { return time.Now() }

// NewTimer wraps time.NewTimer.
func (RealClock) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

// AfterFunc wraps time.AfterFunc.
func (RealClock) AfterFunc(d time.Duration, f func()) Timer { return realTimer{time.AfterFunc(d, f)} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Stop() bool                 { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// FakeClock is a manually advanced clock for tests. Timers fire when Advance
// or Set moves time past their deadline; AfterFunc callbacks run in their own
// goroutine, as with the real clock.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock starts at t.
func NewFakeClock(t time.Time) *FakeClock { return &FakeClock{now: t} }

// Now returns the fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimer returns a channel timer.
func (c *FakeClock) NewTimer(d time.Duration) Timer {
	return c.add(&fakeTimer{c: c, ch: make(chan time.Time, 1)}, d)
}

// AfterFunc returns a callback timer.
func (c *FakeClock) AfterFunc(d time.Duration, f func()) Timer {
	return c.add(&fakeTimer{c: c, f: f}, d)
}

func (c *FakeClock) add(t *fakeTimer, d time.Duration) *fakeTimer {
	c.mu.Lock()
	t.at, t.active = c.now.Add(d), true
	c.timers = append(c.timers, t)
	c.mu.Unlock()
	c.fire()
	return t
}

// Advance moves time forward.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
	c.fire()
}

// Set jumps to t (forwards or backwards), like an NTP step.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
	c.fire()
}

func (c *FakeClock) fire() {
	c.mu.Lock()
	now := c.now
	var due []*fakeTimer
	for _, t := range c.timers {
		if t.active && !t.at.After(now) {
			t.active = false
			due = append(due, t)
		}
	}
	c.mu.Unlock()
	for _, t := range due {
		if t.f != nil {
			go t.f()
			continue
		}
		select {
		case t.ch <- now:
		default:
		}
	}
}

type fakeTimer struct {
	c      *FakeClock
	at     time.Time
	active bool
	ch     chan time.Time
	f      func()
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := t.active
	t.active = false
	return was
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.c.mu.Lock()
	was := t.active
	t.at, t.active = t.c.now.Add(d), true
	t.c.mu.Unlock()
	t.c.fire()
	return was
}
