package api

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// limiter throttles authentication failures per client IP. The LRU bounds
// memory however many addresses an attacker uses.
type limiter struct {
	mu     sync.Mutex
	max    int
	window time.Duration
	seen   *lru.Cache[string, *bucket]
	now    func() time.Time
}

type bucket struct {
	count int
	since time.Time
}

func newLimiter(maxFailures int, window time.Duration, size int, now func() time.Time) *limiter {
	c, _ := lru.New[string, *bucket](size)
	return &limiter{max: maxFailures, window: window, seen: c, now: now}
}

func (l *limiter) blocked(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.seen.Get(ip)
	if !ok {
		return false
	}
	if l.now().Sub(b.since) > l.window {
		l.seen.Remove(ip)
		return false
	}
	return b.count >= l.max
}

func (l *limiter) fail(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b, ok := l.seen.Get(ip)
	if !ok || now.Sub(b.since) > l.window {
		l.seen.Add(ip, &bucket{count: 1, since: now})
		return
	}
	b.count++
}
