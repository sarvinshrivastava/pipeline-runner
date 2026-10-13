package engine

import (
	"container/heap"
	"time"
)

type entryKind int

const (
	entryDue          entryKind = iota // the run's fire instant
	entryLateDeadline                  // a parked run's max_late deadline
)

type entry struct {
	runID string
	at    time.Time
	kind  entryKind
}

// fireHeap is a min-heap of entries by instant. Duplicates and ghosts are
// allowed: dispatch drops anything that is no longer pending.
type fireHeap []entry

func (h fireHeap) Len() int { return len(h) }
func (h fireHeap) Less(i, j int) bool {
	if h[i].at.Equal(h[j].at) {
		return h[i].runID < h[j].runID
	}
	return h[i].at.Before(h[j].at)
}
func (h fireHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *fireHeap) Push(x any)   { *h = append(*h, x.(entry)) }
func (h *fireHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (h *fireHeap) push(e entry) { heap.Push(h, e) }

func (h *fireHeap) peek() (entry, bool) {
	if len(*h) == 0 {
		return entry{}, false
	}
	return (*h)[0], true
}

func (h *fireHeap) pop() entry { return heap.Pop(h).(entry) }
