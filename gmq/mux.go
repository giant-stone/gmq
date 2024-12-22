package gmq

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

type Mux struct {
	mu     sync.RWMutex
	m      map[string]MuxEntry
	sorted []MuxEntry // order entries arefrom longest to shortest.
	mws    []MiddlewareFunc
}

func NewMux() *Mux {
	return &Mux{}
}

// SetClock implements IHandler.
func (fn *Mux) SetClock(c Clock) {
	panic("unimplemented")
}

// SetIdGenerator implements IHandler.
func (fn *Mux) SetIdGenerator(v IdGenerator) {
	panic("unimplemented")
}

type MuxEntry struct {
	h       IHandler
	pattern string
}

type MiddlewareFunc func(IHandler) IHandler

// Use appends a MiddlewareFunc to the chain.
// Middlewares are executed one by one in the order.
func (it *Mux) Use(mws ...MiddlewareFunc) {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.mws = append(it.mws, mws...)
}

func (it *Mux) match(s string) (h IHandler, pattern string) {
	// Check for exact match first.
	v, ok := it.m[s]
	if ok {
		return v.h, v.pattern
	}

	// Check for longest valid match.
	for _, e := range it.sorted {
		if strings.HasPrefix(s, e.pattern) {
			return e.h, e.pattern
		}
	}
	return nil, ""

}

func (it *Mux) Handler(msg IMsg) (h IHandler, pattern string) {
	it.mu.RLock()
	defer it.mu.RUnlock()

	h, pattern = it.match(msg.GetQueue())
	if h == nil {
		h, pattern = NotFoundHandler(), ""
	}
	for i := len(it.mws) - 1; i >= 0; i-- {
		h = it.mws[i](h)
	}
	return h, pattern
}

// Handle registers the handler for the given pattern.
// If there is duplicated handler then panics.
func (it *Mux) Handle(pattern string, handler IHandler) {
	it.mu.Lock()
	defer it.mu.Unlock()

	if strings.TrimSpace(pattern) == "" {
		panic("invalid pattern")
	}
	if handler == nil {
		panic("nil handler")
	}
	if _, exist := it.m[pattern]; exist {
		panic("multiple registrations for " + pattern)
	}

	if it.m == nil {
		it.m = make(map[string]MuxEntry)
	}
	e := MuxEntry{h: handler, pattern: pattern}
	it.m[pattern] = e
	it.sorted = appendSorted(it.sorted, e)
}

// ProcessMsg dispatches msg to the handler whose pattern most closely matches the queue name.
func (it *Mux) ProcessMsg(ctx context.Context, msg IMsg) error {
	h, _ := it.Handler(msg)
	return h.ProcessMsg(ctx, msg)
}

func (it *Mux) GetPatterns() map[string]MuxEntry {
	it.mu.RLock()
	defer it.mu.RUnlock()
	return it.m
}

func appendSorted(sorted []MuxEntry, e MuxEntry) []MuxEntry {
	n := len(sorted)
	i := sort.Search(n, func(i int) bool {
		return len(sorted[i].pattern) < len(e.pattern)
	})
	if i == n {
		return append(sorted, e)
	}

	// order entries arefrom longest to shortest.
	sorted = append(sorted, MuxEntry{})
	copy(sorted[i+1:], sorted[i:])
	sorted[i] = e
	return sorted
}

func NotFound(ctx context.Context, msg IMsg) error {
	return fmt.Errorf("handler matched msg queue=%s not found", msg.GetQueue())
}

func NotFoundHandler() IHandler {
	return HandlerFunc(NotFound)
}
