// Package testutil provides bounded contexts for the shared broker tests.
package testutil

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Context limits a test, including cleanup, to 30 seconds. A watchdog also
// aborts calls that ignore context cancellation, preserving goroutine stacks.
// The go test -timeout flag remains the outer limit for the entire package.
func Context(t testing.TB) context.Context {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	if dt, ok := t.(interface{ Deadline() (time.Time, bool) }); ok {
		if d, ok := dt.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	name := t.Name()
	timer := time.AfterFunc(time.Until(deadline), func() {
		panic(fmt.Sprintf("test %s exceeded its deadline", name))
	})
	t.Cleanup(func() { timer.Stop(); cancel() })
	return ctx
}
