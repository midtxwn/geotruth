package integration

import (
	"testing"
	"time"
)

func assertEventually(tb testing.TB, fn func() bool, timeout time.Duration) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	tb.Fatalf("condition never met within %s", timeout)
}
