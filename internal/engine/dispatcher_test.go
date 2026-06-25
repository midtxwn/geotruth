package engine

import (
	"errors"
	"strings"
	"testing"

	"github.com/midtxwn/geotruth/internal/gtevents"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/messages"
)

type testPublisher struct {
	submitted []*gtevents.CommitEnvelope
	results   chan gtevents.CommitResult
}

func newTestPublisher() *testPublisher {
	return &testPublisher{results: make(chan gtevents.CommitResult)}
}

func (p *testPublisher) Submit(commit *gtevents.CommitEnvelope) {
	p.submitted = append(p.submitted, commit)
}

func (p *testPublisher) Results() <-chan gtevents.CommitResult {
	return p.results
}

func TestNewDispatcherRequiresInstanceIDFactory(t *testing.T) {
	_, err := NewDispatcher(nil, nil, nil, nil, nil, func(string, []byte) {})
	if err == nil {
		t.Fatal("NewDispatcher succeeded with nil nextInstanceID")
	}
}

func TestNewDispatcherRequiresReplyFunc(t *testing.T) {
	_, err := NewDispatcher(nil, nil, nil, nil, func() string { return "test-instance" }, nil)
	if err == nil {
		t.Fatal("NewDispatcher succeeded with nil reply")
	}
}

func TestNewDispatcherAcceptsExplicitFactories(t *testing.T) {
	d, err := NewDispatcher(nil, nil, nil, nil, func() string { return "test-instance" }, func(string, []byte) {})
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}
	if got := d.nextInstanceID(); got != "test-instance" {
		t.Fatalf("nextInstanceID() = %q", got)
	}
}

func TestDuplicateRegisterRejectsWhileFirstRegisterInFlight(t *testing.T) {
	eng := NewEngine(fixedResolver{"0"})
	pub := newTestPublisher()
	replies := make(map[string][]byte)
	d, err := NewDispatcher(eng, nil, pub, fixedResolver{"0"}, func() string { return "b1i1" }, func(reply string, data []byte) {
		replies[reply] = data
	})
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}

	dims := domain.ObjectDimensions{Width: 1, Height: 1}
	d.onRegister(NewRegisterEnvelope("obj1", "", "first", dims))
	if len(pub.submitted) != 1 {
		t.Fatalf("submitted commits = %d, want 1", len(pub.submitted))
	}
	if _, ok := replies["first"]; ok {
		t.Fatal("first register replied before commit result")
	}

	d.onRegister(NewRegisterEnvelope("obj1", "", "duplicate", dims))
	if len(pub.submitted) != 1 {
		t.Fatalf("duplicate register submitted commit; submits = %d, want 1", len(pub.submitted))
	}

	ctl, ok := eng.lookupCtl("obj1")
	if !ok {
		t.Fatal("object control missing")
	}
	if len(ctl.queue) != 0 {
		t.Fatalf("duplicate register was queued: len=%d", len(ctl.queue))
	}

	err = messages.Err(replies["duplicate"])
	if err == nil {
		t.Fatalf("duplicate register reply was not an error: %s", string(replies["duplicate"]))
	}
	var remote messages.RemoteError
	if !errors.As(err, &remote) {
		t.Fatalf("duplicate register error type = %T, want messages.RemoteError", err)
	}
	if !strings.Contains(remote.Message, "already registered") {
		t.Fatalf("duplicate register error = %q, want already registered", remote.Message)
	}
}
