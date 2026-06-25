package engine

import "testing"

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
