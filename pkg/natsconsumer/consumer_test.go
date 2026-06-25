package natsconsumer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSubjectMatches(t *testing.T) {
	tests := []struct {
		pattern  string
		subject  string
		expected bool
	}{
		{">", "foo.bar.baz", true},
		{">", "foo", true},
		{"*", "foo", true},
		{"*", "foo.bar", false},
		{"*.*", "foo.bar", true},
		{"*.*", "foo.bar.baz", false},
		{"*.>", "foo.bar", true},
		{"*.>", "foo.bar.baz", true},
		{"*.>", "foo", false},
		{"gt.events.v1.>", "gt.events.v1.object.sensor_1.position.updated", true},
		{"gt.events.v1.>", "gt.events.v1.detector.booted", true},
		{"gt.events.v1.>", "gt.events.v2.object.sensor_1.position.updated", false},
		{"gt.events.v1.>", "gt.events.v1", false},
		{"gt.events.v1.object.*.>", "gt.events.v1.object.sensor_1.position.updated", true},
		{"gt.events.v1.object.*.>", "gt.events.v1.object.sensor_1.geofence.zone_a.entered", true},
		{"gt.events.v1.object.*.>", "gt.events.v1.object.sensor_1", false},
		{"gt.events.v1.detector.booted", "gt.events.v1.detector.booted", true},
		{"gt.events.v1.detector.booted", "gt.events.v1.detector.other", false},
		{"gt.events.v1.object.*.geofence.*.>", "gt.events.v1.object.sensor_1.geofence.zone_a.entered", true},
		{"gt.events.v1.object.*.geofence.*.>", "gt.events.v1.object.sensor_1.position.updated", false},
		{"exact.match", "exact.match", true},
		{"exact.match", "exact.other", false},
	}

	for _, tt := range tests {
		got := SubjectMatches(tt.pattern, tt.subject)
		if got != tt.expected {
			t.Errorf("SubjectMatches(%q, %q) = %v, want %v", tt.pattern, tt.subject, got, tt.expected)
		}
	}
}

func TestPatternContains(t *testing.T) {
	tests := []struct {
		container string
		contained string
		expected  bool
	}{
		{">", "foo.bar", true},
		{">", "foo.*", true},
		{">", "foo.>", true},
		{"foo.>", "foo.bar", true},
		{"foo.>", "foo.*", true},
		{"foo.>", "foo.bar.baz", true},
		{"foo.*", "foo.bar", true},
		{"foo.*", "foo.>", false},
		{"foo.*", "foo.bar.baz", false},
		{"foo.bar", "foo.*", false},
		{"foo.bar", "foo.bar", true},
		{"foo.bar", "foo.quux", false},
		{"foo.*.bar", "foo.*.baz", false},
		{"gt.events.v1.>", "gt.events.v1.object.*.position.updated", true},
		{"gt.events.v1.>", "gt.events.v1.detector.booted", true},
		{"gt.events.v1.object.>", "gt.events.v1.object.*.position.updated", true},
		{"gt.events.v1.object.*.position.updated", "gt.events.v1.object.sensor_1.position.updated", true},
		{"gt.events.v1.object.*.position.updated", "gt.events.v1.object.>", false},
		{"gt.events.v1.object.*.position.updated", "gt.events.v1.object.*.geofence.*.entered", false},
		{"foo.*", "foo.*", true},
		{"foo.>", "foo.>", true},
		{"foo.>", "foo", false},
	}
	for _, tt := range tests {
		got := patternContains(tt.container, tt.contained)
		if got != tt.expected {
			t.Errorf("patternContains(%q, %q) = %v, want %v", tt.container, tt.contained, got, tt.expected)
		}
	}
}

func TestTrieInsertAndMatch(t *testing.T) {
	tri := newSubTrie()

	e1 := subEntry{id: 1}
	e2 := subEntry{id: 2}
	e3 := subEntry{id: 3}

	tri.insert("gt.events.v1.>", e1)
	tri.insert("gt.events.v1.object.*.position.updated", e2)
	tri.insert("gt.events.v1.detector.booted", e3)

	matches := tri.match("gt.events.v1.object.sensor_1.position.updated")
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(matches))
	}
	ids := make(map[uint64]bool)
	for _, m := range matches {
		ids[m.id] = true
	}
	if !ids[1] || !ids[2] {
		t.Errorf("expected ids 1 and 2, got %v", ids)
	}
	if ids[3] {
		t.Error("did not expect id 3")
	}

	matches = tri.match("gt.events.v1.detector.booted")
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(matches))
	}
	ids = make(map[uint64]bool)
	for _, m := range matches {
		ids[m.id] = true
	}
	if !ids[1] || !ids[3] {
		t.Errorf("expected ids 1 and 3, got %v", ids)
	}

	matches = tri.match("gt.events.v2.other.event")
	if len(matches) != 0 {
		t.Errorf("expected 0 matches, got %d", len(matches))
	}
}

func TestTrieRemove(t *testing.T) {
	tri := newSubTrie()

	e1 := subEntry{id: 1}
	e2 := subEntry{id: 2}

	tri.insert("gt.events.v1.>", e1)
	tri.insert("gt.events.v1.>", e2)

	matches := tri.match("gt.events.v1.object.sensor_1.position.updated")
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(matches))
	}

	removed := tri.remove("gt.events.v1.>", 1)
	if removed.id != 1 {
		t.Errorf("expected removed id 1, got %d", removed.id)
	}

	matches = tri.match("gt.events.v1.object.sensor_1.position.updated")
	if len(matches) != 1 {
		t.Fatalf("expected 1 match after remove, got %d", len(matches))
	}
	if matches[0].id != 2 {
		t.Errorf("expected remaining id 2, got %d", matches[0].id)
	}

	removed = tri.remove("gt.events.v1.>", 2)
	if removed.id != 2 {
		t.Errorf("expected removed id 2, got %d", removed.id)
	}

	matches = tri.match("gt.events.v1.object.sensor_1.position.updated")
	if len(matches) != 0 {
		t.Errorf("expected 0 matches after removing all, got %d", len(matches))
	}
}

func TestTrieWildcardStarMatch(t *testing.T) {
	tri := newSubTrie()

	e1 := subEntry{id: 1}
	tri.insert("gt.events.v1.*.position.updated", e1)

	tests := []struct {
		subject  string
		expected int
	}{
		{"gt.events.v1.object.position.updated", 1},
		{"gt.events.v1.sensor_1.position.updated", 1},
		{"gt.events.v1.sensor_1.position.updated.extra", 0},
		{"gt.events.v1.position.updated", 0},
	}
	for _, tt := range tests {
		matches := tri.match(tt.subject)
		if len(matches) != tt.expected {
			t.Errorf("match(%q) = %d matches, want %d", tt.subject, len(matches), tt.expected)
		}
	}
}

func TestTrieWildcardGtMidMatch(t *testing.T) {
	tri := newSubTrie()

	e1 := subEntry{id: 1}
	tri.insert("gt.events.v1.object.>", e1)

	tests := []struct {
		subject  string
		expected int
	}{
		{"gt.events.v1.object.sensor_1.position.updated", 1},
		{"gt.events.v1.object.sensor_1", 1},
		{"gt.events.v1.object", 0},
		{"gt.events.v1.detector.booted", 0},
	}
	for _, tt := range tests {
		matches := tri.match(tt.subject)
		if len(matches) != tt.expected {
			t.Errorf("match(%q) = %d matches, want %d", tt.subject, len(matches), tt.expected)
		}
	}
}

func TestTrieMultiplePatternsSameSubject(t *testing.T) {
	tri := newSubTrie()

	e1 := subEntry{id: 1}
	e2 := subEntry{id: 2}
	e3 := subEntry{id: 3}

	tri.insert("gt.events.v1.>", e1)
	tri.insert("gt.events.v1.object.sensor_1.position.updated", e2)
	tri.insert("gt.events.v1.object.*.position.updated", e3)

	matches := tri.match("gt.events.v1.object.sensor_1.position.updated")
	if len(matches) != 3 {
		t.Fatalf("expected 3 matches, got %d", len(matches))
	}
}

func TestTrieRemoveNonWildcard(t *testing.T) {
	tri := newSubTrie()

	e1 := subEntry{id: 1}
	tri.insert("gt.events.v1.detector.booted", e1)

	matches := tri.match("gt.events.v1.detector.booted")
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}

	removed := tri.remove("gt.events.v1.detector.booted", 1)
	if removed.id != 1 {
		t.Errorf("expected removed id 1, got %d", removed.id)
	}

	matches = tri.match("gt.events.v1.detector.booted")
	if len(matches) != 0 {
		t.Errorf("expected 0 matches after remove, got %d", len(matches))
	}
}

func TestValidateSubject(t *testing.T) {
	tests := []struct {
		subject string
		wantErr bool
	}{
		{">", false},
		{"gt.events.v1.>", false},
		{"gt.events.v1.object.sensor_1.position.updated", false},
		{"gt.events.v1.*.position.updated", false},
		{"gt.events.v1.>.booted", true},
		{"foo.>.bar", true},
		{"", false},
	}
	for _, tt := range tests {
		err := validateSubject(tt.subject)
		if (err != nil) != tt.wantErr {
			t.Errorf("validateSubject(%q) = %v, want error=%v", tt.subject, err, tt.wantErr)
		}
	}
}

func startTestNATS(tb testing.TB) (*nats.Conn, jetstream.JetStream) {
	tb.Helper()
	opts := &natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  tb.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		tb.Fatalf("start nats: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		tb.Fatal("nats server not ready")
	}
	tb.Cleanup(func() { s.Shutdown() })

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		tb.Fatalf("nats connect: %v", err)
	}
	tb.Cleanup(func() { nc.Drain() })

	js, err := jetstream.New(nc)
	if err != nil {
		tb.Fatalf("jetstream: %v", err)
	}

	ctx := context.Background()
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		tb.Fatalf("create stream: %v", err)
	}

	return nc, js
}

func createTestConsumer(tb testing.TB, ctx context.Context, js jetstream.JetStream, cfg Config) (*Consumer, func()) {
	tb.Helper()
	cons, err := New(js, "TEST", cfg)
	if err != nil {
		tb.Fatalf("New consumer: %v", err)
	}
	cleanup := func() {
		cons.Close()
	}
	return cons, cleanup
}

func defaultTestConfig() Config {
	return Config{
		DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy,
		AckWait:       60 * time.Second,
		MaxDeliver:    -1,
	}
}

func waitForConsumer(tb testing.TB, ctx context.Context, js jetstream.JetStream, streamName, consumerName string) jetstream.Consumer {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		cons, err := js.Consumer(ctx, streamName, consumerName)
		if err == nil {
			return cons
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("consumer %s not found within timeout: %v", consumerName, lastErr)
	return nil
}

func waitForConsumerMissing(tb testing.TB, ctx context.Context, js jetstream.JetStream, streamName, consumerName string) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, err := js.Consumer(ctx, streamName, consumerName)
		if err != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("consumer %s still exists after timeout", consumerName)
}

func TestIsRetriableIteratorError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "heartbeat", err: fmt.Errorf("wrapped: %w", jetstream.ErrNoHeartbeat), want: true},
		{name: "timeout", err: fmt.Errorf("wrapped: %w", nats.ErrTimeout), want: true},
		{name: "consumer deleted", err: jetstream.ErrConsumerDeleted, want: false},
		{name: "plain error", err: fmt.Errorf("plain"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetriableIteratorError(tt.err); got != tt.want {
				t.Fatalf("isRetriableIteratorError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsConsumerMissingError(t *testing.T) {
	apiErr := &jetstream.APIError{
		Code:        400,
		ErrorCode:   jetstream.JSErrCodeConsumerDoesNotExist,
		Description: "consumer does not exist",
	}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "consumer deleted", err: jetstream.ErrConsumerDeleted, want: true},
		{name: "consumer does not exist", err: jetstream.ErrConsumerDoesNotExist, want: true},
		{name: "wrapped API error", err: fmt.Errorf("wrapped: %w", apiErr), want: true},
		{name: "consumer not found", err: jetstream.ErrConsumerNotFound, want: false},
		{name: "plain error", err: fmt.Errorf("plain"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isConsumerMissingError(tt.err); got != tt.want {
				t.Fatalf("isConsumerMissingError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConsumerSubscribeAndReceive(t *testing.T) {
	nc, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cons, cleanup := createTestConsumer(t, ctx, js, defaultTestConfig())
	defer cleanup()

	msgCh, unsub, err := cons.Subscribe(ctx, "test.events.>")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	err = nc.Publish("test.events.hello", []byte(`{"msg":"hello"}`))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case msg := <-msgCh:
		if string(msg.Data()) != `{"msg":"hello"}` {
			t.Errorf("got data %q, want {\"msg\":\"hello\"}", string(msg.Data()))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	unsub()
}

func TestConsumerMultipleSubscriptions(t *testing.T) {
	nc, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cons, cleanup := createTestConsumer(t, ctx, js, defaultTestConfig())
	defer cleanup()

	ch1, unsub1, err := cons.Subscribe(ctx, "test.events.>")
	if err != nil {
		t.Fatalf("Subscribe 1: %v", err)
	}

	ch2, unsub2, err := cons.Subscribe(ctx, "test.events.>")
	if err != nil {
		t.Fatalf("Subscribe 2: %v", err)
	}

	err = nc.Publish("test.events.fanout", []byte("fanout"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-ch1:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message on ch1")
	}
	select {
	case <-ch2:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message on ch2")
	}

	unsub1()
	unsub2()
}

func TestConsumerUnsubscribeClosesChannel(t *testing.T) {
	_, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cons, cleanup := createTestConsumer(t, ctx, js, defaultTestConfig())
	defer cleanup()

	msgCh, unsub, err := cons.Subscribe(ctx, "test.events.>")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	unsub()

	_, ok := <-msgCh
	if ok {
		t.Error("expected channel to be closed after unsubscribe")
	}
}

func TestConsumerFilterSubjectsOverlap(t *testing.T) {
	_, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cons, cleanup := createTestConsumer(t, ctx, js, defaultTestConfig())
	defer cleanup()

	_, unsub1, err := cons.Subscribe(ctx, "test.events.>")
	if err != nil {
		t.Fatalf("Subscribe 1: %v", err)
	}

	_, unsub2, err := cons.Subscribe(ctx, "test.events.specific")
	if err != nil {
		t.Fatalf("Subscribe 2: %v", err)
	}

	fs := cons.FilterSubjects()
	if len(fs) != 1 || fs[0] != "test.events.>" {
		t.Errorf("expected FilterSubjects = [test.events.>], got %v", fs)
	}

	unsub1()

	fs = cons.FilterSubjects()
	if len(fs) != 1 || fs[0] != "test.events.specific" {
		t.Errorf("expected FilterSubjects = [test.events.specific] after unsub, got %v", fs)
	}

	unsub2()
}

func TestConsumerFilterSubjectsTwoSubscribersSameSubject(t *testing.T) {
	_, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := defaultTestConfig()
	cfg.Name = fmt.Sprintf("same_subj_%d", time.Now().UnixNano())

	cons, cleanup := createTestConsumer(t, ctx, js, cfg)
	defer cleanup()

	_, unsub1, err := cons.Subscribe(ctx, "test.events.alpha")
	if err != nil {
		t.Fatalf("Subscribe 1: %v", err)
	}

	_, unsub2, err := cons.Subscribe(ctx, "test.events.alpha")
	if err != nil {
		t.Fatalf("Subscribe 2: %v", err)
	}

	fs := cons.FilterSubjects()
	if len(fs) != 1 || fs[0] != "test.events.alpha" {
		t.Errorf("expected FilterSubjects = [test.events.alpha], got %v", fs)
	}

	unsub1()

	fs = cons.FilterSubjects()
	if len(fs) != 1 || fs[0] != "test.events.alpha" {
		t.Errorf("expected FilterSubjects still [test.events.alpha] after one unsub, got %v", fs)
	}

	unsub2()

	time.Sleep(100 * time.Millisecond)

	_, err = js.Consumer(ctx, "TEST", cfg.Name)
	if err == nil {
		t.Error("expected consumer to be deleted after both unsubscribes")
	}
}

func TestConsumerUnsubscribeOneOfTwoSubjectsUpdatesFilters(t *testing.T) {
	_, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := defaultTestConfig()
	cfg.Name = fmt.Sprintf("two_subj_%d", time.Now().UnixNano())

	cons, cleanup := createTestConsumer(t, ctx, js, cfg)
	defer cleanup()

	_, unsubAlpha, err := cons.Subscribe(ctx, "test.events.alpha")
	if err != nil {
		t.Fatalf("Subscribe alpha: %v", err)
	}

	_, unsubBeta, err := cons.Subscribe(ctx, "test.events.beta")
	if err != nil {
		t.Fatalf("Subscribe beta: %v", err)
	}

	fs := cons.FilterSubjects()
	if len(fs) != 2 || fs[0] != "test.events.alpha" || fs[1] != "test.events.beta" {
		t.Errorf("expected FilterSubjects = [test.events.alpha, test.events.beta], got %v", fs)
	}

	unsubAlpha()

	fs = cons.FilterSubjects()
	if len(fs) != 1 || fs[0] != "test.events.beta" {
		t.Errorf("expected FilterSubjects = [test.events.beta] after unsub alpha, got %v", fs)
	}

	_, err = js.Consumer(ctx, "TEST", cfg.Name)
	if err != nil {
		t.Error("expected server-side consumer to still exist with one subject remaining")
	}

	unsubBeta()
}

func TestConsumerLastUnsubscribeDeletesServerConsumer(t *testing.T) {
	_, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := defaultTestConfig()
	cfg.Name = fmt.Sprintf("del_test_%d", time.Now().UnixNano())

	cons, cleanup := createTestConsumer(t, ctx, js, cfg)
	defer cleanup()

	_, unsub, err := cons.Subscribe(ctx, "test.events.>")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	fs := cons.FilterSubjects()
	if len(fs) == 0 {
		t.Fatal("expected filter subjects after subscribe")
	}

	unsub()

	time.Sleep(100 * time.Millisecond)

	_, err = js.Consumer(ctx, "TEST", cfg.Name)
	if err == nil {
		t.Error("expected consumer to be deleted after final unsubscribe")
	}
}

func TestConsumerReusableAfterZeroSubscriptions(t *testing.T) {
	nc, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := Config{
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckWait:       60 * time.Second,
		MaxDeliver:    -1,
	}

	cons, cleanup := createTestConsumer(t, ctx, js, cfg)
	defer cleanup()

	msgCh1, unsub1, err := cons.Subscribe(ctx, "test.events.>")
	if err != nil {
		t.Fatalf("Subscribe 1: %v", err)
	}

	nc.Publish("test.events.first", []byte("first"))

	select {
	case <-msgCh1:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for first message")
	}

	unsub1()

	time.Sleep(100 * time.Millisecond)

	msgCh2, unsub2, err := cons.Subscribe(ctx, "test.events.>")
	if err != nil {
		t.Fatalf("Subscribe 2 after zero: %v", err)
	}
	defer unsub2()

	nc.Publish("test.events.second", []byte("second"))

	select {
	case msg := <-msgCh2:
		if string(msg.Data()) != "second" {
			t.Errorf("expected 'second', got %q", string(msg.Data()))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for second message")
	}
}

func TestConsumerReusableAfterInternalSubscriptionReset(t *testing.T) {
	nc, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cons, cleanup := createTestConsumer(t, ctx, js, defaultTestConfig())
	defer cleanup()

	msgCh1, _, err := cons.Subscribe(ctx, "test.events.reset")
	if err != nil {
		t.Fatalf("Subscribe 1: %v", err)
	}

	cons.mu.Lock()
	consName := cons.cons.CachedInfo().Name
	cons.resetActiveSubscriptionsLocked()
	cons.mu.Unlock()
	if err := js.DeleteConsumer(ctx, "TEST", consName); err != nil {
		t.Fatalf("delete reset server consumer: %v", err)
	}

	select {
	case _, ok := <-msgCh1:
		if ok {
			t.Fatal("expected reset subscription channel to be closed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for reset subscription channel close")
	}

	msgCh2, unsub2, err := cons.Subscribe(ctx, "test.events.reset")
	if err != nil {
		t.Fatalf("Subscribe 2 after reset: %v", err)
	}
	defer unsub2()

	if err := nc.Publish("test.events.reset", []byte("after-reset")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case msg := <-msgCh2:
		if string(msg.Data()) != "after-reset" {
			t.Fatalf("got %q, want after-reset", string(msg.Data()))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message after reset")
	}
}

func TestEphemeralConsumerRecoversAfterServerDelete(t *testing.T) {
	nc, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := Config{
		Name:          fmt.Sprintf("recover_ephemeral_%d", time.Now().UnixNano()),
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckWait:       60 * time.Second,
		MaxDeliver:    -1,
	}
	cons, cleanup := createTestConsumer(t, ctx, js, cfg)
	defer cleanup()

	msgCh, unsub, err := cons.Subscribe(ctx, "test.events.recover")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer unsub()

	stream, err := js.Stream(ctx, "TEST")
	if err != nil {
		t.Fatalf("stream lookup: %v", err)
	}
	if err := stream.DeleteConsumer(ctx, cfg.Name); err != nil {
		t.Fatalf("delete server consumer: %v", err)
	}

	recovered := waitForConsumer(t, ctx, js, "TEST", cfg.Name)
	info := recovered.CachedInfo()
	if len(info.Config.FilterSubjects) != 1 || info.Config.FilterSubjects[0] != "test.events.recover" {
		t.Fatalf("recovered filter subjects = %v, want [test.events.recover]", info.Config.FilterSubjects)
	}

	if err := nc.Publish("test.events.recover", []byte("after-recover")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case msg, ok := <-msgCh:
		if !ok {
			t.Fatal("subscription channel closed during ephemeral recovery")
		}
		if string(msg.Data()) != "after-recover" {
			t.Fatalf("got %q, want after-recover", string(msg.Data()))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for recovered message")
	}
}

func TestEphemeralConsumerInactiveDeletionIsRecoverable(t *testing.T) {
	nc, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := Config{
		Name:              fmt.Sprintf("inactive_ephemeral_%d", time.Now().UnixNano()),
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		AckWait:           60 * time.Second,
		MaxDeliver:        -1,
		InactiveThreshold: 200 * time.Millisecond,
	}
	cons, cleanup := createTestConsumer(t, ctx, js, cfg)
	defer cleanup()

	cons.mu.Lock()
	if err := cons.createConsumerLocked(ctx); err != nil {
		cons.mu.Unlock()
		t.Fatalf("create inactive consumer: %v", err)
	}
	cons.mu.Unlock()

	waitForConsumerMissing(t, ctx, js, "TEST", cfg.Name)

	msgCh, unsub, err := cons.Subscribe(ctx, "test.events.inactive.recover")
	if err != nil {
		t.Fatalf("Subscribe after inactive deletion: %v", err)
	}
	defer unsub()

	recovered := waitForConsumer(t, ctx, js, "TEST", cfg.Name)
	info := recovered.CachedInfo()
	if len(info.Config.FilterSubjects) != 1 || info.Config.FilterSubjects[0] != "test.events.inactive.recover" {
		t.Fatalf("recovered filter subjects = %v, want [test.events.inactive.recover]", info.Config.FilterSubjects)
	}

	if err := nc.Publish("test.events.inactive.recover", []byte("after-inactive-recover")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case msg, ok := <-msgCh:
		if !ok {
			t.Fatal("subscription channel closed during inactive ephemeral recovery")
		}
		if string(msg.Data()) != "after-inactive-recover" {
			t.Fatalf("got %q, want after-inactive-recover", string(msg.Data()))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for recovered message")
	}
}

func TestDurableConsumerDeleteIsTerminal(t *testing.T) {
	_, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := defaultTestConfig()
	cfg.Durable = true
	cfg.Name = fmt.Sprintf("terminal_durable_%d", time.Now().UnixNano())

	cons, cleanup := createTestConsumer(t, ctx, js, cfg)
	defer cleanup()

	msgCh, unsub, err := cons.Subscribe(ctx, "test.events.terminal")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer unsub()

	stream, err := js.Stream(ctx, "TEST")
	if err != nil {
		t.Fatalf("stream lookup: %v", err)
	}
	if err := stream.DeleteConsumer(ctx, cfg.Name); err != nil {
		t.Fatalf("delete server consumer: %v", err)
	}

	select {
	case _, ok := <-msgCh:
		if ok {
			t.Fatal("expected durable deletion to close subscription channel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for subscription channel close")
	}

	if _, _, err := cons.Subscribe(ctx, "test.events.terminal.again"); err == nil {
		t.Fatal("expected subscribe after durable deletion to fail")
	}
}

func TestDurableConsumerInactiveDeletionIsTerminal(t *testing.T) {
	_, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := defaultTestConfig()
	cfg.Durable = true
	cfg.Name = fmt.Sprintf("inactive_terminal_durable_%d", time.Now().UnixNano())
	cfg.DeliverPolicy = jetstream.DeliverNewPolicy
	cfg.InactiveThreshold = 200 * time.Millisecond

	cons, cleanup := createTestConsumer(t, ctx, js, cfg)
	defer cleanup()

	cons.mu.Lock()
	if err := cons.createConsumerLocked(ctx); err != nil {
		cons.mu.Unlock()
		t.Fatalf("create inactive durable consumer: %v", err)
	}
	cons.mu.Unlock()

	waitForConsumerMissing(t, ctx, js, "TEST", cfg.Name)

	if _, _, err := cons.Subscribe(ctx, "test.events.inactive.terminal.again"); err == nil {
		t.Fatal("expected subscribe after durable inactive deletion to fail")
	}
}

func TestConsumerClosePreventsReuse(t *testing.T) {
	_, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cons, cleanup := createTestConsumer(t, ctx, js, defaultTestConfig())
	defer cleanup()

	_, unsub, err := cons.Subscribe(ctx, "test.events.>")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	unsub()

	cleanup()

	_, _, err = cons.Subscribe(ctx, "test.events.>")
	if err == nil {
		t.Error("expected error subscribing after close")
	}
}

func TestDurableFinalUnsubscribeDeletesConsumer(t *testing.T) {
	_, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := defaultTestConfig()
	cfg.Durable = true
	cfg.Name = fmt.Sprintf("dur_test_%d", time.Now().UnixNano())

	cons, cleanup := createTestConsumer(t, ctx, js, cfg)
	defer cleanup()

	_, unsub, err := cons.Subscribe(ctx, "test.events.>")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	unsub()

	time.Sleep(100 * time.Millisecond)

	_, err = js.Consumer(ctx, "TEST", cfg.Name)
	if err == nil {
		t.Error("expected durable consumer to be deleted after final unsubscribe")
	}

	cleanup()
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.DeliverPolicy != jetstream.DeliverNewPolicy {
		t.Errorf("expected DeliverNewPolicy, got %v", cfg.DeliverPolicy)
	}
	if cfg.AckWait != 60*time.Second {
		t.Errorf("expected AckWait 60s, got %v", cfg.AckWait)
	}
	if cfg.MaxDeliver != -1 {
		t.Errorf("expected MaxDeliver -1, got %d", cfg.MaxDeliver)
	}
	if cfg.Durable {
		t.Error("expected Durable false by default")
	}
}

func TestNewDurableWithoutName(t *testing.T) {
	_, js := startTestNATS(t)

	cfg := defaultTestConfig()
	cfg.Durable = true
	cfg.Name = ""

	_, err := New(js, "TEST", cfg)
	if err == nil {
		t.Error("expected error for durable consumer without name")
	}
}

func TestConsumerUnsubscribeDuringDispatch(t *testing.T) {
	nc, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cons, cleanup := createTestConsumer(t, ctx, js, defaultTestConfig())
	defer cleanup()

	var unsubs []func()
	for i := 0; i < 5; i++ {
		_, unsub, err := cons.Subscribe(ctx, "test.events.>")
		if err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		unsubs = append(unsubs, unsub)
	}

	var done atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !done.Load(); i++ {
			nc.Publish("test.events.pong", []byte(fmt.Sprintf("msg-%d", i)))
		}
	}()

	time.Sleep(50 * time.Millisecond)

	for _, unsub := range unsubs {
		unsub()
	}

	done.Store(true)
	wg.Wait()
}

func TestConsumerPartialUnsubscribeDuringDispatch(t *testing.T) {
	nc, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cons, cleanup := createTestConsumer(t, ctx, js, defaultTestConfig())
	defer cleanup()

	var unsubs []func()
	for i := 0; i < 5; i++ {
		_, unsub, err := cons.Subscribe(ctx, "test.events.>")
		if err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		unsubs = append(unsubs, unsub)
	}

	var done atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !done.Load(); i++ {
			nc.Publish("test.events.pong", []byte(fmt.Sprintf("msg-%d", i)))
		}
	}()

	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 3; i++ {
		unsubs[i]()
	}

	done.Store(true)
	wg.Wait()

	_, unsub4, err := cons.Subscribe(ctx, "test.events.specific")
	if err != nil {
		t.Fatalf("Subscribe after partial unsub: %v", err)
	}
	unsubs[3]()
	unsubs[4]()
	unsub4()
}

func TestConsumerCloseDuringDispatch(t *testing.T) {
	nc, js := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cons, _ := createTestConsumer(t, ctx, js, defaultTestConfig())

	for i := 0; i < 5; i++ {
		_, _, err := cons.Subscribe(ctx, "test.events.>")
		if err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
	}

	var done atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !done.Load(); i++ {
			nc.Publish("test.events.pong", []byte(fmt.Sprintf("msg-%d", i)))
		}
	}()

	time.Sleep(50 * time.Millisecond)

	cons.Close()
	done.Store(true)
	wg.Wait()
}
