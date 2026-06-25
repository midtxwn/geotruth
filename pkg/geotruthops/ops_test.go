package geotruthops

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/internal/gtevents"
	"github.com/midtxwn/geotruth/pkg/natswatch"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestParseEventRecordReadsGeofenceTransitions(t *testing.T) {
	msg := &jetstream.RawStreamMsg{
		Sequence: 7,
		Subject:  natswatch.GTSubjectPositionUpdated("obj1"),
		Data: []byte(`{
			"e":"gt:p:obj1:b1i1:2",
			"k":"position.updated",
			"o":"obj1",
			"i":"b1i1",
			"s":2,
			"cp":{
				"l":"a",
				"g":[
					{"a":"zone-a","e":1}
				]
			}
		}`),
	}

	rec, ok, err := parseEventRecord(msg)
	if err != nil {
		t.Fatalf("parseEventRecord: %v", err)
	}
	if !ok {
		t.Fatal("parseEventRecord returned ok=false")
	}
	if !rec.IsCommit {
		t.Fatal("record was not marked as commit")
	}
	if rec.StreamSeq != 7 || rec.ObjectID != "obj1" || rec.InstanceID != "b1i1" || rec.CommitSeq != 2 {
		t.Fatalf("bad record identity: %+v", rec)
	}
	if rec.Lifecycle != gtevents.LifecycleActive {
		t.Fatalf("lifecycle = %s, want %s", rec.Lifecycle, gtevents.LifecycleActive)
	}
	if len(rec.GeofenceTransitions) != 1 {
		t.Fatalf("geofence transitions = %d, want 1", len(rec.GeofenceTransitions))
	}
	if got := rec.GeofenceTransitions[0]; got.AreaID != "zone-a" || !got.Entered {
		t.Fatalf("geofence transition = %+v, want zone-a entered", got)
	}
}

func TestParseEventRecordRejectsUnknownLifecycle(t *testing.T) {
	msg := &jetstream.RawStreamMsg{
		Sequence: 8,
		Subject:  natswatch.GTSubjectObjectRegistered("obj1"),
		Data: []byte(`{
			"e":"gt:o:r:obj1:b1i1:1",
			"k":"object.registered",
			"o":"obj1",
			"i":"b1i1",
			"s":1,
			"cp":{"l":"z"}
		}`),
	}

	if _, _, err := parseEventRecord(msg); err == nil {
		t.Fatal("parseEventRecord accepted unknown lifecycle")
	}
}

func TestNewCreatesOpsClient(t *testing.T) {
	ctx := context.Background()
	nc, _ := startOpsTestNATS(t)

	ops, err := New(nc)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := ops.Stats(ctx); err != nil {
		t.Fatalf("Stats from New client: %v", err)
	}

	if _, err := New(nil); err == nil {
		t.Fatal("New nil connection succeeded")
	}
}

func TestGeofenceEventIDHelpers(t *testing.T) {
	if got := geofenceEnterEventID("zone-a", "obj1", "b1i1", 36); got != "gt:gf:e:zone-a:obj1:b1i1:10" {
		t.Fatalf("geofenceEnterEventID = %q", got)
	}
	if got := geofenceExitEventID("zone-a", "obj1", "b1i1", 36); got != "gt:gf:x:zone-a:obj1:b1i1:10" {
		t.Fatalf("geofenceExitEventID = %q", got)
	}
}

func TestPlanCompactionProtectsLatestActiveCommitAndReferencedProjection(t *testing.T) {
	ctx := context.Background()
	_, js := startOpsTestNATS(t)
	ops := NewWithJetStream(js)

	oldActiveSeq := publishGTEvent(t, ctx, js, natswatch.GTSubjectObjectRegistered("obj1"), commitPayload(
		"gt:o:r:obj1:b1i1:1", natswatch.EventTypeObjectRegistered, "obj1", "b1i1", 1, gtevents.LifecycleActive, nil,
	))
	staleProjectionSeq := publishGTEvent(t, ctx, js, natswatch.GTSubjectGeofenceEntered("obj1", "stale-zone"), projectionPayload(
		"gt:gf:e:stale-zone:obj1:b1i1:1", natswatch.EventTypeGeofenceEntered, "obj1", "b1i1", 1, "stale-zone",
	))
	latestActiveSeq := publishGTEvent(t, ctx, js, natswatch.GTSubjectPositionUpdated("obj1"), commitPayload(
		"gt:p:obj1:b1i1:2", natswatch.EventTypePositionUpdated, "obj1", "b1i1", 2, gtevents.LifecycleActive,
		[]gtevents.GeofenceTransitionRecord{{AreaID: "zone-a", Entered: true}},
	))
	protectedProjectionSeq := publishGTEvent(t, ctx, js, natswatch.GTSubjectGeofenceEntered("obj1", "zone-a"), projectionPayload(
		"gt:gf:e:zone-a:obj1:b1i1:2", natswatch.EventTypeGeofenceEntered, "obj1", "b1i1", 2, "zone-a",
	))
	removedOldSeq := publishGTEvent(t, ctx, js, natswatch.GTSubjectObjectRegistered("obj2"), commitPayload(
		"gt:o:r:obj2:b1i2:1", natswatch.EventTypeObjectRegistered, "obj2", "b1i2", 1, gtevents.LifecycleActive, nil,
	))
	removedLatestSeq := publishGTEvent(t, ctx, js, natswatch.GTSubjectObjectRemoved("obj2"), commitPayload(
		"gt:o:d:obj2:b1i2:2", natswatch.EventTypeObjectRemoved, "obj2", "b1i2", 2, gtevents.LifecycleRemoved, nil,
	))

	stats, err := ops.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.GTEvents.Messages != 6 || stats.GTEvents.LastSeq != removedLatestSeq {
		t.Fatalf("stats = %+v, want 6 messages last seq %d", stats.GTEvents, removedLatestSeq)
	}

	plan, err := ops.PlanCompaction(ctx, CompactionOptions{IncludePublicGTEvents: true, IncludeRemovedTombstones: true})
	if err != nil {
		t.Fatalf("PlanCompaction: %v", err)
	}
	assertPlanContains(t, plan, oldActiveSeq, staleProjectionSeq, removedOldSeq, removedLatestSeq)
	assertPlanOmits(t, plan, latestActiveSeq, protectedProjectionSeq)

	keepRemoved, err := ops.PlanCompaction(ctx, CompactionOptions{IncludePublicGTEvents: true, IncludeRemovedTombstones: false})
	if err != nil {
		t.Fatalf("PlanCompaction keep removed: %v", err)
	}
	assertPlanContains(t, keepRemoved, oldActiveSeq, staleProjectionSeq, removedOldSeq)
	assertPlanOmits(t, keepRemoved, latestActiveSeq, protectedProjectionSeq, removedLatestSeq)
}

func TestCompactExportsAndDeletesPlannedMessages(t *testing.T) {
	ctx := context.Background()
	_, js := startOpsTestNATS(t)
	ops := NewWithJetStream(js)

	deleteSeq := publishGTEvent(t, ctx, js, natswatch.GTSubjectObjectRegistered("obj1"), commitPayload(
		"gt:o:r:obj1:b1i1:1", natswatch.EventTypeObjectRegistered, "obj1", "b1i1", 1, gtevents.LifecycleActive, nil,
	))
	keepSeq := publishGTEvent(t, ctx, js, natswatch.GTSubjectPositionUpdated("obj1"), commitPayload(
		"gt:p:obj1:b1i1:2", natswatch.EventTypePositionUpdated, "obj1", "b1i1", 2, gtevents.LifecycleActive, nil,
	))

	plan, err := ops.PlanCompaction(ctx, CompactionOptions{IncludePublicGTEvents: true, IncludeRemovedTombstones: true})
	if err != nil {
		t.Fatalf("PlanCompaction: %v", err)
	}
	assertPlanContains(t, plan, deleteSeq)
	assertPlanOmits(t, plan, keepSeq)

	if _, err := ops.Compact(ctx, plan, CompactOptions{}); err == nil {
		t.Fatal("Compact without Execute succeeded")
	}

	exportDir := t.TempDir()
	result, err := ops.Compact(ctx, plan, CompactOptions{Execute: true, ExportDir: exportDir})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if result.ExportedGTEvents != 1 || result.DeletedGTEvents != 1 {
		t.Fatalf("result = %+v, want one export and one delete", result)
	}

	stream, err := js.Stream(ctx, StreamGTEvents)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if _, err := stream.GetMsg(ctx, deleteSeq); err == nil {
		t.Fatalf("seq %d still exists after compaction", deleteSeq)
	}
	if _, err := stream.GetMsg(ctx, keepSeq); err != nil {
		t.Fatalf("protected seq %d missing after compaction: %v", keepSeq, err)
	}

	data, err := os.ReadFile(filepath.Join(exportDir, exportGTEvents+".ndjson"))
	if err != nil {
		t.Fatalf("read export: %v", err)
	}
	var rec ExportRecord
	if err := json.Unmarshal(bytes.TrimSpace(data), &rec); err != nil {
		t.Fatalf("decode export: %v; data=%s", err, string(data))
	}
	if rec.Stream != StreamGTEvents || rec.StreamSeq != deleteSeq || rec.Subject != natswatch.GTSubjectObjectRegistered("obj1") {
		t.Fatalf("export record = %+v", rec)
	}
	if len(rec.Data) == 0 || rec.DataBase64 != "" {
		t.Fatalf("export payload = data %s base64 %q", string(rec.Data), rec.DataBase64)
	}
}

func TestValidatePlanRejectsProtectedOrStaleSequence(t *testing.T) {
	ctx := context.Background()
	_, js := startOpsTestNATS(t)
	ops := NewWithJetStream(js)
	protectedSeq := publishGTEvent(t, ctx, js, natswatch.GTSubjectObjectRegistered("obj1"), commitPayload(
		"gt:o:r:obj1:b1i1:1", natswatch.EventTypeObjectRegistered, "obj1", "b1i1", 1, gtevents.LifecycleActive, nil,
	))

	err := ops.validatePlan(ctx, CompactionPlan{DeleteGTEvents: []GTEventRef{{StreamSeq: protectedSeq}}})
	if err == nil {
		t.Fatal("validatePlan accepted protected latest active commit")
	}
}

func TestCompactAllowsAppendOnlyGrowthWhenPlanRemainsSafe(t *testing.T) {
	ctx := context.Background()
	_, js := startOpsTestNATS(t)
	ops := NewWithJetStream(js)

	deleteSeq := publishGTEvent(t, ctx, js, natswatch.GTSubjectObjectRegistered("obj1"), commitPayload(
		"gt:o:r:obj1:b1i1:1", natswatch.EventTypeObjectRegistered, "obj1", "b1i1", 1, gtevents.LifecycleActive, nil,
	))
	keepSeq := publishGTEvent(t, ctx, js, natswatch.GTSubjectPositionUpdated("obj1"), commitPayload(
		"gt:p:obj1:b1i1:2", natswatch.EventTypePositionUpdated, "obj1", "b1i1", 2, gtevents.LifecycleActive, nil,
	))

	plan, err := ops.PlanCompaction(ctx, CompactionOptions{IncludePublicGTEvents: true, IncludeRemovedTombstones: true})
	if err != nil {
		t.Fatalf("PlanCompaction: %v", err)
	}
	assertPlanContains(t, plan, deleteSeq)

	appendedSeq := publishGTEvent(t, ctx, js, natswatch.GTSubjectObjectRegistered("obj2"), commitPayload(
		"gt:o:r:obj2:b1i2:1", natswatch.EventTypeObjectRegistered, "obj2", "b1i2", 1, gtevents.LifecycleActive, nil,
	))

	result, err := ops.Compact(ctx, plan, CompactOptions{Execute: true})
	if err != nil {
		t.Fatalf("Compact after append-only growth: %v", err)
	}
	if result.DeletedGTEvents != 1 {
		t.Fatalf("deleted = %d, want 1", result.DeletedGTEvents)
	}

	stream, err := js.Stream(ctx, StreamGTEvents)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if _, err := stream.GetMsg(ctx, deleteSeq); err == nil {
		t.Fatalf("seq %d still exists after compaction", deleteSeq)
	}
	if _, err := stream.GetMsg(ctx, keepSeq); err != nil {
		t.Fatalf("protected seq %d missing after compaction: %v", keepSeq, err)
	}
	if _, err := stream.GetMsg(ctx, appendedSeq); err != nil {
		t.Fatalf("appended seq %d missing after compaction: %v", appendedSeq, err)
	}
}

func TestCompactRejectsAlreadyAppliedPlan(t *testing.T) {
	ctx := context.Background()
	_, js := startOpsTestNATS(t)
	ops := NewWithJetStream(js)

	deleteSeq := publishGTEvent(t, ctx, js, natswatch.GTSubjectObjectRegistered("obj1"), commitPayload(
		"gt:o:r:obj1:b1i1:1", natswatch.EventTypeObjectRegistered, "obj1", "b1i1", 1, gtevents.LifecycleActive, nil,
	))
	publishGTEvent(t, ctx, js, natswatch.GTSubjectPositionUpdated("obj1"), commitPayload(
		"gt:p:obj1:b1i1:2", natswatch.EventTypePositionUpdated, "obj1", "b1i1", 2, gtevents.LifecycleActive, nil,
	))

	plan, err := ops.PlanCompaction(ctx, CompactionOptions{IncludePublicGTEvents: true, IncludeRemovedTombstones: true})
	if err != nil {
		t.Fatalf("PlanCompaction: %v", err)
	}
	assertPlanContains(t, plan, deleteSeq)

	if _, err := ops.Compact(ctx, plan, CompactOptions{Execute: true}); err != nil {
		t.Fatalf("first Compact: %v", err)
	}
	if _, err := ops.Compact(ctx, plan, CompactOptions{Execute: true}); err == nil {
		t.Fatal("second Compact with already-applied plan succeeded")
	} else if !strings.Contains(err.Error(), "regenerate the plan") {
		t.Fatalf("second Compact error = %v, want regenerate-plan guidance", err)
	}
}

func startOpsTestNATS(tb testing.TB) (*nats.Conn, jetstream.JetStream) {
	tb.Helper()
	s, err := natsserver.NewServer(&natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  tb.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		tb.Fatalf("new nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		s.Shutdown()
		tb.Fatal("nats server not ready")
	}
	tb.Cleanup(func() { s.Shutdown() })

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		tb.Fatalf("connect nats: %v", err)
	}
	tb.Cleanup(func() { nc.Close() })

	js, err := jetstream.New(nc)
	if err != nil {
		tb.Fatalf("jetstream: %v", err)
	}
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     StreamGTEvents,
		Subjects: []string{natswatch.GTEventsWildcard},
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		tb.Fatalf("create stream: %v", err)
	}
	return nc, js
}

func publishGTEvent(tb testing.TB, ctx context.Context, js jetstream.JetStream, subject string, payload []byte) uint64 {
	tb.Helper()
	ack, err := js.Publish(ctx, subject, payload)
	if err != nil {
		tb.Fatalf("publish %s: %v", subject, err)
	}
	return ack.Sequence
}

func commitPayload(eventID, eventType, objectID, instanceID string, commitSeq uint64, lifecycle gtevents.Lifecycle, transitions []gtevents.GeofenceTransitionRecord) []byte {
	data, _ := json.Marshal(map[string]any{
		"e": eventID,
		"k": eventType,
		"o": objectID,
		"i": instanceID,
		"s": commitSeq,
		"cp": map[string]any{
			"l": compactLifecycleForPayload(lifecycle),
			"g": compactTransitionsForPayload(transitions),
		},
	})
	return data
}

func compactLifecycleForPayload(lifecycle gtevents.Lifecycle) string {
	switch lifecycle {
	case gtevents.LifecycleActive:
		return "a"
	case gtevents.LifecycleRemoved:
		return "r"
	default:
		panic("unknown lifecycle")
	}
}

func compactTransitionsForPayload(transitions []gtevents.GeofenceTransitionRecord) []map[string]any {
	if len(transitions) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(transitions))
	for _, tr := range transitions {
		entered := 0
		if tr.Entered {
			entered = 1
		}
		out = append(out, map[string]any{"a": tr.AreaID, "e": entered})
	}
	return out
}

func projectionPayload(eventID, eventType, objectID, instanceID string, commitSeq uint64, areaID string) []byte {
	data, _ := json.Marshal(map[string]any{
		"e": eventID,
		"k": eventType,
		"o": objectID,
		"i": instanceID,
		"s": commitSeq,
		"g": areaID,
	})
	return data
}

func assertPlanContains(tb testing.TB, plan CompactionPlan, seqs ...uint64) {
	tb.Helper()
	have := make(map[uint64]bool)
	for _, ref := range plan.DeleteGTEvents {
		have[ref.StreamSeq] = true
	}
	for _, seq := range seqs {
		if !have[seq] {
			tb.Fatalf("plan missing seq %d; plan=%+v", seq, plan.DeleteGTEvents)
		}
	}
}

func assertPlanOmits(tb testing.TB, plan CompactionPlan, seqs ...uint64) {
	tb.Helper()
	for _, ref := range plan.DeleteGTEvents {
		for _, seq := range seqs {
			if ref.StreamSeq == seq {
				tb.Fatalf("plan includes protected seq %d; plan=%+v", seq, plan.DeleteGTEvents)
			}
		}
	}
}
