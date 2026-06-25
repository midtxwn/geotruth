package geotruthops

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/internal/gtevents"
	internalkeys "github.com/midtxwn/geotruth/internal/natskeys"
	"github.com/midtxwn/geotruth/internal/spatialstream"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/natspublish"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCompactionKeepsCommittedButUnackedSourceState(t *testing.T) {
	ctx := context.Background()
	_, js, shutdown := runOpsNATS(t)
	defer shutdown()
	ensureOpsStreams(t, ctx, js)

	cons := ensureGeoTruthConsumer(t, ctx, js)
	sourceSeq, sourceMsg := publishSpatialRemoveAndFetch(t, ctx, js, cons, "race-obj")
	publicSeq, stateSeq := publishRemovedCommit(t, ctx, js, "race-obj", sourceSeq)

	ops := NewWithJetStream(js)
	plan, err := ops.PlanCompaction(ctx, CompactionOptions{
		IncludeSpatial:           true,
		IncludePublicGTEvents:    true,
		IncludeRemovedTombstones: true,
	})
	if err != nil {
		t.Fatalf("plan compaction: %v", err)
	}
	if plan.CutoffSeq != 0 {
		t.Fatalf("cutoff = %d, want 0 before SPATIAL ack is recorded", plan.CutoffSeq)
	}
	if plan.Spatial.Enabled || len(plan.PublicGTEvents) != 0 || len(plan.RemovedTombstones) != 0 {
		t.Fatalf("unacked committed source should not be compactable: %+v", plan)
	}

	exportDir := t.TempDir()
	if _, err := ops.Compact(ctx, plan, CompactOptions{ExportDir: exportDir, Execute: true}); err != nil {
		t.Fatalf("compact unacked plan: %v", err)
	}
	assertStreamMsgExists(t, ctx, js, StreamSpatial, sourceSeq)
	assertStreamMsgExists(t, ctx, js, StreamGTEvents, publicSeq)
	assertStreamMsgExists(t, ctx, js, StreamGTEvents, stateSeq)

	if err := sourceMsg.DoubleAck(ctx); err != nil {
		t.Fatalf("ack source message: %v", err)
	}
	assertAckFloor(t, ctx, cons, sourceSeq)

	plan, err = ops.PlanCompaction(ctx, CompactionOptions{
		IncludeSpatial:           true,
		IncludePublicGTEvents:    true,
		IncludeRemovedTombstones: true,
	})
	if err != nil {
		t.Fatalf("plan compaction after ack: %v", err)
	}
	if plan.CutoffSeq < sourceSeq {
		t.Fatalf("cutoff = %d, want >= %d", plan.CutoffSeq, sourceSeq)
	}
	if !plan.Spatial.Enabled {
		t.Fatal("SPATIAL should be compactable after ack")
	}
	if len(plan.PublicGTEvents) != 1 {
		t.Fatalf("public GT_EVENTS planned = %d, want 1", len(plan.PublicGTEvents))
	}
	if len(plan.RemovedTombstones) != 1 {
		t.Fatalf("removed tombstones planned = %d, want 1", len(plan.RemovedTombstones))
	}

	result, err := ops.Compact(ctx, plan, CompactOptions{ExportDir: exportDir, Execute: true})
	if err != nil {
		t.Fatalf("compact after ack: %v", err)
	}
	if !result.PurgedSpatial || result.DeletedPublicGTEvents != 1 || result.DeletedRemovedTombstones != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	assertStreamMsgDeleted(t, ctx, js, StreamSpatial, sourceSeq)
	assertStreamMsgDeleted(t, ctx, js, StreamGTEvents, publicSeq)
	assertStreamMsgDeleted(t, ctx, js, StreamGTEvents, stateSeq)
	assertExportNonEmpty(t, exportDir, exportSpatial)
	assertExportNonEmpty(t, exportDir, exportGTEventsPublic)
	assertExportNonEmpty(t, exportDir, exportGTEventsRemovedState)
}

func TestCompactionDeletesPublicHistoryButKeepsActiveState(t *testing.T) {
	ctx := context.Background()
	_, js, shutdown := runOpsNATS(t)
	defer shutdown()
	ensureOpsStreams(t, ctx, js)

	cons := ensureGeoTruthConsumer(t, ctx, js)
	sourceSeq, sourceMsg := publishSpatialRegisterAndFetch(t, ctx, js, cons, "active-obj")
	publicSeq, stateSeq := publishRegisterCommit(t, ctx, js, "active-obj", sourceSeq)
	if err := sourceMsg.DoubleAck(ctx); err != nil {
		t.Fatalf("ack source message: %v", err)
	}
	assertAckFloor(t, ctx, cons, sourceSeq)

	ops := NewWithJetStream(js)
	plan, err := ops.PlanCompaction(ctx, CompactionOptions{
		IncludePublicGTEvents:    true,
		IncludeRemovedTombstones: true,
	})
	if err != nil {
		t.Fatalf("plan compaction: %v", err)
	}
	if len(plan.PublicGTEvents) != 1 {
		t.Fatalf("public GT_EVENTS planned = %d, want 1", len(plan.PublicGTEvents))
	}
	if len(plan.RemovedTombstones) != 0 {
		t.Fatalf("removed tombstones planned = %d, want 0", len(plan.RemovedTombstones))
	}

	if _, err := ops.Compact(ctx, plan, CompactOptions{ExportDir: t.TempDir(), Execute: true}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	assertStreamMsgDeleted(t, ctx, js, StreamGTEvents, publicSeq)
	assertStreamMsgExists(t, ctx, js, StreamGTEvents, stateSeq)
}

func TestCompactionExportFailurePreventsDeletes(t *testing.T) {
	ctx := context.Background()
	_, js, shutdown := runOpsNATS(t)
	defer shutdown()
	ensureOpsStreams(t, ctx, js)

	cons := ensureGeoTruthConsumer(t, ctx, js)
	sourceSeq, sourceMsg := publishSpatialRemoveAndFetch(t, ctx, js, cons, "export-fail-obj")
	publicSeq, stateSeq := publishRemovedCommit(t, ctx, js, "export-fail-obj", sourceSeq)
	if err := sourceMsg.DoubleAck(ctx); err != nil {
		t.Fatalf("ack source message: %v", err)
	}
	assertAckFloor(t, ctx, cons, sourceSeq)

	ops := NewWithJetStream(js)
	plan, err := ops.PlanCompaction(ctx, CompactionOptions{
		IncludePublicGTEvents:    true,
		IncludeRemovedTombstones: true,
	})
	if err != nil {
		t.Fatalf("plan compaction: %v", err)
	}

	exportPath := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(exportPath, []byte("file"), 0o644); err != nil {
		t.Fatalf("write blocking export file: %v", err)
	}
	if _, err := ops.Compact(ctx, plan, CompactOptions{ExportDir: exportPath, Execute: true}); err == nil {
		t.Fatal("compact should fail when export dir cannot be created")
	}

	assertStreamMsgExists(t, ctx, js, StreamGTEvents, publicSeq)
	assertStreamMsgExists(t, ctx, js, StreamGTEvents, stateSeq)
}

func TestCompactRejectsForgedPlanAboveAckFloor(t *testing.T) {
	ctx := context.Background()
	_, js, shutdown := runOpsNATS(t)
	defer shutdown()
	ensureOpsStreams(t, ctx, js)

	ensureGeoTruthConsumer(t, ctx, js)
	_, publicSeq := publishRegisterCommit(t, ctx, js, "forged-obj", 99)

	ops := NewWithJetStream(js)
	_, err := ops.Compact(ctx, CompactionPlan{
		CutoffSeq: 99,
		PublicGTEvents: []GTEventRef{{
			StreamSeq: publicSeq,
			Subject:   "gt.events.v1.object.forged-obj.registered",
			SourceSeq: 99,
			ObjectID:  "forged-obj",
		}},
	}, CompactOptions{ExportDir: t.TempDir(), Execute: true})
	if err == nil {
		t.Fatal("expected forged plan above ack floor to be rejected")
	}
	assertStreamMsgExists(t, ctx, js, StreamGTEvents, publicSeq)
}

func TestStatsRejectsMisconfiguredGeoTruthConsumerFilter(t *testing.T) {
	ctx := context.Background()
	_, js, shutdown := runOpsNATS(t)
	defer shutdown()
	ensureOpsStreams(t, ctx, js)

	if _, err := js.CreateOrUpdateConsumer(ctx, StreamSpatial, jetstream.ConsumerConfig{
		Durable:       internalkeys.DurableGeoTruth,
		FilterSubject: "pos.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}); err != nil {
		t.Fatalf("create narrowed geotruth consumer: %v", err)
	}

	ops := NewWithJetStream(js)
	if _, err := ops.Stats(ctx); err == nil {
		t.Fatal("expected stats to reject narrowed geotruth consumer")
	}
	if _, err := ops.PlanCompaction(ctx, CompactionOptions{IncludeSpatial: true}); err == nil {
		t.Fatal("expected plan to reject narrowed geotruth consumer")
	}
}

func TestCompactRequiresExportDirForDestructiveWork(t *testing.T) {
	ops := NewWithJetStream(nil)
	_, err := ops.Compact(context.Background(), CompactionPlan{
		CutoffSeq:      1,
		PublicGTEvents: []GTEventRef{{StreamSeq: 1, SourceSeq: 1}},
	}, CompactOptions{Execute: true})
	if err == nil {
		t.Fatal("expected export-dir requirement error")
	}
}

func runOpsNATS(t *testing.T) (*nats.Conn, jetstream.JetStream, func()) {
	t.Helper()
	opts := &natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready")
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		s.Shutdown()
		t.Fatalf("nats connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		s.Shutdown()
		t.Fatalf("jetstream: %v", err)
	}
	return nc, js, func() {
		nc.Close()
		s.Shutdown()
	}
}

func ensureOpsStreams(t *testing.T, ctx context.Context, js jetstream.JetStream) {
	t.Helper()
	if _, err := spatialstream.EnsureStream(ctx, js, spatialstream.Config{Storage: jetstream.FileStorage}); err != nil {
		t.Fatalf("ensure SPATIAL: %v", err)
	}
	if _, err := gtevents.EnsureStream(ctx, js, gtevents.StreamConfig{Storage: jetstream.FileStorage}); err != nil {
		t.Fatalf("ensure GT_EVENTS: %v", err)
	}
}

func ensureGeoTruthConsumer(t *testing.T, ctx context.Context, js jetstream.JetStream) jetstream.Consumer {
	t.Helper()
	cons, err := js.CreateOrUpdateConsumer(ctx, StreamSpatial, jetstream.ConsumerConfig{
		Durable:        internalkeys.DurableGeoTruth,
		FilterSubjects: internalkeys.StreamSubjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		MaxDeliver:     -1,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("create geotruth consumer: %v", err)
	}
	return cons
}

func publishSpatialRemoveAndFetch(t *testing.T, ctx context.Context, js jetstream.JetStream, cons jetstream.Consumer, id string) (uint64, jetstream.Msg) {
	t.Helper()
	data, err := json.Marshal(natspublish.ObjectRemoveMsg{ID: id})
	if err != nil {
		t.Fatalf("marshal remove: %v", err)
	}
	ack, err := js.Publish(ctx, internalkeys.SubjectCmdObjRemove, data)
	if err != nil {
		t.Fatalf("publish spatial remove: %v", err)
	}
	msg, err := cons.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatalf("fetch spatial remove: %v", err)
	}
	return ack.Sequence, msg
}

func publishSpatialRegisterAndFetch(t *testing.T, ctx context.Context, js jetstream.JetStream, cons jetstream.Consumer, id string) (uint64, jetstream.Msg) {
	t.Helper()
	data, err := json.Marshal(natspublish.ObjectRegisterMsg{
		ID:   id,
		Dims: domain.ObjectDimensions{Width: 1, Height: 1},
	})
	if err != nil {
		t.Fatalf("marshal register: %v", err)
	}
	ack, err := js.Publish(ctx, internalkeys.SubjectCmdObjRegister, data)
	if err != nil {
		t.Fatalf("publish spatial register: %v", err)
	}
	msg, err := cons.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatalf("fetch spatial register: %v", err)
	}
	return ack.Sequence, msg
}

func publishRemovedCommit(t *testing.T, ctx context.Context, js jetstream.JetStream, objectID string, sourceSeq uint64) (uint64, uint64) {
	t.Helper()
	msgs, err := gtevents.BuildCommitMsgs(gtevents.CommitInput{
		ObjectID:  objectID,
		SourceSeq: sourceSeq,
		Lifecycle: gtevents.LifecycleRemoved,
		Dims:      gtevents.EventDims{Width: 1, Height: 1},
	})
	if err != nil {
		t.Fatalf("build removed commit: %v", err)
	}
	return publishCommitMsgs(t, ctx, js, msgs)
}

func publishRegisterCommit(t *testing.T, ctx context.Context, js jetstream.JetStream, objectID string, sourceSeq uint64) (uint64, uint64) {
	t.Helper()
	msgs, err := gtevents.BuildRegisterCommitMsgs(objectID, sourceSeq, "", domain.ObjectDimensions{Width: 1, Height: 1})
	if err != nil {
		t.Fatalf("build register commit: %v", err)
	}
	return publishCommitMsgs(t, ctx, js, msgs)
}

func publishCommitMsgs(t *testing.T, ctx context.Context, js jetstream.JetStream, msgs []*nats.Msg) (uint64, uint64) {
	t.Helper()
	var publicSeq, stateSeq uint64
	for i, msg := range msgs {
		ack, err := js.PublishMsg(ctx, msg)
		if err != nil {
			t.Fatalf("publish commit msg %d: %v", i, err)
		}
		if i == 0 {
			publicSeq = ack.Sequence
		}
		stateSeq = ack.Sequence
	}
	return publicSeq, stateSeq
}

func assertAckFloor(t *testing.T, ctx context.Context, cons jetstream.Consumer, seq uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		info, err := cons.Info(ctx)
		if err == nil && info.AckFloor.Stream >= seq {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("consumer info after ack: %v", err)
	}
	t.Fatalf("ack floor = %d, want >= %d", info.AckFloor.Stream, seq)
}

func assertStreamMsgExists(t *testing.T, ctx context.Context, js jetstream.JetStream, streamName string, seq uint64) {
	t.Helper()
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		t.Fatalf("get stream %s: %v", streamName, err)
	}
	if _, err := stream.GetMsg(ctx, seq); err != nil {
		t.Fatalf("stream %s seq %d should exist: %v", streamName, seq, err)
	}
}

func assertStreamMsgDeleted(t *testing.T, ctx context.Context, js jetstream.JetStream, streamName string, seq uint64) {
	t.Helper()
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		t.Fatalf("get stream %s: %v", streamName, err)
	}
	_, err = stream.GetMsg(ctx, seq)
	if err == nil {
		t.Fatalf("stream %s seq %d should be deleted", streamName, seq)
	}
	if !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("stream %s seq %d unexpected error: %v", streamName, seq, err)
	}
}

func assertExportNonEmpty(t *testing.T, dir, kind string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, kind+".ndjson"))
	if err != nil {
		t.Fatalf("read export %s: %v", kind, err)
	}
	if len(data) == 0 {
		t.Fatalf("export %s is empty", kind)
	}
}
