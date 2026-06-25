// Package geotruthops provides operational helpers for inspecting and
// compacting the NATS streams used by Ingester and GeoTruth.
package geotruthops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/midtxwn/geotruth/internal/gtevents"
	internalkeys "github.com/midtxwn/geotruth/internal/natskeys"
	"github.com/midtxwn/geotruth/pkg/natskeys"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamSpatial  = natskeys.StreamName
	StreamGTEvents = natskeys.GTStreamName

	exportSpatial              = "spatial"
	exportGTEventsPublic       = "gt_events_public"
	exportGTEventsRemovedState = "gt_events_removed_state"
)

// Ops is a public operational client for stream statistics and compaction.
type Ops struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// New creates an Ops client using the provided NATS connection.
func New(nc *nats.Conn) (*Ops, error) {
	if nc == nil {
		return nil, fmt.Errorf("nats connection is required")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	return &Ops{nc: nc, js: js}, nil
}

// NewWithJetStream creates an Ops client from an existing JetStream context.
func NewWithJetStream(js jetstream.JetStream) *Ops {
	return &Ops{js: js}
}

type StreamStats struct {
	Name       string  `json:"name"`
	Bytes      uint64  `json:"bytes"`
	Messages   uint64  `json:"messages"`
	FirstSeq   uint64  `json:"first_seq"`
	LastSeq    uint64  `json:"last_seq"`
	MaxBytes   int64   `json:"max_bytes"`
	UsageRatio float64 `json:"usage_ratio"`
}

type ConsumerStats struct {
	Stream         string `json:"stream"`
	Name           string `json:"name"`
	AckFloorSeq    uint64 `json:"ack_floor_seq"`
	NumAckPending  int    `json:"num_ack_pending"`
	NumRedelivered int    `json:"num_redelivered"`
	NumPending     uint64 `json:"num_pending"`
}

type Stats struct {
	Spatial              StreamStats   `json:"spatial"`
	GTEvents             StreamStats   `json:"gt_events"`
	GeoTruthConsumer     ConsumerStats `json:"geotruth_consumer"`
	SafeCompactionCutoff uint64        `json:"safe_compaction_cutoff"`
}

type CompactionOptions struct {
	IncludeSpatial           bool
	IncludePublicGTEvents    bool
	IncludeRemovedTombstones bool
}

type CompactOptions struct {
	ExportDir string
	Execute   bool
}

type SpatialPlan struct {
	Enabled          bool   `json:"enabled"`
	DeleteThroughSeq uint64 `json:"delete_through_seq"`
	FirstSeq         uint64 `json:"first_seq"`
	LastSeq          uint64 `json:"last_seq"`
	Messages         uint64 `json:"messages"`
}

type GTEventRef struct {
	StreamSeq uint64 `json:"stream_seq"`
	Subject   string `json:"subject"`
	SourceSeq uint64 `json:"source_seq"`
	ObjectID  string `json:"object_id,omitempty"`
}

type CompactionPlan struct {
	CutoffSeq         uint64       `json:"cutoff_seq"`
	Spatial           SpatialPlan  `json:"spatial"`
	PublicGTEvents    []GTEventRef `json:"public_gt_events"`
	RemovedTombstones []GTEventRef `json:"removed_tombstones"`
}

type CompactionResult struct {
	CutoffSeq                 uint64 `json:"cutoff_seq"`
	ExportedSpatial           uint64 `json:"exported_spatial"`
	ExportedPublicGTEvents    uint64 `json:"exported_public_gt_events"`
	ExportedRemovedTombstones uint64 `json:"exported_removed_tombstones"`
	DeletedPublicGTEvents     uint64 `json:"deleted_public_gt_events"`
	DeletedRemovedTombstones  uint64 `json:"deleted_removed_tombstones"`
	PurgedSpatial             bool   `json:"purged_spatial"`
}

// Stats returns authoritative stream and GeoTruth durable consumer state.
func (o *Ops) Stats(ctx context.Context) (Stats, error) {
	spatial, err := o.streamStats(ctx, StreamSpatial)
	if err != nil {
		return Stats{}, err
	}
	gt, err := o.streamStats(ctx, StreamGTEvents)
	if err != nil {
		return Stats{}, err
	}
	consumer, err := o.geoTruthConsumerStats(ctx)
	if err != nil {
		return Stats{}, err
	}
	return Stats{
		Spatial:              spatial,
		GTEvents:             gt,
		GeoTruthConsumer:     consumer,
		SafeCompactionCutoff: consumer.AckFloorSeq,
	}, nil
}

// PlanCompaction computes the messages that are safe to compact. The cutoff is
// always the GeoTruth durable consumer's ack floor on the SPATIAL stream.
func (o *Ops) PlanCompaction(ctx context.Context, opts CompactionOptions) (CompactionPlan, error) {
	consumer, err := o.geoTruthConsumerStats(ctx)
	if err != nil {
		return CompactionPlan{}, err
	}
	cutoff := consumer.AckFloorSeq

	plan := CompactionPlan{CutoffSeq: cutoff}

	if opts.IncludeSpatial {
		spatial, err := o.js.Stream(ctx, StreamSpatial)
		if err != nil {
			return CompactionPlan{}, fmt.Errorf("get SPATIAL stream: %w", err)
		}
		info, err := spatial.Info(ctx)
		if err != nil {
			return CompactionPlan{}, fmt.Errorf("SPATIAL info: %w", err)
		}
		last := minUint64(cutoff, info.State.LastSeq)
		plan.Spatial = SpatialPlan{
			Enabled:          cutoff > 0 && info.State.FirstSeq > 0 && info.State.FirstSeq <= last,
			DeleteThroughSeq: cutoff,
			FirstSeq:         info.State.FirstSeq,
			LastSeq:          last,
		}
		if plan.Spatial.Enabled {
			plan.Spatial.Messages = last - info.State.FirstSeq + 1
		}
	}

	if opts.IncludePublicGTEvents || opts.IncludeRemovedTombstones {
		if err := o.planGTEvents(ctx, cutoff, opts, &plan); err != nil {
			return CompactionPlan{}, err
		}
	}

	return plan, nil
}

// Compact exports the planned messages and, when Execute is true, deletes them.
// Export always happens before any destructive operation.
func (o *Ops) Compact(ctx context.Context, plan CompactionPlan, opts CompactOptions) (CompactionResult, error) {
	result := CompactionResult{CutoffSeq: plan.CutoffSeq}
	if opts.Execute && opts.ExportDir == "" && planHasDeletes(plan) {
		return result, fmt.Errorf("export dir is required for destructive compaction")
	}
	if err := o.validatePlan(ctx, plan); err != nil {
		return result, err
	}

	writer, err := newExportWriter(opts.ExportDir)
	if err != nil {
		return result, err
	}

	if err := o.exportPlan(ctx, writer, plan, &result); err != nil {
		_ = writer.Close()
		return result, err
	}
	if err := writer.Close(); err != nil {
		return result, fmt.Errorf("finalize export: %w", err)
	}

	if !opts.Execute {
		return result, nil
	}

	if err := o.deleteGTEvents(ctx, plan, &result); err != nil {
		return result, err
	}
	if plan.Spatial.Enabled {
		if err := o.purgeSpatial(ctx, plan.Spatial.DeleteThroughSeq); err != nil {
			return result, err
		}
		result.PurgedSpatial = true
	}

	return result, nil
}

func planHasDeletes(plan CompactionPlan) bool {
	return plan.Spatial.Enabled || len(plan.PublicGTEvents) > 0 || len(plan.RemovedTombstones) > 0
}

func (o *Ops) streamStats(ctx context.Context, streamName string) (StreamStats, error) {
	stream, err := o.js.Stream(ctx, streamName)
	if err != nil {
		return StreamStats{}, fmt.Errorf("get stream %s: %w", streamName, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return StreamStats{}, fmt.Errorf("stream %s info: %w", streamName, err)
	}
	stats := StreamStats{
		Name:     streamName,
		Bytes:    info.State.Bytes,
		Messages: info.State.Msgs,
		FirstSeq: info.State.FirstSeq,
		LastSeq:  info.State.LastSeq,
		MaxBytes: info.Config.MaxBytes,
	}
	if info.Config.MaxBytes > 0 {
		stats.UsageRatio = float64(info.State.Bytes) / float64(info.Config.MaxBytes)
	}
	return stats, nil
}

func (o *Ops) geoTruthConsumerStats(ctx context.Context) (ConsumerStats, error) {
	consumer, err := o.js.Consumer(ctx, StreamSpatial, internalkeys.DurableGeoTruth)
	if err != nil {
		return ConsumerStats{}, fmt.Errorf("get GeoTruth SPATIAL consumer: %w", err)
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		return ConsumerStats{}, fmt.Errorf("GeoTruth SPATIAL consumer info: %w", err)
	}
	if err := validateGeoTruthConsumerFilters(info.Config); err != nil {
		return ConsumerStats{}, err
	}
	return ConsumerStats{
		Stream:         StreamSpatial,
		Name:           internalkeys.DurableGeoTruth,
		AckFloorSeq:    info.AckFloor.Stream,
		NumAckPending:  info.NumAckPending,
		NumRedelivered: info.NumRedelivered,
		NumPending:     info.NumPending,
	}, nil
}

func (o *Ops) planGTEvents(ctx context.Context, cutoff uint64, opts CompactionOptions, plan *CompactionPlan) error {
	stream, err := o.js.Stream(ctx, StreamGTEvents)
	if err != nil {
		return fmt.Errorf("get GT_EVENTS stream: %w", err)
	}
	if opts.IncludePublicGTEvents {
		refs, err := o.scanPublicGTEvents(ctx, stream, cutoff)
		if err != nil {
			return err
		}
		plan.PublicGTEvents = refs
	}
	if opts.IncludeRemovedTombstones {
		refs, err := o.scanRemovedTombstones(ctx, stream, cutoff)
		if err != nil {
			return err
		}
		plan.RemovedTombstones = refs
	}
	return nil
}

func (o *Ops) scanPublicGTEvents(ctx context.Context, stream jetstream.Stream, cutoff uint64) ([]GTEventRef, error) {
	name := fmt.Sprintf("GTOPS_PUBLIC_SCAN_%d", time.Now().UnixNano())
	cons, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          name,
		FilterSubject: "gt.events.v1.>",
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("create public GT_EVENTS scan consumer: %w", err)
	}
	defer func() {
		_ = stream.DeleteConsumer(ctx, name)
	}()

	var refs []GTEventRef
	for {
		msg, err := cons.Next(jetstream.FetchMaxWait(200 * time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				break
			}
			return nil, fmt.Errorf("next public GT_EVENTS: %w", err)
		}
		sourceSeq, ok := publicSourceSeq(msg.Data())
		if ok && sourceSeq <= cutoff {
			meta, err := msg.Metadata()
			if err != nil {
				_ = msg.Ack()
				return nil, fmt.Errorf("public GT_EVENTS metadata: %w", err)
			}
			refs = append(refs, GTEventRef{
				StreamSeq: meta.Sequence.Stream,
				Subject:   msg.Subject(),
				SourceSeq: sourceSeq,
				ObjectID:  publicObjectID(msg.Data()),
			})
		}
		_ = msg.Ack()
	}
	return refs, nil
}

func (o *Ops) scanRemovedTombstones(ctx context.Context, stream jetstream.Stream, cutoff uint64) ([]GTEventRef, error) {
	name := fmt.Sprintf("GTOPS_STATE_SCAN_%d", time.Now().UnixNano())
	cons, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          name,
		FilterSubject: gtevents.SubjectObjectStateWildcard,
		DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("create state scan consumer: %w", err)
	}
	defer func() {
		_ = stream.DeleteConsumer(ctx, name)
	}()

	var refs []GTEventRef
	for {
		msg, err := cons.Next(jetstream.FetchMaxWait(200 * time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				break
			}
			return nil, fmt.Errorf("next state record: %w", err)
		}
		ref, ok, err := removedTombstoneMsgRef(msg)
		if err != nil {
			_ = msg.Ack()
			return nil, fmt.Errorf("parse state record: %w", err)
		}
		if ok && ref.SourceSeq <= cutoff {
			refs = append(refs, ref)
		}
		_ = msg.Ack()
	}
	return refs, nil
}

func (o *Ops) exportPlan(ctx context.Context, writer *exportWriter, plan CompactionPlan, result *CompactionResult) error {
	if writer == nil {
		return nil
	}
	if plan.Spatial.Enabled {
		spatial, err := o.js.Stream(ctx, StreamSpatial)
		if err != nil {
			return fmt.Errorf("get SPATIAL stream: %w", err)
		}
		for seq := plan.Spatial.FirstSeq; seq <= plan.Spatial.LastSeq; seq++ {
			msg, err := spatial.GetMsg(ctx, seq)
			if err != nil {
				if errors.Is(err, jetstream.ErrMsgNotFound) {
					continue
				}
				return fmt.Errorf("export SPATIAL seq %d: %w", seq, err)
			}
			if err := writer.Write(exportSpatial, msg); err != nil {
				return err
			}
			result.ExportedSpatial++
			if seq == math.MaxUint64 {
				break
			}
		}
	}

	gt, err := o.js.Stream(ctx, StreamGTEvents)
	if err != nil && (len(plan.PublicGTEvents) > 0 || len(plan.RemovedTombstones) > 0) {
		return fmt.Errorf("get GT_EVENTS stream: %w", err)
	}
	for _, ref := range plan.PublicGTEvents {
		msg, err := gt.GetMsg(ctx, ref.StreamSeq)
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				continue
			}
			return fmt.Errorf("export GT_EVENTS public seq %d: %w", ref.StreamSeq, err)
		}
		if err := writer.Write(exportGTEventsPublic, msg); err != nil {
			return err
		}
		result.ExportedPublicGTEvents++
	}
	for _, ref := range plan.RemovedTombstones {
		msg, err := gt.GetMsg(ctx, ref.StreamSeq)
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				continue
			}
			return fmt.Errorf("export GT_EVENTS removed state seq %d: %w", ref.StreamSeq, err)
		}
		if err := writer.Write(exportGTEventsRemovedState, msg); err != nil {
			return err
		}
		result.ExportedRemovedTombstones++
	}
	return nil
}

func (o *Ops) deleteGTEvents(ctx context.Context, plan CompactionPlan, result *CompactionResult) error {
	gt, err := o.js.Stream(ctx, StreamGTEvents)
	if err != nil && (len(plan.PublicGTEvents) > 0 || len(plan.RemovedTombstones) > 0) {
		return fmt.Errorf("get GT_EVENTS stream: %w", err)
	}
	for _, ref := range plan.PublicGTEvents {
		if err := gt.DeleteMsg(ctx, ref.StreamSeq); err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
			return fmt.Errorf("delete GT_EVENTS public seq %d: %w", ref.StreamSeq, err)
		}
		result.DeletedPublicGTEvents++
	}
	for _, ref := range plan.RemovedTombstones {
		if err := gt.DeleteMsg(ctx, ref.StreamSeq); err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
			return fmt.Errorf("delete GT_EVENTS removed state seq %d: %w", ref.StreamSeq, err)
		}
		result.DeletedRemovedTombstones++
	}
	return nil
}

func (o *Ops) validatePlan(ctx context.Context, plan CompactionPlan) error {
	consumer, err := o.geoTruthConsumerStats(ctx)
	if err != nil {
		return err
	}
	if plan.CutoffSeq > consumer.AckFloorSeq {
		return fmt.Errorf("compaction cutoff %d is above current SPATIAL ack floor %d", plan.CutoffSeq, consumer.AckFloorSeq)
	}
	if plan.Spatial.Enabled && plan.Spatial.DeleteThroughSeq > consumer.AckFloorSeq {
		return fmt.Errorf("SPATIAL purge seq %d is above current SPATIAL ack floor %d", plan.Spatial.DeleteThroughSeq, consumer.AckFloorSeq)
	}

	gt, err := o.js.Stream(ctx, StreamGTEvents)
	if err != nil && (len(plan.PublicGTEvents) > 0 || len(plan.RemovedTombstones) > 0) {
		return fmt.Errorf("get GT_EVENTS stream: %w", err)
	}
	for _, ref := range plan.PublicGTEvents {
		msg, err := gt.GetMsg(ctx, ref.StreamSeq)
		if err != nil {
			return fmt.Errorf("validate GT_EVENTS public seq %d: %w", ref.StreamSeq, err)
		}
		if !isPublicGTEvent(msg.Subject) {
			return fmt.Errorf("GT_EVENTS seq %d subject %q is not public history", ref.StreamSeq, msg.Subject)
		}
		sourceSeq, ok := publicSourceSeq(msg.Data)
		if !ok {
			return fmt.Errorf("GT_EVENTS public seq %d has no source_seq", ref.StreamSeq)
		}
		if sourceSeq != ref.SourceSeq {
			return fmt.Errorf("GT_EVENTS public seq %d source_seq changed from %d to %d", ref.StreamSeq, ref.SourceSeq, sourceSeq)
		}
		if sourceSeq > consumer.AckFloorSeq {
			return fmt.Errorf("GT_EVENTS public seq %d source_seq %d is above current SPATIAL ack floor %d", ref.StreamSeq, sourceSeq, consumer.AckFloorSeq)
		}
	}
	for _, ref := range plan.RemovedTombstones {
		msg, err := gt.GetMsg(ctx, ref.StreamSeq)
		if err != nil {
			return fmt.Errorf("validate removed tombstone seq %d: %w", ref.StreamSeq, err)
		}
		parsed, ok, err := removedTombstoneRef(msg)
		if err != nil {
			return fmt.Errorf("validate removed tombstone seq %d: %w", ref.StreamSeq, err)
		}
		if !ok {
			return fmt.Errorf("GT_EVENTS seq %d is not a removed tombstone", ref.StreamSeq)
		}
		if parsed.SourceSeq != ref.SourceSeq {
			return fmt.Errorf("removed tombstone seq %d source_seq changed from %d to %d", ref.StreamSeq, ref.SourceSeq, parsed.SourceSeq)
		}
		if parsed.SourceSeq > consumer.AckFloorSeq {
			return fmt.Errorf("removed tombstone seq %d source_seq %d is above current SPATIAL ack floor %d", ref.StreamSeq, parsed.SourceSeq, consumer.AckFloorSeq)
		}
	}
	return nil
}

func (o *Ops) purgeSpatial(ctx context.Context, cutoff uint64) error {
	if cutoff == 0 {
		return nil
	}
	if cutoff == math.MaxUint64 {
		return fmt.Errorf("cutoff too large")
	}
	spatial, err := o.js.Stream(ctx, StreamSpatial)
	if err != nil {
		return fmt.Errorf("get SPATIAL stream: %w", err)
	}
	if err := spatial.Purge(ctx, jetstream.WithPurgeSequence(cutoff+1)); err != nil {
		return fmt.Errorf("purge SPATIAL through seq %d: %w", cutoff, err)
	}
	return nil
}

func isPublicGTEvent(subject string) bool {
	return strings.HasPrefix(subject, "gt.events.v1.")
}

func publicSourceSeq(data []byte) (uint64, bool) {
	var event struct {
		SourceSeq uint64 `json:"source_seq"`
	}
	if err := json.Unmarshal(data, &event); err != nil || event.SourceSeq == 0 {
		return 0, false
	}
	return event.SourceSeq, true
}

func publicObjectID(data []byte) string {
	var event struct {
		ObjectID string `json:"object_id"`
	}
	_ = json.Unmarshal(data, &event)
	return event.ObjectID
}

func removedTombstoneRef(msg *jetstream.RawStreamMsg) (GTEventRef, bool, error) {
	var state gtevents.ObjectStateRecord
	if err := json.Unmarshal(msg.Data, &state); err != nil {
		return GTEventRef{}, false, err
	}
	if state.Lifecycle != gtevents.LifecycleRemoved {
		return GTEventRef{}, false, nil
	}
	return GTEventRef{
		StreamSeq: msg.Sequence,
		Subject:   msg.Subject,
		SourceSeq: state.DetectorStateSeq,
		ObjectID:  state.ObjectID,
	}, true, nil
}

func removedTombstoneMsgRef(msg jetstream.Msg) (GTEventRef, bool, error) {
	var state gtevents.ObjectStateRecord
	if err := json.Unmarshal(msg.Data(), &state); err != nil {
		return GTEventRef{}, false, err
	}
	if state.Lifecycle != gtevents.LifecycleRemoved {
		return GTEventRef{}, false, nil
	}
	meta, err := msg.Metadata()
	if err != nil {
		return GTEventRef{}, false, err
	}
	return GTEventRef{
		StreamSeq: meta.Sequence.Stream,
		Subject:   msg.Subject(),
		SourceSeq: state.DetectorStateSeq,
		ObjectID:  state.ObjectID,
	}, true, nil
}

func validateGeoTruthConsumerFilters(cfg jetstream.ConsumerConfig) error {
	expected := map[string]bool{}
	for _, subject := range internalkeys.StreamSubjects {
		expected[subject] = false
	}
	if cfg.FilterSubject != "" {
		if _, ok := expected[cfg.FilterSubject]; ok {
			expected[cfg.FilterSubject] = true
		}
	}
	for _, subject := range cfg.FilterSubjects {
		if _, ok := expected[subject]; ok {
			expected[subject] = true
		}
	}
	var missing []string
	for subject, present := range expected {
		if !present {
			missing = append(missing, subject)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("GeoTruth SPATIAL consumer filters missing %s", strings.Join(missing, ", "))
	}
	return nil
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
