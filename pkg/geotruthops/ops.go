// Package geotruthops provides operational helpers for inspecting and
// compacting the GT_EVENTS stream used by GeoTruth.
package geotruthops

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/midtxwn/geotruth/internal/gtevents"
	"github.com/midtxwn/geotruth/pkg/natskeys"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamGTEvents = natskeys.GTStreamName

	exportGTEvents = "gt_events"
)

// Ops is a public operational client for GT_EVENTS stream statistics and
// compaction.
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

type Stats struct {
	GTEvents StreamStats `json:"gt_events"`
}

type CompactionOptions struct {
	IncludePublicGTEvents    bool
	IncludeRemovedTombstones bool
}

type CompactOptions struct {
	ExportDir string
	Execute   bool
}

type GTEventRef struct {
	StreamSeq uint64 `json:"stream_seq"`
	Subject   string `json:"subject"`
	EventID   string `json:"event_id,omitempty"`
	ObjectID  string `json:"object_id,omitempty"`
	Kind      string `json:"kind"`
}

type CompactionPlan struct {
	CutoffSeq      uint64       `json:"cutoff_seq"`
	DeleteGTEvents []GTEventRef `json:"delete_gt_events"`
}

type CompactionResult struct {
	CutoffSeq        uint64 `json:"cutoff_seq"`
	ExportedGTEvents uint64 `json:"exported_gt_events"`
	DeletedGTEvents  uint64 `json:"deleted_gt_events"`
}

// Stats returns authoritative GT_EVENTS stream state.
func (o *Ops) Stats(ctx context.Context) (Stats, error) {
	gt, err := o.streamStats(ctx, StreamGTEvents)
	if err != nil {
		return Stats{}, err
	}
	return Stats{GTEvents: gt}, nil
}

func (o *Ops) streamStats(ctx context.Context, name string) (StreamStats, error) {
	stream, err := o.js.Stream(ctx, name)
	if err != nil {
		return StreamStats{}, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return StreamStats{}, err
	}
	ratio := 0.0
	if info.Config.MaxBytes > 0 {
		ratio = float64(info.State.Bytes) / float64(info.Config.MaxBytes)
		if math.IsInf(ratio, 0) || math.IsNaN(ratio) {
			ratio = 0
		}
	}
	return StreamStats{
		Name:       name,
		Bytes:      info.State.Bytes,
		Messages:   info.State.Msgs,
		FirstSeq:   info.State.FirstSeq,
		LastSeq:    info.State.LastSeq,
		MaxBytes:   info.Config.MaxBytes,
		UsageRatio: ratio,
	}, nil
}

// PlanCompaction creates a GT_EVENTS-only deletion plan. Latest active object
// commits and the projection event IDs referenced by those commits are always
// protected. For objects whose latest commit is removed, the entire object
// history can be deleted together because no surviving older commit remains to
// resurrect on the next boot.
func (o *Ops) PlanCompaction(ctx context.Context, opts CompactionOptions) (CompactionPlan, error) {
	stream, err := o.js.Stream(ctx, StreamGTEvents)
	if err != nil {
		return CompactionPlan{}, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return CompactionPlan{}, err
	}

	records, err := scanGTEvents(ctx, stream, info.State.FirstSeq, info.State.LastSeq)
	if err != nil {
		return CompactionPlan{}, err
	}

	latest := make(map[string]eventRecord)
	byObject := make(map[string][]eventRecord)
	projections := make(map[string][]eventRecord)
	for _, rec := range records {
		if rec.ObjectID == "" {
			continue
		}
		if rec.IsCommit {
			byObject[rec.ObjectID] = append(byObject[rec.ObjectID], rec)
			if rec.StreamSeq >= latest[rec.ObjectID].StreamSeq {
				latest[rec.ObjectID] = rec
			}
			continue
		}
		if rec.IsProjection {
			projections[rec.ObjectID] = append(projections[rec.ObjectID], rec)
		}
	}

	protected := make(map[uint64]struct{})
	protectedProjectionID := make(map[string]struct{})
	for objectID, rec := range latest {
		if rec.Lifecycle != gtevents.LifecycleActive {
			continue
		}
		protected[rec.StreamSeq] = struct{}{}
		for _, tr := range rec.GeofenceTransitions {
			if tr.Entered {
				protectedProjectionID[geofenceEnterEventID(tr.AreaID, objectID, rec.InstanceID, rec.CommitSeq)] = struct{}{}
			} else {
				protectedProjectionID[geofenceExitEventID(tr.AreaID, objectID, rec.InstanceID, rec.CommitSeq)] = struct{}{}
			}
		}
	}

	var deletes []GTEventRef
	if opts.IncludePublicGTEvents {
		for objectID, commits := range byObject {
			latestRec := latest[objectID]
			for _, rec := range commits {
				if _, ok := protected[rec.StreamSeq]; ok {
					continue
				}
				if latestRec.Lifecycle == gtevents.LifecycleActive && rec.StreamSeq == latestRec.StreamSeq {
					continue
				}
				if latestRec.Lifecycle == gtevents.LifecycleRemoved && !opts.IncludeRemovedTombstones && rec.StreamSeq == latestRec.StreamSeq {
					continue
				}
				deletes = append(deletes, rec.ref("commit"))
			}
		}
	}

	for objectID, recs := range projections {
		latestRec := latest[objectID]
		for _, rec := range recs {
			if latestRec.Lifecycle == gtevents.LifecycleActive {
				if _, ok := protectedProjectionID[rec.EventID]; ok {
					continue
				}
			}
			deletes = append(deletes, rec.ref("projection"))
		}
	}

	return CompactionPlan{CutoffSeq: info.State.LastSeq, DeleteGTEvents: deletes}, nil
}

func (o *Ops) Compact(ctx context.Context, plan CompactionPlan, opts CompactOptions) (CompactionResult, error) {
	if !opts.Execute {
		return CompactionResult{CutoffSeq: plan.CutoffSeq}, fmt.Errorf("compact called without Execute=true")
	}
	if err := o.validatePlan(ctx, plan); err != nil {
		return CompactionResult{}, err
	}

	stream, err := o.js.Stream(ctx, StreamGTEvents)
	if err != nil {
		return CompactionResult{}, err
	}
	exporter, err := newExportWriter(opts.ExportDir)
	if err != nil {
		return CompactionResult{}, err
	}
	defer exporter.Close()

	result := CompactionResult{CutoffSeq: plan.CutoffSeq}
	for _, ref := range plan.DeleteGTEvents {
		msg, err := stream.GetMsg(ctx, ref.StreamSeq)
		if err != nil {
			return result, fmt.Errorf("fetch GT_EVENTS seq %d: %w", ref.StreamSeq, err)
		}
		if err := exporter.Write(exportGTEvents, msg); err != nil {
			return result, err
		}
		result.ExportedGTEvents++
		if err := stream.DeleteMsg(ctx, ref.StreamSeq); err != nil {
			return result, fmt.Errorf("delete GT_EVENTS seq %d: %w", ref.StreamSeq, err)
		}
		result.DeletedGTEvents++
	}
	return result, nil
}

func (o *Ops) validatePlan(ctx context.Context, plan CompactionPlan) error {
	fresh, err := o.PlanCompaction(ctx, CompactionOptions{IncludePublicGTEvents: true, IncludeRemovedTombstones: true})
	if err != nil {
		return err
	}
	allowed := make(map[uint64]struct{}, len(fresh.DeleteGTEvents))
	for _, ref := range fresh.DeleteGTEvents {
		allowed[ref.StreamSeq] = struct{}{}
	}
	for _, ref := range plan.DeleteGTEvents {
		if _, ok := allowed[ref.StreamSeq]; !ok {
			return fmt.Errorf("compaction plan is stale or unsafe: GT_EVENTS seq %d is no longer eligible for deletion; regenerate the plan", ref.StreamSeq)
		}
	}
	return nil
}

type eventRecord struct {
	StreamSeq           uint64
	Subject             string
	EventID             string
	EventType           string
	ObjectID            string
	InstanceID          string
	CommitSeq           uint64
	Lifecycle           gtevents.Lifecycle
	GeofenceTransitions []gtevents.GeofenceTransitionRecord
	IsCommit            bool
	IsProjection        bool
}

func (r eventRecord) ref(kind string) GTEventRef {
	return GTEventRef{
		StreamSeq: r.StreamSeq,
		Subject:   r.Subject,
		EventID:   r.EventID,
		ObjectID:  r.ObjectID,
		Kind:      kind,
	}
}

func scanGTEvents(ctx context.Context, stream jetstream.Stream, first, last uint64) ([]eventRecord, error) {
	var out []eventRecord
	for seq := first; seq <= last && seq != 0; seq++ {
		msg, err := stream.GetMsg(ctx, seq)
		if err != nil {
			continue
		}
		rec, ok, err := parseEventRecord(msg)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

func parseEventRecord(msg *jetstream.RawStreamMsg) (eventRecord, bool, error) {
	var meta struct {
		EventID    string `json:"e"`
		EventType  string `json:"k"`
		ObjectID   string `json:"o"`
		InstanceID string `json:"i"`
		CommitSeq  uint64 `json:"s"`
	}
	if err := json.Unmarshal(msg.Data, &meta); err != nil {
		return eventRecord{}, false, fmt.Errorf("decode GT_EVENTS seq %d subject %s: %w", msg.Sequence, msg.Subject, err)
	}
	rec := eventRecord{
		StreamSeq:  msg.Sequence,
		Subject:    msg.Subject,
		EventID:    meta.EventID,
		EventType:  meta.EventType,
		ObjectID:   meta.ObjectID,
		InstanceID: meta.InstanceID,
		CommitSeq:  meta.CommitSeq,
	}
	state, hasCheckpoint, err := gtevents.CheckpointStateFromPublicEvent(msg.Data)
	if err != nil {
		return eventRecord{}, false, fmt.Errorf("decode GT_EVENTS checkpoint seq %d subject %s: %w", msg.Sequence, msg.Subject, err)
	}
	if hasCheckpoint {
		rec.IsCommit = true
		rec.Lifecycle = state.Lifecycle
		rec.GeofenceTransitions = state.GeofenceTransitions
		if rec.ObjectID == "" {
			rec.ObjectID = state.ObjectID
		}
		if rec.InstanceID == "" {
			rec.InstanceID = state.InstanceID
		}
		if rec.CommitSeq == 0 {
			rec.CommitSeq = state.CommitSeq
		}
	}
	if strings.Contains(msg.Subject, ".geofence.") {
		rec.IsProjection = true
	}
	return rec, rec.ObjectID != "", nil
}

func geofenceEnterEventID(areaID, objectID, instanceID string, commitSeq uint64) string {
	return "gt:gf:e:" + areaID + ":" + objectID + ":" + instanceID + ":" + formatCommitSeq(commitSeq)
}

func geofenceExitEventID(areaID, objectID, instanceID string, commitSeq uint64) string {
	return "gt:gf:x:" + areaID + ":" + objectID + ":" + instanceID + ":" + formatCommitSeq(commitSeq)
}

func formatCommitSeq(seq uint64) string {
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"
	if seq == 0 {
		return "0"
	}
	var buf [13]byte
	i := len(buf)
	for seq > 0 {
		i--
		buf[i] = digits[seq%36]
		seq /= 36
	}
	return string(buf[i:])
}
