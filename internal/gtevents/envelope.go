package gtevents

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// PrevInsideMutationKind classifies how the dispatcher should update
// RegionState.prevInside after a commit succeeds. prevInside is the only
// non-idempotent mutation in the worker path; R-tree updates, pubRegion,
// hasPubRegion, and deleted are idempotent and can safely remain in workers.
// Deferring prevInside to post-commit makes redelivery safe: on retry,
// transitions are re-detected against the old committed prevInside snapshot.
type PrevInsideMutationKind int

const (
	PrevInsideNoop   PrevInsideMutationKind = iota // registration without position
	PrevInsideSet                                  // same-region update or init
	PrevInsideMove                                 // region transition
	PrevInsideDelete                               // object removal
)

// PrevInsideMutation carries the deferred prevInside write that the
// dispatcher applies in onCommitResult AFTER the commit envelope is
// confirmed published, BEFORE releasing the object mailbox.
type PrevInsideMutation struct {
	Kind          PrevInsideMutationKind
	ObjectID      string
	NewRegion     string          // Set/Move: target region
	OldRegion     string          // Move/Delete: source region
	CurrentInside map[string]bool // Set/Move: inside areas on new region (cloned)
}

// CommitEnvelope is an immutable description of everything that needs to be
// written to GT_EVENTS for one SPATIAL source message, together with a
// reference to the source message for ack.
//
// Once created, a CommitEnvelope must not be mutated. If any individual
// publish fails, the same envelope is retried from the start; Nats-Msg-Id
// dedup on each message ensures already-published messages are safely skipped
// by the server on retry.
//
// Message ordering within Messages is load-bearing:
// geofence events -> position/lifecycle event -> state record.
// The state record (last) is the commit marker.
type CommitEnvelope struct {
	ObjectID  string
	SourceSeq uint64

	// Messages are the ordered nats.Msg slice for individual publish.
	// Order matters: geofence events -> position/lifecycle event -> state record.
	Messages []*nats.Msg

	// Mutation is the deferred prevInside write, applied by the dispatcher
	// in onCommitResult after commit success. The publisher does NOT touch
	// engine state.
	Mutation PrevInsideMutation

	// SourceMsg is the SPATIAL JetStream message. The commit stage acks
	// it only after all messages are confirmed published.
	SourceMsg jetstream.Msg
}
