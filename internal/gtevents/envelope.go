package gtevents

import (
	"github.com/nats-io/nats.go"
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
// written to GT_EVENTS for one processed object command.
//
// Once created, a CommitEnvelope must not be mutated. The commit object event
// is published first and acts as the durable checkpoint. Projection publishes
// may be retried or repaired from the commit event after restart.
type CommitEnvelope struct {
	ObjectID   string
	InstanceID string
	CommitSeq  uint64
	Reply      string

	Commit      *nats.Msg
	Projections []*nats.Msg

	// Mutation is the deferred prevInside write, applied by the dispatcher
	// in onCommitResult after commit success. The publisher does NOT touch
	// engine state.
	Mutation PrevInsideMutation
}
