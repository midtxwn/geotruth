package engine

import (
	"sync"

	"github.com/midtxwn/geotruth/internal/gtevents"
	"github.com/midtxwn/geotruth/pkg/domain"
)

type ObjCtl struct {
	// routeRegion is dispatcher-private routing state. pubRegion below is the
	// query-visible committed projection protected by pubMu; the two can skew
	// while a transition commit is in flight.
	routeRegion    string
	hasRouteRegion bool
	head           *Envelope
	queue          []*Envelope
	lastAppliedSeq uint64
	pendingSeqs    map[uint64]struct{}

	dims domain.ObjectDimensions

	committing       bool
	commitEnvelope   *gtevents.CommitEnvelope
	detectorStateSeq uint64 // last seq with a durable state record in GT_EVENTS

	pubMu        sync.RWMutex
	pubRegion    string
	hasPubRegion bool
	deleted      bool
}

func newObjCtl(dims domain.ObjectDimensions) *ObjCtl {
	return &ObjCtl{
		dims:        dims,
		pendingSeqs: make(map[uint64]struct{}),
	}
}
