package engine

import (
	"github.com/midtxwn/geotruth/internal/geo"
	"github.com/midtxwn/geotruth/internal/gtevents"
	floorrtree "github.com/midtxwn/geotruth/internal/rtree"
)

type TransitionExec struct {
	eng   *Engine
	inbox chan WorkerTask
	pubCh chan<- WorkerResult
}

func NewTransitionExec(eng *Engine, pubCh chan<- WorkerResult) *TransitionExec {
	return &TransitionExec{
		eng:   eng,
		inbox: make(chan WorkerTask, 64),
		pubCh: pubCh,
	}
}

func (x *TransitionExec) Run() {
	for task := range x.inbox {
		x.processTransition(task)
	}
}

// processTransition moves an object from one region to another. Exit
// transitions are computed from the old region's prevInside; enter transitions
// are computed in the new region. prevInside mutations are deferred to the
// dispatcher so commit failure/redelivery preserves transition correctness.
func (x *TransitionExec) processTransition(t WorkerTask) {
	ctl, ok := x.eng.lookupCtl(t.ID)
	if !ok {
		t.Msg.Nak()
		x.pubCh <- WorkerResult{ObjectID: t.ID, StreamSeq: t.StreamSeq, ClientOpID: t.ClientOpID, NewRegion: t.Region, Outcome: outcomeNacked}
		return
	}

	lo, hi := t.OldRegion, t.Region
	if lo > hi {
		lo, hi = hi, lo
	}

	oldRS := x.eng.regions[t.OldRegion]
	newRS := x.eng.regions[t.Region]

	ctl.pubMu.Lock()
	x.eng.regions[lo].mu.Lock()
	x.eng.regions[hi].mu.Lock()

	// prevInside is not deleted here; the dispatcher applies that mutation only
	// after the GT_EVENTS state record is confirmed durable.
	var exitTransitions []gtevents.GeofenceTransition
	if prev := oldRS.prevInside[t.ID]; prev != nil {
		for areaID := range prev {
			exitTransitions = append(exitTransitions, gtevents.GeofenceTransition{
				AreaID:  areaID,
				Entered: false,
			})
		}
	}
	oldRS.tree.Delete(t.ID)

	obb := geo.NewOBB(t.X, t.Y, t.Dims.Width/2, t.Dims.Height/2, t.RotY)
	item := &floorrtree.ObjectItem{
		ID: t.ID, OBB: obb,
		Dims:  [2]float64{t.Dims.Width, t.Dims.Height},
		LastZ: t.Z, X: t.X, Y: t.Y, RotY: t.RotY,
	}
	newRS.tree.Upsert(item)
	gfResult := collectGeofenceResultLocked(newRS, item)

	ctl.pubRegion = t.Region
	ctl.hasPubRegion = true

	x.eng.regions[hi].mu.Unlock()
	x.eng.regions[lo].mu.Unlock()
	ctl.pubMu.Unlock()

	// Publish exits before enters so consumers see a stable leave-then-enter
	// order for cross-region movement.
	var allTransitions []gtevents.GeofenceTransition
	allTransitions = append(allTransitions, exitTransitions...)
	allTransitions = append(allTransitions, gfResult.Transitions...)

	x.pubCh <- WorkerResult{
		ObjectID:          t.ID,
		StreamSeq:         t.StreamSeq,
		ClientOpID:        t.ClientOpID,
		NewRegion:         t.Region,
		Outcome:           outcomeReady,
		PostX:             t.X,
		PostY:             t.Y,
		PostZ:             t.Z,
		PostRotY:          t.RotY,
		PostDims:          t.Dims,
		PostOldRegion:     t.OldRegion,
		PostInsideAreaIDs: gfResult.InsideAreaIDs,
		PostCurrentInside: gfResult.CurrentInside,
		Transitions:       allTransitions,
	}
}
