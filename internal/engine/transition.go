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

// processTransition moves an object from one region to another. Geofence exits
// are computed from the old region's prevInside; enters are computed in the new
// region. prevInside mutations are deferred to the dispatcher so commit
// failure/redelivery preserves geofence correctness.
func (x *TransitionExec) processTransition(t WorkerTask) {
	ctl, ok := x.eng.lookupCtl(t.ID)
	if !ok {
		x.pubCh <- WorkerResult{ObjectID: t.ID, ClientOpID: t.ClientOpID, NewRegion: t.Region, Outcome: outcomeRejected}
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
	// after the GT_EVENTS public checkpoint is confirmed durable.
	var exitGeofenceTransitions []gtevents.GeofenceTransition
	if prev := oldRS.prevInside[t.ID]; prev != nil {
		for areaID := range prev {
			exitGeofenceTransitions = append(exitGeofenceTransitions, gtevents.GeofenceTransition{
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
	var geofenceTransitions []gtevents.GeofenceTransition
	geofenceTransitions = append(geofenceTransitions, exitGeofenceTransitions...)
	geofenceTransitions = append(geofenceTransitions, gfResult.GeofenceTransitions...)

	x.pubCh <- WorkerResult{
		ObjectID:            t.ID,
		ClientOpID:          t.ClientOpID,
		NewRegion:           t.Region,
		Outcome:             outcomeReady,
		PostX:               t.X,
		PostY:               t.Y,
		PostZ:               t.Z,
		PostRotY:            t.RotY,
		PostDims:            t.Dims,
		PostOldRegion:       t.OldRegion,
		PostInsideAreaIDs:   gfResult.InsideAreaIDs,
		PostCurrentInside:   gfResult.CurrentInside,
		GeofenceTransitions: geofenceTransitions,
	}
}
