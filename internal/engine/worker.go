package engine

import (
	"log"

	"github.com/midtxwn/geotruth/internal/geo"
	"github.com/midtxwn/geotruth/internal/gtevents"
	floorrtree "github.com/midtxwn/geotruth/internal/rtree"
)

type RegionWorker struct {
	region string
	eng    *Engine
	inbox  chan WorkerTask
	pubCh  chan<- WorkerResult
}

func NewRegionWorker(region string, eng *Engine, pubCh chan<- WorkerResult) *RegionWorker {
	return &RegionWorker{
		region: region,
		eng:    eng,
		inbox:  make(chan WorkerTask, 256),
		pubCh:  pubCh,
	}
}

func (w *RegionWorker) Run() {
	for task := range w.inbox {
		switch task.Kind {
		case taskInit:
			w.processInit(task)
		case taskUpdate:
			w.processUpdate(task)
		case taskRemove:
			w.processRemove(task)
		}
	}
}

func (w *RegionWorker) processInit(t WorkerTask) {
	ctl, ok := w.eng.lookupCtl(t.ID)
	if !ok {
		w.pubCh <- WorkerResult{ObjectID: t.ID, ClientOpID: t.ClientOpID, NewRegion: t.Region, Outcome: outcomeRejected}
		return
	}

	obb := geo.NewOBB(t.X, t.Y, t.Dims.Width/2, t.Dims.Height/2, t.RotY)
	item := &floorrtree.ObjectItem{
		ID: t.ID, OBB: obb,
		Dims:  [2]float64{t.Dims.Width, t.Dims.Height},
		LastZ: t.Z, X: t.X, Y: t.Y, RotY: t.RotY,
	}

	ctl.pubMu.Lock()
	rs := w.eng.regions[t.Region]
	rs.mu.Lock()
	rs.tree.Upsert(item)
	gfResult := collectGeofenceResultLocked(rs, item)
	ctl.pubRegion = t.Region
	ctl.hasPubRegion = true
	rs.mu.Unlock()
	ctl.pubMu.Unlock()

	w.pubCh <- WorkerResult{
		ObjectID:            t.ID,
		ClientOpID:          t.ClientOpID,
		NewRegion:           t.Region,
		Outcome:             outcomeReady,
		PostX:               t.X,
		PostY:               t.Y,
		PostZ:               t.Z,
		PostRotY:            t.RotY,
		PostDims:            t.Dims,
		PostInsideAreaIDs:   gfResult.InsideAreaIDs,
		PostCurrentInside:   gfResult.CurrentInside,
		GeofenceTransitions: gfResult.GeofenceTransitions,
	}
}

func (w *RegionWorker) processUpdate(t WorkerTask) {
	ctl, ok := w.eng.lookupCtl(t.ID)
	if !ok || ctl.deleted {
		w.pubCh <- WorkerResult{ObjectID: t.ID, ClientOpID: t.ClientOpID, NewRegion: t.Region, Outcome: outcomeRejected}
		return
	}

	ctl.pubMu.RLock()
	if !ctl.hasPubRegion || ctl.pubRegion != t.Region {
		ctl.pubMu.RUnlock()
		log.Printf("[worker:%s] invariant: pubRegion=%q has=%v != task.Region=%q for %s",
			w.region, ctl.pubRegion, ctl.hasPubRegion, t.Region, t.ID)
		w.pubCh <- WorkerResult{ObjectID: t.ID, ClientOpID: t.ClientOpID, NewRegion: t.Region, Outcome: outcomeRejected}
		return
	}

	obb := geo.NewOBB(t.X, t.Y, t.Dims.Width/2, t.Dims.Height/2, t.RotY)
	item := &floorrtree.ObjectItem{
		ID: t.ID, OBB: obb,
		Dims:  [2]float64{t.Dims.Width, t.Dims.Height},
		LastZ: t.Z, X: t.X, Y: t.Y, RotY: t.RotY,
	}

	rs := w.eng.regions[t.Region]
	rs.mu.Lock()
	rs.tree.Upsert(item)
	gfResult := collectGeofenceResultLocked(rs, item)
	rs.mu.Unlock()
	ctl.pubMu.RUnlock()

	w.pubCh <- WorkerResult{
		ObjectID:            t.ID,
		ClientOpID:          t.ClientOpID,
		NewRegion:           t.Region,
		Outcome:             outcomeReady,
		PostX:               t.X,
		PostY:               t.Y,
		PostZ:               t.Z,
		PostRotY:            t.RotY,
		PostDims:            t.Dims,
		PostInsideAreaIDs:   gfResult.InsideAreaIDs,
		PostCurrentInside:   gfResult.CurrentInside,
		GeofenceTransitions: gfResult.GeofenceTransitions,
	}
}

// processRemove removes the object from the R-tree and marks it deleted.
// prevInside deletion is deferred to the dispatcher's post-commit
// PrevInsideMutation. If commit publish has to retry, prevInside remains intact
// and the remove keeps the same exit transitions.
func (w *RegionWorker) processRemove(t WorkerTask) {
	ctl, ok := w.eng.lookupCtl(t.ID)
	if !ok {
		w.pubCh <- WorkerResult{
			ObjectID:   t.ID,
			ClientOpID: t.ClientOpID,
			NewRegion:  t.Region,
			Outcome:    outcomeReady,
			PostDims:   t.Dims,
		}
		return
	}

	ctl.pubMu.Lock()
	rs := w.eng.regions[t.Region]
	rs.mu.Lock()

	var geofenceTransitions []gtevents.GeofenceTransition
	var insideIDs []string
	var postX, postY, postZ, postRotY float64
	var hadPosition bool
	if item := rs.tree.Get(t.ID); item != nil {
		postX = item.X
		postY = item.Y
		postZ = item.LastZ
		postRotY = item.RotY
		hadPosition = true
	}

	// Generate exit transitions from prevInside, but leave the map untouched
	// until the commit stage confirms durability.
	if prev := rs.prevInside[t.ID]; prev != nil {
		for areaID := range prev {
			geofenceTransitions = append(geofenceTransitions, gtevents.GeofenceTransition{
				AreaID:  areaID,
				Entered: false,
			})
		}
	}
	rs.tree.Delete(t.ID)
	ctl.hasPubRegion = false
	ctl.deleted = true

	rs.mu.Unlock()
	ctl.pubMu.Unlock()

	w.pubCh <- WorkerResult{
		ObjectID:            t.ID,
		ClientOpID:          t.ClientOpID,
		NewRegion:           t.Region,
		Outcome:             outcomeReady,
		PostOldRegion:       t.Region,
		PostX:               postX,
		PostY:               postY,
		PostZ:               postZ,
		PostRotY:            postRotY,
		PostDims:            ctl.dims,
		PostInsideAreaIDs:   insideIDs,
		GeofenceTransitions: geofenceTransitions,
		PostHadPosition:     hadPosition,
	}
}
