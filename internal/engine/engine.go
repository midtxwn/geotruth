package engine

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	"github.com/midtxwn/geotruth/internal/geo"
	"github.com/midtxwn/geotruth/internal/gtevents"
	floorrtree "github.com/midtxwn/geotruth/internal/rtree"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/natsquery"
	"github.com/midtxwn/geotruth/pkg/regionresolver"
)

type RegionState struct {
	mu         sync.RWMutex
	tree       floorrtree.ObjectTree
	areaTree   floorrtree.AreaTree
	prevInside map[string]map[string]bool
}

type Engine struct {
	ctlMu    sync.RWMutex
	ctls     map[string]*ObjCtl
	regions  map[string]*RegionState
	resolver regionresolver.Resolver
}

func NewEngine(resolver regionresolver.Resolver) *Engine {
	regions := resolver.KnownRegions()
	if err := validateKnownRegions(regions); err != nil {
		panic(err.Error())
	}

	regionMap := make(map[string]*RegionState, len(regions))
	for _, r := range regions {
		regionMap[r] = &RegionState{
			prevInside: make(map[string]map[string]bool),
		}
	}
	return &Engine{
		ctls:     make(map[string]*ObjCtl),
		regions:  regionMap,
		resolver: resolver,
	}
}

func validateKnownRegions(regions []string) error {
	if len(regions) == 0 {
		return fmt.Errorf("engine: resolver KnownRegions() returned empty list")
	}
	seen := make(map[string]bool, len(regions))
	for _, r := range regions {
		if r == "" {
			return fmt.Errorf("engine: resolver returned empty region ID")
		}
		if seen[r] {
			return fmt.Errorf("engine: resolver returned duplicate region ID %q", r)
		}
		seen[r] = true
	}
	return nil
}

func (e *Engine) Resolver() regionresolver.Resolver {
	return e.resolver
}

func (e *Engine) NumRegions() int {
	return len(e.regions)
}

func (e *Engine) lookupCtl(id string) (*ObjCtl, bool) {
	e.ctlMu.RLock()
	ctl, ok := e.ctls[id]
	e.ctlMu.RUnlock()
	return ctl, ok
}

func (e *Engine) RegisterObject(id string, dims domain.ObjectDimensions) {
	if strings.Contains(id, ".") {
		panic(fmt.Sprintf("engine: object ID %q contains '.' which is not allowed", id))
	}
	ctl := newObjCtl(dims)
	e.ctlMu.Lock()
	e.ctls[id] = ctl
	e.ctlMu.Unlock()
}

func (e *Engine) DirectRemoveObject(id string) []gtevents.GeofenceTransition {
	var transitions []gtevents.GeofenceTransition

	ctl, ok := e.lookupCtl(id)
	if !ok {
		return nil
	}

	ctl.pubMu.Lock()
	ctl.deleted = true
	hasRegion := ctl.hasPubRegion
	region := ctl.pubRegion
	ctl.hasPubRegion = false
	ctl.pubMu.Unlock()

	if hasRegion {
		rs := e.regions[region]
		rs.mu.Lock()

		if prev := rs.prevInside[id]; prev != nil {
			for areaID := range prev {
				transitions = append(transitions, gtevents.GeofenceTransition{
					AreaID:  areaID,
					Entered: false,
				})
			}
			delete(rs.prevInside, id)
		}

		rs.tree.Delete(id)
		rs.mu.Unlock()
	}

	e.ctlMu.Lock()
	delete(e.ctls, id)
	e.ctlMu.Unlock()

	return transitions
}

func (e *Engine) snapshotObjectState(id string, ctl *ObjCtl, sourceSeq uint64) gtevents.ObjectStateRecord {
	state := gtevents.ObjectStateRecord{
		ObjectID:         id,
		Lifecycle:        gtevents.LifecycleActive,
		DetectorStateSeq: sourceSeq,
		Dims: gtevents.EventDims{
			Width:  ctl.dims.Width,
			Height: ctl.dims.Height,
		},
	}

	ctl.pubMu.RLock()
	if ctl.deleted {
		ctl.pubMu.RUnlock()
		state.Lifecycle = gtevents.LifecycleRemoved
		return state
	}
	if !ctl.hasPubRegion {
		ctl.pubMu.RUnlock()
		return state
	}

	region := ctl.pubRegion
	state.Region = &region
	rs := e.regions[region]
	rs.mu.RLock()
	ctl.pubMu.RUnlock()

	if item := rs.tree.Get(id); item != nil {
		state.Position = &gtevents.EventPosition{
			X:    item.X,
			Y:    item.Y,
			Z:    item.LastZ,
			RotY: item.RotY,
		}
	}
	if inside := rs.prevInside[id]; len(inside) > 0 {
		state.InsideAreaIDs = make([]string, 0, len(inside))
		for areaID := range inside {
			state.InsideAreaIDs = append(state.InsideAreaIDs, areaID)
		}
		sort.Strings(state.InsideAreaIDs)
	}
	rs.mu.RUnlock()

	return state
}

func (e *Engine) RegisterArea(id string, region string, points []domain.Point) error {
	if strings.Contains(id, ".") {
		return fmt.Errorf("area ID %q contains '.' which is not allowed", id)
	}
	if len(points) < 3 {
		return fmt.Errorf("area %s: need at least 3 points, got %d", id, len(points))
	}
	if !geo.IsSimplePolygon(geo.Polygon(points)) {
		return fmt.Errorf("area %s: polygon is self-intersecting", id)
	}

	rs, ok := e.regions[region]
	if !ok {
		return fmt.Errorf("area %s: region %q not found", id, region)
	}

	poly := geo.Polygon(points)
	minX, minY, maxX, maxY := geo.PolygonBBox(poly)
	cx, cy, err := geo.CentroidFromPolygon(poly)
	if err != nil {
		cx, cy = (minX+maxX)/2, (minY+maxY)/2
	}
	px, py, _ := geo.Polylabel(poly, 0.01) // NOTE: Sub cm precision

	triangles := geo.Triangulate(poly)

	rs.mu.Lock()
	rs.areaTree.Upsert(&floorrtree.AreaItem{
		ID: id, Region: region, Triangles: triangles,
		CX: cx, CY: cy, PX: px, PY: py,
		MinX: minX, MinY: minY, MaxX: maxX, MaxY: maxY,
	})
	rs.mu.Unlock()
	return nil
}

func (e *Engine) RemoveArea(id string) {
	for _, rs := range e.regions {
		rs.mu.Lock()
		area := rs.areaTree.Get(id)
		if area == nil {
			rs.mu.Unlock()
			continue
		}

		for _, areas := range rs.prevInside {
			if areas[id] {
				delete(areas, id)
			}
		}
		for objID, areas := range rs.prevInside {
			if len(areas) == 0 {
				delete(rs.prevInside, objID)
			}
		}

		rs.areaTree.Delete(id)
		rs.mu.Unlock()
	}
}

func (e *Engine) BootstrapPlaceObject(id string, x, y, z, rotY float64, region string) bool {
	ctl, ok := e.lookupCtl(id)
	if !ok {
		return false
	}
	rs, ok := e.regions[region]
	if !ok {
		log.Printf("[engine] bootstrap: skipping object %s, unknown region %q", id, region)
		return false
	}

	obb := geo.NewOBB(x, y, ctl.dims.Width/2, ctl.dims.Height/2, rotY)
	item := &floorrtree.ObjectItem{
		ID: id, OBB: obb,
		Dims:  [2]float64{ctl.dims.Width, ctl.dims.Height},
		LastZ: z, X: x, Y: y, RotY: rotY,
	}

	ctl.pubMu.Lock()
	rs.mu.Lock()
	rs.tree.Upsert(item)
	ctl.pubRegion = region
	ctl.hasPubRegion = true
	rs.mu.Unlock()
	ctl.pubMu.Unlock()

	ctl.routeRegion = region
	ctl.hasRouteRegion = true
	return true
}

func (e *Engine) BootstrapFromState(state *gtevents.ObjectStateRecord) {
	ctl := newObjCtl(domain.ObjectDimensions{
		Width:  state.Dims.Width,
		Height: state.Dims.Height,
	})
	ctl.detectorStateSeq = state.DetectorStateSeq

	e.ctlMu.Lock()
	e.ctls[state.ObjectID] = ctl
	e.ctlMu.Unlock()

	if state.Lifecycle != gtevents.LifecycleActive || state.Position == nil || state.Region == nil {
		return
	}
	if _, ok := e.regions[*state.Region]; !ok {
		log.Printf("[engine] bootstrap: skipping object %s, unknown region %q", state.ObjectID, *state.Region)
		return
	}

	if !e.BootstrapPlaceObject(state.ObjectID, state.Position.X, state.Position.Y, state.Position.Z, state.Position.RotY, *state.Region) {
		return
	}

	if len(state.InsideAreaIDs) > 0 {
		rs := e.regions[*state.Region]
		rs.mu.Lock()
		current := make(map[string]bool, len(state.InsideAreaIDs))
		for _, id := range state.InsideAreaIDs {
			current[id] = true
		}
		rs.prevInside[state.ObjectID] = current
		rs.mu.Unlock()
	}
}

func (e *Engine) BootstrapRemoveObject(id string) {
	e.DirectRemoveObject(id)
}

func (e *Engine) ObjectCount() int {
	e.ctlMu.RLock()
	n := len(e.ctls)
	e.ctlMu.RUnlock()
	return n
}

func (e *Engine) sortedRegions() []string {
	sorted := make([]string, 0, len(e.regions))
	for r := range e.regions {
		sorted = append(sorted, r)
	}
	sort.Strings(sorted)
	return sorted
}

func errNotFound(format string, args ...interface{}) error {
	return fmt.Errorf("%w: "+format, append([]interface{}{natsquery.ErrNotFound}, args...)...)
}
