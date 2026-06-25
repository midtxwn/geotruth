package engine

import (
	"github.com/midtxwn/geotruth/internal/gtevents"
	floorrtree "github.com/midtxwn/geotruth/internal/rtree"
)

type GeofenceResult struct {
	Transitions   []gtevents.GeofenceTransition
	InsideAreaIDs []string
	CurrentInside map[string]bool
}

// collectGeofenceResultLocked compares the object's current area intersections
// with the region's committed prevInside snapshot. It only computes the next
// snapshot; the dispatcher writes prevInside after the GT_EVENTS commit.
func collectGeofenceResultLocked(rs *RegionState, item *floorrtree.ObjectItem) GeofenceResult {
	minX, minY, maxX, maxY := item.OBB.AABB()
	candidates := rs.areaTree.Search(minX, minY, maxX, maxY)

	currentInside := make(map[string]bool)
	for _, area := range candidates {
		if item.OBB.IntersectsTriangles(area.Triangles) {
			currentInside[area.ID] = true
		}
	}

	prev := rs.prevInside[item.ID]
	if prev == nil {
		prev = make(map[string]bool)
	}

	var transitions []gtevents.GeofenceTransition
	for areaID := range currentInside {
		if !prev[areaID] {
			transitions = append(transitions, gtevents.GeofenceTransition{
				AreaID:  areaID,
				Entered: true,
			})
		}
	}
	for areaID := range prev {
		if !currentInside[areaID] {
			transitions = append(transitions, gtevents.GeofenceTransition{
				AreaID:  areaID,
				Entered: false,
			})
		}
	}

	var insideIDs []string
	for id := range currentInside {
		insideIDs = append(insideIDs, id)
	}

	return GeofenceResult{
		Transitions:   transitions,
		InsideAreaIDs: insideIDs,
		CurrentInside: currentInside,
	}
}
