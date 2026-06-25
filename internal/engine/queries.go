package engine

import (
	"fmt"
	"regexp"

	"github.com/midtxwn/geotruth/internal/geo"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/natsquery"
	"github.com/midtxwn/geotruth/pkg/regionresolver"
)

type areaSnap struct {
	ID        string
	Region    string
	CX        float64
	CY        float64
	PX        float64
	PY        float64
	Triangles []domain.Triangle
}

func matchesID(re *regexp.Regexp, id string) bool {
	return re == nil || re.MatchString(id)
}

func (e *Engine) Nearby(region string, x, y, radius float64, re *regexp.Regexp) ([]domain.Object, error) {
	rs, ok := e.regions[region]
	if !ok {
		return nil, errNotFound("region %q not found", region)
	}

	rs.mu.RLock()
	candidates := rs.tree.Nearby(x, y, radius)
	snapshots := make([]struct {
		ID   string
		OBB  geo.OBB
		X    float64
		Y    float64
		Z    float64
		RotY float64
	}, len(candidates))
	for i, item := range candidates {
		snapshots[i].ID = item.ID
		snapshots[i].OBB = item.OBB
		snapshots[i].X = item.X
		snapshots[i].Y = item.Y
		snapshots[i].Z = item.LastZ
		snapshots[i].RotY = item.RotY
	}
	rs.mu.RUnlock()

	results := make([]domain.Object, 0, len(snapshots))
	for _, s := range snapshots {
		if !matchesID(re, s.ID) {
			continue
		}
		if s.OBB.IntersectsCircle(x, y, radius) {
			regionCopy := region
			results = append(results, domain.Object{
				ID: s.ID, Region: &regionCopy, X: s.X, Y: s.Y, Z: s.Z, RotY: s.RotY,
			})
		}
	}
	return results, nil
}

func (e *Engine) NearbyOf(objectID string, radius float64, re *regexp.Regexp) ([]natsquery.ObjectOriented, error) {
	ctl, ok := e.lookupCtl(objectID)
	if !ok {
		return nil, errNotFound("object %s not found", objectID)
	}

	ctl.pubMu.RLock()
	if ctl.deleted || !ctl.hasPubRegion {
		ctl.pubMu.RUnlock()
		return nil, errNotFound("object %s not found", objectID)
	}
	region := ctl.pubRegion
	rs := e.regions[region]
	rs.mu.RLock()
	ctl.pubMu.RUnlock()

	origin := rs.tree.Get(objectID)
	if origin == nil {
		rs.mu.RUnlock()
		return nil, errNotFound("object %s not in R-tree on region %q", objectID, region)
	}
	originX := origin.X
	originY := origin.Y

	candidates := rs.tree.Nearby(originX, originY, radius)
	type candSnap struct {
		ID  string
		OBB geo.OBB
		X   float64
		Y   float64
		Z   float64
	}
	snaps := make([]candSnap, 0, len(candidates))
	for _, item := range candidates {
		if item.ID == objectID {
			continue
		}
		snaps = append(snaps, candSnap{ID: item.ID, OBB: item.OBB, X: item.X, Y: item.Y, Z: item.LastZ})
	}
	rs.mu.RUnlock()

	results := make([]natsquery.ObjectOriented, 0, len(snaps))
	for _, s := range snaps {
		if !matchesID(re, s.ID) {
			continue
		}
		if !s.OBB.IntersectsCircle(originX, originY, radius) {
			continue
		}
		corners := s.OBB.Corners()
		var bounds natsquery.OrientedBounds
		cornerPtrs := [4]*domain.Point{&bounds.TL, &bounds.TR, &bounds.BR, &bounds.BL}
		for i := 0; i < 4; i++ {
			*cornerPtrs[i] = corners[i]
		}
		results = append(results, natsquery.ObjectOriented{
			ID:     s.ID,
			Bounds: bounds,
			Position: natsquery.Position3D{
				X: s.X,
				Y: s.Y,
				Z: s.Z,
			},
		})
	}
	return results, nil
}

func (e *Engine) WithinArea(region string, areaID string, re *regexp.Regexp) ([]domain.Object, error) {
	rs, ok := e.regions[region]
	if !ok {
		return nil, errNotFound("region %q not found", region)
	}

	rs.mu.RLock()
	area := rs.areaTree.Get(areaID)
	if area == nil {
		rs.mu.RUnlock()
		return nil, errNotFound("area %s not found on region %q", areaID, region)
	}
	areaTris := make([]domain.Triangle, len(area.Triangles))
	copy(areaTris, area.Triangles)
	candidates := rs.tree.Search([2]float64{area.MinX, area.MinY}, [2]float64{area.MaxX, area.MaxY})
	type objSnap struct {
		ID   string
		X    float64
		Y    float64
		Z    float64
		RotY float64
		OBB  geo.OBB
	}
	snaps := make([]objSnap, 0, len(candidates))
	for _, item := range candidates {
		snaps = append(snaps, objSnap{ID: item.ID, X: item.X, Y: item.Y, Z: item.LastZ, RotY: item.RotY, OBB: item.OBB})
	}
	rs.mu.RUnlock()

	results := make([]domain.Object, 0)
	for _, s := range snaps {
		if !matchesID(re, s.ID) {
			continue
		}
		if s.OBB.IntersectsTriangles(areaTris) {
			regionCopy := region
			results = append(results, domain.Object{
				ID: s.ID, Region: &regionCopy, X: s.X, Y: s.Y, Z: s.Z, RotY: s.RotY,
			})
		}
	}
	return results, nil
}

func (e *Engine) AreasAtPoint(region string, x, y float64, re *regexp.Regexp) ([]domain.Area, error) {
	rs, ok := e.regions[region]
	if !ok {
		return nil, errNotFound("region %q not found", region)
	}

	rs.mu.RLock()
	candidates := rs.areaTree.Search(x, y, x, y)
	snaps := make([]areaSnap, 0, len(candidates))
	for _, area := range candidates {
		tris := make([]domain.Triangle, len(area.Triangles))
		copy(tris, area.Triangles)
		snaps = append(snaps, areaSnap{ID: area.ID, Region: area.Region, CX: area.CX, CY: area.CY, PX: area.PX, PY: area.PY, Triangles: tris})
	}
	rs.mu.RUnlock()

	results := make([]domain.Area, 0, len(snaps))
	for _, s := range snaps {
		if !matchesID(re, s.ID) {
			continue
		}
		if geo.TrianglesContainPoint(x, y, s.Triangles) {
			results = append(results, domain.Area{
				ID:     s.ID,
				Region: s.Region,
				CX:     s.CX,
				CY:     s.CY,
				PX:     s.PX,
				PY:     s.PY,
			})
		}
	}
	return results, nil
}

func (e *Engine) AreasContaining(objectID string, re *regexp.Regexp) ([]domain.Area, error) {
	ctl, ok := e.lookupCtl(objectID)
	if !ok {
		return nil, errNotFound("object %s not found", objectID)
	}

	ctl.pubMu.RLock()
	if ctl.deleted || !ctl.hasPubRegion {
		ctl.pubMu.RUnlock()
		return nil, errNotFound("object %s not found", objectID)
	}
	region := ctl.pubRegion
	rs := e.regions[region]
	rs.mu.RLock()
	ctl.pubMu.RUnlock()

	item := rs.tree.Get(objectID)
	if item == nil {
		rs.mu.RUnlock()
		return nil, errNotFound("object %s not in R-tree on region %q", objectID, region)
	}
	obb := item.OBB
	candidates := rs.areaTree.Search(obb.AABB())
	snaps := make([]areaSnap, 0, len(candidates))
	for _, area := range candidates {
		tris := make([]domain.Triangle, len(area.Triangles))
		copy(tris, area.Triangles)
		snaps = append(snaps, areaSnap{ID: area.ID, Region: area.Region, CX: area.CX, CY: area.CY, PX: area.PX, PY: area.PY, Triangles: tris})
	}
	rs.mu.RUnlock()

	results := make([]domain.Area, 0, len(snaps))
	for _, s := range snaps {
		if !matchesID(re, s.ID) {
			continue
		}
		if obb.IntersectsTriangles(s.Triangles) {
			results = append(results, domain.Area{
				ID:     s.ID,
				Region: s.Region,
				CX:     s.CX,
				CY:     s.CY,
				PX:     s.PX,
				PY:     s.PY,
			})
		}
	}
	return results, nil
}

func (e *Engine) Intersecting(objectID string, re *regexp.Regexp) ([]domain.Object, error) {
	ctl, ok := e.lookupCtl(objectID)
	if !ok {
		return nil, errNotFound("object %s not found", objectID)
	}

	ctl.pubMu.RLock()
	if ctl.deleted || !ctl.hasPubRegion {
		ctl.pubMu.RUnlock()
		return nil, errNotFound("object %s not found", objectID)
	}
	region := ctl.pubRegion
	rs := e.regions[region]
	rs.mu.RLock()
	ctl.pubMu.RUnlock()

	origin := rs.tree.Get(objectID)
	if origin == nil {
		rs.mu.RUnlock()
		return nil, errNotFound("object %s not in R-tree on region %q", objectID, region)
	}
	originOBB := origin.OBB
	minX, minY, maxX, maxY := originOBB.AABB()
	candidates := rs.tree.Search([2]float64{minX, minY}, [2]float64{maxX, maxY})
	type candSnap struct {
		ID   string
		OBB  geo.OBB
		X    float64
		Y    float64
		Z    float64
		RotY float64
	}
	snaps := make([]candSnap, 0, len(candidates))
	for _, item := range candidates {
		if item.ID == objectID {
			continue
		}
		snaps = append(snaps, candSnap{ID: item.ID, OBB: item.OBB, X: item.X, Y: item.Y, Z: item.LastZ, RotY: item.RotY})
	}
	rs.mu.RUnlock()

	results := make([]domain.Object, 0, len(snaps))
	for _, s := range snaps {
		if !matchesID(re, s.ID) {
			continue
		}
		if originOBB.IntersectsOBB(s.OBB) {
			regionCopy := region
			results = append(results, domain.Object{
				ID: s.ID, Region: &regionCopy, X: s.X, Y: s.Y, Z: s.Z, RotY: s.RotY,
			})
		}
	}
	return results, nil
}

func (e *Engine) Bounds(objectID string) (natsquery.OrientedBounds, error) {
	ctl, ok := e.lookupCtl(objectID)
	if !ok {
		return natsquery.OrientedBounds{}, errNotFound("object %s not found", objectID)
	}

	ctl.pubMu.RLock()
	if ctl.deleted || !ctl.hasPubRegion {
		ctl.pubMu.RUnlock()
		return natsquery.OrientedBounds{}, errNotFound("object %s not found", objectID)
	}
	region := ctl.pubRegion
	rs := e.regions[region]
	rs.mu.RLock()
	ctl.pubMu.RUnlock()

	item := rs.tree.Get(objectID)
	if item == nil {
		rs.mu.RUnlock()
		return natsquery.OrientedBounds{}, errNotFound("object %s not in R-tree on region %q", objectID, region)
	}
	obb := item.OBB
	rs.mu.RUnlock()

	corners := obb.Corners()
	var bounds natsquery.OrientedBounds
	cornerPtrs := [4]*domain.Point{&bounds.TL, &bounds.TR, &bounds.BR, &bounds.BL}
	for i := 0; i < 4; i++ {
		*cornerPtrs[i] = corners[i]
	}
	return bounds, nil
}

func (e *Engine) ObjectPosition(objectID string) (domain.Object, error) {
	ctl, ok := e.lookupCtl(objectID)
	if !ok {
		return domain.Object{}, errNotFound("object %s not found", objectID)
	}

	ctl.pubMu.RLock()
	if ctl.deleted || !ctl.hasPubRegion {
		ctl.pubMu.RUnlock()
		return domain.Object{}, errNotFound("object %s not found", objectID)
	}
	region := ctl.pubRegion
	rs := e.regions[region]
	rs.mu.RLock()
	ctl.pubMu.RUnlock()

	item := rs.tree.Get(objectID)
	if item == nil {
		rs.mu.RUnlock()
		return domain.Object{}, errNotFound("object %s not in R-tree on region %q", objectID, region)
	}
	x := item.X
	y := item.Y
	z := item.LastZ
	rotY := item.RotY
	rs.mu.RUnlock()

	regionCopy := region
	return domain.Object{ID: objectID, Region: &regionCopy, X: x, Y: y, Z: z, RotY: rotY}, nil
}

func (e *Engine) NearbyAreas(region string, x, y, radius float64, re *regexp.Regexp) ([]domain.Area, error) {
	rs, ok := e.regions[region]
	if !ok {
		return nil, errNotFound("region %q not found", region)
	}

	rs.mu.RLock()
	candidates := rs.areaTree.Search(
		x-radius, y-radius,
		x+radius, y+radius,
	)
	snaps := make([]areaSnap, 0, len(candidates))
	for _, area := range candidates {
		tris := make([]domain.Triangle, len(area.Triangles))
		copy(tris, area.Triangles)
		snaps = append(snaps, areaSnap{ID: area.ID, Region: area.Region, CX: area.CX, CY: area.CY, PX: area.PX, PY: area.PY, Triangles: tris})
	}
	rs.mu.RUnlock()

	results := make([]domain.Area, 0, len(snaps))
	for _, s := range snaps {
		if !matchesID(re, s.ID) {
			continue
		}
		if geo.TrianglesIntersectCircle(s.Triangles, x, y, radius) {
			results = append(results, domain.Area{
				ID:        s.ID,
				Region:    s.Region,
				Triangles: s.Triangles,
				CX:        s.CX,
				CY:        s.CY,
				PX:        s.PX,
				PY:        s.PY,
			})
		}
	}
	return results, nil
}

func (e *Engine) Area(areaID string) (domain.Area, error) {
	for _, rs := range e.regions {
		rs.mu.RLock()
		area := rs.areaTree.Get(areaID)
		if area != nil {
			tris := make([]domain.Triangle, len(area.Triangles))
			copy(tris, area.Triangles)
			result := domain.Area{
				ID:        area.ID,
				Region:    area.Region,
				Triangles: tris,
				CX:        area.CX,
				CY:        area.CY,
				PX:        area.PX,
				PY:        area.PY,
			}
			rs.mu.RUnlock()
			return result, nil
		}
		rs.mu.RUnlock()
	}
	return domain.Area{}, errNotFound("area %s not found", areaID)
}

func (e *Engine) AllObjectsOriented(re *regexp.Regexp) natsquery.AllObjectsOrientedResp {
	sorted := e.sortedRegions()
	for _, r := range sorted {
		e.regions[r].mu.RLock()
	}

	type objSnap struct {
		ID  string
		OBB geo.OBB
		X   float64
		Y   float64
		Z   float64
	}

	resp := natsquery.AllObjectsOrientedResp{Regions: make(map[string][]natsquery.ObjectOriented)}
	snaps := make(map[string][]objSnap, len(sorted))
	for _, r := range sorted {
		items := e.regions[r].tree.All()
		regionSnaps := make([]objSnap, 0, len(items))
		for _, item := range items {
			regionSnaps = append(regionSnaps, objSnap{ID: item.ID, OBB: item.OBB, X: item.X, Y: item.Y, Z: item.LastZ})
		}
		snaps[r] = regionSnaps
	}

	for _, r := range sorted {
		e.regions[r].mu.RUnlock()
	}

	for _, r := range sorted {
		for _, s := range snaps[r] {
			if !matchesID(re, s.ID) {
				continue
			}
			corners := s.OBB.Corners()
			var bounds natsquery.OrientedBounds
			cornerPtrs := [4]*domain.Point{&bounds.TL, &bounds.TR, &bounds.BR, &bounds.BL}
			for i := 0; i < 4; i++ {
				*cornerPtrs[i] = corners[i]
			}
			resp.Regions[r] = append(resp.Regions[r], natsquery.ObjectOriented{
				ID:     s.ID,
				Bounds: bounds,
				Position: natsquery.Position3D{
					X: s.X,
					Y: s.Y,
					Z: s.Z,
				},
			})
		}
	}

	return resp
}

func (e *Engine) AllObjects(re *regexp.Regexp) natsquery.AllObjectsResp {
	sorted := e.sortedRegions()

	for _, r := range sorted {
		e.regions[r].mu.RLock()
	}

	resp := natsquery.AllObjectsResp{Regions: make(map[string][]domain.Object)}
	for _, r := range sorted {
		for _, item := range e.regions[r].tree.All() {
			if !matchesID(re, item.ID) {
				continue
			}
			regionCopy := r
			resp.Regions[r] = append(resp.Regions[r], domain.Object{
				ID: item.ID, Region: &regionCopy, X: item.X, Y: item.Y, Z: item.LastZ, RotY: item.RotY,
			})
		}
	}

	for _, r := range sorted {
		e.regions[r].mu.RUnlock()
	}

	return resp
}

func (e *Engine) RegionOf(objectID string) (string, error) {
	ctl, ok := e.lookupCtl(objectID)
	if !ok {
		return "", errNotFound("object %s not found", objectID)
	}

	ctl.pubMu.RLock()
	if ctl.deleted || !ctl.hasPubRegion {
		ctl.pubMu.RUnlock()
		return "", errNotFound("object %s not found", objectID)
	}
	region := ctl.pubRegion
	ctl.pubMu.RUnlock()

	return region, nil
}

func (e *Engine) RegionFromPoint(x, y, z float64) (string, error) {
	region, err := e.resolver.Resolve(x, y, z, regionresolver.NoPrevRegion)
	if err != nil {
		return "", fmt.Errorf("resolve point region: %w", err)
	}
	if _, ok := e.regions[region]; !ok {
		return "", fmt.Errorf("unknown region %q", region)
	}
	return region, nil
}

func (e *Engine) AllAreas(re *regexp.Regexp) natsquery.AllAreasResp {
	sorted := e.sortedRegions()

	for _, r := range sorted {
		e.regions[r].mu.RLock()
	}

	resp := natsquery.AllAreasResp{Regions: make(map[string][]domain.Area)}
	for _, r := range sorted {
		for _, area := range e.regions[r].areaTree.All() {
			if !matchesID(re, area.ID) {
				continue
			}
			tris := make([]domain.Triangle, len(area.Triangles))
			copy(tris, area.Triangles)
			resp.Regions[r] = append(resp.Regions[r], domain.Area{
				ID:        area.ID,
				Region:    area.Region,
				Triangles: tris,
				CX:        area.CX,
				CY:        area.CY,
				PX:        area.PX,
				PY:        area.PY,
			})
		}
	}

	for _, r := range sorted {
		e.regions[r].mu.RUnlock()
	}

	return resp
}
