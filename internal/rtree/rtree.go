package rtree

import (
	"github.com/midtxwn/geotruth/internal/geo"
	"github.com/midtxwn/geotruth/pkg/domain"

	"github.com/tidwall/rtree"
)

type ObjectItem struct {
	ID    string
	OBB   geo.OBB
	Dims  [2]float64
	LastZ float64
	X, Y  float64
	RotY  float64
}

type ObjectTree struct {
	inner rtree.RTreeG[*ObjectItem]
	byID  map[string]objectIndexEntry
}

type objectIndexEntry struct {
	min  [2]float64
	max  [2]float64
	item *ObjectItem
}

func (t *ObjectTree) ensureIndex() {
	if t.byID == nil {
		t.byID = make(map[string]objectIndexEntry)
	}
}

func (t *ObjectTree) Upsert(item *ObjectItem) {
	t.Delete(item.ID)
	minX, minY, maxX, maxY := item.OBB.AABB()
	min := [2]float64{minX, minY}
	max := [2]float64{maxX, maxY}
	t.inner.Insert(min, max, item)
	t.ensureIndex()
	t.byID[item.ID] = objectIndexEntry{min: min, max: max, item: item}
}

func (t *ObjectTree) Delete(id string) {
	if t.byID != nil {
		if entry, ok := t.byID[id]; ok {
			t.inner.Delete(entry.min, entry.max, entry.item)
			delete(t.byID, id)
		}
	}
}

func (t *ObjectTree) Get(id string) *ObjectItem {
	if t.byID == nil {
		return nil
	}
	return t.byID[id].item
}

func (t *ObjectTree) Nearby(cx, cy, radius float64) []*ObjectItem {
	var results []*ObjectItem
	minPt := [2]float64{cx - radius, cy - radius}
	maxPt := [2]float64{cx + radius, cy + radius}
	t.inner.Search(minPt, maxPt, func(_, _ [2]float64, item *ObjectItem) bool {
		results = append(results, item)
		return true
	})
	return results
}

func (t *ObjectTree) Search(min, max [2]float64) []*ObjectItem {
	var results []*ObjectItem
	t.inner.Search(min, max, func(_, _ [2]float64, item *ObjectItem) bool {
		results = append(results, item)
		return true
	})
	return results
}

func (t *ObjectTree) All() []*ObjectItem {
	var results []*ObjectItem
	t.inner.Scan(func(_, _ [2]float64, item *ObjectItem) bool {
		results = append(results, item)
		return true
	})
	return results
}

// ---- area R-tree --------------------------------------------------------------

type AreaItem struct {
	ID        string
	Region    string
	Triangles []domain.Triangle
	CX, CY    float64
	PX, PY    float64

	MinX, MinY, MaxX, MaxY float64
}

type AreaTree struct {
	inner rtree.RTreeG[*AreaItem]
	byID  map[string]areaIndexEntry
}

type areaIndexEntry struct {
	min  [2]float64
	max  [2]float64
	item *AreaItem
}

func (at *AreaTree) ensureIndex() {
	if at.byID == nil {
		at.byID = make(map[string]areaIndexEntry)
	}
}

func (at *AreaTree) Upsert(item *AreaItem) {
	at.Delete(item.ID)
	min := [2]float64{item.MinX, item.MinY}
	max := [2]float64{item.MaxX, item.MaxY}
	at.inner.Insert(min, max, item)
	at.ensureIndex()
	at.byID[item.ID] = areaIndexEntry{min: min, max: max, item: item}
}

func (at *AreaTree) Delete(id string) {
	if at.byID != nil {
		if entry, ok := at.byID[id]; ok {
			at.inner.Delete(entry.min, entry.max, entry.item)
			delete(at.byID, id)
		}
	}
}

func (at *AreaTree) Get(id string) *AreaItem {
	if at.byID == nil {
		return nil
	}
	return at.byID[id].item
}

func (at *AreaTree) Search(minX, minY, maxX, maxY float64) []*AreaItem {
	var results []*AreaItem
	at.inner.Search([2]float64{minX, minY}, [2]float64{maxX, maxY}, func(_, _ [2]float64, item *AreaItem) bool {
		results = append(results, item)
		return true
	})
	return results
}

func (at *AreaTree) All() []*AreaItem {
	var results []*AreaItem
	at.inner.Scan(func(_, _ [2]float64, item *AreaItem) bool {
		results = append(results, item)
		return true
	})
	return results
}
