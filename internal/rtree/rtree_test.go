package rtree

import (
	"testing"

	"github.com/midtxwn/geotruth/internal/geo"
	"github.com/midtxwn/geotruth/pkg/domain"
)

func TestObjectTreeIndexedUpsertGetDelete(t *testing.T) {
	var tree ObjectTree

	first := &ObjectItem{ID: "obj1", OBB: geo.NewOBB(0, 0, 1, 1, 0), X: 0, Y: 0}
	tree.Upsert(first)
	if got := tree.Get("obj1"); got != first {
		t.Fatalf("Get after first upsert = %p, want %p", got, first)
	}

	second := &ObjectItem{ID: "obj1", OBB: geo.NewOBB(10, 10, 1, 1, 0), X: 10, Y: 10}
	tree.Upsert(second)
	if got := tree.Get("obj1"); got != second {
		t.Fatalf("Get after replacement = %p, want %p", got, second)
	}

	results := tree.Search([2]float64{-2, -2}, [2]float64{2, 2})
	if len(results) != 0 {
		t.Fatalf("old bounds should have been removed, got %d results", len(results))
	}

	tree.Delete("obj1")
	if got := tree.Get("obj1"); got != nil {
		t.Fatalf("Get after delete = %p, want nil", got)
	}
}

func TestAreaTreeIndexedUpsertGetDelete(t *testing.T) {
	var tree AreaTree

	first := &AreaItem{
		ID: "area1", Region: "0",
		Triangles: []domain.Triangle{{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 0, Y: 1}}},
		MinX:      0, MinY: 0, MaxX: 1, MaxY: 1,
	}

	tree.Upsert(first)
	if got := tree.Get("area1"); got != first {
		t.Fatalf("Get after first upsert = %p, want %p", got, first)
	}

	second := &AreaItem{
		ID: "area1", Region: "0",
		Triangles: []domain.Triangle{{{X: 10, Y: 10}, {X: 11, Y: 10}, {X: 10, Y: 11}}},
		MinX:      10, MinY: 10, MaxX: 11, MaxY: 11,
	}
	tree.Upsert(second)
	if got := tree.Get("area1"); got != second {
		t.Fatalf("Get after replacement = %p, want %p", got, second)
	}

	results := tree.Search(0, 0, 1, 1)
	if len(results) != 0 {
		t.Fatalf("old bounds should have been removed, got %d results", len(results))
	}

	tree.Delete("area1")
	if got := tree.Get("area1"); got != nil {
		t.Fatalf("Get after delete = %p, want nil", got)
	}
}
