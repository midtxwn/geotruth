package geo

import (
	"testing"

	"github.com/midtxwn/geotruth/pkg/domain"
)

func TestTriangulateSquare(t *testing.T) {
	poly := Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	tris := Triangulate(poly)
	if len(tris) != 2 {
		t.Fatalf("expected 2 triangles, got %d", len(tris))
	}
}

func TestTriangulateTriangle(t *testing.T) {
	poly := Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 0, Y: 10}}
	tris := Triangulate(poly)
	if len(tris) != 1 {
		t.Fatalf("expected 1 triangle, got %d", len(tris))
	}
}

func TestTriangulateConcave(t *testing.T) {
	uShape := Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 8, Y: 10}, {X: 8, Y: 2}, {X: 2, Y: 2}, {X: 2, Y: 10}, {X: 0, Y: 10}}
	tris := Triangulate(uShape)
	if len(tris) != 6 {
		t.Fatalf("expected 6 triangles for U-shape (8 vertices), got %d", len(tris))
	}

	for i, tri := range tris {
		if !isValidTriangle(tri) {
			t.Errorf("triangle %d is degenerate: %v", i, tri)
		}
	}
}

func TestTriangulatePentagon(t *testing.T) {
	poly := Polygon{{X: 5, Y: 0}, {X: 10, Y: 4}, {X: 8, Y: 9}, {X: 2, Y: 9}, {X: 0, Y: 4}}
	tris := Triangulate(poly)
	if len(tris) != 3 {
		t.Fatalf("expected 3 triangles for pentagon, got %d", len(tris))
	}
}

func TestTriangulateDegenerate(t *testing.T) {
	if Triangulate(Polygon{}) != nil {
		t.Error("expected nil for empty polygon")
	}
	if Triangulate(Polygon{{X: 0, Y: 0}, {X: 1, Y: 0}}) != nil {
		t.Error("expected nil for degenerate polygon")
	}
}

func TestTriangulateCoverage(t *testing.T) {
	square := Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	tris := Triangulate(square)

	testPoints := []struct {
		x, y   float64
		inside bool
	}{
		{5, 5, true},
		{1, 1, true},
		{9, 9, true},
		{50, 50, false},
		{-1, 5, false},
	}

	for _, tp := range testPoints {
		got := TrianglesContainPoint(tp.x, tp.y, tris)
		if got != tp.inside {
			t.Errorf("point (%.0f,%.0f): got inside=%v, want %v", tp.x, tp.y, got, tp.inside)
		}
	}
}

func TestTriangulateConcaveCoverage(t *testing.T) {
	uShape := Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 8, Y: 10}, {X: 8, Y: 2}, {X: 2, Y: 2}, {X: 2, Y: 10}, {X: 0, Y: 10}}
	tris := Triangulate(uShape)

	testPoints := []struct {
		x, y   float64
		inside bool
	}{
		{1, 5, true},
		{9, 5, true},
		{5, 1, true},
		{5, 6, false},
		{50, 50, false},
	}

	for _, tp := range testPoints {
		got := TrianglesContainPoint(tp.x, tp.y, tris)
		if got != tp.inside {
			t.Errorf("point (%.0f,%.0f): got inside=%v, want %v", tp.x, tp.y, got, tp.inside)
		}
	}
}

func isValidTriangle(tri domain.Triangle) bool {
	ab := tri[1].Sub(tri[0])
	ac := tri[2].Sub(tri[0])
	return ab.Cross(ac) != 0
}
