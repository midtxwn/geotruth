package geo

import (
	"math"
	"testing"

	"github.com/midtxwn/geotruth/pkg/domain"
)

func TestOBBIntersectsOBB(t *testing.T) {
	a := NewOBB(0, 0, 1, 1, 0)
	b := NewOBB(0, 0, 1, 1, math.Pi/4)
	if !a.IntersectsOBB(b) {
		t.Error("co-located boxes should intersect")
	}

	c := NewOBB(0, 0, 1, 1, 0)
	d := NewOBB(100, 100, 1, 1, 0)
	if c.IntersectsOBB(d) {
		t.Error("far-apart boxes should not intersect")
	}

	e := NewOBB(0, 0, 1, 1, 0)
	f := NewOBB(1.5, 0, 1, 1, 0)
	if !e.IntersectsOBB(f) {
		t.Error("overlapping boxes should intersect")
	}
}

func TestOBBIntersectsTriangleConvex(t *testing.T) {
	squareTris := Triangulate(Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}})
	inside := NewOBB(5, 5, 0.25, 0.25, 0)
	if !inside.IntersectsTriangles(squareTris) {
		t.Error("OBB inside triangle-decomposed square should intersect")
	}

	outside := NewOBB(50, 50, 0.25, 0.25, 0)
	if outside.IntersectsTriangles(squareTris) {
		t.Error("OBB far outside should not intersect")
	}
}

func TestOBBIntersectsTriangleConcaveGap(t *testing.T) {
	uShapeTris := Triangulate(Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 8, Y: 10}, {X: 8, Y: 2}, {X: 2, Y: 2}, {X: 2, Y: 10}, {X: 0, Y: 10}})
	obb := NewOBB(5, 6, 1, 1, 0)
	if obb.IntersectsTriangles(uShapeTris) {
		t.Error("OBB in concave gap should NOT intersect U-shape")
	}
}

func TestOBBIntersectsTriangleConcaveInside(t *testing.T) {
	uShapeTris := Triangulate(Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 8, Y: 10}, {X: 8, Y: 2}, {X: 2, Y: 2}, {X: 2, Y: 10}, {X: 0, Y: 10}})
	obb := NewOBB(1, 5, 0.5, 2, 0)
	if !obb.IntersectsTriangles(uShapeTris) {
		t.Error("OBB inside left arm of U-shape should intersect")
	}
}

func TestOBBIntersectsTriangleConcaveStraddle(t *testing.T) {
	uShapeTris := Triangulate(Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 8, Y: 10}, {X: 8, Y: 2}, {X: 2, Y: 2}, {X: 2, Y: 10}, {X: 0, Y: 10}})
	obb := NewOBB(2, 5, 1, 1, 0)
	if !obb.IntersectsTriangles(uShapeTris) {
		t.Error("OBB straddling concave boundary should intersect")
	}
}

func TestOBBIntersectsTriangleRotatedTouching(t *testing.T) {
	squareTris := Triangulate(Polygon{{X: 0, Y: 0}, {X: 2, Y: 0}, {X: 2, Y: 2}, {X: 0, Y: 2}})
	obb := NewOBB(2.7071, 1, 0.5, 0.5, 0.785398)
	if !obb.IntersectsTriangles(squareTris) {
		t.Error("rotated OBB touching polygon edge should intersect")
	}
}

func TestOBBIntersectsTriangleRotatedOutside(t *testing.T) {
	squareTris := Triangulate(Polygon{{X: 0, Y: 0}, {X: 2, Y: 0}, {X: 2, Y: 2}, {X: 0, Y: 2}})
	obb := NewOBB(2.72, 1, 0.5, 0.5, 0.785398)
	if obb.IntersectsTriangles(squareTris) {
		t.Error("rotated OBB just outside polygon should NOT intersect")
	}
}

func TestOBBContainsPoint(t *testing.T) {
	obb := NewOBB(5, 5, 2, 2, 0)
	if !obb.ContainsPoint(5, 5) {
		t.Error("center should be contained")
	}
	if !obb.ContainsPoint(5, 7) {
		t.Error("edge point should be contained")
	}
	if obb.ContainsPoint(5, 7.1) {
		t.Error("point just outside should not be contained")
	}
}

func TestOBBIntersectsCircle(t *testing.T) {
	inside := NewOBB(5, 5, 2, 2, 0)
	if !inside.IntersectsCircle(5, 5, 1) {
		t.Error("circle center inside OBB should intersect")
	}

	tangent := NewOBB(5, 0, 1, 1, 0)
	if !tangent.IntersectsCircle(0, 0, 4) {
		t.Error("circle tangent to OBB edge should intersect")
	}

	rotated := NewOBB(4.5, 0, 2, 1, math.Pi/4)
	if !rotated.IntersectsCircle(0, 0, 3) {
		t.Error("rotated OBB whose body reaches the circle should intersect")
	}

	miss := NewOBB(3.6, 3.6, 0.5, 0.5, 0)
	if miss.IntersectsCircle(0, 0, 3) {
		t.Error("OBB AABB can overlap query square without intersecting circle")
	}
}

func TestOBBAABB(t *testing.T) {
	obb := NewOBB(5, 5, 1, 1, 0)
	minX, minY, maxX, maxY := obb.AABB()
	if minX != 4 || minY != 4 || maxX != 6 || maxY != 6 {
		t.Fatalf("expected (4,4,6,6), got (%.1f,%.1f,%.1f,%.1f)", minX, minY, maxX, maxY)
	}
}

func TestOBBAABBRotated(t *testing.T) {
	obb := NewOBB(0, 0, 1, 1, math.Pi/4)
	minX, _, maxX, _ := obb.AABB()
	sqrt2 := math.Sqrt(2)
	if math.Abs(minX-(-sqrt2)) > 0.01 {
		t.Fatalf("expected minX â‰ˆ -1.414, got %.3f", minX)
	}
	if math.Abs(maxX-sqrt2) > 0.01 {
		t.Fatalf("expected maxX â‰ˆ 1.414, got %.3f", maxX)
	}
}

func TestOBBCorners(t *testing.T) {
	obb := NewOBB(0, 0, 1, 1, 0)
	corners := obb.Corners()
	if corners[0].X != -1 || corners[0].Y != -1 {
		t.Fatalf("corner 0: expected (-1,-1), got (%.0f,%.0f)", corners[0].X, corners[0].Y)
	}
	if corners[2].X != 1 || corners[2].Y != 1 {
		t.Fatalf("corner 2: expected (1,1), got (%.0f,%.0f)", corners[2].X, corners[2].Y)
	}
}

func TestOBBIntersectsSingleTriangle(t *testing.T) {
	tri := domain.Triangle{
		{X: 0, Y: 0},
		{X: 10, Y: 0},
		{X: 0, Y: 10},
	}
	inside := NewOBB(3, 3, 0.5, 0.5, 0)
	if !inside.IntersectsTriangle(tri) {
		t.Error("OBB inside triangle should intersect")
	}
	outside := NewOBB(50, 50, 0.5, 0.5, 0)
	if outside.IntersectsTriangle(tri) {
		t.Error("OBB far outside triangle should not intersect")
	}
}

func TestTriangleIntersectsCircle(t *testing.T) {
	tri := domain.Triangle{
		{X: 0, Y: 0},
		{X: 10, Y: 0},
		{X: 0, Y: 10},
	}

	if !TriangleIntersectsCircle(tri, 2, 2, 0.5) {
		t.Error("circle center inside triangle should intersect")
	}
	if !TriangleIntersectsCircle(tri, 5, -1, 1) {
		t.Error("circle tangent to triangle edge should intersect")
	}
	if !TriangleIntersectsCircle(tri, -1, -1, math.Sqrt2) {
		t.Error("circle containing a triangle vertex should intersect")
	}
	if TriangleIntersectsCircle(tri, 20, 20, 1) {
		t.Error("distant circle should not intersect triangle")
	}
}
