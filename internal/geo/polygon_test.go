package geo

import (
	"math"
	"testing"

	"github.com/midtxwn/geotruth/pkg/domain"
)

func TestCentroidFromPolygonSquare(t *testing.T) {
	poly := Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	cx, cy, err := CentroidFromPolygon(poly)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(cx-5) > 0.01 || math.Abs(cy-5) > 0.01 {
		t.Fatalf("expected (5,5), got (%.2f,%.2f)", cx, cy)
	}
}

func TestCentroidFromPolygonTriangle(t *testing.T) {
	poly := Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 0, Y: 10}}
	cx, cy, err := CentroidFromPolygon(poly)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(cx-10.0/3) > 0.01 || math.Abs(cy-10.0/3) > 0.01 {
		t.Fatalf("expected (3.33,3.33), got (%.2f,%.2f)", cx, cy)
	}
}

func TestCentroidFromPolygonConcave(t *testing.T) {
	uShape := Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 8, Y: 10}, {X: 8, Y: 2}, {X: 2, Y: 2}, {X: 2, Y: 10}, {X: 0, Y: 10}}
	cx, cy, err := CentroidFromPolygon(uShape)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(cx-5.0) > 0.02 {
		t.Fatalf("expected cx â‰ˆ 5.00, got %.2f", cx)
	}
	if math.Abs(cy-4.08) > 0.02 {
		t.Fatalf("expected cy â‰ˆ 4.08, got %.2f", cy)
	}
}

func TestCentroidFromPolygonEmpty(t *testing.T) {
	_, _, err := CentroidFromPolygon(Polygon{})
	if err == nil {
		t.Fatal("expected error for empty polygon")
	}
}

func TestCentroidFromPolygonDegenerate(t *testing.T) {
	_, _, err := CentroidFromPolygon(Polygon{{X: 0, Y: 0}, {X: 1, Y: 0}})
	if err == nil {
		t.Fatal("expected error for degenerate polygon")
	}
}

func TestPolygonBBox(t *testing.T) {
	poly := Polygon{{X: 1, Y: 2}, {X: 5, Y: 2}, {X: 5, Y: 8}, {X: 1, Y: 8}}
	minX, minY, maxX, maxY := PolygonBBox(poly)
	if minX != 1 || minY != 2 || maxX != 5 || maxY != 8 {
		t.Fatalf("expected (1,2,5,8), got (%.0f,%.0f,%.0f,%.0f)", minX, minY, maxX, maxY)
	}
}

func TestPolygonBBoxEmpty(t *testing.T) {
	minX, _, _, _ := PolygonBBox(Polygon{})
	if minX != math.MaxFloat64 {
		t.Fatalf("expected MaxFloat64 for empty, got %f", minX)
	}
}

func TestPointInTriangle(t *testing.T) {
	tri := domain.Triangle{
		{X: 0, Y: 0},
		{X: 10, Y: 0},
		{X: 0, Y: 10},
	}
	if !PointInTriangle(3, 3, tri) {
		t.Error("point (3,3) should be inside triangle")
	}
	if PointInTriangle(50, 50, tri) {
		t.Error("point (50,50) should be outside triangle")
	}
}

func TestIsSimplePolygonValid(t *testing.T) {
	square := Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	if !IsSimplePolygon(square) {
		t.Error("square should be simple")
	}

	triangle := Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 5, Y: 10}}
	if !IsSimplePolygon(triangle) {
		t.Error("triangle should be simple")
	}

	concave := Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 8, Y: 10}, {X: 8, Y: 2}, {X: 2, Y: 2}, {X: 2, Y: 10}, {X: 0, Y: 10}}
	if !IsSimplePolygon(concave) {
		t.Error("concave U-shape should be simple")
	}
}

func TestIsSimplePolygonSelfIntersecting(t *testing.T) {
	bowtie := Polygon{{X: 0, Y: 0}, {X: 10, Y: 10}, {X: 10, Y: 0}, {X: 0, Y: 10}}
	if IsSimplePolygon(bowtie) {
		t.Error("bowtie/butterfly polygon should not be simple")
	}

	lessThan3 := Polygon{{X: 0, Y: 0}, {X: 1, Y: 1}}
	if IsSimplePolygon(lessThan3) {
		t.Error("polygon with < 3 points should not be simple")
	}
}

func TestIsSimplePolygonRejectsRepeatedVertex(t *testing.T) {
	poly := Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 10, Y: 0}, {X: 0, Y: 10}}
	if IsSimplePolygon(poly) {
		t.Error("polygon with repeated vertex should not be simple")
	}
}

func TestIsSimplePolygonRejectsOverlappingColinearEdges(t *testing.T) {
	poly := Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 3, Y: 0}, {X: 0, Y: 10}}
	if IsSimplePolygon(poly) {
		t.Error("polygon with overlapping colinear non-adjacent edges should not be simple")
	}
}

func TestTrianglesContainPoint(t *testing.T) {
	squareTris := Triangulate(Polygon{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}})
	if !TrianglesContainPoint(5, 5, squareTris) {
		t.Error("point (5,5) should be inside square")
	}
	if TrianglesContainPoint(50, 50, squareTris) {
		t.Error("point (50,50) should be outside square")
	}
}
