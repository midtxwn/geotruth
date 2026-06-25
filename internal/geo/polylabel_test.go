package geo

import (
	"math"
	"testing"

	"github.com/midtxwn/geotruth/pkg/domain"
)

func TestPolylabelSquare(t *testing.T) {
	poly := Polygon{
		{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10},
	}
	px, py, dist := Polylabel(poly, 0.01)

	if math.Abs(px-5) > 0.1 || math.Abs(py-5) > 0.1 {
		t.Errorf("expected ~(5,5) for square, got (%.2f, %.2f)", px, py)
	}
	if dist < 4.9 {
		t.Errorf("expected distance ~5 from boundary, got %.2f", dist)
	}
}

func TestPolylabelTriangle(t *testing.T) {
	poly := Polygon{
		{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 5, Y: 10},
	}
	px, py, dist := Polylabel(poly, 0.01)

	if !TrianglesContainPoint(px, py, Triangulate(poly)) {
		t.Errorf("polylabel (%.2f, %.2f) not inside triangle", px, py)
	}
	if dist <= 0 {
		t.Errorf("expected positive distance for interior point, got %.2f", dist)
	}
}

func TestPolylabelConcaveLShape(t *testing.T) {
	poly := Polygon{
		{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 5},
		{X: 5, Y: 5}, {X: 5, Y: 10}, {X: 0, Y: 10},
	}
	px, py, dist := Polylabel(poly, 0.01)

	if !TrianglesContainPoint(px, py, Triangulate(poly)) {
		t.Errorf("polylabel (%.2f, %.2f) not inside L-shape", px, py)
	}
	if dist <= 0 {
		t.Errorf("expected positive distance for interior point, got %.2f", dist)
	}

	cx, cy, _ := CentroidFromPolygon(poly)
	centroidInside := TrianglesContainPoint(cx, cy, Triangulate(poly))
	if centroidInside {
		t.Log("centroid happens to be inside this L-shape (not always the case)")
	} else {
		t.Logf("centroid (%.2f, %.2f) is outside L-shape as expected - polylabel is the interior alternative", cx, cy)
	}
}

func TestPolylabelUShape(t *testing.T) {
	poly := Polygon{
		{X: 0, Y: 0}, {X: 3, Y: 0}, {X: 3, Y: 8},
		{X: 7, Y: 8}, {X: 7, Y: 0}, {X: 10, Y: 0},
		{X: 10, Y: 10}, {X: 0, Y: 10},
	}
	px, py, dist := Polylabel(poly, 0.01)

	if !TrianglesContainPoint(px, py, Triangulate(poly)) {
		t.Errorf("polylabel (%.2f, %.2f) not inside U-shape", px, py)
	}
	if dist <= 0 {
		t.Errorf("expected positive distance, got %.2f", dist)
	}
	if py < 5 {
		t.Errorf("expected polylabel in the bottom of the U (y<5), got y=%.2f - should be in one of the arms", py)
	}
}

func TestPolylabelDegenerate(t *testing.T) {
	poly := Polygon{
		{X: 5, Y: 5}, {X: 5, Y: 5}, {X: 5, Y: 5},
	}
	px, py, dist := Polylabel(poly, 0.01)

	if px != 5 || py != 5 {
		t.Errorf("expected (5,5) for degenerate polygon, got (%.2f, %.2f)", px, py)
	}
	if dist != 0 {
		t.Errorf("expected 0 distance for degenerate polygon, got %.2f", dist)
	}
}

func TestPolylabelPrecisionConvergence(t *testing.T) {
	poly := Polygon{
		{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10},
	}
	px1, py1, _ := Polylabel(poly, 1.0)
	px2, py2, _ := Polylabel(poly, 0.01)

	if math.Abs(px1-px2) > 1.0 || math.Abs(py1-py2) > 1.0 {
		t.Errorf("precision 1.0 (%.2f, %.2f) and 0.01 (%.2f, %.2f) should be close", px1, py1, px2, py2)
	}
}

func TestPolylabelConcaveCentroidOutside(t *testing.T) {
	poly := Polygon{
		{X: 0, Y: 0}, {X: 30, Y: 0}, {X: 30, Y: 4},
		{X: 8, Y: 4}, {X: 8, Y: 26}, {X: 30, Y: 26},
		{X: 30, Y: 30}, {X: 0, Y: 30},
	}
	px, py, dist := Polylabel(poly, 0.01)

	if !TrianglesContainPoint(px, py, Triangulate(poly)) {
		t.Errorf("polylabel (%.2f, %.2f) not inside", px, py)
	}
	if dist <= 0 {
		t.Errorf("expected positive distance, got %.2f", dist)
	}

	cx, cy, _ := CentroidFromPolygon(poly)
	if TrianglesContainPoint(cx, cy, Triangulate(poly)) {
		t.Errorf("centroid (%.2f, %.2f) should be outside this C-shape but was inside", cx, cy)
	}
}

func TestPolylabelPentagon(t *testing.T) {
	poly := Polygon{
		{X: 5, Y: 0}, {X: 10, Y: 3.5}, {X: 8, Y: 9},
		{X: 2, Y: 9}, {X: 0, Y: 3.5},
	}
	px, py, dist := Polylabel(poly, 0.01)

	if !TrianglesContainPoint(px, py, Triangulate(poly)) {
		t.Errorf("polylabel (%.2f, %.2f) not inside pentagon", px, py)
	}
	if dist <= 0 {
		t.Errorf("expected positive distance, got %.2f", dist)
	}
}

func TestPointToPolygonDist(t *testing.T) {
	square := []domain.Point{
		{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10},
	}

	d := pointToPolygonDist(5, 5, square)
	if d < 4.9 || d > 5.1 {
		t.Errorf("expected ~5 for center of unit square, got %.2f", d)
	}

	d = pointToPolygonDist(-1, 5, square)
	if d >= 0 {
		t.Errorf("expected negative distance for outside point, got %.2f", d)
	}

	d = pointToPolygonDist(0, 0, square)
	if d != 0 {
		t.Errorf("expected 0 for vertex, got %.2f", d)
	}
}
