package geo

import (
	"fmt"
	"math"

	"github.com/midtxwn/geotruth/pkg/domain"
)

func CentroidFromPolygon(poly Polygon) (x, y float64, err error) {
	if len(poly) == 0 {
		return 0, 0, fmt.Errorf("empty polygon")
	}
	n := len(poly)
	if n < 3 {
		return 0, 0, fmt.Errorf("degenerate polygon")
	}

	var area2 float64
	var cx, cy float64
	for i := 0; i < n; i++ {
		j := (i + 1) % n
		cross := poly[i].Cross(poly[j])
		area2 += cross
		cx += (poly[i].X + poly[j].X) * cross
		cy += (poly[i].Y + poly[j].Y) * cross
	}

	if math.Abs(area2) < 1e-10 {
		var sx, sy float64
		for i := 0; i < n; i++ {
			sx += poly[i].X
			sy += poly[i].Y
		}
		return sx / float64(n), sy / float64(n), nil
	}
	return cx / (3 * area2), cy / (3 * area2), nil
}

// IsSimplePolygon checks that a polygon has no self-intersections.
// A simple polygon has no two non-consecutive edges that cross each other.
// The O(n^2) scan is acceptable because validation runs when areas are loaded.
func IsSimplePolygon(poly Polygon) bool {
	n := len(poly)
	if n < 3 {
		return false
	}
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if samePoint(poly[i], poly[j]) {
				return false
			}
		}
	}
	for i := 0; i < n; i++ {
		iNext := (i + 1) % n
		for j := i + 2; j < n; j++ {
			jNext := (j + 1) % n
			if i == jNext || iNext == j {
				continue
			}
			if segmentsIntersect(poly[i], poly[iNext], poly[j], poly[jNext]) {
				return false
			}
		}
	}
	return true
}

func samePoint(a, b domain.Point) bool {
	return a.X == b.X && a.Y == b.Y
}

func segmentsIntersect(a, b, c, d domain.Point) bool {
	d1 := d.Sub(c).Cross(a.Sub(c))
	d2 := d.Sub(c).Cross(b.Sub(c))
	d3 := b.Sub(a).Cross(c.Sub(a))
	d4 := b.Sub(a).Cross(d.Sub(a))
	if ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
		((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0)) {
		return true
	}
	if d1 == 0 && pointOnSegment(a, c, d) {
		return true
	}
	if d2 == 0 && pointOnSegment(b, c, d) {
		return true
	}
	if d3 == 0 && pointOnSegment(c, a, b) {
		return true
	}
	if d4 == 0 && pointOnSegment(d, a, b) {
		return true
	}
	return false
}

func pointOnSegment(p, a, b domain.Point) bool {
	return p.X >= math.Min(a.X, b.X) &&
		p.X <= math.Max(a.X, b.X) &&
		p.Y >= math.Min(a.Y, b.Y) &&
		p.Y <= math.Max(a.Y, b.Y)
}

func PolygonBBox(poly Polygon) (minX, minY, maxX, maxY float64) {
	minX, minY = math.MaxFloat64, math.MaxFloat64
	maxX, maxY = -math.MaxFloat64, -math.MaxFloat64
	for _, pt := range poly {
		if pt.X < minX {
			minX = pt.X
		}
		if pt.X > maxX {
			maxX = pt.X
		}
		if pt.Y < minY {
			minY = pt.Y
		}
		if pt.Y > maxY {
			maxY = pt.Y
		}
	}
	return
}
