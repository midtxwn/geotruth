package geo

import (
	"math"

	"github.com/midtxwn/geotruth/pkg/domain"
)

type Polygon []domain.Point

func Triangulate(poly Polygon) []domain.Triangle {
	n := len(poly)
	if n < 3 {
		return nil
	}
	if n == 3 {
		return []domain.Triangle{{poly[0], poly[1], poly[2]}}
	}

	signedArea := 0.0
	for i := 0; i < n; i++ {
		j := (i + 1) % n
		signedArea += poly[i].Cross(poly[j])
	}

	indices := make([]int, n)
	if signedArea > 0 {
		for i := 0; i < n; i++ {
			indices[i] = i
		}
	} else {
		for i := 0; i < n; i++ {
			indices[i] = n - 1 - i
		}
	}

	var result []domain.Triangle
	remaining := n
	count := 2 * remaining

	for remaining > 3 {
		if count <= 0 {
			break
		}
		count--

		prevIdx := indices[(remaining-1)%remaining]
		currIdx := indices[0]
		nextIdx := indices[1]

		if isEar(poly, indices, remaining, prevIdx, currIdx, nextIdx) {
			result = append(result, domain.Triangle{
				poly[prevIdx],
				poly[currIdx],
				poly[nextIdx],
			})
			copy(indices, indices[1:remaining])
			remaining--
			count = 2 * remaining
		} else {
			first := indices[0]
			copy(indices, indices[1:remaining])
			indices[remaining-1] = first
		}
	}

	if remaining == 3 {
		result = append(result, domain.Triangle{
			poly[indices[0]],
			poly[indices[1]],
			poly[indices[2]],
		})
	}

	return result
}

func isEar(poly Polygon, indices []int, n, a, b, c int) bool {
	ab := poly[b].Sub(poly[a])
	ac := poly[c].Sub(poly[a])
	if ab.Cross(ac) <= 0 {
		return false
	}

	for i := 0; i < n; i++ {
		idx := indices[i]
		if idx == a || idx == b || idx == c {
			continue
		}
		if pointInTriangle(poly[idx],
			poly[a], poly[b], poly[c]) {
			return false
		}
	}
	return true
}

func pointInTriangle(p, a, b, c domain.Point) bool {
	d1 := triCross(p, a, b)
	d2 := triCross(p, b, c)
	d3 := triCross(p, c, a)

	hasNeg := (d1 < 0) || (d2 < 0) || (d3 < 0)
	hasPos := (d1 > 0) || (d2 > 0) || (d3 > 0)

	return !(hasNeg && hasPos)
}

func triCross(p, a, b domain.Point) float64 {
	return b.Sub(a).Cross(p.Sub(a))
}

func PointInTriangle(px, py float64, tri domain.Triangle) bool {
	p := domain.Point{X: px, Y: py}
	a := tri[0]
	b := tri[1]
	c := tri[2]
	return pointInTriangle(p, a, b, c)
}

func TrianglesContainPoint(px, py float64, tris []domain.Triangle) bool {
	for _, tri := range tris {
		if PointInTriangle(px, py, tri) {
			return true
		}
	}
	return false
}

func TriangleIntersectsCircle(tri domain.Triangle, cx, cy, radius float64) bool {
	if PointInTriangle(cx, cy, tri) {
		return true
	}

	r2 := radius * radius
	center := domain.Point{X: cx, Y: cy}
	for _, pt := range tri {
		delta := pt.Sub(center)
		if delta.Dot(delta) <= r2 {
			return true
		}
	}

	for i := 0; i < 3; i++ {
		a := tri[i]
		b := tri[(i+1)%3]
		if distanceSquaredPointToSegment(cx, cy, a, b) <= r2 {
			return true
		}
	}

	return false
}

func TrianglesIntersectCircle(tris []domain.Triangle, cx, cy, radius float64) bool {
	for _, tri := range tris {
		if TriangleIntersectsCircle(tri, cx, cy, radius) {
			return true
		}
	}
	return false
}

func distanceSquaredPointToSegment(px, py float64, a, b domain.Point) float64 {
	p := domain.Point{X: px, Y: py}
	ab := b.Sub(a)
	ap := p.Sub(a)
	len2 := ab.Dot(ab)
	if len2 == 0 {
		return ap.Dot(ap)
	}

	t := ap.Dot(ab) / len2
	t = math.Max(0, math.Min(1, t))
	closest := a.Add(ab.Scale(t))
	delta := p.Sub(closest)
	return delta.Dot(delta)
}

func (o OBB) IntersectsTriangle(tri domain.Triangle) bool {
	obbAxes := [2]domain.Point{
		o.axisX(),
		o.axisY(),
	}
	obbCorners := o.Corners()

	for _, axis := range obbAxes {
		aMin, aMax := projectPoints4(obbCorners, axis)
		bMin, bMax := projectTriangle(tri, axis)
		if aMax < bMin || bMax < aMin {
			return false
		}
	}

	for i := 0; i < 3; i++ {
		edge := tri[(i+1)%3].Sub(tri[i])
		axis := domain.Point{X: -edge.Y, Y: edge.X}
		if axis.X == 0 && axis.Y == 0 {
			continue
		}
		aMin, aMax := projectPoints4(obbCorners, axis)
		bMin, bMax := projectTriangle(tri, axis)
		if aMax < bMin || bMax < aMin {
			return false
		}
	}

	return true
}

func projectTriangle(tri domain.Triangle, axis domain.Point) (float64, float64) {
	min, max := math.MaxFloat64, -math.MaxFloat64
	for _, p := range tri {
		proj := p.Dot(axis)
		if proj < min {
			min = proj
		}
		if proj > max {
			max = proj
		}
	}
	return min, max
}

func (o OBB) IntersectsTriangles(tris []domain.Triangle) bool {
	for _, tri := range tris {
		if o.IntersectsTriangle(tri) {
			return true
		}
	}
	return false
}
