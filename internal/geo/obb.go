package geo

import (
	"math"

	"github.com/midtxwn/geotruth/pkg/domain"
)

type OBB struct {
	CX, CY float64
	HW, HH float64
	Rot    float64
	cosRot float64
	sinRot float64
}

func NewOBB(cx, cy, hw, hh, rot float64) OBB {
	return OBB{
		CX: cx, CY: cy, HW: hw, HH: hh, Rot: rot,
		cosRot: math.Cos(rot),
		sinRot: math.Sin(rot),
	}
}

func (o OBB) AABB() (minX, minY, maxX, maxY float64) {
	corners := o.Corners()
	minX, minY = math.MaxFloat64, math.MaxFloat64
	maxX, maxY = -math.MaxFloat64, -math.MaxFloat64
	for _, c := range corners {
		if c.X < minX {
			minX = c.X
		}
		if c.X > maxX {
			maxX = c.X
		}
		if c.Y < minY {
			minY = c.Y
		}
		if c.Y > maxY {
			maxY = c.Y
		}
	}
	return
}

func (o OBB) center() domain.Point {
	return domain.Point{X: o.CX, Y: o.CY}
}

func (o OBB) axisX() domain.Point {
	return domain.Point{X: o.cosRot, Y: o.sinRot}
}

func (o OBB) axisY() domain.Point {
	return domain.Point{X: -o.sinRot, Y: o.cosRot}
}

func (o OBB) Corners() [4]domain.Point {
	cos, sin := o.cosRot, o.sinRot
	local := [4]domain.Point{
		{X: -o.HW, Y: -o.HH},
		{X: o.HW, Y: -o.HH},
		{X: o.HW, Y: o.HH},
		{X: -o.HW, Y: o.HH},
	}
	var corners [4]domain.Point
	for i, p := range local {
		corners[i] = domain.Point{
			X: o.CX + p.X*cos - p.Y*sin,
			Y: o.CY + p.X*sin + p.Y*cos,
		}
	}
	return corners
}

func (o OBB) IntersectsOBB(other OBB) bool {
	a, b := o, other
	axes := [4]domain.Point{
		a.axisX(),
		a.axisY(),
		b.axisX(),
		b.axisY(),
	}
	aCorners := a.Corners()
	bCorners := b.Corners()
	for _, axis := range axes {
		aMin, aMax := projectPoints4(aCorners, axis)
		bMin, bMax := projectPoints4(bCorners, axis)
		if aMax < bMin || bMax < aMin {
			return false
		}
	}
	return true
}

func (o OBB) ContainsPoint(px, py float64) bool {
	d := (domain.Point{X: px, Y: py}).Sub(o.center())
	localX := d.Dot(o.axisX())
	localY := d.Dot(o.axisY())
	return math.Abs(localX) <= o.HW && math.Abs(localY) <= o.HH
}

func (o OBB) IntersectsCircle(cx, cy, radius float64) bool {
	d := (domain.Point{X: cx, Y: cy}).Sub(o.center())
	local := domain.Point{
		X: d.Dot(o.axisX()),
		Y: d.Dot(o.axisY()),
	}

	closest := domain.Point{
		X: math.Max(-o.HW, math.Min(local.X, o.HW)),
		Y: math.Max(-o.HH, math.Min(local.Y, o.HH)),
	}
	delta := local.Sub(closest)
	return delta.Dot(delta) <= radius*radius
}

func projectPoints4(points [4]domain.Point, axis domain.Point) (float64, float64) {
	min, max := math.MaxFloat64, -math.MaxFloat64
	for _, p := range points {
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
