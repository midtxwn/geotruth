package domain

import (
	"math"
)

type Point = Vec2

type Vec2 struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

func (a Vec2) Add(b Vec2) Vec2      { return Vec2{a.X + b.X, a.Y + b.Y} }
func (a Vec2) Sub(b Vec2) Vec2      { return Vec2{a.X - b.X, a.Y - b.Y} }
func (a Vec2) Scale(s float64) Vec2 { return Vec2{a.X * s, a.Y * s} }
func (a Vec2) Dot(b Vec2) float64   { return a.X*b.X + a.Y*b.Y }
func (a Vec2) Cross(b Vec2) float64 { return a.X*b.Y - a.Y*b.X }
func (a Vec2) Length() float64      { return math.Sqrt(a.Dot(a)) }
func (a Vec2) Normalize() Vec2 {
	l := a.Length()
	if l == 0 {
		return Vec2{}
	}
	return a.Scale(1 / l)
}
