package geo

import (
	"fmt"
	"math"
	"testing"

	"github.com/midtxwn/geotruth/pkg/domain"
)

var (
	benchIntersects bool
	benchTriangles  []domain.Triangle
	benchSimple     bool
)

func BenchmarkOBBIntersectsTriangle(b *testing.B) {
	cases := []struct {
		name string
		obb  OBB
		tri  domain.Triangle
	}{
		{
			name: "intersecting",
			obb:  NewOBB(0, 0, 0.5, 0.5, 0.25),
			tri:  domain.Triangle{{X: -0.25, Y: -0.25}, {X: 0.75, Y: 0}, {X: 0, Y: 0.75}},
		},
		{
			name: "separated",
			obb:  NewOBB(0, 0, 0.5, 0.5, 0.25),
			tri:  domain.Triangle{{X: 4, Y: 4}, {X: 5, Y: 4}, {X: 4, Y: 5}},
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchIntersects = tc.obb.IntersectsTriangle(tc.tri)
			}
		})
	}
}

func BenchmarkTriangulate(b *testing.B) {
	for _, vertices := range []int{8, 32, 128, 512} {
		poly := regularPolygon(vertices, 100)
		b.Run(fmt.Sprintf("%d_vertices", vertices), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchTriangles = Triangulate(poly)
			}
		})
	}
}

func BenchmarkIsSimplePolygon(b *testing.B) {
	for _, vertices := range []int{8, 32, 128, 512} {
		poly := regularPolygon(vertices, 100)
		b.Run(fmt.Sprintf("%d_vertices", vertices), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchSimple = IsSimplePolygon(poly)
			}
		})
	}
}

func regularPolygon(vertices int, radius float64) Polygon {
	poly := make(Polygon, vertices)
	for i := 0; i < vertices; i++ {
		angle := 2 * math.Pi * float64(i) / float64(vertices)
		poly[i] = domain.Point{
			X: math.Cos(angle) * radius,
			Y: math.Sin(angle) * radius,
		}
	}
	return poly
}
