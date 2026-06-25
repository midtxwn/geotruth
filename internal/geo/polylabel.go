package geo

import (
	"container/heap"
	"math"

	"github.com/midtxwn/geotruth/pkg/domain"
)

func Polylabel(poly Polygon, precision float64) (px, py, distance float64) {
	if precision <= 0 {
		precision = 1.0
	}

	minX, minY, maxX, maxY := PolygonBBox(poly)
	width := maxX - minX
	height := maxY - minY
	cellSize := math.Min(width, height)
	if cellSize == 0 {
		return minX, minY, 0
	}
	half := cellSize / 2

	pq := &cellQueue{}
	for x := minX; x < maxX; x += cellSize {
		for y := minY; y < maxY; y += cellSize {
			heap.Push(pq, newCell(x+half, y+half, half, poly))
		}
	}

	cx, cy, _ := CentroidFromPolygon(poly)
	best := newCell(cx, cy, 0, poly)
	bboxCenter := newCell(minX+width/2, minY+height/2, 0, poly)
	if bboxCenter.d > best.d {
		best = bboxCenter
	}

	for pq.Len() > 0 {
		c := heap.Pop(pq).(cell)
		if c.d > best.d {
			best = c
		}
		if c.max-best.d <= precision {
			continue
		}
		q := c.half / 2
		heap.Push(pq, newCell(c.x-q, c.y-q, q, poly))
		heap.Push(pq, newCell(c.x+q, c.y-q, q, poly))
		heap.Push(pq, newCell(c.x-q, c.y+q, q, poly))
		heap.Push(pq, newCell(c.x+q, c.y+q, q, poly))
	}

	return best.x, best.y, best.d
}

type cell struct {
	x, y float64
	half float64
	d    float64
	max  float64
}

func newCell(x, y, half float64, poly Polygon) cell {
	d := pointToPolygonDist(x, y, poly)
	return cell{
		x:    x,
		y:    y,
		half: half,
		d:    d,
		max:  d + half*math.Sqrt2,
	}
}

type cellQueue []cell

func (q cellQueue) Len() int            { return len(q) }
func (q cellQueue) Less(i, j int) bool  { return q[i].max > q[j].max }
func (q cellQueue) Swap(i, j int)       { q[i], q[j] = q[j], q[i] }
func (q *cellQueue) Push(x interface{}) { *q = append(*q, x.(cell)) }
func (q *cellQueue) Pop() interface{} {
	old := *q
	n := len(old)
	c := old[n-1]
	*q = old[:n-1]
	return c
}

func pointToPolygonDist(px, py float64, poly []domain.Point) float64 {
	inside := false
	minDist := math.Inf(1)
	n := len(poly)

	for i, j := 0, n-1; i < n; j, i = i, i+1 {
		a, b := poly[i], poly[j]

		if (a.Y > py) != (b.Y > py) &&
			px < (b.X-a.X)*(py-a.Y)/(b.Y-a.Y)+a.X {
			inside = !inside
		}

		d := distanceSquaredPointToSegment(px, py, a, b)
		if d < minDist {
			minDist = d
		}
	}

	dist := math.Sqrt(minDist)
	if inside {
		return dist
	}
	return -dist
}
