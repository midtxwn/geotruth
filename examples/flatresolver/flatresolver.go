// Package flatresolver contains a copyable example implementation of
// regionresolver.Resolver for flat, Z-sliced regions.
//
// This package is not imported by the production binary. Adapt or copy this
// code into cmd/geotruth/resolver.go when flat floor behavior is appropriate
// for a deployment.
package flatresolver

import (
	"fmt"
	"strconv"

	"github.com/midtxwn/geotruth/pkg/regionresolver"
)

type Resolver struct {
	floorHeight float64
	hysteresis  float64
	floors      int
}

func New(floors int, floorHeight float64, hysteresis float64) (*Resolver, error) {
	if floors <= 0 {
		return nil, fmt.Errorf("floors must be positive")
	}
	if floorHeight <= 0 {
		return nil, fmt.Errorf("floor height must be positive")
	}
	if hysteresis < 0 {
		return nil, fmt.Errorf("hysteresis must be non-negative")
	}
	return &Resolver{
		floorHeight: floorHeight,
		hysteresis:  hysteresis,
		floors:      floors,
	}, nil
}

func (r *Resolver) Resolve(_, _, z float64, prevRegion *string) (string, error) {
	if z < 0 {
		z = 0
	}
	naiveFloor := int(z / r.floorHeight)
	if naiveFloor >= r.floors {
		naiveFloor = r.floors - 1
	}

	if prevRegion == regionresolver.NoPrevRegion {
		return strconv.Itoa(naiveFloor), nil
	}

	prev, err := strconv.Atoi(*prevRegion)
	if err != nil {
		return strconv.Itoa(naiveFloor), nil
	}

	base := float64(prev) * r.floorHeight
	if z > base+r.floorHeight+r.hysteresis && prev < r.floors-1 {
		return strconv.Itoa(prev + 1), nil
	}
	if z < base-r.hysteresis && prev > 0 {
		return strconv.Itoa(prev - 1), nil
	}
	return *prevRegion, nil
}

func (r *Resolver) KnownRegions() []string {
	regions := make([]string, r.floors)
	for i := 0; i < r.floors; i++ {
		regions[i] = strconv.Itoa(i)
	}
	return regions
}
