package main

import "github.com/midtxwn/geotruth/pkg/regionresolver"

// geotruthResolver is the standalone binary's resolver hook. The checked-in
// default is intentionally minimal: all points belong to one region named "0"
// so the binary can run for smoke tests and local development.
//
// Real deployments should usually start GeoTruth from their own Go process via
// embedded.RunGeoTruth and inject the domain resolver there, instead of editing
// this local runner.
func geotruthResolver() (regionresolver.Resolver, error) {
	return oneRegionResolver{}, nil
}

type oneRegionResolver struct{}

func (oneRegionResolver) Resolve(_, _, _ float64, _ *string) (string, error) {
	return "0", nil
}

func (oneRegionResolver) KnownRegions() []string {
	return []string{"0"}
}
