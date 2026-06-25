package main

import "github.com/midtxwn/geotruth/pkg/regionresolver"

// geotruthResolver is the production resolver customization point for this
// binary. Replace this function to provide the deployment's real spatial
// topology. The checked-in default is intentionally minimal: all points belong
// to one region named "0" so the binary can run before a custom resolver is
// installed.
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
