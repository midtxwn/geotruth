package main

import (
	"reflect"
	"testing"

	"github.com/midtxwn/geotruth/pkg/regionresolver"
)

func TestGeoTruthResolverDefault(t *testing.T) {
	resolver, err := geotruthResolver()
	if err != nil {
		t.Fatal(err)
	}

	got, err := resolver.Resolve(1, 2, 3, regionresolver.NoPrevRegion)
	if err != nil {
		t.Fatal(err)
	}
	if got != "0" {
		t.Fatalf("Resolve() = %q, want %q", got, "0")
	}

	regions := resolver.KnownRegions()
	if want := []string{"0"}; !reflect.DeepEqual(regions, want) {
		t.Fatalf("KnownRegions() = %v, want %v", regions, want)
	}
}
