package flatresolver

import (
	"reflect"
	"testing"

	"github.com/midtxwn/geotruth/pkg/regionresolver"
)

func TestKnownRegions(t *testing.T) {
	resolver, err := New(3, 4.0, 0.2)
	if err != nil {
		t.Fatal(err)
	}

	got := resolver.KnownRegions()
	want := []string{"0", "1", "2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("KnownRegions() = %v, want %v", got, want)
	}
}

func TestResolveInitialPlacement(t *testing.T) {
	resolver, err := New(3, 4.0, 0.2)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		z    float64
		want string
	}{
		{name: "negative clamps to first floor", z: -1, want: "0"},
		{name: "first floor", z: 1, want: "0"},
		{name: "second floor", z: 4.5, want: "1"},
		{name: "above top clamps to last floor", z: 99, want: "2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolver.Resolve(0, 0, tt.z, regionresolver.NoPrevRegion)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("Resolve() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveHysteresis(t *testing.T) {
	resolver, err := New(2, 4.0, 0.2)
	if err != nil {
		t.Fatal(err)
	}

	prev := "0"
	got, err := resolver.Resolve(0, 0, 4.1, &prev)
	if err != nil {
		t.Fatal(err)
	}
	if got != "0" {
		t.Fatalf("Resolve() inside hysteresis band = %q, want %q", got, "0")
	}

	got, err = resolver.Resolve(0, 0, 4.3, &prev)
	if err != nil {
		t.Fatal(err)
	}
	if got != "1" {
		t.Fatalf("Resolve() beyond hysteresis band = %q, want %q", got, "1")
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name       string
		floors     int
		height     float64
		hysteresis float64
	}{
		{name: "zero floors", floors: 0, height: 4, hysteresis: 0.2},
		{name: "zero height", floors: 1, height: 0, hysteresis: 0.2},
		{name: "negative hysteresis", floors: 1, height: 4, hysteresis: -0.1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.floors, tt.height, tt.hysteresis); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
