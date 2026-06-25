package engine

import (
	"testing"

	"github.com/midtxwn/geotruth/internal/gtevents"
	"github.com/midtxwn/geotruth/pkg/domain"
)

type fixedResolver []string

func (r fixedResolver) Resolve(_, _, _ float64, _ *string) (string, error) {
	return r[0], nil
}

func (r fixedResolver) KnownRegions() []string {
	return []string(r)
}

func TestEngineDirectObjectHelpers(t *testing.T) {
	e := NewEngine(fixedResolver{"0"})
	dims := domain.ObjectDimensions{Width: 2, Height: 3}

	e.RegisterObject("obj1", dims)
	if e.ObjectCount() != 1 {
		t.Fatalf("ObjectCount = %d, want 1", e.ObjectCount())
	}
	if got := DimsFromPublic(dims); got != (gtevents.EventDims{Width: 2, Height: 3}) {
		t.Fatalf("DimsFromPublic = %+v", got)
	}

	if !e.BootstrapPlaceObject("obj1", 5, 6, 1, 0.25, "0") {
		t.Fatal("BootstrapPlaceObject returned false")
	}
	e.regions["0"].prevInside["obj1"] = map[string]bool{"zone-a": true}

	transitions := e.DirectRemoveObject("obj1")
	if len(transitions) != 1 || transitions[0].AreaID != "zone-a" || transitions[0].Entered {
		t.Fatalf("DirectRemoveObject transitions = %+v", transitions)
	}
	if e.ObjectCount() != 0 {
		t.Fatalf("ObjectCount after remove = %d, want 0", e.ObjectCount())
	}
	if got := e.DirectRemoveObject("missing"); got != nil {
		t.Fatalf("DirectRemoveObject missing = %+v, want nil", got)
	}
}
