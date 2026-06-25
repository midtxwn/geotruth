package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/embedded"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/natspublish"
	"github.com/midtxwn/geotruth/pkg/natsquery"
)

func startServices(tb testing.TB, numFloors int) (*embedded.Services, natspublish.Publish, natsquery.Query) {
	tb.Helper()
	cfg := embedded.DefaultConfig
	deps := embedded.DefaultDependencies
	deps.Resolver = embedded.NewFlatResolver(numFloors)
	svc, err := embedded.Run(context.Background(), cfg, deps)
	if err != nil {
		tb.Fatalf("start services: %v", err)
	}
	tb.Cleanup(func() { svc.Shutdown() })
	nc := svc.NATSConn()
	return svc, natspublish.New(nc), natsquery.New(nc)
}

func TestEndToEnd_ObjectLifecycle(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	dims := domain.ObjectDimensions{Width: 2.0, Height: 3.0}
	if err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject failed: %v", err)
	}

	if err := pub.UpdateObjectPosition(ctx, "obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition failed: %v", err)
	}

	assertEventually(t, func() bool {
		nearby, err := query.NearbyObjects(ctx, "0", 5.0, 5.0, 10.0, nil)
		if err != nil {
			return false
		}
		for _, obj := range nearby {
			if obj.ID == "obj1" && obj.X == 5.0 && obj.Y == 5.0 {
				return true
			}
		}
		return false
	}, 5*time.Second)

	bounds, err := query.ObjectBounds(ctx, "obj1")
	if err != nil {
		t.Fatalf("ObjectBounds failed: %v", err)
	}
	if bounds == nil {
		t.Fatal("expected non-nil bounds")
	}
	if bounds.TL.X == bounds.BR.X || bounds.TL.Y == bounds.BR.Y {
		t.Error("bounds appear to be degenerate")
	}

	if err := pub.RemoveObject(ctx, "obj1"); err != nil {
		t.Fatalf("RemoveObject failed: %v", err)
	}

	assertEventually(t, func() bool {
		nearby, err := query.NearbyObjects(ctx, "0", 5.0, 5.0, 10.0, nil)
		if err != nil {
			return false
		}
		for _, obj := range nearby {
			if obj.ID == "obj1" {
				return false
			}
		}
		return true
	}, 5*time.Second)
}

func TestEndToEnd_QueryInvalidRegex(t *testing.T) {
	_, _, query := startServices(t, 1)

	ctx := context.Background()
	pattern := "["
	_, err := query.NearbyObjects(ctx, "0", 0, 0, 10.0, &pattern)
	if err == nil {
		t.Fatal("expected invalid regex error")
	}
}

func TestEndToEnd_AreaLifecycle(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	points := []domain.Point{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	if err := pub.RegisterArea(ctx, "zone-a", "0", points); err != nil {
		t.Fatalf("RegisterArea failed: %v", err)
	}

	assertEventually(t, func() bool {
		_, err := query.Area(ctx, "zone-a")
		return err == nil
	}, 5*time.Second)

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject failed: %v", err)
	}
	if err := pub.UpdateObjectPosition(ctx, "obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition failed: %v", err)
	}

	assertEventually(t, func() bool {
		areas, err := query.AreasAtPoint(ctx, "0", 5.0, 5.0, nil)
		if err != nil {
			return false
		}
		for _, area := range areas {
			if area.ID == "zone-a" {
				return true
			}
		}
		return false
	}, 5*time.Second)

	assertEventually(t, func() bool {
		objs, err := query.ObjectsWithinArea(ctx, "0", "zone-a", nil)
		if err != nil {
			return false
		}
		for _, obj := range objs {
			if obj.ID == "obj1" {
				return true
			}
		}
		return false
	}, 5*time.Second)

	if err := pub.RemoveArea(ctx, "zone-a"); err != nil {
		t.Fatalf("RemoveArea failed: %v", err)
	}

	assertEventually(t, func() bool {
		_, err := query.Area(ctx, "zone-a")
		return err != nil
	}, 5*time.Second)
}

func TestEndToEnd_NearbyQueries(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	positions := []struct {
		id string
		x  float64
		y  float64
	}{
		{"obj1", 0, 0},
		{"obj2", 5, 5},
		{"obj3", 10, 10},
		{"obj4", 100, 100},
	}

	for _, pos := range positions {
		dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
		err := pub.RegisterObject(ctx, pos.id, dims)
		if err != nil {
			t.Fatalf("RegisterObject %s failed: %v", pos.id, err)
		}
		err = pub.UpdateObjectPosition(ctx, pos.id, pos.x, pos.y, 1.0, 0.0)
		if err != nil {
			t.Fatalf("UpdateObjectPosition %s failed: %v", pos.id, err)
		}
	}

	assertEventually(t, func() bool {
		nearby, err := query.NearbyObjects(ctx, "0", 0, 0, 8.0, nil)
		if err != nil {
			return false
		}
		found := make(map[string]bool)
		for _, obj := range nearby {
			found[obj.ID] = true
		}
		return found["obj1"] && found["obj2"] && !found["obj3"] && !found["obj4"]
	}, 5*time.Second)

	nearbyOf, err := query.NearbyObjectsOf(ctx, "obj1", 8.0, nil)
	if err != nil {
		t.Fatalf("NearbyObjectsOf failed: %v", err)
	}
	found := make(map[string]bool)
	for _, obj := range nearbyOf {
		found[obj.ID] = true
	}
	if !found["obj2"] {
		t.Errorf("obj2 should be nearby obj1 within radius 8")
	}
}

func TestEndToEnd_IntersectionDetection(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	dims := domain.ObjectDimensions{Width: 2.0, Height: 2.0}

	if err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject obj1 failed: %v", err)
	}
	if err := pub.UpdateObjectPosition(ctx, "obj1", 0, 0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition obj1 failed: %v", err)
	}

	if err := pub.RegisterObject(ctx, "obj2", dims); err != nil {
		t.Fatalf("RegisterObject obj2 failed: %v", err)
	}
	if err := pub.UpdateObjectPosition(ctx, "obj2", 1, 1, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition obj2 failed: %v", err)
	}

	assertEventually(t, func() bool {
		intersecting, err := query.IntersectingObjects(ctx, "obj1", nil)
		if err != nil {
			return false
		}
		for _, obj := range intersecting {
			if obj.ID == "obj2" {
				return true
			}
		}
		return false
	}, 5*time.Second)

	if err := pub.UpdateObjectPosition(ctx, "obj2", 10, 10, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition obj2 failed: %v", err)
	}

	assertEventually(t, func() bool {
		intersecting, err := query.IntersectingObjects(ctx, "obj1", nil)
		if err != nil {
			return false
		}
		for _, obj := range intersecting {
			if obj.ID == "obj2" {
				return false
			}
		}
		return true
	}, 5*time.Second)
}

func TestEndToEnd_MultiFloor(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject failed: %v", err)
	}

	if err := pub.UpdateObjectPosition(ctx, "obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition failed: %v", err)
	}

	assertEventually(t, func() bool {
		nearby, err := query.NearbyObjects(ctx, "0", 5.0, 5.0, 10.0, nil)
		if err != nil {
			return false
		}
		for _, obj := range nearby {
			if obj.ID == "obj1" {
				return true
			}
		}
		return false
	}, 5*time.Second)

	nearby, err := query.NearbyObjects(ctx, "1", 5.0, 5.0, 10.0, nil)
	if err != nil {
		t.Fatalf("NearbyObjects on floor 1 failed: %v", err)
	}
	for _, obj := range nearby {
		if obj.ID == "obj1" {
			t.Error("obj1 should not be on floor 1")
		}
	}
}

func TestEndToEnd_AllQueries(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	area1Pts := []domain.Point{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	area2Pts := []domain.Point{{X: 20, Y: 20}, {X: 30, Y: 20}, {X: 30, Y: 30}, {X: 20, Y: 30}}

	if err := pub.RegisterArea(ctx, "zone-a", "0", area1Pts); err != nil {
		t.Fatalf("RegisterArea zone-a failed: %v", err)
	}
	if err := pub.RegisterArea(ctx, "zone-b", "0", area2Pts); err != nil {
		t.Fatalf("RegisterArea zone-b failed: %v", err)
	}

	assertEventually(t, func() bool {
		areaA, err := query.Area(ctx, "zone-a")
		if err != nil {
			return false
		}
		if areaA == nil || areaA.ID != "zone-a" {
			return false
		}
		areaB, err := query.Area(ctx, "zone-b")
		if err != nil {
			return false
		}
		return areaB != nil && areaB.ID == "zone-b"
	}, 5*time.Second)

	for i := 1; i <= 3; i++ {
		dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
		id := fmt.Sprintf("obj%d", i)
		if err := pub.RegisterObject(ctx, id, dims); err != nil {
			t.Fatalf("RegisterObject %s failed: %v", id, err)
		}
		if err := pub.UpdateObjectPosition(ctx, id, float64(i*5), float64(i*5), 1.0, 0.0); err != nil {
			t.Fatalf("UpdateObjectPosition %s failed: %v", id, err)
		}
	}

	assertEventually(t, func() bool {
		all, err := query.NearbyObjects(ctx, "0", 0, 0, 1000.0, nil)
		if err != nil {
			return false
		}
		return len(all) >= 3
	}, 5*time.Second)
}

func TestEndToEnd_RegionOf(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject failed: %v", err)
	}
	if err := pub.UpdateObjectPosition(ctx, "obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition failed: %v", err)
	}

	assertEventually(t, func() bool {
		region, err := query.RegionOf(ctx, "obj1")
		if err != nil {
			return false
		}
		return region == "0"
	}, 5*time.Second)
}

func TestEndToEnd_RegionFromPoint(t *testing.T) {
	_, _, query := startServices(t, 2)

	ctx := context.Background()

	region, err := query.RegionFromPoint(ctx, 5.0, 5.0, 1.0)
	if err != nil {
		t.Fatalf("RegionFromPoint z=1.0 failed: %v", err)
	}
	if region != "0" {
		t.Fatalf("expected region 0, got %s", region)
	}

	region, err = query.RegionFromPoint(ctx, 5.0, 5.0, 5.5)
	if err != nil {
		t.Fatalf("RegionFromPoint z=5.5 failed: %v", err)
	}
	if region != "1" {
		t.Fatalf("expected region 1, got %s", region)
	}
}

func TestEndToEnd_AllObjectsOriented(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject failed: %v", err)
	}
	if err := pub.UpdateObjectPosition(ctx, "obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition failed: %v", err)
	}

	assertEventually(t, func() bool {
		resp, err := query.AllObjectsOriented(ctx, nil)
		if err != nil {
			return false
		}
		if len(resp.Regions["0"]) < 1 {
			return false
		}
		for _, obj := range resp.Regions["0"] {
			if obj.ID == "obj1" && obj.Position.X == 5.0 && obj.Position.Y == 5.0 && obj.Position.Z == 1.0 {
				return true
			}
		}
		return false
	}, 5*time.Second)
}

func TestEndToEnd_AllObjects(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject failed: %v", err)
	}
	if err := pub.UpdateObjectPosition(ctx, "obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition failed: %v", err)
	}

	assertEventually(t, func() bool {
		resp, err := query.AllObjects(ctx, nil)
		if err != nil {
			return false
		}
		if len(resp.Regions["0"]) < 1 {
			return false
		}
		for _, obj := range resp.Regions["0"] {
			if obj.ID == "obj1" && obj.X == 5.0 && obj.Y == 5.0 && obj.Z == 1.0 {
				return true
			}
		}
		return false
	}, 5*time.Second)
}
