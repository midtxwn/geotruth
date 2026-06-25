package natsquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/messages"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func startEmbeddedNATS(tb testing.TB) *nats.Conn {
	tb.Helper()
	opts := &natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  tb.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		tb.Fatalf("start nats: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		tb.Fatal("nats server not ready")
	}
	tb.Cleanup(func() { s.Shutdown() })

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		tb.Fatalf("nats connect: %v", err)
	}
	tb.Cleanup(func() { nc.Drain() })
	return nc
}

func TestQueryNew(t *testing.T) {
	nc := startEmbeddedNATS(t)

	q := New(nc)
	if q.nc == nil {
		t.Fatal("expected non-nil nats connection in Query")
	}
}

func TestNearbyObjects_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	expectedObjs := []Object{
		{ID: "obj1", Region: stringPtr("0"), X: 1.0, Y: 2.0, Z: 0.0},
		{ID: "obj2", Region: stringPtr("0"), X: 2.0, Y: 3.0, Z: 0.0},
	}

	sub, err := nc.Subscribe(QueryNearby, func(msg *nats.Msg) {
		var req NearbyReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		if req.Region != "0" || req.X != 5.0 || req.Y != 5.0 || req.RadiusMeters != 10.0 {
			t.Logf("unexpected request: %+v", req)
		}
		_ = msg.Respond(messages.OKDataResp(expectedObjs))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	objs, err := q.NearbyObjects(ctx, "0", 5.0, 5.0, 10.0, nil)

	if err != nil {
		t.Fatalf("NearbyObjects failed: %v", err)
	}
	if len(objs) != 2 {
		t.Errorf("expected 2 objects, got %d", len(objs))
	}
	if objs[0].ID != "obj1" {
		t.Errorf("expected obj1 first, got %s", objs[0].ID)
	}
}

func TestNearbyObjects_WithRegex(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()
	pattern := "^obj-"

	sub, err := nc.Subscribe(QueryNearby, func(msg *nats.Msg) {
		var req NearbyReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		if req.Regex == nil || *req.Regex != pattern {
			t.Logf("expected regex %q, got %#v", pattern, req.Regex)
			return
		}
		_ = msg.Respond(messages.OKDataResp([]Object{}))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	if _, err := q.NearbyObjects(ctx, "0", 5.0, 5.0, 10.0, &pattern); err != nil {
		t.Fatalf("NearbyObjects failed: %v", err)
	}
}

func TestRegexOmittedWhenNil(t *testing.T) {
	data, err := json.Marshal(NearbyReq{Region: "1", X: 2, Y: 3, RadiusMeters: 4})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := fields["regex"]; ok {
		t.Fatalf("expected regex to be omitted for nil pointer, got %s", data)
	}
}

func TestNearbyObjects_EmptyResult(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	sub, err := nc.Subscribe(QueryNearby, func(msg *nats.Msg) {
		_ = msg.Respond(messages.OKDataResp([]Object{}))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	objs, err := q.NearbyObjects(ctx, "0", 0.0, 0.0, 1.0, nil)

	if err != nil {
		t.Fatalf("NearbyObjects failed: %v", err)
	}
	if len(objs) != 0 {
		t.Errorf("expected empty result, got %d objects", len(objs))
	}
}

func TestNearbyObjectsOf_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	expected := []ObjectOriented{
		{
			ID:       "obj2",
			Bounds:   OrientedBounds{TL: domain.Point{X: 0, Y: 0}, TR: domain.Point{X: 1, Y: 0}, BR: domain.Point{X: 1, Y: 1}, BL: domain.Point{X: 0, Y: 1}},
			Position: Position3D{X: 5.0, Y: 5.0, Z: 1.0},
		},
	}

	sub, err := nc.Subscribe(QueryNearbyOf, func(msg *nats.Msg) {
		var req NearbyOfReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		if req.ObjectID != "obj1" || req.RadiusMeters != 5.0 {
			t.Logf("unexpected request: %+v", req)
		}
		_ = msg.Respond(messages.OKDataResp(expected))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	objs, err := q.NearbyObjectsOf(ctx, "obj1", 5.0, nil)

	if err != nil {
		t.Fatalf("NearbyObjectsOf failed: %v", err)
	}
	if len(objs) != 1 {
		t.Errorf("expected 1 object, got %d", len(objs))
	}
	if objs[0].ID != "obj2" {
		t.Errorf("expected obj2, got %s", objs[0].ID)
	}
	if objs[0].Position.Z != 1.0 {
		t.Errorf("expected Position.Z=1.0, got %f", objs[0].Position.Z)
	}
}

func TestNearbyObjectsOf_ServerError(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	sub, err := nc.Subscribe(QueryNearbyOf, func(msg *nats.Msg) {
		_ = msg.Respond(messages.ErrResp(errNotFound("object not found")))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	_, err = q.NearbyObjectsOf(ctx, "nonexistent", 5.0, nil)

	if err == nil {
		t.Error("expected error for non-existent object")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestAllObjects_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	expected := AllObjectsResp{
		Regions: map[string][]Object{
			"0": {{ID: "obj1", Region: stringPtr("0"), X: 5.0, Y: 5.0, Z: 1.0}},
			"1": {{ID: "obj2", Region: stringPtr("1"), X: 10.0, Y: 10.0, Z: 4.0}},
		},
	}

	sub, err := nc.Subscribe(QueryAllObjects, func(msg *nats.Msg) {
		_ = msg.Respond(messages.OKDataResp(expected))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	result, err := q.AllObjects(ctx, nil)

	if err != nil {
		t.Fatalf("AllObjects failed: %v", err)
	}
	if len(result.Regions["0"]) != 1 || result.Regions["0"][0].ID != "obj1" {
		t.Errorf("unexpected region 0 objects: %+v", result.Regions["0"])
	}
	if len(result.Regions["1"]) != 1 || result.Regions["1"][0].ID != "obj2" {
		t.Errorf("unexpected region 1 objects: %+v", result.Regions["1"])
	}
}

func TestAllObjectsOriented_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	expected := AllObjectsOrientedResp{
		Regions: map[string][]ObjectOriented{
			"0": {{
				ID:       "obj1",
				Bounds:   OrientedBounds{TL: domain.Point{X: 4, Y: 4}, TR: domain.Point{X: 6, Y: 4}, BR: domain.Point{X: 6, Y: 6}, BL: domain.Point{X: 4, Y: 6}},
				Position: Position3D{X: 5.0, Y: 5.0, Z: 1.0},
			}},
		},
	}

	sub, err := nc.Subscribe(QueryAllObjectsOriented, func(msg *nats.Msg) {
		_ = msg.Respond(messages.OKDataResp(expected))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	result, err := q.AllObjectsOriented(ctx, nil)

	if err != nil {
		t.Fatalf("AllObjectsOriented failed: %v", err)
	}
	if len(result.Regions["0"]) != 1 {
		t.Fatalf("expected 1 object on region 0, got %d", len(result.Regions["0"]))
	}
	obj := result.Regions["0"][0]
	if obj.ID != "obj1" {
		t.Errorf("expected obj1, got %s", obj.ID)
	}
	if obj.Position.X != 5.0 || obj.Position.Y != 5.0 || obj.Position.Z != 1.0 {
		t.Errorf("expected position (5,5,1), got (%v,%v,%v)", obj.Position.X, obj.Position.Y, obj.Position.Z)
	}
	if obj.Bounds.TL.X != 4 || obj.Bounds.BR.X != 6 {
		t.Errorf("unexpected bounds: %+v", obj.Bounds)
	}
}

func TestRegionOf_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	sub, err := nc.Subscribe(QueryRegionOf, func(msg *nats.Msg) {
		var req RegionOfReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			_ = msg.Respond(messages.ErrResp(err))
			return
		}
		var region string
		switch req.ObjectID {
		case "obj1":
			region = "0"
		case "obj2":
			region = "1"
		default:
			_ = msg.Respond(messages.ErrResp(errNotFound("object %s not found", req.ObjectID)))
			return
		}
		_ = msg.Respond(messages.OKDataResp(RegionOfResp{Region: region}))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	region, err := q.RegionOf(ctx, "obj1")

	if err != nil {
		t.Fatalf("RegionOf failed: %v", err)
	}
	if region != "0" {
		t.Errorf("expected region 0, got %s", region)
	}

	region, err = q.RegionOf(ctx, "obj2")
	if err != nil {
		t.Fatalf("RegionOf failed: %v", err)
	}
	if region != "1" {
		t.Errorf("expected region 1, got %s", region)
	}
}

func TestRegionOf_NotFound(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	sub, err := nc.Subscribe(QueryRegionOf, func(msg *nats.Msg) {
		_ = msg.Respond(messages.ErrResp(errNotFound("object nonexistent not found")))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	_, err = q.RegionOf(ctx, "nonexistent")

	if err == nil {
		t.Error("expected error for non-existent object")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestRegionFromPoint_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	sub, err := nc.Subscribe(QueryRegionFromPoint, func(msg *nats.Msg) {
		var req RegionFromPointReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			_ = msg.Respond(messages.ErrResp(err))
			return
		}
		if req.X != 5 || req.Y != 6 || req.Z != 7 {
			_ = msg.Respond(messages.ErrResp(fmt.Errorf("unexpected request: %+v", req)))
			return
		}
		_ = msg.Respond(messages.OKDataResp(RegionOfResp{Region: "north"}))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	region, err := q.RegionFromPoint(ctx, 5, 6, 7)
	if err != nil {
		t.Fatalf("RegionFromPoint failed: %v", err)
	}
	if region != "north" {
		t.Errorf("expected region north, got %s", region)
	}
}

func TestNearbyAreas_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	expected := []Area{
		{ID: "area1", Region: "0"},
		{ID: "area2", Region: "0"},
	}

	sub, err := nc.Subscribe(QueryNearbyAreas, func(msg *nats.Msg) {
		var req NearbyAreasReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		if req.Region != "0" || req.X != 5.0 || req.Y != 5.0 || req.RadiusMeters != 20.0 {
			t.Logf("unexpected request: %+v", req)
		}
		_ = msg.Respond(messages.OKDataResp(expected))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	areas, err := q.NearbyAreas(ctx, "0", 5.0, 5.0, 20.0, nil)

	if err != nil {
		t.Fatalf("NearbyAreas failed: %v", err)
	}
	if len(areas) != 2 {
		t.Errorf("expected 2 areas, got %d", len(areas))
	}
}

func TestObjectsWithinArea_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	expected := []Object{
		{ID: "obj1", Region: stringPtr("0"), X: 5.0, Y: 5.0, Z: 0.0},
	}

	sub, err := nc.Subscribe(QueryWithinArea, func(msg *nats.Msg) {
		var req WithinAreaReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		if req.Region != "0" || req.AreaID != "zone-a" {
			t.Logf("unexpected request: %+v", req)
		}
		_ = msg.Respond(messages.OKDataResp(expected))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	objs, err := q.ObjectsWithinArea(ctx, "0", "zone-a", nil)

	if err != nil {
		t.Fatalf("ObjectsWithinArea failed: %v", err)
	}
	if len(objs) != 1 || objs[0].ID != "obj1" {
		t.Errorf("unexpected result: %+v", objs)
	}
}

func TestAreasAtPoint_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	expected := []Area{
		{ID: "zone-a", Region: "0"},
	}

	sub, err := nc.Subscribe(QueryAreasAtPoint, func(msg *nats.Msg) {
		var req AreasAtPointReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		if req.Region != "0" || req.X != 5.0 || req.Y != 5.0 {
			t.Logf("unexpected request: %+v", req)
		}
		_ = msg.Respond(messages.OKDataResp(expected))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	areas, err := q.AreasAtPoint(ctx, "0", 5.0, 5.0, nil)

	if err != nil {
		t.Fatalf("AreasAtPoint failed: %v", err)
	}
	if len(areas) != 1 || areas[0].ID != "zone-a" {
		t.Errorf("unexpected result: %+v", areas)
	}
}

func TestAreasContainingObject_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	expected := []Area{
		{ID: "zone-a", Region: "0"},
		{ID: "zone-b", Region: "0"},
	}

	sub, err := nc.Subscribe(QueryAreasContainingObj, func(msg *nats.Msg) {
		var req AreasContainingReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		if req.ObjectID != "obj1" {
			t.Logf("unexpected request: %+v", req)
		}
		_ = msg.Respond(messages.OKDataResp(expected))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	areas, err := q.AreasContainingObject(ctx, "obj1", nil)

	if err != nil {
		t.Fatalf("AreasContainingObject failed: %v", err)
	}
	if len(areas) != 2 {
		t.Errorf("expected 2 areas, got %d", len(areas))
	}
}

func TestIntersectingObjects_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	expected := []Object{
		{ID: "obj2", Region: stringPtr("0"), X: 10.0, Y: 10.0, Z: 0.0},
	}

	sub, err := nc.Subscribe(QueryIntersecting, func(msg *nats.Msg) {
		var req IntersectingReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		if req.ObjectID != "obj1" {
			t.Logf("unexpected request: %+v", req)
		}
		_ = msg.Respond(messages.OKDataResp(expected))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	objs, err := q.IntersectingObjects(ctx, "obj1", nil)

	if err != nil {
		t.Fatalf("IntersectingObjects failed: %v", err)
	}
	if len(objs) != 1 || objs[0].ID != "obj2" {
		t.Errorf("unexpected result: %+v", objs)
	}
}

func TestObjectBounds_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	expected := OrientedBounds{
		TL: domain.Point{X: 0, Y: 0},
		TR: domain.Point{X: 2, Y: 0},
		BR: domain.Point{X: 2, Y: 3},
		BL: domain.Point{X: 0, Y: 3},
	}

	sub, err := nc.Subscribe(QueryObjectBounds, func(msg *nats.Msg) {
		var req BoundsReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		if req.ObjectID != "obj1" {
			t.Logf("unexpected request: %+v", req)
		}
		_ = msg.Respond(messages.OKDataResp(expected))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	bounds, err := q.ObjectBounds(ctx, "obj1")

	if err != nil {
		t.Fatalf("ObjectBounds failed: %v", err)
	}
	if bounds == nil {
		t.Fatal("expected non-nil bounds")
	}
	if bounds.TL.X != 0 || bounds.TL.Y != 0 {
		t.Errorf("unexpected TL: %+v", bounds.TL)
	}
	if bounds.BR.X != 2 || bounds.BR.Y != 3 {
		t.Errorf("unexpected BR: %+v", bounds.BR)
	}
}

func TestArea_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	expected := Area{
		ID:        "zone-a",
		Region:    "0",
		Triangles: []domain.Triangle{{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}}, {{X: 0, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}},
	}

	sub, err := nc.Subscribe(QueryArea, func(msg *nats.Msg) {
		var req AreaReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		if req.AreaID != "zone-a" {
			t.Logf("unexpected request: %+v", req)
		}
		_ = msg.Respond(messages.OKDataResp(expected))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	q := New(nc)
	area, err := q.Area(ctx, "zone-a")

	if err != nil {
		t.Fatalf("Area failed: %v", err)
	}
	if area == nil {
		t.Fatal("expected non-nil area")
	}
	if area.ID != "zone-a" || area.Region != "0" {
		t.Errorf("unexpected area: %+v", area)
	}
}

func TestRequestTypes_JSONMarshaling(t *testing.T) {
	nearby := NearbyReq{Region: "0", X: 5.0, Y: 5.0, RadiusMeters: 10.0}
	data, _ := json.Marshal(nearby)
	var nearbyParsed NearbyReq
	json.Unmarshal(data, &nearbyParsed)
	if nearbyParsed.Region != nearby.Region || nearbyParsed.RadiusMeters != nearby.RadiusMeters {
		t.Error("NearbyReq round-trip failed")
	}

	nearbyOf := NearbyOfReq{ObjectID: "obj1", RadiusMeters: 5.0}
	data, _ = json.Marshal(nearbyOf)
	var nearbyOfParsed NearbyOfReq
	json.Unmarshal(data, &nearbyOfParsed)
	if nearbyOfParsed.ObjectID != nearbyOf.ObjectID {
		t.Error("NearbyOfReq round-trip failed")
	}

	within := WithinAreaReq{Region: "0", AreaID: "zone-a"}
	data, _ = json.Marshal(within)
	var withinParsed WithinAreaReq
	json.Unmarshal(data, &withinParsed)
	if withinParsed.AreaID != within.AreaID {
		t.Error("WithinAreaReq round-trip failed")
	}

	ptReq := AreasAtPointReq{Region: "0", X: 5.0, Y: 5.0}
	data, _ = json.Marshal(ptReq)
	var ptParsed AreasAtPointReq
	json.Unmarshal(data, &ptParsed)
	if ptParsed.X != ptReq.X || ptParsed.Y != ptReq.Y {
		t.Error("AreasAtPointReq round-trip failed")
	}

	contReq := AreasContainingReq{ObjectID: "obj1"}
	data, _ = json.Marshal(contReq)
	var contParsed AreasContainingReq
	json.Unmarshal(data, &contParsed)
	if contParsed.ObjectID != contReq.ObjectID {
		t.Error("AreasContainingReq round-trip failed")
	}

	intReq := IntersectingReq{ObjectID: "obj1"}
	data, _ = json.Marshal(intReq)
	var intParsed IntersectingReq
	json.Unmarshal(data, &intParsed)
	if intParsed.ObjectID != intReq.ObjectID {
		t.Error("IntersectingReq round-trip failed")
	}

	boundsReq := BoundsReq{ObjectID: "obj1"}
	data, _ = json.Marshal(boundsReq)
	var boundsParsed BoundsReq
	json.Unmarshal(data, &boundsParsed)
	if boundsParsed.ObjectID != boundsReq.ObjectID {
		t.Error("BoundsReq round-trip failed")
	}

	areasReq := NearbyAreasReq{Region: "0", X: 5.0, Y: 5.0, RadiusMeters: 10.0}
	data, _ = json.Marshal(areasReq)
	var areasParsed NearbyAreasReq
	json.Unmarshal(data, &areasParsed)
	if areasParsed.X != areasReq.X {
		t.Error("NearbyAreasReq round-trip failed")
	}

	areaReq := AreaReq{AreaID: "zone-a"}
	data, _ = json.Marshal(areaReq)
	var areaParsed AreaReq
	json.Unmarshal(data, &areaParsed)
	if areaParsed.AreaID != areaReq.AreaID {
		t.Error("AreaReq round-trip failed")
	}

	regionOfReq := RegionOfReq{ObjectID: "obj1"}
	data, _ = json.Marshal(regionOfReq)
	var regionOfParsed RegionOfReq
	json.Unmarshal(data, &regionOfParsed)
	if regionOfParsed.ObjectID != regionOfReq.ObjectID {
		t.Error("RegionOfReq round-trip failed")
	}

	regionFromPointReq := RegionFromPointReq{X: 5.0, Y: 6.0, Z: 7.0}
	data, _ = json.Marshal(regionFromPointReq)
	var regionFromPointParsed RegionFromPointReq
	json.Unmarshal(data, &regionFromPointParsed)
	if regionFromPointParsed.X != regionFromPointReq.X || regionFromPointParsed.Y != regionFromPointReq.Y || regionFromPointParsed.Z != regionFromPointReq.Z {
		t.Error("RegionFromPointReq round-trip failed")
	}
}

func errNotFound(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrNotFound, fmt.Sprintf(format, args...))
}

func stringPtr(s string) *string { return &s }
