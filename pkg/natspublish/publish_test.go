package natspublish

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/messages"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// startEmbeddedNATS starts an embedded NATS server for testing
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

func TestNew(t *testing.T) {
	nc := startEmbeddedNATS(t)

	pub := New(nc)
	if pub.nc == nil {
		t.Fatal("expected non-nil nats connection in Publish")
	}
}

func TestRegisterObject_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	// Start mock responder for ingester
	received := make(chan ObjectRegisterMsg, 1)
	sub, err := nc.Subscribe(IngesterRegisterObjectSubject("obj1"), func(msg *nats.Msg) {
		var req ObjectRegisterMsg
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		received <- req
		_ = msg.Respond(messages.OKResp())
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Call the API
	pub := New(nc)
	dims := domain.ObjectDimensions{Width: 2.0, Height: 3.0}
	err = pub.RegisterObject(ctx, "obj1", dims)

	if err != nil {
		t.Fatalf("RegisterObject failed: %v", err)
	}

	// Verify request was received
	select {
	case req := <-received:
		if req.ID != "obj1" {
			t.Errorf("expected ID obj1, got %s", req.ID)
		}
		if req.Dims.Width != 2.0 || req.Dims.Height != 3.0 {
			t.Errorf("expected dims {2.0, 3.0}, got %v", req.Dims)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request")
	}
}

func TestRegisterObject_ServerError(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	// Mock responder that returns error
	sub, err := nc.Subscribe(IngesterRegisterObjectSubject("obj1"), func(msg *nats.Msg) {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("object already exists")))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	pub := New(nc)
	dims := domain.ObjectDimensions{Width: 2.0, Height: 3.0}
	err = pub.RegisterObject(ctx, "obj1", dims)

	// Should return error from server
	if err == nil {
		t.Error("expected error from server response")
	}
	if err.Error() != "object already exists" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRegisterObject_Timeout(t *testing.T) {
	nc := startEmbeddedNATS(t)

	// Don't set up responder - will timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	pub := New(nc)
	dims := domain.ObjectDimensions{Width: 2.0, Height: 3.0}
	err := pub.RegisterObject(ctx, "obj1", dims)

	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestUpdateObjectPosition_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	received := make(chan UpdatePositionReq, 1)
	sub, err := nc.Subscribe(IngesterUpdatePositionSubject("obj1"), func(msg *nats.Msg) {
		var req UpdatePositionReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		received <- req
		_ = msg.Respond(messages.OKResp())
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	pub := New(nc)
	err = pub.UpdateObjectPosition(ctx, "obj1", 10.0, 20.0, 1.5, 45.0)

	if err != nil {
		t.Fatalf("UpdateObjectPosition failed: %v", err)
	}

	select {
	case req := <-received:
		if req.ID != "obj1" {
			t.Errorf("expected ID obj1, got %s", req.ID)
		}
		if req.X != 10.0 || req.Y != 20.0 || req.Z != 1.5 || req.RotY != 45.0 {
			t.Errorf("unexpected position: x=%f y=%f z=%f rotY=%f", req.X, req.Y, req.Z, req.RotY)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request")
	}
}

func TestUpdateObjectPosition_ExtremeValues(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	sub, err := nc.Subscribe(IngesterObjectWildcard, func(msg *nats.Msg) {
		_ = msg.Respond(messages.OKResp())
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	pub := New(nc)

	// Test with negative coordinates
	err = pub.UpdateObjectPosition(ctx, "obj1", -100.0, -200.0, 0.0, -90.0)
	if err != nil {
		t.Errorf("UpdateObjectPosition with negative values failed: %v", err)
	}

	// Test with zero coordinates
	err = pub.UpdateObjectPosition(ctx, "obj2", 0.0, 0.0, 0.0, 0.0)
	if err != nil {
		t.Errorf("UpdateObjectPosition with zero values failed: %v", err)
	}
}

func TestRemoveObject_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	received := make(chan RemoveObjectReq, 1)
	sub, err := nc.Subscribe(IngesterRemoveObjectSubject("obj1"), func(msg *nats.Msg) {
		var req RemoveObjectReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		received <- req
		_ = msg.Respond(messages.OKResp())
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	pub := New(nc)
	err = pub.RemoveObject(ctx, "obj1")

	if err != nil {
		t.Fatalf("RemoveObject failed: %v", err)
	}

	select {
	case req := <-received:
		if req.ID != "obj1" {
			t.Errorf("expected ID obj1, got %s", req.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request")
	}
}

func TestMessageTypes_JSONMarshaling(t *testing.T) {
	// Test ObjectRegisterMsg
	regMsg := ObjectRegisterMsg{
		ID:   "test-obj",
		Dims: domain.ObjectDimensions{Width: 2.5, Height: 3.5},
	}
	data, err := json.Marshal(regMsg)
	if err != nil {
		t.Fatalf("marshal ObjectRegisterMsg: %v", err)
	}

	var regParsed ObjectRegisterMsg
	if err := json.Unmarshal(data, &regParsed); err != nil {
		t.Fatalf("unmarshal ObjectRegisterMsg: %v", err)
	}
	if regParsed.ID != regMsg.ID || regParsed.Dims.Width != regMsg.Dims.Width {
		t.Error("ObjectRegisterMsg round-trip failed")
	}

	// Test UpdatePositionReq
	posReq := UpdatePositionReq{
		ID:   "test-obj",
		X:    10.0,
		Y:    20.0,
		Z:    1.5,
		RotY: 45.0,
	}
	data, err = json.Marshal(posReq)
	if err != nil {
		t.Fatalf("marshal UpdatePositionReq: %v", err)
	}

	var posParsed UpdatePositionReq
	if err := json.Unmarshal(data, &posParsed); err != nil {
		t.Fatalf("unmarshal UpdatePositionReq: %v", err)
	}
	if posParsed.X != posReq.X || posParsed.Y != posReq.Y {
		t.Error("UpdatePositionReq round-trip failed")
	}

	// Test RemoveObjectReq
	rmReq := RemoveObjectReq{ID: "test-obj"}
	data, err = json.Marshal(rmReq)
	if err != nil {
		t.Fatalf("marshal RemoveObjectReq: %v", err)
	}

	var rmParsed RemoveObjectReq
	if err := json.Unmarshal(data, &rmParsed); err != nil {
		t.Fatalf("unmarshal RemoveObjectReq: %v", err)
	}
	if rmParsed.ID != rmReq.ID {
		t.Error("RemoveObjectReq round-trip failed")
	}

	// Test AreaKV
	areaKV := AreaKV{
		Region: "1",
		Points: []domain.Point{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}},
	}
	data, err = json.Marshal(areaKV)
	if err != nil {
		t.Fatalf("marshal AreaKV: %v", err)
	}

	var areaParsed AreaKV
	if err := json.Unmarshal(data, &areaParsed); err != nil {
		t.Fatalf("unmarshal AreaKV: %v", err)
	}
	if areaParsed.Region != areaKV.Region || len(areaParsed.Points) != len(areaKV.Points) {
		t.Error("AreaKV round-trip failed")
	}
}

func TestPositionMsg_JSONMarshaling(t *testing.T) {
	posMsg := PositionMsg{
		ID:   "pos-obj",
		X:    100.0,
		Y:    200.0,
		Z:    2.5,
		RotY: 180.0,
	}

	data, err := json.Marshal(posMsg)
	if err != nil {
		t.Fatalf("marshal PositionMsg: %v", err)
	}

	var parsed PositionMsg
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal PositionMsg: %v", err)
	}

	if parsed.ID != posMsg.ID || parsed.X != posMsg.X || parsed.Y != posMsg.Y || parsed.Z != posMsg.Z || parsed.RotY != posMsg.RotY {
		t.Error("PositionMsg round-trip failed")
	}
}

func TestRegisterAreaReq_JSONMarshaling(t *testing.T) {
	req := RegisterAreaReq{
		ID:     "area-1",
		Region: "2",
		Points: []domain.Point{{X: 0, Y: 0}, {X: 5, Y: 0}, {X: 5, Y: 5}, {X: 0, Y: 5}},
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal RegisterAreaReq: %v", err)
	}

	var parsed RegisterAreaReq
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal RegisterAreaReq: %v", err)
	}

	if parsed.ID != req.ID || parsed.Region != req.Region || len(parsed.Points) != len(req.Points) {
		t.Error("RegisterAreaReq round-trip failed")
	}
}

func TestRemoveAreaReq_JSONMarshaling(t *testing.T) {
	req := RemoveAreaReq{ID: "area-1"}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal RemoveAreaReq: %v", err)
	}

	var parsed RemoveAreaReq
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal RemoveAreaReq: %v", err)
	}

	if parsed.ID != req.ID {
		t.Error("RemoveAreaReq round-trip failed")
	}
}

func TestRegisterArea_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	received := make(chan RegisterAreaReq, 1)
	sub, err := nc.Subscribe(AreaRegister, func(msg *nats.Msg) {
		var req RegisterAreaReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		received <- req
		_ = msg.Respond(messages.OKResp())
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	pub := New(nc)
	points := []domain.Point{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	err = pub.RegisterArea(ctx, "zone-a", "2", points)

	if err != nil {
		t.Fatalf("RegisterArea failed: %v", err)
	}

	select {
	case req := <-received:
		if req.ID != "zone-a" {
			t.Errorf("expected ID zone-a, got %s", req.ID)
		}
		if req.Region != "2" {
			t.Errorf("expected region 2, got %s", req.Region)
		}
		if len(req.Points) != 4 {
			t.Errorf("expected 4 points, got %d", len(req.Points))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request")
	}
}

func TestRegisterArea_ServerError(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	sub, err := nc.Subscribe(AreaRegister, func(msg *nats.Msg) {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("area already exists")))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	pub := New(nc)
	points := []domain.Point{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}}
	err = pub.RegisterArea(ctx, "zone-a", "0", points)

	if err == nil {
		t.Error("expected error from server response")
	}
	if err.Error() != "area already exists" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRemoveArea_Success(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	received := make(chan RemoveAreaReq, 1)
	sub, err := nc.Subscribe(AreaRemove, func(msg *nats.Msg) {
		var req RemoveAreaReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Logf("unmarshal error: %v", err)
			return
		}
		received <- req
		_ = msg.Respond(messages.OKResp())
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	pub := New(nc)
	err = pub.RemoveArea(ctx, "zone-a")

	if err != nil {
		t.Fatalf("RemoveArea failed: %v", err)
	}

	select {
	case req := <-received:
		if req.ID != "zone-a" {
			t.Errorf("expected ID zone-a, got %s", req.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request")
	}
}

func TestRemoveArea_ServerError(t *testing.T) {
	nc := startEmbeddedNATS(t)
	ctx := context.Background()

	sub, err := nc.Subscribe(AreaRemove, func(msg *nats.Msg) {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("area not found")))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	pub := New(nc)
	err = pub.RemoveArea(ctx, "nonexistent")

	if err == nil {
		t.Error("expected error from server response")
	}
	if err.Error() != "area not found" {
		t.Errorf("unexpected error message: %v", err)
	}
}
