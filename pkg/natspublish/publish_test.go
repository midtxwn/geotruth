package natspublish

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/messages"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func startPublishTestNATS(tb testing.TB) *nats.Conn {
	tb.Helper()
	s, err := natsserver.NewServer(&natsserver.Options{
		Port:   -1,
		NoLog:  true,
		NoSigs: true,
	})
	if err != nil {
		tb.Fatalf("new nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		s.Shutdown()
		tb.Fatal("nats server not ready")
	}
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		s.Shutdown()
		tb.Fatalf("connect nats: %v", err)
	}
	tb.Cleanup(func() {
		nc.Close()
		s.Shutdown()
	})
	return nc
}

func subscribeRequest(tb testing.TB, nc *nats.Conn, subject string, handler func(*nats.Msg)) {
	tb.Helper()
	if _, err := nc.Subscribe(subject, handler); err != nil {
		tb.Fatalf("subscribe %s: %v", subject, err)
	}
	if err := nc.Flush(); err != nil {
		tb.Fatalf("flush subscription: %v", err)
	}
}

func TestObjectWriteMethodsUseGeoTruthSubjectsAndDecodeCommitAck(t *testing.T) {
	nc := startPublishTestNATS(t)
	pub := New(nc)

	tests := []struct {
		name    string
		subject string
		call    func(context.Context) (CommitAck, error)
		check   func(t *testing.T, data []byte)
	}{
		{
			name:    "register",
			subject: GeoTruthRegisterObjectSubject("obj1"),
			call: func(ctx context.Context) (CommitAck, error) {
				return pub.RegisterObject(ctx, "obj1", domain.ObjectDimensions{Width: 2, Height: 3})
			},
			check: func(t *testing.T, data []byte) {
				var req RegisterObjectReq
				if err := json.Unmarshal(data, &req); err != nil {
					t.Fatalf("decode register req: %v", err)
				}
				if req.ID != "obj1" || req.Dims.Width != 2 || req.Dims.Height != 3 {
					t.Fatalf("register req = %+v", req)
				}
			},
		},
		{
			name:    "position",
			subject: GeoTruthUpdatePositionSubject("obj1"),
			call: func(ctx context.Context) (CommitAck, error) {
				return pub.UpdateObjectPosition(ctx, "obj1", 1, 2, 3, 0.5)
			},
			check: func(t *testing.T, data []byte) {
				var req UpdatePositionReq
				if err := json.Unmarshal(data, &req); err != nil {
					t.Fatalf("decode position req: %v", err)
				}
				if req.ID != "obj1" || req.X != 1 || req.Y != 2 || req.Z != 3 || req.RotY != 0.5 {
					t.Fatalf("position req = %+v", req)
				}
			},
		},
		{
			name:    "remove",
			subject: GeoTruthRemoveObjectSubject("obj1"),
			call: func(ctx context.Context) (CommitAck, error) {
				return pub.RemoveObject(ctx, "obj1")
			},
			check: func(t *testing.T, data []byte) {
				var req RemoveObjectReq
				if err := json.Unmarshal(data, &req); err != nil {
					t.Fatalf("decode remove req: %v", err)
				}
				if req.ID != "obj1" {
					t.Fatalf("remove req = %+v", req)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ack := CommitAck{InstanceID: "b1i1", CommitSeq: 7}
			subscribeRequest(t, nc, tt.subject, func(msg *nats.Msg) {
				tt.check(t, msg.Data)
				_ = msg.Respond(messages.OKDataResp(ack))
			})

			got, err := tt.call(context.Background())
			if err != nil {
				t.Fatalf("call: %v", err)
			}
			if got != ack {
				t.Fatalf("ack = %+v, want %+v", got, ack)
			}
		})
	}
}

func TestObjectWriteMethodsPropagateErrorEnvelope(t *testing.T) {
	nc := startPublishTestNATS(t)
	pub := New(nc)
	subscribeRequest(t, nc, GeoTruthUpdatePositionSubject("obj1"), func(msg *nats.Msg) {
		_ = msg.Respond(messages.ErrResp(errors.New("boom")))
	})

	_, err := pub.UpdateObjectPosition(context.Background(), "obj1", 1, 2, 3, 0)
	if err == nil || err.Error() != "boom" {
		t.Fatalf("err = %v, want boom", err)
	}
}

func TestObjectWriteMethodsRejectMissingCommitAck(t *testing.T) {
	nc := startPublishTestNATS(t)
	pub := New(nc)
	subscribeRequest(t, nc, GeoTruthRegisterObjectSubject("obj1"), func(msg *nats.Msg) {
		_ = msg.Respond(messages.OKDataResp(CommitAck{}))
	})

	_, err := pub.RegisterObject(context.Background(), "obj1", domain.ObjectDimensions{Width: 1, Height: 1})
	if err == nil {
		t.Fatal("expected missing commit ack error")
	}
}

func TestAreaMethodsUsePublicSubjectsAndDecodeErrors(t *testing.T) {
	nc := startPublishTestNATS(t)
	pub := New(nc)

	subscribeRequest(t, nc, AreaRegister, func(msg *nats.Msg) {
		var req RegisterAreaReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Fatalf("decode area req: %v", err)
		}
		if req.ID != "zone1" || req.Region != "r1" || len(req.Points) != 1 || req.Points[0].X != 3 {
			t.Fatalf("register area req = %+v", req)
		}
		_ = msg.Respond(messages.OKResp())
	})

	if err := pub.RegisterArea(context.Background(), "zone1", "r1", []domain.Point{{X: 3, Y: 4}}); err != nil {
		t.Fatalf("RegisterArea: %v", err)
	}

	subscribeRequest(t, nc, AreaRemove, func(msg *nats.Msg) {
		var req RemoveAreaReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Fatalf("decode remove area req: %v", err)
		}
		if req.ID != "zone1" {
			t.Fatalf("remove area req = %+v", req)
		}
		_ = msg.Respond(messages.ErrResp(errors.New("remove denied")))
	})

	err := pub.RemoveArea(context.Background(), "zone1")
	if err == nil || err.Error() != "remove denied" {
		t.Fatalf("RemoveArea err = %v, want remove denied", err)
	}
}
