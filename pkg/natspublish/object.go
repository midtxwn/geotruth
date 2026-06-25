package natspublish

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/messages"

	"github.com/nats-io/nats.go"
)

type ObjectRegisterMsg struct {
	ID         string                  `json:"id"`
	Dims       domain.ObjectDimensions `json:"dims"`
	ClientOpID string                  `json:"client_op_id,omitempty"`
}

type ObjectRemoveMsg struct {
	ID         string `json:"id"`
	ClientOpID string `json:"client_op_id,omitempty"`
}

type RegisterObjectReq struct {
	ID         string                  `json:"id"`
	Dims       domain.ObjectDimensions `json:"dims"`
	ClientOpID string                  `json:"client_op_id,omitempty"`
}

type RemoveObjectReq struct {
	ID         string `json:"id"`
	ClientOpID string `json:"client_op_id,omitempty"`
}

type UpdatePositionReq struct {
	ID         string  `json:"id"`
	X          float64 `json:"x"`
	Y          float64 `json:"y"`
	Z          float64 `json:"z"`
	RotY       float64 `json:"rot_y"`
	ClientOpID string  `json:"client_op_id,omitempty"`
}

type PositionMsg struct {
	ID         string  `json:"id"`
	X          float64 `json:"x"`
	Y          float64 `json:"y"`
	Z          float64 `json:"z"`
	RotY       float64 `json:"rot_y"`
	ClientOpID string  `json:"client_op_id,omitempty"`
}

// RegisterObject registers objectID through the synchronous ingester path.
// It returns after the ingester has received a SPATIAL PubAck. Callers are
// expected to serialize ownership of each objectID; concurrent writers for
// the same object are not coordinated by this package.
func (p Publish) RegisterObject(ctx context.Context, objectID string, objectDimensions domain.ObjectDimensions) error {
	data, err := json.Marshal(RegisterObjectReq{ID: objectID, Dims: objectDimensions})
	if err != nil {
		return err
	}

	resp, err := p.nc.RequestMsgWithContext(ctx, &nats.Msg{
		Subject: IngesterRegisterObjectSubject(objectID),
		Data:    data,
	})
	if err != nil {
		return fmt.Errorf("register object request: %w", err)
	}

	return messages.Err(resp.Data)
}

// UpdateObjectPosition uses the high-throughput queued ingester path. It
// returns after validation and shard admission, before the SPATIAL PubAck.
// Use UpdateObjectPositionSync when the caller needs ingress durability.
func (p Publish) UpdateObjectPosition(ctx context.Context, objectID string, x, y, z, rotY float64) error {
	data, err := json.Marshal(UpdatePositionReq{ID: objectID, X: x, Y: y, Z: z, RotY: rotY})
	if err != nil {
		return err
	}

	resp, err := p.nc.RequestMsgWithContext(ctx, &nats.Msg{
		Subject: IngesterUpdatePositionSubject(objectID),
		Data:    data,
	})
	if err != nil {
		return fmt.Errorf("update object position request: %w", err)
	}

	return messages.Err(resp.Data)
}

// UpdateObjectPositionSync publishes a position update through the synchronous
// ingester path. It returns after the ingester has received a SPATIAL PubAck.
func (p Publish) UpdateObjectPositionSync(ctx context.Context, objectID string, x, y, z, rotY float64) error {
	data, err := json.Marshal(UpdatePositionReq{ID: objectID, X: x, Y: y, Z: z, RotY: rotY})
	if err != nil {
		return err
	}

	resp, err := p.nc.RequestMsgWithContext(ctx, &nats.Msg{
		Subject: IngesterUpdatePositionSyncSubject(objectID),
		Data:    data,
	})
	if err != nil {
		return fmt.Errorf("sync update object position request: %w", err)
	}

	return messages.Err(resp.Data)
}

// RemoveObject removes objectID through the synchronous ingester path. It
// returns after the ingester has received a SPATIAL PubAck.
func (p Publish) RemoveObject(ctx context.Context, objectID string) error {
	data, err := json.Marshal(RemoveObjectReq{ID: objectID})
	if err != nil {
		return err
	}

	resp, err := p.nc.RequestMsgWithContext(ctx, &nats.Msg{
		Subject: IngesterRemoveObjectSubject(objectID),
		Data:    data,
	})
	if err != nil {
		return fmt.Errorf("remove object request: %w", err)
	}

	return messages.Err(resp.Data)
}
