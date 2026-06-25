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

// CommitAck is returned by processed object write requests. It identifies the
// committed incarnation of the object and the per-incarnation commit sequence.
type CommitAck struct {
	InstanceID string `json:"instance_id"`
	CommitSeq  uint64 `json:"commit_seq"`
}

// RegisterObject registers objectID through GeoTruth's processed request path.
// It returns after GeoTruth has updated its live state and committed the
// matching public GT_EVENTS object event. Callers are expected to serialize
// ownership of each objectID; concurrent writers for the same object are not
// coordinated by this package.
func (p Publish) RegisterObject(ctx context.Context, objectID string, objectDimensions domain.ObjectDimensions) (CommitAck, error) {
	data, err := json.Marshal(RegisterObjectReq{ID: objectID, Dims: objectDimensions})
	if err != nil {
		return CommitAck{}, err
	}

	return p.requestCommitAck(ctx, GeoTruthRegisterObjectSubject(objectID), data, "register object")
}

// UpdateObjectPosition updates objectID through GeoTruth's processed request
// path. It returns after GeoTruth has updated live state and committed the
// matching public GT_EVENTS position event.
func (p Publish) UpdateObjectPosition(ctx context.Context, objectID string, x, y, z, rotY float64) (CommitAck, error) {
	data, err := json.Marshal(UpdatePositionReq{ID: objectID, X: x, Y: y, Z: z, RotY: rotY})
	if err != nil {
		return CommitAck{}, err
	}

	return p.requestCommitAck(ctx, GeoTruthUpdatePositionSubject(objectID), data, "update object position")
}

// RemoveObject removes objectID through GeoTruth's processed request path. It
// returns after GeoTruth has removed the object from live state and committed
// the matching public GT_EVENTS removal event.
func (p Publish) RemoveObject(ctx context.Context, objectID string) (CommitAck, error) {
	data, err := json.Marshal(RemoveObjectReq{ID: objectID})
	if err != nil {
		return CommitAck{}, err
	}

	return p.requestCommitAck(ctx, GeoTruthRemoveObjectSubject(objectID), data, "remove object")
}

func (p Publish) requestCommitAck(ctx context.Context, subject string, data []byte, op string) (CommitAck, error) {
	resp, err := p.nc.RequestMsgWithContext(ctx, &nats.Msg{
		Subject: subject,
		Data:    data,
	})
	if err != nil {
		return CommitAck{}, fmt.Errorf("%s request: %w", op, err)
	}

	ack, err := messages.Data[CommitAck](resp.Data)
	if err != nil {
		return CommitAck{}, err
	}
	if ack.InstanceID == "" || ack.CommitSeq == 0 {
		return CommitAck{}, fmt.Errorf("%s response missing commit ack", op)
	}
	return ack, nil
}
