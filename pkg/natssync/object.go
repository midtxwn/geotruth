package natssync

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/messages"
	"github.com/midtxwn/geotruth/pkg/natspublish"

	"github.com/nats-io/nats.go"
)

// RegisterObject returns after the live GeoTruth instance emits the matching
// object-registered event. It assumes the caller owns and serializes objectID.
func (c *Client) RegisterObject(ctx context.Context, objectID string, dims domain.ObjectDimensions) error {
	return c.syncOperation(ctx, objectID, syncEventRegistered, natspublish.IngesterRegisterObjectSubject(objectID),
		func(clientOpID string) interface{} {
			return natspublish.RegisterObjectReq{ID: objectID, Dims: dims, ClientOpID: clientOpID}
		}, true, false)
}

// UpdateObjectPosition uses the queued ingester position API, then waits for
// the live GeoTruth instance to emit the matching position event by ClientOpID.
func (c *Client) UpdateObjectPosition(ctx context.Context, objectID string, x, y, z, rotY float64) error {
	return c.syncOperation(ctx, objectID, syncEventPositionUpdated, natspublish.IngesterUpdatePositionSubject(objectID),
		func(clientOpID string) interface{} {
			return natspublish.UpdatePositionReq{ID: objectID, X: x, Y: y, Z: z, RotY: rotY, ClientOpID: clientOpID}
		}, false, false)
}

// RemoveObject returns after the live GeoTruth instance emits the matching
// object-removed event. It assumes the caller owns and serializes objectID.
func (c *Client) RemoveObject(ctx context.Context, objectID string) error {
	return c.syncOperation(ctx, objectID, syncEventRemoved, natspublish.IngesterRemoveObjectSubject(objectID),
		func(clientOpID string) interface{} {
			return natspublish.RemoveObjectReq{ID: objectID, ClientOpID: clientOpID}
		}, true, true)
}

func (c *Client) syncOperation(ctx context.Context, objectID string, kind syncEventKind, ingesterSubject string, buildReq func(string) interface{}, needsSourceSeq bool, evictAfter bool) error {
	watcher, err := c.ensureObjectWatcher(objectID)
	if err != nil {
		return err
	}

	clientOpID := c.nextClientOpID()
	waitKey, waitCh, err := watcher.addWaiter(kind, clientOpID)
	if err != nil {
		return err
	}
	waiterActive := true
	defer func() {
		if waiterActive {
			watcher.removeWaiter(waitKey, waitCh)
		}
	}()

	req := buildReq(clientOpID)
	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	resp, err := c.nc.RequestMsgWithContext(ctx, &nats.Msg{
		Subject: ingesterSubject,
		Data:    data,
	})
	if err != nil {
		return fmt.Errorf("request to %s: %w", ingesterSubject, err)
	}

	if needsSourceSeq {
		seqPayload, err := messages.Data[struct {
			SourceSeq uint64 `json:"source_seq"`
		}](resp.Data)
		if err != nil {
			return fmt.Errorf("response error: %w", err)
		}
		if seqPayload.SourceSeq == 0 {
			return fmt.Errorf("response missing source_seq")
		}
	} else if err := messages.Err(resp.Data); err != nil {
		return fmt.Errorf("response error: %w", err)
	}

	if err := watcher.wait(ctx, waitKey, waitCh); err != nil {
		return err
	}
	waiterActive = false
	if evictAfter {
		c.evictObjectWatcher(objectID, watcher)
	}
	return nil
}
