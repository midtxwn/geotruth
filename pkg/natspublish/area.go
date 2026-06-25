package natspublish

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/messages"

	"github.com/nats-io/nats.go"
)

type RegisterAreaReq struct {
	ID     string         `json:"id"`
	Region string         `json:"region"`
	Points []domain.Point `json:"points"`
}

type RemoveAreaReq struct {
	ID string `json:"id"`
}

type AreaKV struct {
	Region string         `json:"region"`
	Points []domain.Point `json:"points"`
}

func (p Publish) RegisterArea(ctx context.Context, id string, region string, points []domain.Point) error {
	data, err := json.Marshal(RegisterAreaReq{ID: id, Region: region, Points: points})
	if err != nil {
		return err
	}

	resp, err := p.nc.RequestMsgWithContext(ctx, &nats.Msg{
		Subject: AreaRegister,
		Data:    data,
	})
	if err != nil {
		return fmt.Errorf("register area request: %w", err)
	}

	return messages.Err(resp.Data)
}

func (p Publish) RemoveArea(ctx context.Context, id string) error {
	data, err := json.Marshal(RemoveAreaReq{ID: id})
	if err != nil {
		return err
	}

	resp, err := p.nc.RequestMsgWithContext(ctx, &nats.Msg{
		Subject: AreaRemove,
		Data:    data,
	})
	if err != nil {
		return fmt.Errorf("remove area request: %w", err)
	}

	return messages.Err(resp.Data)
}
