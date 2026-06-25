package natsquery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/midtxwn/geotruth/pkg/messages"

	"github.com/nats-io/nats.go"
)

type Query struct {
	nc *nats.Conn
}

func New(nc *nats.Conn) Query {
	return Query{nc: nc}
}

func (q Query) request(ctx context.Context, subject string, req interface{}) ([]byte, error) {
	conn := q.nc
	if conn == nil {
		return nil, fmt.Errorf("nats not initialized")
	}

	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	resp, err := conn.RequestMsgWithContext(ctx, &nats.Msg{Subject: subject, Data: data})
	if err != nil {
		return nil, fmt.Errorf("nats request: %w", err)
	}

	return resp.Data, nil
}

func requestData[T any](q Query, ctx context.Context, subject string, req interface{}) (T, error) {
	raw, err := q.request(ctx, subject, req)
	if err != nil {
		var zero T
		return zero, err
	}
	return messages.Data[T](raw)
}
