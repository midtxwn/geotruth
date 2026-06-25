package natsclient

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

type Client struct {
	nc *nats.Conn
}

func New(ctx context.Context, natsURL string) (*Client, error) {
	if natsURL == "" {
		natsURL = os.Getenv("NATS_URL")
		if natsURL == "" {
			natsURL = nats.DefaultURL
		}
	}

	nc, err := nats.Connect(natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	for nc.Status() != nats.CONNECTED {
		select {
		case <-nc.StatusChanged(nats.CONNECTED):
		case <-ctx.Done():
			nc.Close()
			return nil, fmt.Errorf("nats connect: %w", ctx.Err())
		}
	}

	return &Client{nc: nc}, nil
}

func (c *Client) Conn() *nats.Conn {
	return c.nc
}

func (c *Client) Close() {
	if c.nc != nil {
		c.nc.Drain()
	}
}
