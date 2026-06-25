package natspublish

import (
	"github.com/nats-io/nats.go"
)

type Publish struct {
	nc *nats.Conn
}

func New(nc *nats.Conn) Publish {
	return Publish{nc: nc}
}
