// Package ingester defines the public configuration contract for running the
// Ingester service as a library.
package ingester

import (
	"github.com/midtxwn/geotruth/pkg/geotruthops"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Config struct {
	Storage         jetstream.StorageType `mapstructure:"storageType"`
	SpatialMaxBytes int64                 `mapstructure:"spatialMaxBytes"`
	Replicas        int                   `mapstructure:"replicas"`
	// ShardCount controls object publish shard parallelism. Zero selects the
	// service default; negative values are rejected.
	ShardCount int `mapstructure:"shardCount"`
	// ShardQueueCapacity bounds each shard queue. Full queues apply
	// backpressure by blocking ingress routing.
	ShardQueueCapacity int                               `mapstructure:"shardQueueCapacity"`
	PressureMonitor    geotruthops.PressureMonitorConfig `mapstructure:"pressureMonitor"`
}

// NATSConnector creates NATS connections for ingester-owned roles.
//
// The ingester drains or closes every connection returned by the connector.
// Callers should apply the same URL, auth, TLS, and reconnect policy they want
// the service to use, optionally varying client name or callbacks by role.
// Known roles are "main" and "shard-N".
type NATSConnector func(role string) (*nats.Conn, error)

type Dependencies struct {
	// NATS creates ingester-owned NATS connections. It is called once for the
	// main/control connection and once per object publish shard.
	NATS NATSConnector
}
