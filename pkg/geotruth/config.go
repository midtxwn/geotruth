// Package geotruth defines the public configuration contract for running the
// GeoTruth service as a library.
package geotruth

import (
	"time"

	"github.com/midtxwn/geotruth/pkg/geotruthops"
	"github.com/midtxwn/geotruth/pkg/regionresolver"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type PublisherConfig struct {
	Workers              int           `mapstructure:"workers"`
	CommitBuffer         int           `mapstructure:"commitBuffer"`
	ResultBuffer         int           `mapstructure:"resultBuffer"`
	MaxInFlightPerWorker int           `mapstructure:"maxInFlightPerWorker"`
	InitialBackoff       time.Duration `mapstructure:"initialBackoff"`
	MaxBackoff           time.Duration `mapstructure:"maxBackoff"`
	InProgressInterval   time.Duration `mapstructure:"inProgressInterval"`
}

type Config struct {
	Storage          jetstream.StorageType             `mapstructure:"storageType"`
	GTEventsMaxBytes int64                             `mapstructure:"eventsMaxBytes"`
	Replicas         int                               `mapstructure:"replicas"`
	Publisher        PublisherConfig                   `mapstructure:"publisher"`
	PressureMonitor  geotruthops.PressureMonitorConfig `mapstructure:"pressureMonitor"`
}

// NATSConnector creates NATS connections for GeoTruth-owned roles.
//
// GeoTruth drains or closes every connection returned by the connector.
// Callers should apply the same URL, auth, TLS, and reconnect policy they want
// the service to use, optionally varying client name or callbacks by role.
// Known roles are "main" and "publisher-N".
type NATSConnector func(role string) (*nats.Conn, error)

type Dependencies struct {
	// NATS creates GeoTruth-owned NATS connections. It is called once for the
	// main/control connection and once per GT_EVENTS publisher worker.
	NATS     NATSConnector
	Resolver regionresolver.Resolver
}
