package embedded

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	appgeotruth "github.com/midtxwn/geotruth/internal/app/geotruth"
	"github.com/midtxwn/geotruth/pkg/geotruth"
	"github.com/midtxwn/geotruth/pkg/regionresolver"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type NATSServerConfig struct {
	Port int `'mapstructure:"port"`
}

type Config struct {
	GeoTruth geotruth.Config  `mapstructure:"geotruth"`
	NATS     NATSServerConfig `mapstructure:"natsserver"`
}

// Dependencies contains caller-provided dependencies for the all-in-one
// embedded stack. Run owns the embedded NATS server and injects service
// connection factories internally; use RunGeoTruth to run the service with
// external caller-provided dependencies.
type Dependencies struct {
	Resolver regionresolver.Resolver
}

// flatResolver reimplements the original flat-floor behavior for
// integration testing. Regions are horizontal slices of equal height,
// identified by zero-based string index ("0", "1", "2", ...).
//
// This resolver exists as a convenience test region resolver.
// Production callers should provide their own regionresolver.Resolver.
type flatResolver struct {
	floorHeight float64
	hysteresis  float64
	floors      int
}

func newFlatResolver(floors int) *flatResolver {
	return &flatResolver{floorHeight: 4.0, hysteresis: 0.2, floors: floors}
}

func (r *flatResolver) Resolve(x, y, z float64, prevRegion *string) (string, error) {
	if z < 0 {
		z = 0
	}
	naiveFloor := int(z / r.floorHeight)
	if naiveFloor >= r.floors {
		naiveFloor = r.floors - 1
	}

	if prevRegion == nil {
		return strconv.Itoa(naiveFloor), nil
	}

	prev, err := strconv.Atoi(*prevRegion)
	if err != nil {
		return strconv.Itoa(naiveFloor), nil
	}

	base := float64(prev) * r.floorHeight
	if z > base+r.floorHeight+r.hysteresis && prev < r.floors-1 {
		return strconv.Itoa(prev + 1), nil
	}
	if z < base-r.hysteresis && prev > 0 {
		return strconv.Itoa(prev - 1), nil
	}
	return *prevRegion, nil
}

func (r *flatResolver) KnownRegions() []string {
	regions := make([]string, r.floors)
	for i := 0; i < r.floors; i++ {
		regions[i] = strconv.Itoa(i)
	}
	return regions
}

var DefaultConfig = Config{
	NATS: NATSServerConfig{
		Port: -1,
	},
	GeoTruth: geotruth.Config{
		Storage:          jetstream.MemoryStorage,
		GTEventsMaxBytes: 0,
		Replicas:         1,
	},
}

var DefaultDependencies = Dependencies{
	Resolver: newFlatResolver(1),
}

type NATSServer struct {
	nc     *nats.Conn
	server *natsserver.Server
	cancel context.CancelFunc
	once   sync.Once
}

func (s *NATSServer) NATSConn() *nats.Conn {
	return s.nc
}

func (s *NATSServer) NATSURL() string {
	return s.server.ClientURL()
}

func (s *NATSServer) Shutdown() {
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		s.nc.Drain()
		s.server.Shutdown()
	})
}

type Service interface {
	Ready() <-chan struct{}
	Done() <-chan struct{}
	Err() error
}

type Services struct {
	GeoTruth Service
	nats     *NATSServer
	cancel   context.CancelFunc
	chReady  chan struct{}
}

func (s *Services) NATSConn() *nats.Conn {
	return s.nats.NATSConn()
}

func (s *Services) NATSURL() string {
	return s.nats.NATSURL()
}

func (s *Services) Ready() <-chan struct{} {
	return s.chReady
}

func (s *Services) Shutdown() {
	if s.cancel != nil {
		s.cancel()
	}
	s.nats.Shutdown()
}

func RunNATSServer(ctx context.Context, cfg NATSServerConfig) (*NATSServer, error) {
	storeDir, err := os.MkdirTemp("", "nats-embedded-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}

	port := cfg.Port
	if port == 0 {
		port = -1
	}

	opts := &natsserver.Options{
		Port:      port,
		JetStream: true,
		StoreDir:  storeDir,
		NoLog:     true,
		NoSigs:    true,
	}
	server, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("create nats server: %w", err)
	}
	go server.Start()
	if !server.ReadyForConnections(5 * time.Second) {
		server.Shutdown()
		return nil, fmt.Errorf("nats server not ready")
	}

	nc, err := connectNATS(server.ClientURL())
	if err != nil {
		server.Shutdown()
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	serverCtx, cancel := context.WithCancel(ctx)
	natsSvc := &NATSServer{nc: nc, server: server, cancel: cancel}
	go func() {
		<-serverCtx.Done()
		natsSvc.Shutdown()
	}()

	return natsSvc, nil
}

func connectNATS(url string) (*nats.Conn, error) {
	return nats.Connect(url,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
}

// RunGeoTruth starts only the GeoTruth service with caller-provided
// dependencies, including the NATS connector.
func RunGeoTruth(ctx context.Context, cfg geotruth.Config, deps geotruth.Dependencies) (Service, error) {
	gt, err := appgeotruth.Run(ctx, cfg, deps)
	if err != nil {
		return nil, err
	}
	select {
	case <-gt.Ready():
		return gt, nil
	case <-gt.Done():
		return nil, gt.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func Run(ctx context.Context, cfg Config, deps Dependencies) (*Services, error) {
	if deps.Resolver == nil {
		return nil, fmt.Errorf("embedded: geotruth resolver is required")
	}

	stackCtx, cancel := context.WithCancel(ctx)
	natsSvc, err := RunNATSServer(stackCtx, cfg.NATS)
	if err != nil {
		cancel()
		return nil, err
	}

	gt, err := RunGeoTruth(stackCtx, cfg.GeoTruth, geotruth.Dependencies{
		NATS: func(role string) (*nats.Conn, error) {
			return connectNATS(natsSvc.NATSURL())
		},
		Resolver: deps.Resolver,
	})
	if err != nil {
		cancel()
		natsSvc.Shutdown()
		return nil, fmt.Errorf("geotruth run: %w", err)
	}

	chReady := make(chan struct{})
	close(chReady)

	return &Services{
		GeoTruth: gt,
		nats:     natsSvc,
		cancel:   cancel,
		chReady:  chReady,
	}, nil
}

// NewFlatResolver creates a flatResolver for use in embedded test stacks.
// This is a convenience function for test code that needs the standard
// flat-floor behavior with a specific floor count.
func NewFlatResolver(floors int) regionresolver.Resolver {
	return newFlatResolver(floors)
}
