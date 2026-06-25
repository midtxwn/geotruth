package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	appgeotruth "github.com/midtxwn/geotruth/internal/app/geotruth"
	"github.com/midtxwn/geotruth/pkg/geotruth"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	connectNATS := func(role string) (*nats.Conn, error) {
		nc, err := nats.Connect(natsURL,
			nats.Name("geotruth-"+role),
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(2*time.Second),
		)
		if err != nil {
			return nil, err
		}
		if nc.Status() != nats.CONNECTED {
			log.Printf("Waiting for NATS connection (%s)...", role)
		}
		for nc.Status() != nats.CONNECTED {
			select {
			case <-nc.StatusChanged(nats.CONNECTED):
			case <-time.After(time.Second * 2):
			}
		}
		log.Printf("NATS connected successfully (%s)", role)
		return nc, nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		log.Println("[geotruth] shutting down")
		cancel()
	}()

	resolver, err := geotruthResolver()
	if err != nil {
		log.Fatalf("geotruth resolver: %v", err)
	}

	cfg := geotruth.Config{
		Storage: jetstream.FileStorage,
	}

	gt, err := appgeotruth.Run(ctx, cfg, geotruth.Dependencies{NATS: connectNATS, Resolver: resolver})
	if err != nil {
		log.Fatalf("geotruth run: %v", err)
	}

	select {
	case <-gt.Ready():
	case <-gt.Done():
		if err := gt.Err(); err != nil && err != context.Canceled {
			log.Fatalf("geotruth run: %v", err)
		}
		return
	case <-ctx.Done():
		return
	}

	select {
	case <-ctx.Done():
	case <-gt.Done():
		if err := gt.Err(); err != nil && err != context.Canceled {
			log.Fatalf("geotruth run: %v", err)
		}
	}
}
