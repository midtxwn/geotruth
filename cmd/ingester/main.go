package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	appingester "github.com/midtxwn/geotruth/internal/app/ingester"
	"github.com/midtxwn/geotruth/pkg/ingester"

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
			nats.Name("ingester-"+role),
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
		log.Println("[ingester] shutting down")
		cancel()
	}()

	ing, err := appingester.Run(ctx, ingester.Config{Storage: jetstream.FileStorage}, ingester.Dependencies{
		NATS: connectNATS,
	})
	if err != nil {
		log.Fatalf("ingester run: %v", err)
	}

	select {
	case <-ing.Ready():
	case <-ing.Done():
		if err := ing.Err(); err != nil && err != context.Canceled {
			log.Fatalf("ingester run: %v", err)
		}
		return
	case <-ctx.Done():
		return
	}

	select {
	case <-ctx.Done():
	case <-ing.Done():
		if err := ing.Err(); err != nil && err != context.Canceled {
			log.Fatalf("ingester run: %v", err)
		}
	}
}
