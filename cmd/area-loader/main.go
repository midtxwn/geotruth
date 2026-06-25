package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/natspublish"

	"github.com/nats-io/nats.go"
)

type areaEntry struct {
	ID     string         `json:"id"`
	Region string         `json:"region"`
	Points []domain.Point `json:"points"`
}

func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	areasFile := os.Getenv("AREAS_FILE")
	if areasFile == "" {
		areasFile = "areas.json"
	}

	nc, err := nats.Connect(natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("nats connect: %v", err)
	}
	defer nc.Drain()

	data, err := os.ReadFile(areasFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[area-loader] file %s not found, nothing to load", areasFile)
			return
		}
		log.Fatalf("read areas file: %v", err)
	}

	var entries []areaEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		log.Fatalf("parse areas file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pub := natspublish.New(nc)

	loaded := 0
	for _, e := range entries {
		if err := pub.RegisterArea(ctx, e.ID, e.Region, e.Points); err != nil {
			log.Printf("[area-loader] register area %s: %v", e.ID, err)
			continue
		}
		loaded++
	}

	log.Printf("[area-loader] loaded %d/%d areas from %s", loaded, len(entries), areasFile)
}
