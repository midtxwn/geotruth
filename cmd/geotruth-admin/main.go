package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/midtxwn/geotruth/pkg/geotruthops"

	"github.com/nats-io/nats.go"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
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

	ops, err := geotruthops.New(nc)
	if err != nil {
		log.Fatalf("ops init: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	switch os.Args[1] {
	case "stats":
		if err := runStats(ctx, ops, os.Args[2:]); err != nil {
			log.Fatalf("stats: %v", err)
		}
	case "compact":
		if err := runCompact(ctx, ops, os.Args[2:]); err != nil {
			log.Fatalf("compact: %v", err)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func runStats(ctx context.Context, ops *geotruthops.Ops, args []string) error {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	stats, err := ops.Stats(ctx)
	if err != nil {
		return err
	}
	return printJSON(stats)
}

func runCompact(ctx context.Context, ops *geotruthops.Ops, args []string) error {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	execute := fs.Bool("execute", false, "perform destructive deletes after export")
	exportDir := fs.String("export-dir", "", "directory for NDJSON audit export files")
	includePublic := fs.Bool("gt-events", true, "include GT_EVENTS compaction")
	includeTombstones := fs.Bool("removed-tombstones", true, "include removed object checkpoint compaction")
	if err := fs.Parse(args); err != nil {
		return err
	}

	plan, err := ops.PlanCompaction(ctx, geotruthops.CompactionOptions{
		IncludePublicGTEvents:    *includePublic,
		IncludeRemovedTombstones: *includeTombstones,
	})
	if err != nil {
		return err
	}

	if !*execute {
		return printJSON(struct {
			DryRun bool                       `json:"dry_run"`
			Plan   geotruthops.CompactionPlan `json:"plan"`
		}{DryRun: true, Plan: plan})
	}

	result, err := ops.Compact(ctx, plan, geotruthops.CompactOptions{
		ExportDir: *exportDir,
		Execute:   true,
	})
	if err != nil {
		return err
	}
	return printJSON(result)
}

func printJSON(v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: geotruth-admin <stats|compact> [flags]\n")
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "commands:\n")
	fmt.Fprintf(os.Stderr, "  stats\n")
	fmt.Fprintf(os.Stderr, "  compact [--execute] [--export-dir DIR] [--gt-events=true] [--removed-tombstones=true]\n")
}
