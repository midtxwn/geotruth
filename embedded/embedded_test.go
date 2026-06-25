package embedded

import (
	"context"
	"strings"
	"testing"

	"github.com/midtxwn/geotruth/pkg/ingester"
)

func TestRunIngesterRequiresNATSConnector(t *testing.T) {
	ctx := context.Background()
	natsSvc, err := RunNATSServer(ctx, NATSServerConfig{Port: -1})
	if err != nil {
		t.Fatalf("start nats: %v", err)
	}
	defer natsSvc.Shutdown()

	_, err = RunIngester(ctx, DefaultConfig.Ingester, ingester.Dependencies{})
	if err == nil {
		t.Fatal("expected missing NATS error")
	}
	if !strings.Contains(err.Error(), "NATS") {
		t.Fatalf("error = %v, want NATS", err)
	}
}
