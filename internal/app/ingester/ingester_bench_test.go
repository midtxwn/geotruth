package ingester

import (
	"encoding/json"
	"testing"

	"github.com/midtxwn/geotruth/pkg/natspublish"
)

var (
	benchPositionSubject string
	benchPositionPayload []byte
)

func BenchmarkBuildPositionPublish(b *testing.B) {
	req := natspublish.UpdatePositionReq{
		ID:   "bench-object",
		X:    123.4,
		Y:    567.8,
		Z:    1.0,
		RotY: 0.25,
	}
	data, err := json.Marshal(req)
	if err != nil {
		b.Fatalf("marshal request: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		subj, payload, err := buildPositionPublish("bench-object", data)
		if err != nil {
			b.Fatalf("build position publish: %v", err)
		}
		benchPositionSubject = subj
		benchPositionPayload = payload
	}
}
