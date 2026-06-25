package gtevents

import (
	"time"

	"github.com/midtxwn/geotruth/pkg/natskeys"

	"github.com/nats-io/nats.go"
)

const (
	TTLPosition = 72 * time.Hour

	HeaderExpectedStream = "Nats-Expected-Stream"
	HeaderTTL            = "Nats-TTL"
	HeaderRollup         = "Nats-Rollup"
	HeaderRollupSub      = "sub"
	HeaderMsgID          = "Nats-Msg-Id"
)

// PublicHeaders returns headers for time-bounded public events (e.g. position
// updates). The eventID is set as Nats-Msg-Id for server-side dedup within the
// stream's Duplicates window. Each eventID is deterministic (encodes source
// stream + sequence), so redeliveries of the same logical event are safely
// deduped by the server.
func PublicHeaders(ttl time.Duration, eventID string) nats.Header {
	h := nats.Header{}
	h.Set(HeaderExpectedStream, natskeys.GTStreamName)
	h.Set(HeaderMsgID, eventID)
	if ttl > 0 {
		h.Set(HeaderTTL, ttl.String())
	}
	return h
}

// PublicNoExpiryHeaders returns headers for permanent public events (geofence
// transitions, object lifecycle). No TTL, but Nats-Msg-Id is always set so
// crash-retry redeliveries are deduped at the server level.
func PublicNoExpiryHeaders(eventID string) nats.Header {
	h := nats.Header{}
	h.Set(HeaderExpectedStream, natskeys.GTStreamName)
	h.Set(HeaderMsgID, eventID)
	return h
}

// StateHeaders returns headers for internal state records. Nats-Rollup:sub
// compacts old state records for the same object, keeping only the latest.
// The state record is always the LAST message in the commit envelope - its
// existence proves all preceding public events were published.
//
// Do not add a TTL parameter here without a commit-aware compaction design.
// Removed object state records are recovery tombstones, not just historical
// events: after GT_EVENTS confirms a removed state record but before the
// matching SPATIAL source message ack is durably observed, a crash can cause
// SPATIAL redelivery. Boot recovery uses detector_state_spatial_seq from the
// tombstone to skip that already-committed source message. A simple age-based
// TTL cannot prove the SPATIAL message is no longer needed, so expiring the
// tombstone can weaken idempotent recovery and reprocess old accepted commands.
// Future SPATIAL compaction must be application-level: delete or archive source
// messages and any removed tombstones only after proving the source sequence is
// committed to GT_EVENTS and no longer needed by the GeoTruth durable consumer.
// Do not use stream DiscardOld, rollup, or MaxAge as substitutes for that proof.
func StateHeaders(eventID string) nats.Header {
	h := nats.Header{}
	h.Set(HeaderExpectedStream, natskeys.GTStreamName)
	h.Set(HeaderMsgID, eventID)
	h.Set(HeaderRollup, HeaderRollupSub)
	return h
}
