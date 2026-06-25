package gtevents

import (
	"strconv"
)

// Event IDs are deterministic, semantic identifiers carried in GT_EVENTS JSON
// payloads for consumer-side deduplication, repair checks, and audit trails.
// They encode the object incarnation and per-incarnation commit sequence so
// retries of the same logical event produce the same ID.
//
// JetStream Nats-Msg-Id headers are intentionally not used on the hot path:
// their duplicate window is temporary and their header overhead is significant.
// Consumer idempotency must therefore be based on the payload event ID.
//
// Object and area IDs must not contain '.' (validated at entry points) and are
// safe to use directly in event ID strings without encoding.

func FormatCommitSeq(seq uint64) string {
	return strconv.FormatUint(seq, 36)
}

func PositionEventID(objectID, instanceID string, commitSeq uint64) string {
	return "gt:p:" + objectID + ":" + instanceID + ":" + FormatCommitSeq(commitSeq)
}

func GeofenceEnterEventID(areaID, objectID, instanceID string, commitSeq uint64) string {
	return "gt:gf:e:" + areaID + ":" + objectID + ":" + instanceID + ":" + FormatCommitSeq(commitSeq)
}

func GeofenceExitEventID(areaID, objectID, instanceID string, commitSeq uint64) string {
	return "gt:gf:x:" + areaID + ":" + objectID + ":" + instanceID + ":" + FormatCommitSeq(commitSeq)
}

func ObjectRegisteredEventID(objectID, instanceID string, commitSeq uint64) string {
	return "gt:o:r:" + objectID + ":" + instanceID + ":" + FormatCommitSeq(commitSeq)
}

func ObjectRemovedEventID(objectID, instanceID string, commitSeq uint64) string {
	return "gt:o:d:" + objectID + ":" + instanceID + ":" + FormatCommitSeq(commitSeq)
}

func GeoTruthBootedEventID(bootEpochSeq uint64) string {
	return "gt:geotruth:booted:" + FormatCommitSeq(bootEpochSeq)
}

func NewInstanceID(bootEpochSeq, localCounter uint64) string {
	return "b" + strconv.FormatUint(bootEpochSeq, 36) + "i" + strconv.FormatUint(localCounter, 36)
}
