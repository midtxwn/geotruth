package gtevents

import (
	"strconv"
)

// Event IDs are deterministic, semantic identifiers used for both Nats-Msg-Id
// headers and consumer-side deduplication. They encode the source stream
// sequence so that redeliveries of the same logical event produce the same ID.
//
// Nats-Msg-Id is set on every publish (no longer blocked by atomic batch).
// Consumer idempotency is still required for dedup beyond the 10m server window.
//
// Object and area IDs must not contain '.' (validated at entry points) so
// they are safe to use directly in event ID strings without encoding.

func PositionEventID(objectID string, sourceSeq uint64) string {
	return "gt:position:" + objectID + ":spatial:" + strconv.FormatUint(sourceSeq, 10)
}

func GeofenceEnterEventID(areaID, objectID string, sourceSeq uint64) string {
	return "gt:geofence:entered:" + areaID + ":" + objectID + ":spatial:" + strconv.FormatUint(sourceSeq, 10)
}

func GeofenceExitEventID(areaID, objectID string, sourceSeq uint64) string {
	return "gt:geofence:exited:" + areaID + ":" + objectID + ":spatial:" + strconv.FormatUint(sourceSeq, 10)
}

func ObjectRegisteredEventID(objectID string, sourceSeq uint64) string {
	return "gt:object:registered:" + objectID + ":spatial:" + strconv.FormatUint(sourceSeq, 10)
}

func ObjectRemovedEventID(objectID string, sourceSeq uint64) string {
	return "gt:object:removed:" + objectID + ":spatial:" + strconv.FormatUint(sourceSeq, 10)
}

func DetectorBootedEventID(bootID string) string {
	return "gt:detector:booted:" + bootID
}

// StateRecordID returns the Nats-Msg-Id for the internal state record.
// The state record is always the last message in the commit envelope -
// its existence proves all preceding public events were published.
func StateRecordID(objectID string, sourceSeq uint64) string {
	return "gt:state:" + objectID + ":spatial:" + strconv.FormatUint(sourceSeq, 10)
}
