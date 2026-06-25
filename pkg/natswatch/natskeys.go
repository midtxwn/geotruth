package natswatch

// GTEventsSubjectVersion is the wire protocol version for GT_EVENTS subjects.
// The Go module may be /v2; this version governs the NATS subject namespace
// only.
const GTEventsSubjectVersion = "v1"

const (
	GTEventsPrefix      = "gt.events." + GTEventsSubjectVersion
	GTObjectEventPrefix = GTEventsPrefix + ".object."
	GTEventsWildcard    = GTEventsPrefix + ".>"
	GTGeoTruthBooted    = GTEventsPrefix + ".geotruth.booted"
)

func GTSubjectPositionUpdated(objectID string) string {
	return GTObjectEventPrefix + objectID + ".position.updated"
}

func GTSubjectGeofenceEntered(objectID, areaID string) string {
	return GTObjectEventPrefix + objectID + ".geofence." + areaID + ".entered"
}

func GTSubjectGeofenceExited(objectID, areaID string) string {
	return GTObjectEventPrefix + objectID + ".geofence." + areaID + ".exited"
}

func GTSubjectObjectRegistered(objectID string) string {
	return GTObjectEventPrefix + objectID + ".registered"
}

func GTSubjectObjectRemoved(objectID string) string {
	return GTObjectEventPrefix + objectID + ".removed"
}

func GTSubjectObjectEvents(objectID string) string {
	return GTObjectEventPrefix + objectID + ".>"
}

func GTSubjectObjectGeofence(objectID string) string {
	return GTObjectEventPrefix + objectID + ".geofence.>"
}

func GTSubjectAreaGeofence(areaID string) string {
	return GTObjectEventPrefix + "*.geofence." + areaID + ".>"
}
