package natswatch

import "strings"

type eventType string

const (
	eventTypePositionUpdated  eventType = "position.updated"
	eventTypeGeofenceEntered  eventType = "geofence.entered"
	eventTypeGeofenceExited   eventType = "geofence.exited"
	eventTypeObjectRegistered eventType = "object.registered"
	eventTypeObjectRemoved    eventType = "object.removed"
	eventTypeDetectorBooted   eventType = "detector.booted"
)

func eventTypeName(subject string) eventType {
	if strings.HasSuffix(subject, ".position.updated") {
		return eventTypePositionUpdated
	}
	if strings.HasSuffix(subject, ".entered") {
		return eventTypeGeofenceEntered
	}
	if strings.HasSuffix(subject, ".exited") {
		return eventTypeGeofenceExited
	}
	if strings.HasSuffix(subject, ".registered") {
		return eventTypeObjectRegistered
	}
	if strings.HasSuffix(subject, ".removed") {
		return eventTypeObjectRemoved
	}
	if subject == GTDetectorBooted {
		return eventTypeDetectorBooted
	}
	return ""
}
