package gtevents

import (
	"github.com/midtxwn/geotruth/pkg/natswatch"
)

const (
	EventTypePositionUpdated  = natswatch.EventTypePositionUpdated
	EventTypeGeofenceEntered  = natswatch.EventTypeGeofenceEntered
	EventTypeGeofenceExited   = natswatch.EventTypeGeofenceExited
	EventTypeObjectRegistered = natswatch.EventTypeObjectRegistered
	EventTypeObjectRemoved    = natswatch.EventTypeObjectRemoved
	EventTypeGeoTruthBooted   = natswatch.EventTypeGeoTruthBooted
)

type EventPosition = natswatch.EventPosition

type EventDims = natswatch.EventDims

type EventMeta = natswatch.EventMeta

type ObjectEventMeta = natswatch.ObjectEventMeta

type PositionUpdatedEvent = natswatch.PositionUpdatedEvent

type GeofenceTransitionEvent = natswatch.GeofenceTransitionEvent

type ObjectRegisteredEvent = natswatch.ObjectRegisteredEvent

type ObjectRemovedEvent = natswatch.ObjectRemovedEvent

type GeoTruthBootedEvent = natswatch.GeoTruthBootedEvent
