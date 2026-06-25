package gtevents

import "github.com/midtxwn/geotruth/pkg/natswatch"

// Internal GT_EVENTS subjects share the same wire version as public ones.
const (
	GTInternalPrefix           = "gt.internal." + natswatch.GTEventsSubjectVersion
	GTInternalWildcard         = GTInternalPrefix + ".>"
	SubjectObjectStatePrefix   = GTInternalPrefix + ".state.object."
	SubjectObjectStateWildcard = SubjectObjectStatePrefix + ">"
)

func SubjectPositionUpdated(objectID string) string {
	return natswatch.GTSubjectPositionUpdated(objectID)
}

func SubjectGeofenceEntered(objectID, areaID string) string {
	return natswatch.GTSubjectGeofenceEntered(objectID, areaID)
}

func SubjectGeofenceExited(objectID, areaID string) string {
	return natswatch.GTSubjectGeofenceExited(objectID, areaID)
}

func SubjectObjectRegistered(objectID string) string {
	return natswatch.GTSubjectObjectRegistered(objectID)
}

func SubjectObjectRemoved(objectID string) string {
	return natswatch.GTSubjectObjectRemoved(objectID)
}

func SubjectObjectState(objectID string) string {
	return SubjectObjectStatePrefix + objectID
}
