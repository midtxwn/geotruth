package gtevents

import "github.com/midtxwn/geotruth/pkg/natswatch"

const (
	SubjectPositionUpdatedWildcard  = natswatch.GTObjectEventPrefix + "*.position.updated"
	SubjectObjectRegisteredWildcard = natswatch.GTObjectEventPrefix + "*.registered"
	SubjectObjectRemovedWildcard    = natswatch.GTObjectEventPrefix + "*.removed"
)

var ObjectCommitFilterSubjects = []string{
	SubjectPositionUpdatedWildcard,
	SubjectObjectRegisteredWildcard,
	SubjectObjectRemovedWildcard,
}

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
