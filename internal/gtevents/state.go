package gtevents

import "time"

type Lifecycle uint8

const (
	LifecycleUnknown Lifecycle = iota
	LifecycleActive
	LifecycleRemoved
)

func (l Lifecycle) String() string {
	switch l {
	case LifecycleActive:
		return "active"
	case LifecycleRemoved:
		return "removed"
	default:
		return "unknown"
	}
}

// GeofenceTransitionRecord is internal recovery metadata stored in a compact
// checkpoint inside the commit-bearing object event.
type GeofenceTransitionRecord struct {
	AreaID  string
	Entered bool
}

// ObjectStateRecord is the internal recovery view rebuilt from compact
// commit-bearing object events. InstanceID identifies one object incarnation,
// and CommitSeq is monotonic within that incarnation.
type ObjectStateRecord struct {
	ObjectID            string
	InstanceID          string
	CommitSeq           uint64
	Lifecycle           Lifecycle
	Region              *string
	Position            *EventPosition
	Dims                EventDims
	InsideAreaIDs       []string
	GeofenceTransitions []GeofenceTransitionRecord
	UpdatedAt           time.Time
}
