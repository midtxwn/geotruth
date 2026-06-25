package gtevents

import "time"

// TransitionRecord is diagnostic metadata stored in the state record.
// It is NOT used for boot recovery (that uses InsideAreaIDs). Useful for
// observability and manual repair - shows which geofence transitions
// occurred alongside this state snapshot.
type TransitionRecord struct {
	AreaID  string `json:"area_id"`
	Entered bool   `json:"entered"`
}

type ObjectStateRecord struct {
	ObjectID         string             `json:"object_id"`
	Lifecycle        string             `json:"lifecycle"`
	DetectorStateSeq uint64             `json:"detector_state_spatial_seq"`
	Region           *string            `json:"region"`
	Position         *EventPosition     `json:"position,omitempty"`
	Dims             EventDims          `json:"dims"`
	InsideAreaIDs    []string           `json:"inside_area_ids"`
	Transitions      []TransitionRecord `json:"transitions,omitempty"`
	UpdatedAt        time.Time          `json:"updated_at"`
}
