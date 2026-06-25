package engine

import (
	"github.com/midtxwn/geotruth/internal/gtevents"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/natspublish"
)

type taskKind int

const (
	taskInit taskKind = iota
	taskUpdate
	taskTransition
	taskRemove
	taskRegister
)

const (
	outcomeReady outcome = iota
	outcomeRejected
)

type outcome int

type Envelope struct {
	ObjectID   string
	ClientOpID string
	Reply      string
	Pos        natspublish.PositionMsg
	RegDims    domain.ObjectDimensions
	Kind       taskKind
}

func NewRegisterEnvelope(objectID, clientOpID, reply string, dims domain.ObjectDimensions) *Envelope {
	return &Envelope{
		ObjectID:   objectID,
		ClientOpID: clientOpID,
		Reply:      reply,
		RegDims:    dims,
		Kind:       taskRegister,
	}
}

func NewPositionEnvelope(objectID, clientOpID, reply string, pos natspublish.PositionMsg) *Envelope {
	return &Envelope{
		ObjectID:   objectID,
		ClientOpID: clientOpID,
		Reply:      reply,
		Pos:        pos,
		Kind:       taskUpdate,
	}
}

func NewRemoveEnvelope(objectID, clientOpID, reply string) *Envelope {
	return &Envelope{
		ObjectID:   objectID,
		ClientOpID: clientOpID,
		Reply:      reply,
		Kind:       taskRemove,
	}
}

type WorkerTask struct {
	Kind       taskKind
	ID         string
	ClientOpID string
	X, Y, Z    float64
	RotY       float64
	Dims       domain.ObjectDimensions
	Region     string
	OldRegion  string
	Resp       chan<- WorkerResult
}

type WorkerResult struct {
	ObjectID   string
	ClientOpID string
	NewRegion  string
	Outcome    outcome
	Err        error

	PostX, PostY, PostZ, PostRotY float64
	PostDims                      domain.ObjectDimensions
	PostInsideAreaIDs             []string
	GeofenceTransitions           []gtevents.GeofenceTransition

	PostCurrentInside map[string]bool

	PostOldRegion   string
	PostHadPosition bool
}
