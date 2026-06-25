package engine

import (
	"github.com/midtxwn/geotruth/internal/gtevents"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/natspublish"

	"github.com/nats-io/nats.go/jetstream"
)

type taskKind int

const (
	taskInit taskKind = iota
	taskUpdate
	taskTransition
	taskRemove
	taskRegister
	taskTerm
)

const (
	outcomeReady outcome = iota
	outcomeNacked
	outcomeTermed
)

type outcome int

type Envelope struct {
	Msg        jetstream.Msg
	ObjectID   string
	StreamSeq  uint64
	ClientOpID string
	Pos        natspublish.PositionMsg
	RegDims    domain.ObjectDimensions
	Kind       taskKind
}

type WorkerTask struct {
	Kind       taskKind
	Msg        jetstream.Msg
	ID         string
	ClientOpID string
	X, Y, Z    float64
	RotY       float64
	Dims       domain.ObjectDimensions
	Region     string
	OldRegion  string
	StreamSeq  uint64
	Resp       chan<- WorkerResult
}

type WorkerResult struct {
	ObjectID   string
	StreamSeq  uint64
	ClientOpID string
	NewRegion  string
	Outcome    outcome
	Err        error

	PostX, PostY, PostZ, PostRotY float64
	PostDims                      domain.ObjectDimensions
	PostInsideAreaIDs             []string
	Transitions                   []gtevents.GeofenceTransition

	PostCurrentInside map[string]bool

	PostOldRegion string
}
