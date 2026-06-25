package geotruthops

import "time"

const (
	OpsSubjectPrefix = "ops.geotruth.v1.stream."

	PressureLevelUnbounded = "unbounded"
	PressureLevelOK        = "ok"
	PressureLevelWarning   = "warning"
	PressureLevelCritical  = "critical"
)

func SubjectStreamPressure(stream string) string {
	return OpsSubjectPrefix + stream + ".pressure"
}

func SubjectStreamPublishFailed(stream string) string {
	return OpsSubjectPrefix + stream + ".publish_failed"
}

type StreamPressureEvent struct {
	Stream            string    `json:"stream"`
	Level             string    `json:"level"`
	Bytes             uint64    `json:"bytes"`
	MaxBytes          int64     `json:"max_bytes"`
	UsageRatio        float64   `json:"usage_ratio"`
	Messages          uint64    `json:"messages"`
	FirstSeq          uint64    `json:"first_seq"`
	LastSeq           uint64    `json:"last_seq"`
	LatestObservedSeq uint64    `json:"latest_observed_seq"`
	OccurredAt        time.Time `json:"occurred_at"`
}

type StreamPublishFailedEvent struct {
	Stream            string    `json:"stream"`
	Subject           string    `json:"subject,omitempty"`
	Error             string    `json:"error"`
	LatestObservedSeq uint64    `json:"latest_observed_seq"`
	OccurredAt        time.Time `json:"occurred_at"`
}
