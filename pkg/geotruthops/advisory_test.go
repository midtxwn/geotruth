package geotruthops

import "testing"

func TestAdvisorySubjects(t *testing.T) {
	if got, want := SubjectStreamPressure("SPATIAL"), "ops.geotruth.v1.stream.SPATIAL.pressure"; got != want {
		t.Fatalf("pressure subject = %q, want %q", got, want)
	}
	if got, want := SubjectStreamPublishFailed("GT_EVENTS"), "ops.geotruth.v1.stream.GT_EVENTS.publish_failed"; got != want {
		t.Fatalf("publish_failed subject = %q, want %q", got, want)
	}
}
