package gtevents

import "testing"

func TestInternalSubjectConstants(t *testing.T) {
	if GTInternalPrefix != "gt.internal.v1" {
		t.Errorf("GTInternalPrefix = %q, want %q", GTInternalPrefix, "gt.internal.v1")
	}
	if GTInternalWildcard != "gt.internal.v1.>" {
		t.Errorf("GTInternalWildcard = %q, want %q", GTInternalWildcard, "gt.internal.v1.>")
	}
	if SubjectObjectStatePrefix != "gt.internal.v1.state.object." {
		t.Errorf("SubjectObjectStatePrefix = %q, want %q", SubjectObjectStatePrefix, "gt.internal.v1.state.object.")
	}
	if SubjectObjectStateWildcard != "gt.internal.v1.state.object.>" {
		t.Errorf("SubjectObjectStateWildcard = %q, want %q", SubjectObjectStateWildcard, "gt.internal.v1.state.object.>")
	}
}

func TestSubjectObjectState(t *testing.T) {
	got := SubjectObjectState("sensor-1")
	want := "gt.internal.v1.state.object.sensor-1"
	if got != want {
		t.Errorf("SubjectObjectState(%q) = %q, want %q", "sensor-1", got, want)
	}
}
