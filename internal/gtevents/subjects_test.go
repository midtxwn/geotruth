package gtevents

import "testing"

func TestObjectCommitFilterSubjects(t *testing.T) {
	want := []string{
		"gt.events.v1.object.*.position.updated",
		"gt.events.v1.object.*.registered",
		"gt.events.v1.object.*.removed",
	}
	if len(ObjectCommitFilterSubjects) != len(want) {
		t.Fatalf("ObjectCommitFilterSubjects len = %d, want %d", len(ObjectCommitFilterSubjects), len(want))
	}
	for i := range want {
		if ObjectCommitFilterSubjects[i] != want[i] {
			t.Fatalf("ObjectCommitFilterSubjects[%d] = %q, want %q", i, ObjectCommitFilterSubjects[i], want[i])
		}
	}
}
