package gtevents

import (
	"fmt"
	"testing"
)

var benchCommitMsgs int

func BenchmarkBuildCommitMsgs(b *testing.B) {
	for _, transitions := range []int{0, 1, 4, 16} {
		input := benchmarkCommitInput(transitions)
		b.Run(fmt.Sprintf("%d_transitions", transitions), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msgs, err := BuildCommitMsgs(input)
				if err != nil {
					b.Fatalf("BuildCommitMsgs: %v", err)
				}
				benchCommitMsgs = len(msgs)
			}
		})
	}
}

func benchmarkCommitInput(transitions int) CommitInput {
	trs := make([]GeofenceTransition, transitions)
	inside := make([]string, transitions)
	for i := 0; i < transitions; i++ {
		areaID := fmt.Sprintf("area-%d", i)
		trs[i] = GeofenceTransition{AreaID: areaID, Entered: i%2 == 0}
		inside[i] = areaID
	}
	return CommitInput{
		ObjectID:      "bench-object",
		SourceSeq:     42,
		ClientOpID:    "bench-client-op",
		Region:        "0",
		Position:      EventPosition{X: 10, Y: 20, Z: 1, RotY: 0.5},
		Dims:          EventDims{Width: 1, Height: 1},
		HasPosition:   true,
		InsideAreaIDs: inside,
		Lifecycle:     LifecycleActive,
		Transitions:   trs,
	}
}
