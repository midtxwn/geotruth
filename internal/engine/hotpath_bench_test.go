package engine

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/midtxwn/geotruth/internal/gtevents"
	privKeys "github.com/midtxwn/geotruth/internal/natskeys"
	floorrtree "github.com/midtxwn/geotruth/internal/rtree"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/natspublish"

	"github.com/nats-io/nats.go/jetstream"
)

const benchmarkObjectID = "bench-object"

var (
	benchTask       WorkerTask
	benchResult     WorkerResult
	benchCommitSize int
	benchQueueLen   int
)

func BenchmarkDispatcherOnPositionHeadUpdate(b *testing.B) {
	for _, objectCount := range []int{1, 100, 1000} {
		for _, regionCount := range []int{1, 8, 64} {
			name := fmt.Sprintf("%d_objects_%d_regions", objectCount, regionCount)
			b.Run(name, func(b *testing.B) {
				_, d, _, ctl := benchmarkEngineFixture(objectCount, regionCount, 0, 0, false)
				msg := benchmarkPositionMsg(benchmarkObjectID, 0, 0, 1, 0, 1)
				worker := d.workers["0"]
				seq := uint64(1)

				b.ReportAllocs()
				for b.Loop() {
					msg.seq = seq
					d.onPosition(msg)
					benchTask = <-worker.inbox
					ctl.head = nil
					delete(ctl.pendingSeqs, seq)
					seq++
				}
			})
		}
	}
}

func BenchmarkDispatcherOnPositionQueuedUpdate(b *testing.B) {
	_, d, _, ctl := benchmarkEngineFixture(1000, 8, 0, 0, false)
	msg := benchmarkPositionMsg(benchmarkObjectID, 0, 0, 1, 0, 1)
	ctl.head = &Envelope{ObjectID: benchmarkObjectID, StreamSeq: 1, Kind: taskUpdate}
	seq := uint64(2)

	b.ReportAllocs()
	for b.Loop() {
		msg.seq = seq
		d.onPosition(msg)
		benchQueueLen = len(ctl.queue)
		ctl.queue = ctl.queue[:0]
		delete(ctl.pendingSeqs, seq)
		seq++
	}
}

func BenchmarkDispatcherReleaseQueuedUpdate(b *testing.B) {
	_, d, _, ctl := benchmarkEngineFixture(1000, 8, 0, 0, false)
	pos := natspublish.PositionMsg{ID: benchmarkObjectID, X: 1, Y: 1, Z: 1, RotY: 0}
	worker := d.workers["0"]
	headMsg := benchmarkPositionMsg(benchmarkObjectID, 0, 0, 1, 0, 1)
	nextMsg := benchmarkPositionMsg(benchmarkObjectID, 1, 1, 1, 0, 2)
	headEnv := &Envelope{Msg: headMsg, ObjectID: benchmarkObjectID, Kind: taskUpdate}
	nextEnv := &Envelope{Msg: nextMsg, ObjectID: benchmarkObjectID, Pos: pos}
	commitEnv := &gtevents.CommitEnvelope{
		ObjectID:  benchmarkObjectID,
		Mutation:  gtevents.PrevInsideMutation{Kind: gtevents.PrevInsideNoop},
		SourceMsg: headMsg,
	}
	seq := uint64(1)

	b.ReportAllocs()
	for b.Loop() {
		headSeq := seq
		nextSeq := seq + 1
		headMsg.seq = headSeq
		nextMsg.seq = nextSeq
		headEnv.StreamSeq = headSeq
		nextEnv.StreamSeq = nextSeq
		commitEnv.SourceSeq = headSeq
		ctl.head = headEnv
		ctl.queue = append(ctl.queue[:0], nextEnv)
		ctl.pendingSeqs[headSeq] = struct{}{}
		ctl.pendingSeqs[nextSeq] = struct{}{}
		ctl.committing = true
		ctl.commitEnvelope = commitEnv

		d.onCommitResult(gtevents.CommitResult{ObjectID: benchmarkObjectID, SourceSeq: headSeq})
		benchTask = <-worker.inbox

		ctl.head = nil
		ctl.queue = ctl.queue[:0]
		ctl.committing = false
		ctl.commitEnvelope = nil
		delete(ctl.pendingSeqs, headSeq)
		delete(ctl.pendingSeqs, nextSeq)
		seq += 2
	}
}

func BenchmarkRegionWorkerProcessUpdate(b *testing.B) {
	cases := []struct {
		name      string
		areas     int
		triangles int
		dense     bool
	}{
		{name: "no_areas", areas: 0, triangles: 0, dense: false},
		{name: "10_sparse", areas: 10, triangles: 1, dense: false},
		{name: "100_sparse", areas: 100, triangles: 1, dense: false},
		{name: "100_dense_10_triangles", areas: 100, triangles: 10, dense: true},
		{name: "1000_dense_10_triangles", areas: 1000, triangles: 10, dense: true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			eng, _, _, _ := benchmarkEngineFixture(1, 1, tc.areas, tc.triangles, tc.dense)
			resultCh := make(chan WorkerResult, 1)
			worker := NewRegionWorker("0", eng, resultCh)
			task := benchmarkUpdateTask(resultCh, "0", "", 1)
			seq := uint64(1)

			b.ReportAllocs()
			for b.Loop() {
				task.StreamSeq = seq
				worker.processUpdate(task)
				benchResult = <-resultCh
				seq++
			}
		})
	}
}

func BenchmarkTransitionExecProcessTransition(b *testing.B) {
	for _, insideCount := range []int{0, 4, 16} {
		for _, newRegionAreas := range []int{0, 10, 100} {
			name := fmt.Sprintf("%d_prev_inside_%d_new_areas", insideCount, newRegionAreas)
			b.Run(name, func(b *testing.B) {
				eng, _, _, _ := benchmarkEngineFixture(1, 2, newRegionAreas, 10, true)
				benchmarkSeedPrevInside(eng, "0", insideCount)
				benchmarkSeedPrevInside(eng, "1", insideCount)
				resultCh := make(chan WorkerResult, 1)
				tx := NewTransitionExec(eng, resultCh)
				oldRegion, newRegion := "0", "1"
				seq := uint64(1)

				b.ReportAllocs()
				for b.Loop() {
					task := benchmarkUpdateTask(resultCh, newRegion, oldRegion, seq)
					tx.processTransition(task)
					benchResult = <-resultCh
					oldRegion, newRegion = newRegion, oldRegion
					seq++
				}
			})
		}
	}
}

func BenchmarkGeoTruthHotPathSameRegion(b *testing.B) {
	for _, areas := range []int{0, 100} {
		b.Run(fmt.Sprintf("%d_dense_areas", areas), func(b *testing.B) {
			_, d, mp, _ := benchmarkEngineFixture(1, 1, areas, 10, true)
			worker := d.workers["0"]
			msg := benchmarkPositionMsg(benchmarkObjectID, 0, 0, 1, 0, 1)
			seq := uint64(1)

			b.ReportAllocs()
			for b.Loop() {
				msg.seq = seq
				d.onPosition(msg)
				task := <-worker.inbox
				worker.processUpdate(task)
				res := <-d.doneCh
				d.onResult(res)
				env := <-mp.commitCh
				benchCommitSize = len(env.Messages)
				d.onCommitResult(gtevents.CommitResult{ObjectID: benchmarkObjectID, SourceSeq: seq})
				seq++
			}
		})
	}
}

func BenchmarkGeoTruthHotPathTransition(b *testing.B) {
	for _, areas := range []int{0, 100} {
		b.Run(fmt.Sprintf("%d_dense_areas", areas), func(b *testing.B) {
			_, d, mp, _ := benchmarkEngineFixture(1, 2, areas, 10, true)
			msg := benchmarkPositionMsg(benchmarkObjectID, 0, 0, 5, 0, 1)
			toRegionOneData := benchmarkPositionData(benchmarkObjectID, 0, 0, 5, 0)
			toRegionZeroData := benchmarkPositionData(benchmarkObjectID, 0, 0, 1, 0)
			seq := uint64(1)
			toRegionOne := true

			b.ReportAllocs()
			for b.Loop() {
				if toRegionOne {
					msg.data = toRegionOneData
				} else {
					msg.data = toRegionZeroData
				}
				msg.seq = seq
				d.onPosition(msg)
				task := <-d.transExec.inbox
				d.transExec.processTransition(task)
				res := <-d.doneCh
				d.onResult(res)
				env := <-mp.commitCh
				benchCommitSize = len(env.Messages)
				d.onCommitResult(gtevents.CommitResult{ObjectID: benchmarkObjectID, SourceSeq: seq})
				toRegionOne = !toRegionOne
				seq++
			}
		})
	}
}

func benchmarkEngineFixture(objectCount, regionCount, areaCount, trianglesPerArea int, denseAreas bool) (*Engine, *Dispatcher, *mockPublisher, *ObjCtl) {
	resolver := newTestFlatResolver(regionCount)
	eng := NewEngine(resolver)
	for i := 0; i < objectCount; i++ {
		id := fmt.Sprintf("bench-object-%d", i)
		if i == 0 {
			id = benchmarkObjectID
		}
		eng.RegisterObject(id, domain.ObjectDimensions{Width: 1, Height: 1})
		ctl, _ := eng.lookupCtl(id)
		benchmarkSetCtlRegion(ctl, "0")
	}
	for _, region := range resolver.KnownRegions() {
		benchmarkPopulateAreas(eng.regions[region], region, areaCount, trianglesPerArea, denseAreas)
	}
	mp := newMockPublisher()
	d := NewDispatcher(eng, make(chan jetstream.Msg, 1), mp, resolver, nil)
	ctl, _ := eng.lookupCtl(benchmarkObjectID)
	return eng, d, mp, ctl
}

func benchmarkSetCtlRegion(ctl *ObjCtl, region string) {
	ctl.routeRegion = region
	ctl.hasRouteRegion = true
	ctl.pubRegion = region
	ctl.hasPubRegion = true
}

func benchmarkUpdateTask(resultCh chan<- WorkerResult, region, oldRegion string, seq uint64) WorkerTask {
	return WorkerTask{
		Kind:      taskUpdate,
		Msg:       benchmarkPositionMsg(benchmarkObjectID, 0, 0, 1, 0, seq),
		ID:        benchmarkObjectID,
		X:         0,
		Y:         0,
		Z:         1,
		RotY:      0,
		Dims:      domain.ObjectDimensions{Width: 1, Height: 1},
		Region:    region,
		OldRegion: oldRegion,
		StreamSeq: seq,
		Resp:      resultCh,
	}
}

func benchmarkPositionMsg(id string, x, y, z, rotY float64, seq uint64) *mockMsg {
	return &mockMsg{
		subject: privKeys.SubjectPosRawObject(id),
		data:    benchmarkPositionData(id, x, y, z, rotY),
		seq:     seq,
	}
}

func benchmarkPositionData(id string, x, y, z, rotY float64) []byte {
	data, _ := json.Marshal(natspublish.PositionMsg{ID: id, X: x, Y: y, Z: z, RotY: rotY})
	return data
}

func benchmarkPopulateAreas(rs *RegionState, region string, count, trianglesPerArea int, dense bool) {
	if count == 0 {
		return
	}
	if trianglesPerArea <= 0 {
		trianglesPerArea = 1
	}
	for i := 0; i < count; i++ {
		minX, minY, maxX, maxY := -10.0, -10.0, 10.0, 10.0
		tris := benchmarkAreaTriangles(trianglesPerArea, dense, i)
		if !dense {
			base := 1000.0 + float64(i*10)
			minX, minY, maxX, maxY = base, base, base+1, base+1
		}
		rs.areaTree.Upsert(&floorrtree.AreaItem{
			ID:        fmt.Sprintf("%s-area-%d", region, i),
			Region:    region,
			Triangles: tris,
			MinX:      minX,
			MinY:      minY,
			MaxX:      maxX,
			MaxY:      maxY,
		})
	}
}

func benchmarkAreaTriangles(count int, dense bool, areaIdx int) []domain.Triangle {
	tris := make([]domain.Triangle, count)
	for i := 0; i < count; i++ {
		base := 1000.0 + float64(areaIdx*10+i)
		if dense {
			base = 4.0 + float64(i%8)*0.5
		}
		tris[i] = domain.Triangle{
			{X: base, Y: base},
			{X: base + 0.25, Y: base},
			{X: base, Y: base + 0.25},
		}
	}
	return tris
}

func benchmarkSeedPrevInside(eng *Engine, region string, insideCount int) {
	if insideCount == 0 {
		return
	}
	inside := make(map[string]bool, insideCount)
	for i := 0; i < insideCount; i++ {
		inside[fmt.Sprintf("%s-prev-area-%d", region, i)] = true
	}
	eng.regions[region].prevInside[benchmarkObjectID] = inside
}
