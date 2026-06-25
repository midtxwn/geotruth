package engine

import (
	"context"
	"fmt"
	"log"

	"github.com/midtxwn/geotruth/internal/gtevents"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/messages"
	"github.com/midtxwn/geotruth/pkg/natspublish"
	"github.com/midtxwn/geotruth/pkg/regionresolver"
)

type publisher interface {
	Submit(*gtevents.CommitEnvelope)
	Results() <-chan gtevents.CommitResult
}

type ReplyFunc func(reply string, data []byte)

// Dispatcher serializes commands per object, fans geometry work out to
// per-region workers, and submits immutable GT_EVENTS commit envelopes. A
// caller response is emitted only after the public commit and all projection
// events are confirmed durable.
type Dispatcher struct {
	eng      *Engine
	msgCh    <-chan *Envelope
	gtPub    publisher
	resolver regionresolver.Resolver

	resultCh   chan WorkerResult
	workers    map[string]*RegionWorker
	transition *TransitionExec

	nextInstanceID func() string
	reply          ReplyFunc
}

func NewDispatcher(
	eng *Engine,
	msgCh <-chan *Envelope,
	gtPub publisher,
	resolver regionresolver.Resolver,
	nextInstanceID func() string,
	reply ReplyFunc,
) (*Dispatcher, error) {
	if nextInstanceID == nil {
		return nil, fmt.Errorf("engine dispatcher: next instance ID factory is required")
	}
	if reply == nil {
		return nil, fmt.Errorf("engine dispatcher: reply function is required")
	}
	return &Dispatcher{
		eng:            eng,
		msgCh:          msgCh,
		gtPub:          gtPub,
		resolver:       resolver,
		resultCh:       make(chan WorkerResult, 4096),
		workers:        make(map[string]*RegionWorker),
		nextInstanceID: nextInstanceID,
		reply:          reply,
	}, nil
}

func (d *Dispatcher) Start(ctx context.Context) {
	for _, region := range d.resolver.KnownRegions() {
		w := NewRegionWorker(region, d.eng, d.resultCh)
		d.workers[region] = w
		go w.Run()
	}
	d.transition = NewTransitionExec(d.eng, d.resultCh)
	go d.transition.Run()
}

func (d *Dispatcher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case env, ok := <-d.msgCh:
			if !ok {
				return
			}
			d.onEnvelope(env)
		case res := <-d.resultCh:
			d.onResult(res)
		case cr := <-d.gtPub.Results():
			d.onCommitResult(cr)
		}
	}
}

func (d *Dispatcher) onEnvelope(env *Envelope) {
	switch env.Kind {
	case taskRegister:
		d.onRegister(env)
	case taskUpdate:
		d.onPosition(env)
	case taskRemove:
		d.onRemove(env)
	default:
		d.replyErr(env, fmt.Errorf("unknown object command"))
	}
}

func (d *Dispatcher) onRegister(env *Envelope) {
	if ctl, ok := d.eng.lookupCtl(env.ObjectID); ok {
		if ctl.deleted || (ctl.head != nil && ctl.head.Kind == taskRemove) {
			d.replyErr(env, fmt.Errorf("object %s is being removed", env.ObjectID))
			return
		}
		d.replyErr(env, fmt.Errorf("object %s already registered", env.ObjectID))
		return
	}

	ctl := newObjCtl(env.RegDims)
	ctl.instanceID = d.nextInstanceID()
	d.eng.ctlMu.Lock()
	d.eng.ctls[env.ObjectID] = ctl
	d.eng.ctlMu.Unlock()

	ctl.head = env
	d.submitNewRegister(ctl, env)
}

func (d *Dispatcher) onPosition(env *Envelope) {
	ctl, ok := d.eng.lookupCtl(env.ObjectID)
	if !ok || ctl.deleted {
		d.replyErr(env, fmt.Errorf("object %s is not registered", env.ObjectID))
		return
	}
	if ctl.head != nil || ctl.committing {
		ctl.queue = append(ctl.queue, env)
		return
	}
	ctl.head = env
	d.dispatchPosition(ctl, env)
}

func (d *Dispatcher) onRemove(env *Envelope) {
	ctl, ok := d.eng.lookupCtl(env.ObjectID)
	if !ok || ctl.deleted {
		d.replyErr(env, fmt.Errorf("object %s is not registered", env.ObjectID))
		return
	}
	if ctl.head != nil || ctl.committing {
		ctl.queue = append(ctl.queue, env)
		return
	}
	ctl.head = env
	d.dispatchRemove(ctl, env)
}

func (d *Dispatcher) dispatchPosition(ctl *ObjCtl, env *Envelope) {
	prevRegion := regionresolver.NoPrevRegion
	if ctl.hasRouteRegion {
		prevRegion = &ctl.routeRegion
	}
	resolved, err := d.resolver.Resolve(env.Pos.X, env.Pos.Y, env.Pos.Z, prevRegion)
	if err != nil {
		d.rejectHead(ctl, env, fmt.Errorf("resolve object %s: %w", env.ObjectID, err))
		return
	}
	if _, ok := d.workers[resolved]; !ok {
		d.rejectHead(ctl, env, fmt.Errorf("unknown region %q", resolved))
		return
	}

	kind := taskInit
	oldRegion := ctl.routeRegion
	if ctl.hasRouteRegion {
		if resolved == ctl.routeRegion {
			kind = taskUpdate
		} else {
			kind = taskTransition
		}
	}
	ctl.routeRegion = resolved
	ctl.hasRouteRegion = true

	task := WorkerTask{
		Kind:       kind,
		ID:         env.ObjectID,
		ClientOpID: env.ClientOpID,
		X:          env.Pos.X,
		Y:          env.Pos.Y,
		Z:          env.Pos.Z,
		RotY:       env.Pos.RotY,
		Dims:       ctl.dims,
		Region:     resolved,
		OldRegion:  oldRegion,
		Resp:       d.resultCh,
	}

	if kind == taskTransition {
		d.transition.inbox <- task
		return
	}
	d.workers[resolved].inbox <- task
}

func (d *Dispatcher) dispatchRemove(ctl *ObjCtl, env *Envelope) {
	if !ctl.hasRouteRegion {
		d.submitRemoveWithoutPosition(ctl, env)
		return
	}
	task := WorkerTask{
		Kind:       taskRemove,
		ID:         env.ObjectID,
		ClientOpID: env.ClientOpID,
		Dims:       ctl.dims,
		Region:     ctl.routeRegion,
		Resp:       d.resultCh,
	}
	d.workers[ctl.routeRegion].inbox <- task
}

func (d *Dispatcher) onResult(res WorkerResult) {
	ctl, ok := d.eng.lookupCtl(res.ObjectID)
	if !ok || ctl.head == nil {
		return
	}
	if res.Outcome == outcomeRejected {
		d.rejectHead(ctl, ctl.head, fmt.Errorf("worker rejected object %s", res.ObjectID))
		return
	}

	head := ctl.head
	input := gtevents.CommitInput{
		ObjectID:                res.ObjectID,
		InstanceID:              ctl.instanceID,
		CommitSeq:               ctl.lastCommitSeq + 1,
		ClientOpID:              res.ClientOpID,
		Region:                  res.NewRegion,
		Position:                gtevents.EventPosition{X: res.PostX, Y: res.PostY, Z: res.PostZ, RotY: res.PostRotY},
		Dims:                    gtevents.EventDims{Width: res.PostDims.Width, Height: res.PostDims.Height},
		HasPosition:             head.Kind != taskRemove,
		ProjectionPositionKnown: head.Kind != taskRemove || res.PostHadPosition,
		InsideAreaIDs:           res.PostInsideAreaIDs,
		Lifecycle:               gtevents.LifecycleActive,
		GeofenceTransitions:     res.GeofenceTransitions,
	}
	mutation := gtevents.PrevInsideMutation{
		Kind:          gtevents.PrevInsideSet,
		ObjectID:      res.ObjectID,
		NewRegion:     res.NewRegion,
		CurrentInside: cloneBoolMap(res.PostCurrentInside),
	}

	if head.Kind == taskTransition {
		mutation.Kind = gtevents.PrevInsideMove
		mutation.OldRegion = res.PostOldRegion
	}
	if head.Kind == taskRemove {
		input.Lifecycle = gtevents.LifecycleRemoved
		input.Region = res.PostOldRegion
		input.HasPosition = false
		input.InsideAreaIDs = nil
		mutation.Kind = gtevents.PrevInsideDelete
		mutation.OldRegion = res.PostOldRegion
		mutation.CurrentInside = nil
	}

	d.submitCommit(ctl, head, input, mutation)
}

func (d *Dispatcher) submitNewRegister(ctl *ObjCtl, env *Envelope) {
	msgs, err := gtevents.BuildRegisterCommitMsgs(env.ObjectID, ctl.instanceID, ctl.lastCommitSeq+1, env.ClientOpID, env.RegDims)
	if err != nil {
		d.rejectHead(ctl, env, err)
		return
	}
	d.submitBuilt(ctl, env, msgs, gtevents.PrevInsideMutation{Kind: gtevents.PrevInsideNoop, ObjectID: env.ObjectID})
}

func (d *Dispatcher) submitRemoveWithoutPosition(ctl *ObjCtl, env *Envelope) {
	input := gtevents.CommitInput{
		ObjectID:   env.ObjectID,
		InstanceID: ctl.instanceID,
		CommitSeq:  ctl.lastCommitSeq + 1,
		ClientOpID: env.ClientOpID,
		Dims:       gtevents.EventDims{Width: ctl.dims.Width, Height: ctl.dims.Height},
		Lifecycle:  gtevents.LifecycleRemoved,
	}
	d.submitCommit(ctl, env, input, gtevents.PrevInsideMutation{Kind: gtevents.PrevInsideNoop, ObjectID: env.ObjectID})
}

func (d *Dispatcher) submitCommit(ctl *ObjCtl, env *Envelope, input gtevents.CommitInput, mutation gtevents.PrevInsideMutation) {
	msgs, err := gtevents.BuildCommitMsgs(input)
	if err != nil {
		d.rejectHead(ctl, env, err)
		return
	}
	d.submitBuilt(ctl, env, msgs, mutation)
}

func (d *Dispatcher) submitBuilt(ctl *ObjCtl, env *Envelope, msgs gtevents.CommitMessages, mutation gtevents.PrevInsideMutation) {
	commitSeq := ctl.lastCommitSeq + 1
	envCommitSeq := commitSeqFromMsg(msgs.CheckpointState.CommitSeq, commitSeq)
	commit := &gtevents.CommitEnvelope{
		ObjectID:    env.ObjectID,
		InstanceID:  ctl.instanceID,
		CommitSeq:   envCommitSeq,
		Reply:       env.Reply,
		Commit:      msgs.Commit,
		Projections: msgs.Projections,
		Mutation:    mutation,
	}
	ctl.committing = true
	ctl.commitEnvelope = commit
	d.gtPub.Submit(commit)
}

func (d *Dispatcher) onCommitResult(cr gtevents.CommitResult) {
	ctl, ok := d.eng.lookupCtl(cr.ObjectID)
	if !ok {
		return
	}
	if cr.Err != nil {
		log.Printf("[dispatcher] commit failed object=%s instance=%s commit=%d: %v", cr.ObjectID, cr.InstanceID, cr.CommitSeq, cr.Err)
		if ctl.head != nil {
			d.replyErr(ctl.head, cr.Err)
		}
		d.releaseHead(ctl)
		return
	}

	if ctl.commitEnvelope != nil {
		d.applyPrevInsideMutation(ctl.commitEnvelope.Mutation)
	}
	ctl.lastCommitSeq = cr.CommitSeq
	if cr.Reply != "" {
		d.reply(cr.Reply, messages.OKDataResp(natspublish.CommitAck{
			InstanceID: cr.InstanceID,
			CommitSeq:  cr.CommitSeq,
		}))
	}

	wasRemove := ctl.head != nil && ctl.head.Kind == taskRemove
	ctl.committing = false
	ctl.commitEnvelope = nil
	ctl.head = nil

	if wasRemove {
		d.rejectQueued(ctl, fmt.Errorf("object %s was removed", cr.ObjectID))
		d.eng.ctlMu.Lock()
		delete(d.eng.ctls, cr.ObjectID)
		d.eng.ctlMu.Unlock()
		return
	}
	d.dispatchNext(ctl)
}

func (d *Dispatcher) applyPrevInsideMutation(m gtevents.PrevInsideMutation) {
	switch m.Kind {
	case gtevents.PrevInsideNoop:
		return
	case gtevents.PrevInsideSet:
		d.setPrevInside(m.NewRegion, m.ObjectID, m.CurrentInside)
	case gtevents.PrevInsideMove:
		d.deletePrevInside(m.OldRegion, m.ObjectID)
		d.setPrevInside(m.NewRegion, m.ObjectID, m.CurrentInside)
	case gtevents.PrevInsideDelete:
		d.deletePrevInside(m.OldRegion, m.ObjectID)
	}
}

func (d *Dispatcher) setPrevInside(region, objectID string, inside map[string]bool) {
	if region == "" {
		return
	}
	rs := d.eng.regions[region]
	rs.mu.Lock()
	if len(inside) == 0 {
		delete(rs.prevInside, objectID)
	} else {
		rs.prevInside[objectID] = cloneBoolMap(inside)
	}
	rs.mu.Unlock()
}

func (d *Dispatcher) deletePrevInside(region, objectID string) {
	if region == "" {
		return
	}
	rs := d.eng.regions[region]
	rs.mu.Lock()
	delete(rs.prevInside, objectID)
	rs.mu.Unlock()
}

func (d *Dispatcher) rejectHead(ctl *ObjCtl, env *Envelope, err error) {
	d.replyErr(env, err)
	ctl.head = nil
	d.dispatchNext(ctl)
}

func (d *Dispatcher) releaseHead(ctl *ObjCtl) {
	ctl.committing = false
	ctl.commitEnvelope = nil
	ctl.head = nil
	d.dispatchNext(ctl)
}

func (d *Dispatcher) dispatchNext(ctl *ObjCtl) {
	if len(ctl.queue) == 0 {
		return
	}
	next := ctl.queue[0]
	ctl.queue = ctl.queue[1:]
	ctl.head = next
	switch next.Kind {
	case taskRegister:
		d.rejectHead(ctl, next, fmt.Errorf("object %s already registered", next.ObjectID))
	case taskUpdate:
		d.dispatchPosition(ctl, next)
	case taskRemove:
		d.dispatchRemove(ctl, next)
	default:
		d.rejectHead(ctl, next, fmt.Errorf("unknown queued object command"))
	}
}

func (d *Dispatcher) rejectQueued(ctl *ObjCtl, err error) {
	for _, env := range ctl.queue {
		d.replyErr(env, err)
	}
	ctl.queue = nil
}

func (d *Dispatcher) replyErr(env *Envelope, err error) {
	if env == nil || env.Reply == "" {
		return
	}
	d.reply(env.Reply, messages.ErrResp(err))
}

func cloneBoolMap(in map[string]bool) map[string]bool {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]bool, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func commitSeqFromMsg(stateSeq, fallback uint64) uint64 {
	if stateSeq != 0 {
		return stateSeq
	}
	return fallback
}

func DimsFromPublic(dims domain.ObjectDimensions) gtevents.EventDims {
	return gtevents.EventDims{Width: dims.Width, Height: dims.Height}
}
