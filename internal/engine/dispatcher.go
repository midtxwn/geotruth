package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/midtxwn/geotruth/internal/gtevents"
	privKeys "github.com/midtxwn/geotruth/internal/natskeys"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/natspublish"
	"github.com/midtxwn/geotruth/pkg/regionresolver"

	"github.com/nats-io/nats.go/jetstream"
)

const maxPositionBeforeRegisterDeliveries uint64 = 120

type publisher interface {
	Submit(env *gtevents.CommitEnvelope)
	Results() <-chan gtevents.CommitResult
}

type Dispatcher struct {
	eng                  *Engine
	msgCh                chan jetstream.Msg
	doneCh               chan WorkerResult
	workers              map[string]*RegionWorker
	transExec            *TransitionExec
	gtPub                publisher
	committedSeqByObject map[string]uint64
	resolver             regionresolver.Resolver
	knownSet             map[string]struct{}
}

func NewDispatcher(eng *Engine, msgCh chan jetstream.Msg, gtPub publisher, resolver regionresolver.Resolver, committedSeqByObject map[string]uint64) *Dispatcher {
	doneCh := make(chan WorkerResult, 1024)

	knownRegions := resolver.KnownRegions()
	workers := make(map[string]*RegionWorker, len(knownRegions))
	for _, r := range knownRegions {
		workers[r] = NewRegionWorker(r, eng, doneCh)
	}

	knownSet := make(map[string]struct{}, len(knownRegions))
	for _, r := range knownRegions {
		knownSet[r] = struct{}{}
	}

	transExec := NewTransitionExec(eng, doneCh)

	if committedSeqByObject == nil {
		committedSeqByObject = make(map[string]uint64)
	}

	return &Dispatcher{
		eng:                  eng,
		msgCh:                msgCh,
		doneCh:               doneCh,
		workers:              workers,
		transExec:            transExec,
		gtPub:                gtPub,
		committedSeqByObject: committedSeqByObject,
		resolver:             resolver,
		knownSet:             knownSet,
	}
}

func (d *Dispatcher) Start(ctx context.Context) {
	for _, w := range d.workers {
		go w.Run()
	}
	go d.transExec.Run()
}

func (d *Dispatcher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-d.msgCh:
			d.onMsg(msg)
		case res := <-d.doneCh:
			d.onResult(res)
		case cr := <-d.gtPub.Results():
			d.onCommitResult(cr)
		}
	}
}

func (d *Dispatcher) onMsg(msg jetstream.Msg) {
	if gtevents.ShouldSkipSource(msg, d.committedSeqByObject) {
		msg.Ack()
		return
	}

	subject := msg.Subject()

	switch {
	case strings.HasPrefix(subject, "pos.raw."):
		d.onPosition(msg)
	case subject == privKeys.SubjectCmdObjRegister:
		d.onRegister(msg)
	case subject == privKeys.SubjectCmdObjRemove:
		d.onRemove(msg)
	default:
		d.termPoisonMsg(nil, msg, streamSeq(msg), "unknown SPATIAL subject")
	}
}

// onRegister goes through the GT_EVENTS commit stage. The object is registered
// in the ctls map immediately so subsequent positions can queue behind it, but
// the SPATIAL ack waits until the registration commit is confirmed.
func (d *Dispatcher) onRegister(msg jetstream.Msg) {
	var reg natspublish.ObjectRegisterMsg
	if err := json.Unmarshal(msg.Data(), &reg); err != nil {
		d.termPoisonMsg(nil, msg, streamSeq(msg), fmt.Sprintf("bad register JSON: %v", err))
		return
	}

	seq := streamSeq(msg)
	if err := validateStreamObjectID(reg.ID); err != nil {
		d.termPoisonMsg(nil, msg, seq, fmt.Sprintf("bad register object ID: %v", err))
		return
	}

	if ctl, ok := d.eng.lookupCtl(reg.ID); ok {
		if d.suppressDuplicateDelivery(ctl, msg, seq) {
			return
		}
		env := &Envelope{
			Msg:        msg,
			ObjectID:   reg.ID,
			StreamSeq:  seq,
			ClientOpID: reg.ClientOpID,
			RegDims:    reg.Dims,
			Kind:       taskRegister,
		}
		ctl.pendingSeqs[seq] = struct{}{}
		if ctl.head == nil {
			ctl.head = env
			d.dispatchExistingRegister(ctl, env)
		} else {
			ctl.queue = append(ctl.queue, env)
		}
		return
	}

	ctl := newObjCtl(reg.Dims)
	d.eng.ctlMu.Lock()
	if existing, ok := d.eng.ctls[reg.ID]; ok {
		d.eng.ctlMu.Unlock()
		if d.suppressDuplicateDelivery(existing, msg, seq) {
			return
		}
		env := &Envelope{
			Msg:        msg,
			ObjectID:   reg.ID,
			StreamSeq:  seq,
			ClientOpID: reg.ClientOpID,
			RegDims:    reg.Dims,
			Kind:       taskRegister,
		}
		existing.pendingSeqs[seq] = struct{}{}
		if existing.head == nil {
			existing.head = env
			d.dispatchExistingRegister(existing, env)
		} else {
			existing.queue = append(existing.queue, env)
		}
		return
	}
	d.eng.ctls[reg.ID] = ctl
	d.eng.ctlMu.Unlock()

	msgs, err := gtevents.BuildRegisterCommitMsgs(reg.ID, seq, reg.ClientOpID, reg.Dims)
	if err != nil {
		log.Fatalf("[dispatcher] build register commit: %v", err)
	}

	commitEnv := &gtevents.CommitEnvelope{
		ObjectID:  reg.ID,
		SourceSeq: seq,
		Messages:  msgs,
		Mutation:  gtevents.PrevInsideMutation{Kind: gtevents.PrevInsideNoop},
		SourceMsg: msg,
	}

	ctl.pendingSeqs[seq] = struct{}{}
	ctl.head = &Envelope{
		Msg:        msg,
		ObjectID:   reg.ID,
		StreamSeq:  seq,
		ClientOpID: reg.ClientOpID,
		RegDims:    reg.Dims,
		Kind:       taskRegister,
	}
	ctl.committing = true
	ctl.commitEnvelope = commitEnv

	d.gtPub.Submit(commitEnv)
}

func (d *Dispatcher) onRemove(msg jetstream.Msg) {
	var rm natspublish.ObjectRemoveMsg
	if err := json.Unmarshal(msg.Data(), &rm); err != nil {
		d.termPoisonMsg(nil, msg, streamSeq(msg), fmt.Sprintf("bad remove JSON: %v", err))
		return
	}

	seq := streamSeq(msg)
	if err := validateStreamObjectID(rm.ID); err != nil {
		d.termPoisonMsg(nil, msg, seq, fmt.Sprintf("bad remove object ID: %v", err))
		return
	}

	ctl, ok := d.eng.lookupCtl(rm.ID)
	if !ok {
		ctl = newObjCtl(domain.ObjectDimensions{})
		d.eng.ctlMu.Lock()
		if existing, exists := d.eng.ctls[rm.ID]; exists {
			ctl = existing
		} else {
			d.eng.ctls[rm.ID] = ctl
		}
		d.eng.ctlMu.Unlock()
		if d.suppressDuplicateDelivery(ctl, msg, seq) {
			return
		}
		env := &Envelope{
			Msg:        msg,
			ObjectID:   rm.ID,
			StreamSeq:  seq,
			ClientOpID: rm.ClientOpID,
			Kind:       taskRemove,
		}
		ctl.pendingSeqs[seq] = struct{}{}
		if ctl.head == nil {
			ctl.head = env
			d.dispatchRemove(ctl, env)
		} else {
			ctl.queue = append(ctl.queue, env)
		}
		return
	}

	if d.suppressDuplicateDelivery(ctl, msg, seq) {
		return
	}

	env := &Envelope{
		Msg:        msg,
		ObjectID:   rm.ID,
		StreamSeq:  seq,
		ClientOpID: rm.ClientOpID,
		Kind:       taskRemove,
	}

	if ctl.head == nil {
		ctl.pendingSeqs[seq] = struct{}{}
		ctl.head = env
		d.dispatchRemove(ctl, env)
	} else {
		ctl.pendingSeqs[seq] = struct{}{}
		ctl.queue = append(ctl.queue, env)
	}
}

func (d *Dispatcher) onPosition(msg jetstream.Msg) {
	var pos natspublish.PositionMsg
	if err := json.Unmarshal(msg.Data(), &pos); err != nil {
		d.termPoisonMsg(nil, msg, streamSeq(msg), fmt.Sprintf("bad position JSON: %v", err))
		return
	}

	objectID := pos.ID

	seq := streamSeq(msg)
	subjectObjectID := objectIDFromPositionSubject(msg.Subject())
	if err := validateStreamObjectID(subjectObjectID); err != nil {
		d.termPoisonMsg(nil, msg, seq, fmt.Sprintf("bad position subject object ID: %v", err))
		return
	}
	if err := validateStreamObjectID(objectID); err != nil {
		d.termPoisonMsg(nil, msg, seq, fmt.Sprintf("bad position body object ID: %v", err))
		return
	}
	if subjectObjectID != objectID {
		d.termPoisonMsg(nil, msg, seq, fmt.Sprintf("position subject/body object ID mismatch: subject=%q body=%q", subjectObjectID, objectID))
		return
	}

	ctl, ok := d.eng.lookupCtl(objectID)
	if !ok {
		d.nakOrTermRecoverable(nil, msg, seq, fmt.Sprintf("position before register object=%s", objectID))
		return
	}

	if d.suppressDuplicateDelivery(ctl, msg, seq) {
		return
	}

	env := &Envelope{
		Msg:        msg,
		ObjectID:   objectID,
		StreamSeq:  seq,
		ClientOpID: pos.ClientOpID,
		Pos:        pos,
	}

	if ctl.head == nil {
		kind, resolvedRegion, err := d.classify(ctl, pos)
		if err != nil || kind == taskTerm {
			d.termPoisonMsg(ctl, msg, seq, errString(err))
			return
		}
		env.Kind = kind
		ctl.head = env
		ctl.pendingSeqs[seq] = struct{}{}
		d.dispatch(ctl, env, resolvedRegion)
	} else {
		ctl.pendingSeqs[seq] = struct{}{}
		ctl.queue = append(ctl.queue, env)
	}
}

func (d *Dispatcher) classify(ctl *ObjCtl, pos natspublish.PositionMsg) (taskKind, string, error) {
	if !ctl.hasRouteRegion {
		return taskInit, "", nil
	}
	resolved, err := d.resolver.Resolve(pos.X, pos.Y, pos.Z, &ctl.routeRegion)
	if err != nil {
		return taskTerm, "", fmt.Errorf("resolve object %s: %w", pos.ID, err)
	}
	if !d.knownRegion(resolved) {
		return taskTerm, "", fmt.Errorf("unknown region %q", resolved)
	}
	if resolved != ctl.routeRegion {
		return taskTransition, resolved, nil
	}
	return taskUpdate, resolved, nil
}

func (d *Dispatcher) dispatch(ctl *ObjCtl, env *Envelope, resolvedRegion string) {
	pos := env.Pos
	task := WorkerTask{
		Kind:       env.Kind,
		Msg:        env.Msg,
		ID:         env.ObjectID,
		ClientOpID: env.ClientOpID,
		X:          pos.X, Y: pos.Y, Z: pos.Z, RotY: pos.RotY,
		Dims:      ctl.dims,
		StreamSeq: env.StreamSeq,
		Resp:      d.doneCh,
	}

	switch env.Kind {
	case taskInit:
		resolved, err := d.resolver.Resolve(pos.X, pos.Y, pos.Z, regionresolver.NoPrevRegion)
		if err != nil || !d.knownRegion(resolved) {
			reason := err
			if reason == nil {
				reason = fmt.Errorf("unknown region %q", resolved)
			}
			d.termPoisonMsg(ctl, env.Msg, env.StreamSeq, reason.Error())
			return
		}
		task.Region = resolved
		w, ok := d.workers[resolved]
		if !ok {
			d.termPoisonMsg(ctl, env.Msg, env.StreamSeq, fmt.Sprintf("missing worker for region %q", resolved))
			return
		}
		w.inbox <- task
	case taskUpdate:
		task.Region = resolvedRegion
		d.workers[task.Region].inbox <- task
	case taskTransition:
		task.OldRegion = ctl.routeRegion
		task.Region = resolvedRegion
		d.transExec.inbox <- task
	}
}

func (d *Dispatcher) dispatchRemove(ctl *ObjCtl, env *Envelope) {
	ctl.pubMu.RLock()
	region := ctl.pubRegion
	hasRegion := ctl.hasPubRegion
	ctl.pubMu.RUnlock()

	if hasRegion {
		task := WorkerTask{
			Kind:       taskRemove,
			Msg:        env.Msg,
			ID:         env.ObjectID,
			ClientOpID: env.ClientOpID,
			Region:     region,
			Dims:       ctl.dims,
			StreamSeq:  env.StreamSeq,
			Resp:       d.doneCh,
		}
		d.workers[region].inbox <- task
	} else {
		// Object was registered but never positioned. Build a removed state
		// directly: there is no region and no prevInside entry to clean up.
		ctl.pubMu.Lock()
		ctl.deleted = true
		ctl.hasPubRegion = false
		ctl.pubMu.Unlock()

		msgs, err := gtevents.BuildCommitMsgs(gtevents.CommitInput{
			ObjectID:   env.ObjectID,
			SourceSeq:  env.StreamSeq,
			ClientOpID: env.ClientOpID,
			Lifecycle:  gtevents.LifecycleRemoved,
			Dims:       gtevents.EventDims{Width: ctl.dims.Width, Height: ctl.dims.Height},
		})
		if err != nil {
			log.Fatalf("[dispatcher] build remove commit: %v", err)
		}
		commitEnv := &gtevents.CommitEnvelope{
			ObjectID:  env.ObjectID,
			SourceSeq: env.StreamSeq,
			Messages:  msgs,
			Mutation:  gtevents.PrevInsideMutation{Kind: gtevents.PrevInsideNoop},
			SourceMsg: env.Msg,
		}
		ctl.committing = true
		ctl.commitEnvelope = commitEnv
		d.gtPub.Submit(commitEnv)
	}
}

// onResult receives WorkerResult from workers. A successful result builds the
// GT_EVENTS commit envelope and submits it to the publisher pool. The SPATIAL
// message is not acked here; onCommitResult does that after commit.
func (d *Dispatcher) onResult(res WorkerResult) {
	ctl, ok := d.eng.lookupCtl(res.ObjectID)
	if !ok {
		return
	}

	if res.Outcome == outcomeNacked || res.Outcome == outcomeTermed {
		delete(ctl.pendingSeqs, res.StreamSeq)
		ctl.head = nil
		d.drainQueue(ctl)
		return
	}

	ctl.routeRegion = res.NewRegion
	ctl.hasRouteRegion = true

	lifecycle := gtevents.LifecycleActive
	isRemove := ctl.head != nil && ctl.head.Kind == taskRemove
	if isRemove {
		lifecycle = gtevents.LifecycleRemoved
	}

	input := gtevents.CommitInput{
		ObjectID:   res.ObjectID,
		SourceSeq:  res.StreamSeq,
		ClientOpID: res.ClientOpID,
		Region:     res.NewRegion,

		Position: gtevents.EventPosition{
			X:    res.PostX,
			Y:    res.PostY,
			Z:    res.PostZ,
			RotY: res.PostRotY,
		},

		Dims: gtevents.EventDims{
			Width:  res.PostDims.Width,
			Height: res.PostDims.Height,
		},
		HasPosition:   res.Outcome == outcomeReady && lifecycle != gtevents.LifecycleRemoved,
		InsideAreaIDs: res.PostInsideAreaIDs,
		Lifecycle:     lifecycle,
		Transitions:   res.Transitions,
	}

	msgs, err := gtevents.BuildCommitMsgs(input)
	if err != nil {
		log.Fatalf("[dispatcher] build commit for object=%s seq=%d: %v", res.ObjectID, res.StreamSeq, err)
	}

	// Build the deferred PrevInsideMutation. Workers update idempotent state
	// like R-trees and published region, but prevInside only advances after the
	// state record is durable so redelivery re-detects transitions correctly.
	var mutation gtevents.PrevInsideMutation
	switch {
	case isRemove:
		if res.PostOldRegion != "" {
			mutation = gtevents.PrevInsideMutation{
				Kind:      gtevents.PrevInsideDelete,
				ObjectID:  res.ObjectID,
				OldRegion: res.PostOldRegion,
			}
		} else {
			mutation = gtevents.PrevInsideMutation{Kind: gtevents.PrevInsideNoop}
		}
	case ctl.head != nil && ctl.head.Kind == taskTransition:
		mutation = gtevents.PrevInsideMutation{
			Kind:          gtevents.PrevInsideMove,
			ObjectID:      res.ObjectID,
			OldRegion:     res.PostOldRegion,
			NewRegion:     res.NewRegion,
			CurrentInside: res.PostCurrentInside,
		}
	default:
		if res.PostCurrentInside != nil {
			mutation = gtevents.PrevInsideMutation{
				Kind:          gtevents.PrevInsideSet,
				ObjectID:      res.ObjectID,
				NewRegion:     res.NewRegion,
				CurrentInside: res.PostCurrentInside,
			}
		} else {
			mutation = gtevents.PrevInsideMutation{Kind: gtevents.PrevInsideNoop}
		}
	}

	commitEnv := &gtevents.CommitEnvelope{
		ObjectID:  res.ObjectID,
		SourceSeq: res.StreamSeq,
		Messages:  msgs,
		Mutation:  mutation,
		SourceMsg: ctl.head.Msg,
	}

	ctl.committing = true
	ctl.commitEnvelope = commitEnv
	d.gtPub.Submit(commitEnv)
}

// onCommitResult is called after the GT_EVENTS commit envelope is confirmed
// published, or after publisher shutdown. The state record is the commit marker:
// on success we apply prevInside, ack the SPATIAL message, update bookkeeping,
// and then release the object mailbox.
func (d *Dispatcher) onCommitResult(cr gtevents.CommitResult) {
	ctl, ok := d.eng.lookupCtl(cr.ObjectID)
	if !ok {
		log.Printf("[dispatcher] onCommitResult: object %s not found in ctls, skipping", cr.ObjectID)
		return
	}

	if cr.Err != nil {
		if errors.Is(cr.Err, context.Canceled) {
			// Clean shutdown: leave the source unacked so SPATIAL can redeliver
			// after restart. GT_EVENTS recovery will skip already committed seqs.
			log.Printf("[dispatcher] commit cancelled for object=%s seq=%d - shutting down",
				cr.ObjectID, cr.SourceSeq)
			return
		}

		// Re-submit the same immutable envelope. Nats-Msg-Id dedup handles any
		// messages that were already published before the failure.
		log.Printf("[dispatcher] commit failed for object=%s seq=%d: %v - resubmitting envelope",
			cr.ObjectID, cr.SourceSeq, cr.Err)
		if ctl.commitEnvelope != nil {
			d.gtPub.Submit(ctl.commitEnvelope)
		}
		return
	}

	applyPrevInsideMutation(d.eng, ctl.commitEnvelope.Mutation)

	if ctl.commitEnvelope != nil && ctl.commitEnvelope.SourceMsg != nil {
		_ = ctl.commitEnvelope.SourceMsg.Ack()
	}

	ctl.detectorStateSeq = cr.SourceSeq
	ctl.lastAppliedSeq = cr.SourceSeq
	delete(ctl.pendingSeqs, cr.SourceSeq)
	ctl.committing = false
	ctl.commitEnvelope = nil

	isRemove := ctl.head != nil && ctl.head.Kind == taskRemove

	ctl.head = nil

	if isRemove {
		if d.releaseQueuedObjectCommandAfterRemove(ctl) {
			return
		}
		d.deleteCtl(cr.ObjectID)
		return
	}

	d.releaseNextQueuedItem(ctl)
}

func (d *Dispatcher) suppressDuplicateDelivery(ctl *ObjCtl, msg jetstream.Msg, seq uint64) bool {
	if seq <= ctl.lastAppliedSeq {
		_ = msg.Ack()
		return true
	}
	if _, pending := ctl.pendingSeqs[seq]; pending {
		_ = msg.InProgress()
		return true
	}
	return false
}

func (d *Dispatcher) dispatchExistingRegister(ctl *ObjCtl, env *Envelope) {
	if dimsDiffer(ctl.dims, env.RegDims) {
		log.Printf("[dispatcher] duplicate register for object %s ignored differing dims existing=(%.6g,%.6g) requested=(%.6g,%.6g)",
			env.ObjectID, ctl.dims.Width, ctl.dims.Height, env.RegDims.Width, env.RegDims.Height)
	}

	state := d.eng.snapshotObjectState(env.ObjectID, ctl, env.StreamSeq)
	msgs, err := gtevents.BuildRegisterCurrentStateCommitMsgs(env.ObjectID, env.StreamSeq, env.ClientOpID, state)
	if err != nil {
		log.Fatalf("[dispatcher] build duplicate register commit: %v", err)
	}

	commitEnv := &gtevents.CommitEnvelope{
		ObjectID:  env.ObjectID,
		SourceSeq: env.StreamSeq,
		Messages:  msgs,
		Mutation:  gtevents.PrevInsideMutation{Kind: gtevents.PrevInsideNoop},
		SourceMsg: env.Msg,
	}
	ctl.committing = true
	ctl.commitEnvelope = commitEnv
	d.gtPub.Submit(commitEnv)
}

func (d *Dispatcher) releaseQueuedObjectCommandAfterRemove(ctl *ObjCtl) (releasedObjectCommand bool) {
	for len(ctl.queue) > 0 {
		next := ctl.queue[0]
		ctl.queue = ctl.queue[1:]
		switch next.Kind {
		case taskRemove:
			ctl.head = next
			d.dispatchRemove(ctl, next)
			return true
		case taskRegister:
			ctl.head = next
			d.dispatchRecreateRegister(ctl, next)
			return true
		default:
			// Position updates queued behind a committed remove are stale. Later
			// object commands still get their correlated commit.
			_ = next.Msg.Ack()
			delete(ctl.pendingSeqs, next.StreamSeq)
		}
	}
	d.drainQueue(ctl)
	return false
}

func (d *Dispatcher) dispatchRecreateRegister(ctl *ObjCtl, env *Envelope) {
	ctl.dims = env.RegDims
	ctl.routeRegion = ""
	ctl.hasRouteRegion = false
	ctl.pubMu.Lock()
	ctl.pubRegion = ""
	ctl.hasPubRegion = false
	ctl.deleted = false
	ctl.pubMu.Unlock()

	msgs, err := gtevents.BuildRegisterCommitMsgs(env.ObjectID, env.StreamSeq, env.ClientOpID, env.RegDims)
	if err != nil {
		log.Fatalf("[dispatcher] build recreate register commit: %v", err)
	}
	commitEnv := &gtevents.CommitEnvelope{
		ObjectID:  env.ObjectID,
		SourceSeq: env.StreamSeq,
		Messages:  msgs,
		Mutation:  gtevents.PrevInsideMutation{Kind: gtevents.PrevInsideNoop},
		SourceMsg: env.Msg,
	}
	ctl.committing = true
	ctl.commitEnvelope = commitEnv
	d.gtPub.Submit(commitEnv)
}

func (d *Dispatcher) deleteCtl(objectID string) {
	d.eng.ctlMu.Lock()
	delete(d.eng.ctls, objectID)
	d.eng.ctlMu.Unlock()
	log.Printf("[dispatcher] removed object %s", objectID)
}

func dimsDiffer(a, b domain.ObjectDimensions) bool {
	return a.Width != b.Width || a.Height != b.Height
}

func (d *Dispatcher) knownRegion(id string) bool {
	_, ok := d.knownSet[id]
	return ok
}

func validateStreamObjectID(id string) error {
	if id == "" {
		return fmt.Errorf("id must not be empty")
	}
	if strings.Contains(id, ".") {
		return fmt.Errorf("id %q contains '.' which is not allowed in NATS subjects", id)
	}
	return nil
}

func objectIDFromPositionSubject(subject string) string {
	const posPrefix = "pos.raw."
	if len(subject) <= len(posPrefix) || !strings.HasPrefix(subject, posPrefix) {
		return ""
	}
	return subject[len(posPrefix):]
}

func streamSeq(msg jetstream.Msg) uint64 {
	meta, _ := msg.Metadata()
	if meta == nil {
		return 0
	}
	return meta.Sequence.Stream
}

func deliveryCount(msg jetstream.Msg) uint64 {
	meta, _ := msg.Metadata()
	if meta == nil {
		return 0
	}
	return meta.NumDelivered
}

func errString(err error) string {
	if err == nil {
		return "poison message"
	}
	return err.Error()
}

func (d *Dispatcher) termPoisonMsg(ctl *ObjCtl, msg jetstream.Msg, seq uint64, reason string) {
	log.Printf("[dispatcher] term poison message: subject=%s seq=%d reason=%s", msg.Subject(), seq, reason)
	if err := msg.Term(); err != nil {
		log.Printf("[dispatcher] term poison message failed: subject=%s seq=%d err=%v", msg.Subject(), seq, err)
		return
	}
	if ctl == nil {
		return
	}
	delete(ctl.pendingSeqs, seq)
	if ctl.head != nil && ctl.head.StreamSeq == seq {
		ctl.head = nil
		d.releaseNextQueuedItem(ctl)
	}
}

// nakOrTermRecoverable handles inputs that might become valid after a related
// lifecycle message arrives. They are retried for a bounded number of deliveries
// and then terminated as poison so a bad publisher cannot loop forever.
func (d *Dispatcher) nakOrTermRecoverable(ctl *ObjCtl, msg jetstream.Msg, seq uint64, reason string) {
	if deliveryCount(msg) >= maxPositionBeforeRegisterDeliveries {
		d.termPoisonMsg(ctl, msg, seq, reason)
		return
	}
	log.Printf("[dispatcher] recoverable message not ready: subject=%s seq=%d reason=%s", msg.Subject(), seq, reason)
	_ = msg.NakWithDelay(500 * time.Millisecond)
}

// releaseNextQueuedItem starts the next mailbox item for an object. It is shared
// by normal commit success and terminal poison handling so per-object ordering is
// preserved even when a single bad message is dropped.
func (d *Dispatcher) releaseNextQueuedItem(ctl *ObjCtl) {
	if len(ctl.queue) == 0 {
		return
	}

	next := ctl.queue[0]
	ctl.queue = ctl.queue[1:]
	ctl.head = next

	switch next.Kind {
	case taskRegister:
		d.dispatchExistingRegister(ctl, next)
	case taskRemove:
		d.dispatchRemove(ctl, next)
	default:
		kind, resolvedRegion, err := d.classify(ctl, next.Pos)
		if err != nil || kind == taskTerm {
			d.termPoisonMsg(ctl, next.Msg, next.StreamSeq, errString(err))
			return
		}
		next.Kind = kind
		d.dispatch(ctl, next, resolvedRegion)
	}
}

// applyPrevInsideMutation writes the deferred prevInside change to the engine's
// region state. It is called after the GT_EVENTS commit succeeds and before the
// object mailbox is released.
func applyPrevInsideMutation(eng *Engine, mut gtevents.PrevInsideMutation) {
	switch mut.Kind {
	case gtevents.PrevInsideSet:
		rs := eng.regions[mut.NewRegion]
		rs.mu.Lock()
		rs.prevInside[mut.ObjectID] = mut.CurrentInside
		rs.mu.Unlock()
	case gtevents.PrevInsideMove:
		oldRS := eng.regions[mut.OldRegion]
		oldRS.mu.Lock()
		delete(oldRS.prevInside, mut.ObjectID)
		oldRS.mu.Unlock()
		newRS := eng.regions[mut.NewRegion]
		newRS.mu.Lock()
		newRS.prevInside[mut.ObjectID] = mut.CurrentInside
		newRS.mu.Unlock()
	case gtevents.PrevInsideDelete:
		rs := eng.regions[mut.OldRegion]
		rs.mu.Lock()
		delete(rs.prevInside, mut.ObjectID)
		rs.mu.Unlock()
	case gtevents.PrevInsideNoop:
	}
}

func (d *Dispatcher) drainQueue(ctl *ObjCtl) {
	for _, env := range ctl.queue {
		env.Msg.Ack()
		delete(ctl.pendingSeqs, env.StreamSeq)
	}
	ctl.queue = nil
}
