package gtevents

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/midtxwn/geotruth/pkg/natskeys"
	"github.com/midtxwn/geotruth/pkg/natswatch"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const StreamDescription = "GeoTruth processed object events with embedded recovery checkpoints"

var StreamSubjects = []string{
	natswatch.GTEventsWildcard,
}

type StreamConfig struct {
	Storage  jetstream.StorageType
	MaxBytes int64
	Replicas int
}

type RecoveredCommit struct {
	State     ObjectStateRecord
	StreamSeq uint64
	Subject   string
	Data      []byte
}

func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg StreamConfig) (jetstream.Stream, error) {
	storage := cfg.Storage
	if storage == 0 {
		storage = jetstream.FileStorage
	}
	replicas := cfg.Replicas
	if replicas <= 0 {
		replicas = 1
	}

	streamCfg := jetstream.StreamConfig{
		Name:        natskeys.GTStreamName,
		Description: StreamDescription,
		Subjects:    StreamSubjects,
		Storage:     storage,
		Retention:   jetstream.LimitsPolicy,
		Discard:     jetstream.DiscardNew,
		MaxAge:      0,
		MaxBytes:    cfg.MaxBytes,
		Duplicates:  10 * time.Minute,
		Replicas:    replicas,
	}

	stream, err := js.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		return nil, err
	}

	log.Printf("[gtevents] stream %s ensured (subjects=%v, storage=%v, replicas=%d, maxBytes=%d)",
		natskeys.GTStreamName, StreamSubjects, storage, replicas, cfg.MaxBytes)
	return stream, nil
}

func RecoverObjectCommits(ctx context.Context, js jetstream.JetStream, bootID string) (map[string]RecoveredCommit, error) {
	commits := make(map[string]RecoveredCommit)

	cons, err := js.CreateConsumer(ctx, natskeys.GTStreamName, jetstream.ConsumerConfig{
		Name:           "GEOTRUTH_STATE_" + bootID,
		FilterSubjects: ObjectCommitFilterSubjects,
		DeliverPolicy:  jetstream.DeliverLastPerSubjectPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("create state recovery consumer: %w", err)
	}
	defer deleteConsumer(ctx, js, cons)

	for {
		msgs, fetchErr := cons.FetchNoWait(256)
		if fetchErr != nil {
			break
		}

		count := 0
		for msg := range msgs.Messages() {
			st, ok, jsonErr := stateAfterFromPublicEvent(msg.Data())
			if jsonErr != nil {
				_ = msg.Ack()
				log.Printf("[gtevents] skipping bad public checkpoint: %v", jsonErr)
				continue
			}
			if !ok {
				_ = msg.Ack()
				log.Printf("[gtevents] skipping public object event without state_after: subject=%s", msg.Subject())
				continue
			}
			meta, metaErr := msg.Metadata()
			if metaErr != nil || meta == nil {
				_ = msg.Ack()
				log.Printf("[gtevents] skipping checkpoint without metadata: subject=%s", msg.Subject())
				continue
			}

			if st.ObjectID != "" && meta.Sequence.Stream >= commits[st.ObjectID].StreamSeq {
				commits[st.ObjectID] = RecoveredCommit{
					State:     st,
					StreamSeq: meta.Sequence.Stream,
					Subject:   msg.Subject(),
					Data:      append([]byte(nil), msg.Data()...),
				}
			}
			_ = msg.Ack()
			count++
		}

		if count == 0 {
			break
		}
	}

	log.Printf("[gtevents] recovered %d object checkpoints", len(commits))
	return commits, nil
}

func RepairLatestProjections(ctx context.Context, js jetstream.JetStream, stream jetstream.Stream, bootID string, commits map[string]RecoveredCommit) error {
	expected := make(map[string]*nats.Msg)
	startSeq := uint64(0)
	for _, commit := range commits {
		projections, err := expectedProjectionMsgs(commit)
		if err != nil {
			return fmt.Errorf("derive expected projections for object %s: %w", commit.State.ObjectID, err)
		}
		if len(projections) == 0 {
			continue
		}
		if startSeq == 0 || commit.StreamSeq+1 < startSeq {
			startSeq = commit.StreamSeq + 1
		}
		for _, msg := range projections {
			expected[eventIDFromBytes(msg.Data)] = msg
		}
	}
	if len(expected) == 0 {
		return nil
	}

	// Use one repair consumer starting at the earliest latest-checkpoint that
	// can require projections. This may scan stale geofence events, but expected
	// IDs include instance_id and commit_seq, so stale projections cannot satisfy
	// a newer checkpoint. A single scan avoids one consumer per object at boot.
	found := make(map[string]struct{}, len(expected))
	cons, err := js.CreateConsumer(ctx, natskeys.GTStreamName, jetstream.ConsumerConfig{
		Name:              "GEOTRUTH_REPAIR_" + bootID,
		FilterSubject:     natswatch.GTSubjectObjectGeofence("*"),
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       startSeq,
		AckPolicy:         jetstream.AckExplicitPolicy,
		InactiveThreshold: 10 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("create projection repair consumer: %w", err)
	}
	defer deleteConsumer(ctx, js, cons)

	for {
		msgs, fetchErr := cons.FetchNoWait(1024)
		if fetchErr != nil {
			break
		}
		count := 0
		for msg := range msgs.Messages() {
			eventID := eventIDFromBytes(msg.Data())
			if _, ok := expected[eventID]; ok {
				found[eventID] = struct{}{}
			}
			_ = msg.Ack()
			count++
		}
		if count == 0 {
			break
		}
	}

	backend := NewJetStreamPublisher(js, stream)
	for eventID, msg := range expected {
		if _, ok := found[eventID]; ok {
			continue
		}
		if _, err := publishOrVerifyOnce(ctx, backend, msg); err != nil {
			return fmt.Errorf("repair projection %s: %w", eventID, err)
		}
	}
	return nil
}

func PublishBooted(ctx context.Context, js jetstream.JetStream, stream jetstream.Stream) (uint64, error) {
	for {
		info, err := stream.Info(ctx)
		if err != nil {
			return 0, fmt.Errorf("gt stream info: %w", err)
		}
		expectedLast := info.State.LastSeq
		bootEpochSeq := expectedLast + 1
		eventID := GeoTruthBootedEventID(bootEpochSeq)
		event := GeoTruthBootedEvent{
			EventID:      eventID,
			EventType:    EventTypeGeoTruthBooted,
			BootEpochSeq: bootEpochSeq,
			OccurredAt:   time.Now().UTC(),
		}
		data, err := json.Marshal(event)
		if err != nil {
			return 0, err
		}
		msg := &nats.Msg{
			Subject: natswatch.GTGeoTruthBooted,
			Data:    data,
		}
		ack, err := js.PublishMsg(ctx, msg, jetstream.WithExpectLastSequence(expectedLast))
		if err != nil {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			default:
			}
			continue
		}
		if ack.Sequence != bootEpochSeq {
			return 0, fmt.Errorf("boot epoch reservation mismatch: got seq %d want %d", ack.Sequence, bootEpochSeq)
		}
		return bootEpochSeq, nil
	}
}

func stateAfterFromPublicEvent(data []byte) (ObjectStateRecord, bool, error) {
	var event struct {
		StateAfter *ObjectStateRecord `json:"state_after"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		return ObjectStateRecord{}, false, err
	}
	if event.StateAfter == nil {
		return ObjectStateRecord{}, false, nil
	}
	return *event.StateAfter, true, nil
}

func expectedProjectionMsgs(commit RecoveredCommit) ([]*nats.Msg, error) {
	if len(commit.State.GeofenceTransitions) == 0 {
		return nil, nil
	}

	input := CommitInput{
		ObjectID:            commit.State.ObjectID,
		InstanceID:          commit.State.InstanceID,
		CommitSeq:           commit.State.CommitSeq,
		Dims:                commit.State.Dims,
		InsideAreaIDs:       commit.State.InsideAreaIDs,
		Lifecycle:           commit.State.Lifecycle,
		GeofenceTransitions: make([]GeofenceTransition, 0, len(commit.State.GeofenceTransitions)),
	}
	if commit.State.Region != nil {
		input.Region = *commit.State.Region
	}
	for _, tr := range commit.State.GeofenceTransitions {
		input.GeofenceTransitions = append(input.GeofenceTransitions, GeofenceTransition{AreaID: tr.AreaID, Entered: tr.Entered})
	}

	switch commit.State.Lifecycle {
	case LifecycleActive:
		if commit.State.Position == nil {
			return nil, fmt.Errorf("active commit with geofence transitions has no position")
		}
		input.Position = *commit.State.Position
		input.HasPosition = true
		input.ProjectionPositionKnown = true
	case LifecycleRemoved:
		var removed ObjectRemovedEvent
		if err := json.Unmarshal(commit.Data, &removed); err != nil {
			return nil, fmt.Errorf("decode removed commit: %w", err)
		}
		if removed.PositionBefore == nil {
			return nil, fmt.Errorf("removed commit with geofence transitions has no position_before")
		}
		input.Position = *removed.PositionBefore
		input.Dims = removed.Dims
		input.ProjectionPositionKnown = true
		if removed.Region != nil {
			input.Region = *removed.Region
		}
	default:
		return nil, fmt.Errorf("unknown recovered lifecycle %q", commit.State.Lifecycle)
	}

	msgs, err := BuildCommitMsgs(input)
	if err != nil {
		return nil, err
	}
	return msgs.Projections, nil
}

func deleteConsumer(ctx context.Context, js jetstream.JetStream, cons jetstream.Consumer) {
	if ci := cons.CachedInfo(); ci != nil {
		_ = js.DeleteConsumer(ctx, natskeys.GTStreamName, ci.Name)
	}
}

func publishOrVerifyOnce(ctx context.Context, publisher MessagePublisher, msg *nats.Msg) (*jetstream.PubAck, error) {
	ack, err := publisher.PublishMsg(ctx, msg)
	if err == nil {
		return ack, nil
	}
	expectedID := eventIDFromBytes(msg.Data)
	raw, getErr := publisher.GetLastMsgForSubject(ctx, msg.Subject)
	if getErr == nil && raw != nil && eventIDFromBytes(raw.Data) == expectedID {
		return &jetstream.PubAck{Stream: natskeys.GTStreamName, Sequence: raw.Sequence, Duplicate: true}, nil
	}
	return nil, err
}

func eventIDFromBytes(data []byte) string {
	var event struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		return ""
	}
	return event.EventID
}
