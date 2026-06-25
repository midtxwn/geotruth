package gtevents

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	internalnatskeys "github.com/midtxwn/geotruth/internal/natskeys"
	"github.com/midtxwn/geotruth/pkg/natskeys"
	"github.com/midtxwn/geotruth/pkg/natspublish"
	"github.com/midtxwn/geotruth/pkg/natswatch"

	"github.com/nats-io/nats.go/jetstream"
)

const StreamDescription = "GeoTruth processed public events and internal recovery state"

var StreamSubjects = []string{
	natswatch.GTEventsWildcard,
	GTInternalWildcard,
}

type StreamConfig struct {
	Storage  jetstream.StorageType
	MaxBytes int64
	Replicas int
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
		AllowRollup: true,
		AllowMsgTTL: true,
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

func RecoverObjectState(ctx context.Context, js jetstream.JetStream, bootID string) (
	states map[string]*ObjectStateRecord,
	committedSeqByObject map[string]uint64,
	err error,
) {
	states = make(map[string]*ObjectStateRecord)
	committedSeqByObject = make(map[string]uint64)

	cons, err := js.CreateConsumer(ctx, natskeys.GTStreamName, jetstream.ConsumerConfig{ // EPHEMERAL BOOT CONSUMER
		Name:          "GEOTRUTH_STATE_" + bootID,
		FilterSubject: SubjectObjectStateWildcard,
		DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("create state recovery consumer: %w", err)
	}
	defer func() {
		if ci := cons.CachedInfo(); ci != nil {
			_ = js.DeleteConsumer(ctx, natskeys.GTStreamName, ci.Name)
		}
	}()

	for {
		msgs, fetchErr := cons.FetchNoWait(256)
		if fetchErr != nil {
			break
		}

		count := 0
		for msg := range msgs.Messages() {
			var st ObjectStateRecord
			if jsonErr := json.Unmarshal(msg.Data(), &st); jsonErr != nil {
				_ = msg.Ack()
				log.Printf("[gtevents] skipping bad state record: %v", jsonErr)
				continue
			}

			states[st.ObjectID] = &st
			committedSeqByObject[st.ObjectID] = st.DetectorStateSeq
			_ = msg.Ack()
			count++
		}

		if count == 0 {
			break
		}
	}

	log.Printf("[gtevents] recovered %d object state records", len(states))
	return states, committedSeqByObject, nil
}

func ShouldSkipSource(msg jetstream.Msg, committedSeqByObject map[string]uint64) bool {
	meta, err := msg.Metadata()
	if err != nil || meta == nil {
		return false
	}
	seq := meta.Sequence.Stream

	objectID := parseObjectIDFromSubject(msg.Subject())
	if objectID == "" {
		objectID = parseObjectIDFromBody(msg)
	}
	if objectID == "" {
		return false
	}

	return seq <= committedSeqByObject[objectID]
}

func parseObjectIDFromSubject(subject string) string {
	const posPrefix = "pos.raw."
	if len(subject) > len(posPrefix) && subject[:len(posPrefix)] == posPrefix {
		return subject[len(posPrefix):]
	}
	return ""
}

func parseObjectIDFromBody(msg jetstream.Msg) string {
	switch msg.Subject() {
	case internalnatskeys.SubjectCmdObjRegister:
		var reg natspublish.ObjectRegisterMsg
		if err := json.Unmarshal(msg.Data(), &reg); err == nil {
			return reg.ID
		}
	case internalnatskeys.SubjectCmdObjRemove:
		var rm natspublish.ObjectRemoveMsg
		if err := json.Unmarshal(msg.Data(), &rm); err == nil {
			return rm.ID
		}
	}
	return ""
}
