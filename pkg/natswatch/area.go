package natswatch

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/midtxwn/geotruth/pkg/natsconsumer"
)

func AreaGeofenceTransitions(ctx context.Context, cons *natsconsumer.Consumer, areaID string) (
	events <-chan GeofenceTransitionEvent, unsubscribe func(), err error) {

	subject := GTSubjectAreaGeofence(areaID)
	msgCh, unsub, err := cons.Subscribe(ctx, subject)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe for area geofence transitions: %w", err)
	}

	ch := make(chan GeofenceTransitionEvent, 256)
	go func() {
		defer close(ch)
		for {
			select {
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				var event GeofenceTransitionEvent
				if err := json.Unmarshal(msg.Data(), &event); err != nil {
					continue
				}
				select {
				case ch <- event:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, unsub, nil
}

func WatchArea(ctx context.Context, cons *natsconsumer.Consumer, areaID string) (
	events <-chan GeofenceTransitionEvent, unsubscribe func(), err error) {

	return AreaGeofenceTransitions(ctx, cons, areaID)
}
