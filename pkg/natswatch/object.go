package natswatch

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/midtxwn/geotruth/pkg/natsconsumer"
)

func PositionUpdates(ctx context.Context, cons *natsconsumer.Consumer, objectID string) (
	events <-chan PositionUpdatedEvent, unsubscribe func(), err error) {

	subject := GTSubjectPositionUpdated(objectID)
	msgCh, unsub, err := cons.Subscribe(ctx, subject)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe for position updates: %w", err)
	}

	ch := make(chan PositionUpdatedEvent, 256)
	go func() {
		defer close(ch)
		for {
			select {
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				if eventTypeName(msg.Subject()) != eventTypePositionUpdated {
					continue
				}
				var event PositionUpdatedEvent
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

func GeofenceTransitions(ctx context.Context, cons *natsconsumer.Consumer, objectID string) (
	events <-chan GeofenceTransitionEvent, unsubscribe func(), err error) {

	subject := GTSubjectObjectGeofence(objectID)
	msgCh, unsub, err := cons.Subscribe(ctx, subject)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe for geofence transitions: %w", err)
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

func ObjectRegistered(ctx context.Context, cons *natsconsumer.Consumer, objectID string) (
	events <-chan ObjectRegisteredEvent, unsubscribe func(), err error) {

	subject := GTSubjectObjectRegistered(objectID)
	msgCh, unsub, err := cons.Subscribe(ctx, subject)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe for object registered: %w", err)
	}

	ch := make(chan ObjectRegisteredEvent, 256)
	go func() {
		defer close(ch)
		for {
			select {
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				if eventTypeName(msg.Subject()) != eventTypeObjectRegistered {
					continue
				}
				var event ObjectRegisteredEvent
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

func ObjectRemoved(ctx context.Context, cons *natsconsumer.Consumer, objectID string) (
	events <-chan ObjectRemovedEvent, unsubscribe func(), err error) {

	subject := GTSubjectObjectRemoved(objectID)
	msgCh, unsub, err := cons.Subscribe(ctx, subject)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe for object removed: %w", err)
	}

	ch := make(chan ObjectRemovedEvent, 256)
	go func() {
		defer close(ch)
		for {
			select {
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				if eventTypeName(msg.Subject()) != eventTypeObjectRemoved {
					continue
				}
				var event ObjectRemovedEvent
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

type ObjectEvent struct {
	PositionUpdated  *PositionUpdatedEvent
	GeofenceEntered  *GeofenceTransitionEvent
	GeofenceExited   *GeofenceTransitionEvent
	ObjectRegistered *ObjectRegisteredEvent
	ObjectRemoved    *ObjectRemovedEvent
}

func WatchObject(ctx context.Context, cons *natsconsumer.Consumer, objectID string) (
	events <-chan ObjectEvent, unsubscribe func(), err error) {

	subject := GTSubjectObjectEvents(objectID)
	msgCh, unsub, err := cons.Subscribe(ctx, subject)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe for watch object: %w", err)
	}

	ch := make(chan ObjectEvent, 256)
	go func() {
		defer close(ch)
		for {
			select {
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				et := eventTypeName(msg.Subject())
				var evt ObjectEvent
				switch et {
				case eventTypePositionUpdated:
					var e PositionUpdatedEvent
					if err := json.Unmarshal(msg.Data(), &e); err != nil {
						continue
					}
					evt.PositionUpdated = &e
				case eventTypeGeofenceEntered:
					var e GeofenceTransitionEvent
					if err := json.Unmarshal(msg.Data(), &e); err != nil {
						continue
					}
					evt.GeofenceEntered = &e
				case eventTypeGeofenceExited:
					var e GeofenceTransitionEvent
					if err := json.Unmarshal(msg.Data(), &e); err != nil {
						continue
					}
					evt.GeofenceExited = &e
				case eventTypeObjectRegistered:
					var e ObjectRegisteredEvent
					if err := json.Unmarshal(msg.Data(), &e); err != nil {
						continue
					}
					evt.ObjectRegistered = &e
				case eventTypeObjectRemoved:
					var e ObjectRemovedEvent
					if err := json.Unmarshal(msg.Data(), &e); err != nil {
						continue
					}
					evt.ObjectRemoved = &e
				default:
					continue
				}
				select {
				case ch <- evt:
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
