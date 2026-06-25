package natswatch

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/midtxwn/geotruth/pkg/natsconsumer"
)

func AllPositionUpdates(ctx context.Context, cons *natsconsumer.Consumer) (
	events <-chan PositionUpdatedEvent, unsubscribe func(), err error) {

	msgCh, unsub, err := cons.Subscribe(ctx, GTEventsWildcard)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe for all position updates: %w", err)
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

func AllGeofenceTransitions(ctx context.Context, cons *natsconsumer.Consumer) (
	events <-chan GeofenceTransitionEvent, unsubscribe func(), err error) {

	msgCh, unsub, err := cons.Subscribe(ctx, GTEventsWildcard)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe for all geofence transitions: %w", err)
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
				et := eventTypeName(msg.Subject())
				if et != eventTypeGeofenceEntered && et != eventTypeGeofenceExited {
					continue
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

func AllObjectRegistered(ctx context.Context, cons *natsconsumer.Consumer) (
	events <-chan ObjectRegisteredEvent, unsubscribe func(), err error) {

	msgCh, unsub, err := cons.Subscribe(ctx, GTEventsWildcard)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe for all object registered: %w", err)
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

func AllObjectRemoved(ctx context.Context, cons *natsconsumer.Consumer) (
	events <-chan ObjectRemovedEvent, unsubscribe func(), err error) {

	msgCh, unsub, err := cons.Subscribe(ctx, GTEventsWildcard)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe for all object removed: %w", err)
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

func GeoTruthBooted(ctx context.Context, cons *natsconsumer.Consumer) (
	events <-chan GeoTruthBootedEvent, unsubscribe func(), err error) {

	msgCh, unsub, err := cons.Subscribe(ctx, GTGeoTruthBooted)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe for geotruth booted: %w", err)
	}

	ch := make(chan GeoTruthBootedEvent, 256)
	go func() {
		defer close(ch)
		for {
			select {
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				var event GeoTruthBootedEvent
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
