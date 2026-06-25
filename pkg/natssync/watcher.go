package natssync

import (
	"context"
	"fmt"
	"sync"

	"github.com/midtxwn/geotruth/pkg/natswatch"
)

type syncEventKind int

const (
	syncEventRegistered syncEventKind = iota
	syncEventPositionUpdated
	syncEventRemoved
)

type syncEventKey struct {
	kind       syncEventKind
	clientOpID string
}

type objectWatcher struct {
	objectID    string
	cancel      context.CancelFunc
	unsubscribe func()

	mu      sync.Mutex
	waiters map[syncEventKey][]chan error
	closed  bool
	err     error
}

func (c *Client) ensureObjectWatcher(objectID string) (*objectWatcher, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, fmt.Errorf("client is closed")
	}
	if watcher := c.watchers[objectID]; watcher != nil {
		if !watcher.isClosed() {
			c.mu.Unlock()
			return watcher, nil
		}
		delete(c.watchers, objectID)
	}
	c.mu.Unlock()

	watchCtx, cancel := context.WithCancel(context.Background())
	events, unsubscribe, err := natswatch.WatchObject(watchCtx, c.cons, objectID)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("watch object %s: %w", objectID, err)
	}

	watcher := &objectWatcher{
		objectID:    objectID,
		cancel:      cancel,
		unsubscribe: unsubscribe,
		waiters:     make(map[syncEventKey][]chan error),
	}
	go watcher.run(events)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		watcher.close()
		return nil, fmt.Errorf("client is closed")
	}
	if existing := c.watchers[objectID]; existing != nil {
		if !existing.isClosed() {
			watcher.close()
			return existing, nil
		}
		delete(c.watchers, objectID)
	}
	c.watchers[objectID] = watcher
	return watcher, nil
}

func (c *Client) evictObjectWatcher(objectID string, watcher *objectWatcher) {
	c.mu.Lock()
	if c.watchers[objectID] == watcher {
		delete(c.watchers, objectID)
	}
	c.mu.Unlock()

	watcher.close()
}

func (w *objectWatcher) addWaiter(kind syncEventKind, clientOpID string) (syncEventKey, chan error, error) {
	key := syncEventKey{kind: kind, clientOpID: clientOpID}
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		err := w.err
		if err != nil {
			return syncEventKey{}, nil, err
		}
		return syncEventKey{}, nil, fmt.Errorf("watcher for object %s is closed", w.objectID)
	}

	ch := make(chan error, 1)
	w.waiters[key] = append(w.waiters[key], ch)
	return key, ch, nil
}

func (w *objectWatcher) wait(ctx context.Context, key syncEventKey, ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		w.removeWaiter(key, ch)
		return fmt.Errorf("timeout waiting for object %s event client_op_id %s: %w", w.objectID, key.clientOpID, ctx.Err())
	}
}

func (w *objectWatcher) isClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

func (w *objectWatcher) removeWaiter(key syncEventKey, ch <-chan error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	waiters := w.waiters[key]
	for i, waiter := range waiters {
		if waiter == ch {
			waiters = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(waiters) == 0 {
		delete(w.waiters, key)
		return
	}
	w.waiters[key] = waiters
}

func (w *objectWatcher) run(events <-chan natswatch.ObjectEvent) {
	for event := range events {
		if key, ok := eventKey(event); ok {
			w.observe(key)
		}
	}
	w.closeWithErr(fmt.Errorf("watcher for object %s is closed", w.objectID))
}

func eventKey(event natswatch.ObjectEvent) (syncEventKey, bool) {
	switch {
	case event.ObjectRegistered != nil:
		if event.ObjectRegistered.ClientOpID == "" {
			return syncEventKey{}, false
		}
		return syncEventKey{kind: syncEventRegistered, clientOpID: event.ObjectRegistered.ClientOpID}, true
	case event.PositionUpdated != nil:
		if event.PositionUpdated.ClientOpID == "" {
			return syncEventKey{}, false
		}
		return syncEventKey{kind: syncEventPositionUpdated, clientOpID: event.PositionUpdated.ClientOpID}, true
	case event.ObjectRemoved != nil:
		if event.ObjectRemoved.ClientOpID == "" {
			return syncEventKey{}, false
		}
		return syncEventKey{kind: syncEventRemoved, clientOpID: event.ObjectRemoved.ClientOpID}, true
	default:
		return syncEventKey{}, false
	}
}

func (w *objectWatcher) observe(key syncEventKey) {
	w.mu.Lock()
	waiters := w.waiters[key]
	if len(waiters) > 0 {
		delete(w.waiters, key)
		w.mu.Unlock()
		for _, waiter := range waiters {
			waiter <- nil
		}
		return
	}

	w.mu.Unlock()
}

func (w *objectWatcher) close() {
	w.cancel()
	if w.unsubscribe != nil {
		w.unsubscribe()
	}
	w.closeWithErr(fmt.Errorf("watcher for object %s is closed", w.objectID))
}

func (w *objectWatcher) closeWithErr(err error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.err = err
	waiters := w.waiters
	w.waiters = make(map[syncEventKey][]chan error)
	w.mu.Unlock()

	for _, waitersForKey := range waiters {
		for _, waiter := range waitersForKey {
			waiter <- err
		}
	}
}
