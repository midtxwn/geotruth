package natsconsumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Config holds the configuration for creating a Consumer. Name is required
// for durable consumers and is auto-generated for ephemeral ones if empty.
// Zero-valued DeliverPolicy, AckWait, and MaxDeliver fields are normalised
// by New to sensible defaults (DeliverNewPolicy, 60s, -1 respectively).
type Config struct {
	Name              string
	Durable           bool
	DeliverPolicy     jetstream.DeliverPolicy
	AckWait           time.Duration
	MaxDeliver        int
	InactiveThreshold time.Duration
	MemoryStorage     bool
	Description       string
}

func DefaultConfig() Config {
	return Config{
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckWait:       60 * time.Second,
		MaxDeliver:    -1,
	}
}

// Consumer wraps a JetStream pull consumer with multiplexed
// subscribe/unsubscribe capability. Multiple subscribers share a single
// Messages() iterator; incoming messages are dispatched to matching
// subscriber channels via a subject trie.
//
// Subscribe adds a subject to the consumer's FilterSubjects (via
// UpdateConsumer) and registers a handler. Unsubscribe removes the
// handler and, if no other subscriber needs that subject, removes it
// from FilterSubjects.
//
// Dispatch policy: messages are delivered to each matching subscriber's
// 256-buffer channel. If a subscriber's channel is full, the dispatch
// loop blocks until space is available or the subscription is cancelled.
// A slow subscriber can therefore delay delivery to other subscribers on
// the same consumer. For full isolation, use separate Consumer instances.
//
// NOTE: The slow-consumer policy may change in a future version to
// disconnect subscribers that cannot keep up, rather than blocking the
// dispatch loop.
type Consumer struct {
	js         jetstream.JetStream
	streamName string
	cfg        Config

	mu             sync.Mutex
	trie           subTrie
	refs           map[string]int
	entries        map[uint64]subEntry
	nextID         uint64
	running        bool
	dispatchCancel context.CancelFunc
	dispatchID     uint64

	cons jetstream.Consumer

	closed bool
}

// New creates a Consumer that owns the JetStream consumer lifecycle.
// The server-side consumer is created lazily on the first Subscribe call
// and deleted on the last Unsubscribe (or on Close). Durable consumers
// require a Name in cfg; if a durable with that name already exists,
// New validates that its DeliverPolicy and AckPolicy match, then reuses
// it with updated FilterSubjects.
//
// Zero-valued fields in cfg are normalised: DeliverPolicy defaults to
// DeliverNewPolicy, AckWait to 60s, MaxDeliver to -1.
//
// Warning: Subscribe() calls UpdateConsumer to modify FilterSubjects on the
// server. Do not modify the consumer's FilterSubjects externally while
// the Consumer is in use.
func New(js jetstream.JetStream, streamName string, cfg Config) (*Consumer, error) {
	if cfg.Durable && cfg.Name == "" {
		return nil, fmt.Errorf("durable consumer requires a Name")
	}
	if cfg.AckWait == 0 {
		cfg.AckWait = 60 * time.Second
	}
	if cfg.MaxDeliver == 0 {
		cfg.MaxDeliver = -1
	}
	if cfg.DeliverPolicy == 0 {
		cfg.DeliverPolicy = jetstream.DeliverNewPolicy
	}
	c := &Consumer{
		js:         js,
		streamName: streamName,
		cfg:        cfg,
		trie:       *newSubTrie(),
		refs:       make(map[string]int),
		entries:    make(map[uint64]subEntry),
	}
	return c, nil
}

// subState guards channel send/close races between dispatch and
// unsubscribe/Close. Without it, dispatch can snapshot a subEntry's
// out channel, then unsubscribe closes that channel before dispatch
// sends on it - panicking. sendMu serialises the send and the close;
// closed is set inside closeOnce.Do so dispatch can skip entries that
// were already closed. All copies of a subEntry share the same
// *subState pointer, so the guard works even though subEntry is
// stored and returned by value in the trie.
type subState struct {
	sendMu    sync.Mutex
	closeOnce sync.Once
	closed    bool
}

type subEntry struct {
	id     uint64
	out    chan jetstream.Msg
	subCtx context.Context
	cancel context.CancelFunc
	state  *subState
}

// Subscribe registers interest in a subject, adds it to the consumer's
// FilterSubjects (if not already present via ref counting), and returns
// a channel of messages matching that subject plus an unsubscribe
// function.
//
// On the first call, the server-side JetStream consumer is created (or
// an existing durable is fetched and validated). Subsequent calls update
// FilterSubjects via UpdateConsumer.
//
// The subject may be a concrete subject or a NATS wildcard pattern
// (containing ">" or "*").  The ">" wildcard may only appear as the last
// token; Subscribe returns an error for invalid patterns like "a.>.b".
//
// Call unsubscribe() to remove the subscription. This decrements the
// ref count for the subject; when the ref count reaches zero, the
// subject is removed from FilterSubjects via UpdateConsumer and, if no
// subjects remain, the server-side consumer is deleted. The returned
// channel is closed on unsubscribe, Close, or internal subscription
// failure. If the channel closes unexpectedly, callers may call Subscribe
// again; if the wrapper has been permanently closed, Subscribe returns
// "consumer is closed".
func (c *Consumer) Subscribe(ctx context.Context, subject string) (
	messages <-chan jetstream.Msg, unsubscribe func(), err error) {

	if err := validateSubject(subject); err != nil {
		return nil, nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil, fmt.Errorf("consumer is closed")
	}

	entryID := c.nextID
	c.nextID++

	subCtx, subCancel := context.WithCancel(context.Background())
	entry := subEntry{
		id:     entryID,
		out:    make(chan jetstream.Msg, 256),
		subCtx: subCtx,
		cancel: subCancel,
		state:  &subState{},
	}

	c.trie.insert(subject, entry)
	c.refs[subject]++
	c.entries[entryID] = entry

	if c.cons == nil {
		if err := c.createConsumerLocked(ctx); err != nil {
			c.removeEntryLocked(subject, entryID)
			return nil, nil, fmt.Errorf("create consumer: %w", err)
		}
	} else {
		if err := c.updateFilterSubjectsLocked(ctx); err != nil {
			if isConsumerMissingError(err) {
				if c.cfg.Durable {
					c.closeConsumerPermanentlyLocked()
					return nil, nil, fmt.Errorf("update filter subjects: %w", err)
				}
				if recoverErr := c.recoverDeletedConsumerLocked(ctx, true); recoverErr != nil {
					c.resetActiveSubscriptionsLocked()
					return nil, nil, fmt.Errorf("recover deleted consumer after update filter subjects: %w", recoverErr)
				}
			} else {
				c.removeEntryLocked(subject, entryID)
				return nil, nil, fmt.Errorf("update filter subjects: %w", err)
			}
		}
	}

	if !c.running {
		if err := c.startDispatchLocked(ctx); err != nil {
			c.removeEntryLocked(subject, entryID)
			return nil, nil, fmt.Errorf("dispatch init: %w", err)
		}
	}

	unsub := func() {
		c.unsubscribe(subject, entryID)
	}

	return entry.out, unsub, nil
}

func (c *Consumer) removeEntryLocked(subject string, entryID uint64) {
	entry, ok := c.entries[entryID]
	if ok {
		entry.cancel()
		entry.state.sendMu.Lock()
		entry.state.closeOnce.Do(func() { close(entry.out); entry.state.closed = true })
		entry.state.sendMu.Unlock()
	}
	c.trie.remove(subject, entryID)
	c.refs[subject]--
	if c.refs[subject] <= 0 {
		delete(c.refs, subject)
	}
	delete(c.entries, entryID)
}

// startDispatchLocked starts the dispatch loop and waits until the Messages
// iterator is ready. c.mu must be held on entry and is held again on return.
func (c *Consumer) startDispatchLocked(ctx context.Context) error {
	if c.closed {
		return fmt.Errorf("consumer is closed")
	}
	if c.running {
		return nil
	}
	if c.cons == nil {
		return fmt.Errorf("no server consumer")
	}

	c.running = true
	c.dispatchID++
	dispatchID := c.dispatchID
	dCtx, dCancel := context.WithCancel(context.Background())
	c.dispatchCancel = dCancel
	dispatchReady := make(chan error, 1)
	go c.dispatch(dCtx, dispatchReady)

	c.mu.Unlock()
	var dispatchErr error
	select {
	case dispatchErr = <-dispatchReady:
	case <-ctx.Done():
		dispatchErr = ctx.Err()
	}
	c.mu.Lock()

	if dispatchErr != nil {
		dCancel()
		if c.dispatchID == dispatchID {
			c.running = false
			c.dispatchCancel = nil
		}
		return dispatchErr
	}

	// Close() may have run while c.mu was unlocked above. If so, the
	// entry's channel is already closed and the consumer is unusable.
	if c.closed {
		dCancel()
		if c.dispatchID == dispatchID {
			c.running = false
			c.dispatchCancel = nil
		}
		return fmt.Errorf("consumer is closed")
	}
	if c.dispatchID != dispatchID {
		dCancel()
		return fmt.Errorf("dispatch init superseded")
	}

	return nil
}

func (c *Consumer) recoverDeletedConsumerLocked(ctx context.Context, cancelCurrent bool) error {
	if c.closed {
		return fmt.Errorf("consumer is closed")
	}
	if cancelCurrent && c.dispatchCancel != nil {
		c.dispatchCancel()
	}
	c.dispatchCancel = nil
	c.running = false
	c.cons = nil

	if len(c.refs) == 0 {
		return nil
	}
	if err := c.createConsumerLocked(ctx); err != nil {
		return fmt.Errorf("create replacement consumer: %w", err)
	}
	if err := c.startDispatchLocked(ctx); err != nil {
		return fmt.Errorf("start replacement dispatch: %w", err)
	}
	return nil
}

// Close stops the dispatch loop, cancels all subscription contexts,
// closes all subscriber channels, and deletes the server-side JetStream
// consumer. After Close, subsequent Subscribe calls return an error.
func (c *Consumer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}

	consName := ""
	if c.cons != nil {
		consName = c.cons.CachedInfo().Name
	}

	c.closeConsumerPermanentlyLocked()
	c.mu.Unlock()

	if consName != "" {
		delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer delCancel()
		if err := c.js.DeleteConsumer(delCtx, c.streamName, consName); err != nil {
			log.Printf("[natsconsumer] delete consumer on close: %v", err)
		}
	}

	return nil
}

// FilterSubjects returns a copy of the current filter subjects on the
// consumer.
func (c *Consumer) FilterSubjects() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.filterSubjectsLocked()
}

// filterSubjectsLocked returns the minimal set of filter subjects for the
// server-side consumer. If a broad wildcard pattern (e.g. "foo.>") contains
// a narrow one (e.g. "foo.bar" or "foo.*"), the narrow one is omitted since
// NATS rejects overlapping filters. The result is sorted for deterministic
// ordering. Must be called with c.mu held.
func (c *Consumer) filterSubjectsLocked() []string {
	subjects := make([]string, 0, len(c.refs))
	for subj := range c.refs {
		subjects = append(subjects, subj)
	}

	var result []string
	for _, subj := range subjects {
		covered := false
		for _, other := range subjects {
			if subj != other && patternContains(other, subj) {
				covered = true
				break
			}
		}
		if !covered {
			result = append(result, subj)
		}
	}

	sort.Strings(result)
	return result
}

// createConsumerLocked creates the server-side JetStream consumer using
// the current refs map as FilterSubjects. For durable consumers that
// already exist, it fetches-and-validates DeliverPolicy/AckPolicy,
// then updates FilterSubjects. Must be called with c.mu held.
func (c *Consumer) createConsumerLocked(ctx context.Context) error {
	filterSubjects := c.filterSubjectsLocked()

	consCfg := jetstream.ConsumerConfig{
		Name:              c.cfg.Name,
		DeliverPolicy:     c.cfg.DeliverPolicy,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           c.cfg.AckWait,
		MaxDeliver:        c.cfg.MaxDeliver,
		FilterSubjects:    filterSubjects,
		InactiveThreshold: c.cfg.InactiveThreshold,
		MemoryStorage:     c.cfg.MemoryStorage,
		Description:       c.cfg.Description,
	}

	if c.cfg.Durable {
		consCfg.Durable = c.cfg.Name
	} else {
		if consCfg.Name == "" {
			consCfg.Name = fmt.Sprintf("ncons-%d", time.Now().UnixNano())
		}
	}

	var cons jetstream.Consumer
	var err error

	if c.cfg.Durable && c.cfg.Name != "" {
		existing, existErr := c.js.Consumer(ctx, c.streamName, c.cfg.Name)
		if existErr == nil {
			info := existing.CachedInfo()
			if info.Config.DeliverPolicy != consCfg.DeliverPolicy {
				return fmt.Errorf("durable consumer %q exists with DeliverPolicy %v, need %v",
					c.cfg.Name, info.Config.DeliverPolicy, consCfg.DeliverPolicy)
			}
			if info.Config.AckPolicy != consCfg.AckPolicy {
				return fmt.Errorf("durable consumer %q exists with AckPolicy %v, need %v",
					c.cfg.Name, info.Config.AckPolicy, consCfg.AckPolicy)
			}
			cons = existing
			updateCfg := info.Config
			updateCfg.FilterSubject = ""
			updateCfg.FilterSubjects = filterSubjects
			updated, err := c.js.UpdateConsumer(ctx, c.streamName, updateCfg)
			if err != nil {
				return fmt.Errorf("update existing durable consumer: %w", err)
			}
			cons = updated
		} else {
			cons, err = c.js.CreateConsumer(ctx, c.streamName, consCfg)
			if err != nil {
				return fmt.Errorf("create durable consumer: %w", err)
			}
		}
	} else {
		cons, err = c.js.CreateConsumer(ctx, c.streamName, consCfg)
		if err != nil {
			return fmt.Errorf("create consumer: %w", err)
		}
	}

	c.cons = cons

	return nil
}

func (c *Consumer) unsubscribe(subject string, id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[id]
	if !ok {
		return
	}

	c.trie.remove(subject, id)
	delete(c.entries, id)
	entry.cancel()
	entry.state.sendMu.Lock()
	entry.state.closeOnce.Do(func() { close(entry.out); entry.state.closed = true })
	entry.state.sendMu.Unlock()

	c.refs[subject]--
	if c.refs[subject] <= 0 {
		delete(c.refs, subject)
	}

	if c.cons == nil {
		return
	}

	needed := c.filterSubjectsLocked()
	if len(needed) == 0 {
		if c.dispatchCancel != nil {
			c.dispatchCancel()
			c.dispatchCancel = nil
		}
		c.running = false

		consName := c.cons.CachedInfo().Name
		delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.js.DeleteConsumer(delCtx, c.streamName, consName); err != nil {
			log.Printf("[natsconsumer] delete consumer on last unsubscribe: %v", err)
		}
		delCancel()
		c.cons = nil
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.updateFilterSubjectsLocked(ctx); err != nil {
			log.Printf("[natsconsumer] update filter subjects on unsubscribe: %v", err)
			if isConsumerMissingError(err) {
				if c.cfg.Durable {
					c.closeConsumerPermanentlyLocked()
					return
				}
				if recoverErr := c.recoverDeletedConsumerLocked(ctx, true); recoverErr != nil {
					log.Printf("[natsconsumer] recover deleted consumer on unsubscribe: %v", recoverErr)
					c.resetActiveSubscriptionsLocked()
				}
			}
		}
	}
}

// updateFilterSubjectsLocked updates the server-side consumer's FilterSubjects
// to match the current refs map. Must be called with c.mu held.
func (c *Consumer) updateFilterSubjectsLocked(ctx context.Context) error {
	if c.cons == nil {
		return nil
	}
	needed := c.filterSubjectsLocked()
	return c.updateFilterSubjectsWith(ctx, needed)
}

// updateFilterSubjectsWith applies the given subjects to the server-side
// consumer via UpdateConsumer and updates c.cons with the returned handle.
func (c *Consumer) updateFilterSubjectsWith(ctx context.Context, subjects []string) error {
	info := c.cons.CachedInfo()
	if info == nil {
		return fmt.Errorf("consumer info not available")
	}
	cfg := info.Config

	cfg.FilterSubject = ""
	cfg.FilterSubjects = subjects

	updated, err := c.js.UpdateConsumer(ctx, c.streamName, cfg)
	if err != nil {
		return fmt.Errorf("update consumer filter subjects: %w", err)
	}
	c.cons = updated
	return nil
}

// dispatch is the main message loop. It creates a Messages iterator on the
// server-side consumer and dispatches each incoming message to matching
// subscribers via the subject trie. dCtx is cancelled on Close or last
// unsubscribe (which stops the dispatch loop and deletes the consumer).
// dispatchReady is signalled once: nil after the iterator is created, or
// with the creation error if it fails.
func (c *Consumer) dispatch(dCtx context.Context, dispatchReady chan<- error) {
	var iter jetstream.MessagesContext
	var err error

	c.mu.Lock()
	cons := c.cons
	c.mu.Unlock()

	if cons == nil {
		dispatchReady <- fmt.Errorf("no server consumer")
		return
	}

	iter, err = cons.Messages(jetstream.PullMaxMessages(1))
	if err != nil {
		dispatchReady <- fmt.Errorf("messages iterator: %w", err)
		c.mu.Lock()
		c.running = false
		c.dispatchCancel = nil
		c.mu.Unlock()
		return
	}
	dispatchReady <- nil
	defer iter.Stop()

	for {
		select {
		case <-dCtx.Done():
			return
		default:
		}

		msg, err := iter.Next()
		if err != nil {
			select {
			case <-dCtx.Done():
				return
			default:
			}

			if isRetriableIteratorError(err) {
				log.Printf("[natsconsumer] retriable iterator error: %v", err)
				continue
			}

			log.Printf("[natsconsumer] iterator error: %v", err)
			c.mu.Lock()
			if isConsumerMissingError(err) {
				if c.cfg.Durable {
					c.closeConsumerPermanentlyLocked()
					c.mu.Unlock()
					return
				}
				if recoverErr := c.recoverDeletedConsumerLocked(context.Background(), false); recoverErr != nil {
					log.Printf("[natsconsumer] recover deleted consumer: %v", recoverErr)
					c.resetActiveSubscriptionsLocked()
				}
				c.mu.Unlock()
				return
			}
			c.resetActiveSubscriptionsLocked()
			c.mu.Unlock()
			return
		}

		c.mu.Lock()
		matches := c.trie.match(msg.Subject())
		c.mu.Unlock()

		for i := range matches {
			e := &matches[i]
			e.state.sendMu.Lock()
			// Skip entries that have been cancelled or closed between the
			// trie snapshot and here. Without these checks, a race between
			// dispatch sending on e.out and unsubscribe/Close closing it
			// can panic. subCtx.Err() catches cancellation; closed catches
			// the case where subCtx is cancelled but the channel close
			// hasn't completed closeOnce yet (or has already completed).
			if e.subCtx.Err() != nil {
				e.state.sendMu.Unlock()
				continue
			}
			if e.state.closed {
				e.state.sendMu.Unlock()
				continue
			}
			select {
			case e.out <- msg:
			case <-e.subCtx.Done():
			case <-dCtx.Done():
				e.state.sendMu.Unlock()
				_ = msg.Ack()
				return
			}
			e.state.sendMu.Unlock()
		}
		_ = msg.Ack()
	}
}

// resetActiveSubscriptionsLocked closes active subscriber channels, cancels
// their contexts, and clears local subscription state. The wrapper remains
// reusable; future Subscribe calls may create a new server-side consumer.
// Must be called with c.mu held.
func (c *Consumer) resetActiveSubscriptionsLocked() {
	if c.dispatchCancel != nil {
		c.dispatchCancel()
	}
	for _, entry := range c.entries {
		entry.cancel()
		entry.state.sendMu.Lock()
		entry.state.closeOnce.Do(func() { close(entry.out); entry.state.closed = true })
		entry.state.sendMu.Unlock()
	}
	c.entries = make(map[uint64]subEntry)
	c.refs = make(map[string]int)
	c.trie = *newSubTrie()
	c.cons = nil
	c.dispatchCancel = nil
	c.running = false
}

// closeConsumerPermanentlyLocked resets active subscriptions and marks the
// wrapper closed. Future Subscribe calls return "consumer is closed". Must be
// called with c.mu held.
func (c *Consumer) closeConsumerPermanentlyLocked() {
	c.resetActiveSubscriptionsLocked()
	c.closed = true
}

func isRetriableIteratorError(err error) bool {
	return errors.Is(err, jetstream.ErrNoHeartbeat) || errors.Is(err, nats.ErrTimeout)
}

func isConsumerMissingError(err error) bool {
	if errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerDoesNotExist) {
		return true
	}

	var jsErr jetstream.JetStreamError
	if errors.As(err, &jsErr) {
		if apiErr := jsErr.APIError(); apiErr != nil && apiErr.ErrorCode == jetstream.JSErrCodeConsumerDoesNotExist {
			return true
		}
	}

	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeConsumerDoesNotExist
}

// validateSubject checks that the subject is a valid NATS subject
// pattern for subscribing. In particular, ">" may only appear as the
// last token.
func validateSubject(subject string) error {
	if subject == ">" || subject == "" {
		return nil
	}
	tokens := strings.Split(subject, ".")
	for i, tok := range tokens {
		if tok == ">" && i != len(tokens)-1 {
			return fmt.Errorf("invalid subject %q: '>' may only appear as the last token", subject)
		}
	}
	return nil
}
