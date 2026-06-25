package geotruthops

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ExportRecord is one newline-delimited JSON audit record written before
// destructive compaction removes a JetStream message.
type ExportRecord struct {
	ExportedAt  time.Time       `json:"exported_at"`
	Stream      string          `json:"stream"`
	StreamSeq   uint64          `json:"stream_seq"`
	Subject     string          `json:"subject"`
	Headers     nats.Header     `json:"headers,omitempty"`
	PublishedAt time.Time       `json:"published_at"`
	Data        json.RawMessage `json:"data,omitempty"`
	DataBase64  string          `json:"data_base64,omitempty"`
}

type exportWriter struct {
	dir   string
	files map[string]*os.File
}

func newExportWriter(dir string) (*exportWriter, error) {
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create export dir: %w", err)
	}
	return &exportWriter{dir: dir, files: make(map[string]*os.File)}, nil
}

func (w *exportWriter) Close() error {
	if w == nil {
		return nil
	}
	var firstErr error
	for _, f := range w.files {
		if err := f.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (w *exportWriter) Write(kind string, msg *jetstream.RawStreamMsg) error {
	if w == nil {
		return nil
	}
	f, err := w.file(kind)
	if err != nil {
		return err
	}

	rec := ExportRecord{
		ExportedAt:  time.Now().UTC(),
		Stream:      kindStream(kind),
		StreamSeq:   msg.Sequence,
		Subject:     msg.Subject,
		Headers:     msg.Header,
		PublishedAt: msg.Time,
	}
	if json.Valid(msg.Data) {
		rec.Data = json.RawMessage(msg.Data)
	} else {
		rec.DataBase64 = base64.StdEncoding.EncodeToString(msg.Data)
	}

	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal export record: %w", err)
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write export record: %w", err)
	}
	return nil
}

func (w *exportWriter) file(kind string) (*os.File, error) {
	if f := w.files[kind]; f != nil {
		return f, nil
	}
	name := filepath.Join(w.dir, kind+".ndjson")
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open export file %s: %w", name, err)
	}
	w.files[kind] = f
	return f, nil
}

func kindStream(kind string) string {
	switch kind {
	case exportSpatial:
		return StreamSpatial
	case exportGTEventsPublic, exportGTEventsRemovedState:
		return StreamGTEvents
	default:
		return kind
	}
}
