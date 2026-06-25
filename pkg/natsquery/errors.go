package natsquery

import (
	"errors"

	"github.com/midtxwn/geotruth/pkg/messages"
)

// ErrNotFound identifies query requests for missing objects, areas, or floors.
var ErrNotFound = errors.New("query resource not found")

func init() {
	messages.MustRegisterError("natsquery.not_found", ErrNotFound)
}
