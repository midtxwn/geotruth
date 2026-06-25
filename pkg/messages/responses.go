package messages

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

type Resp[T any] struct {
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
	Data      T      `json:"data"`
}

type statusResp struct {
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
}

type registeredError struct {
	code   string
	target error
}

var errorRegistry = struct {
	mu      sync.RWMutex
	byCode  map[string]error
	entries []registeredError
}{
	byCode: make(map[string]error),
}

// RemoteError is an error decoded from a response envelope. Its Code is the
// stable wire value used to recover errors.Is behavior across process
// boundaries.
type RemoteError struct {
	Code    string
	Message string
}

func (e RemoteError) Error() string {
	return e.Message
}

func (e RemoteError) Is(target error) bool {
	if e.Code == "" || target == nil {
		return false
	}

	errorRegistry.mu.RLock()
	registered := errorRegistry.byCode[e.Code]
	errorRegistry.mu.RUnlock()
	if registered == nil {
		return false
	}
	return errors.Is(registered, target) || errors.Is(target, registered)
}

// RegisterError maps a stable wire code to a local sentinel error. ErrResp uses
// this registry to encode error_code, and RemoteError.Is uses it to restore
// errors.Is matches after decoding.
func RegisterError(code string, target error) error {
	if code == "" {
		return fmt.Errorf("messages: error code is empty")
	}
	if target == nil {
		return fmt.Errorf("messages: target error for code %q is nil", code)
	}

	errorRegistry.mu.Lock()
	defer errorRegistry.mu.Unlock()

	if existing := errorRegistry.byCode[code]; existing != nil {
		if errors.Is(existing, target) || errors.Is(target, existing) {
			return nil
		}
		return fmt.Errorf("messages: error code %q already registered", code)
	}

	errorRegistry.byCode[code] = target
	errorRegistry.entries = append(errorRegistry.entries, registeredError{code: code, target: target})
	return nil
}

// MustRegisterError is RegisterError for package init functions.
func MustRegisterError(code string, target error) {
	if err := RegisterError(code, target); err != nil {
		panic(err)
	}
}

// ErrorCode returns the stable wire code associated with err, when one exists.
func ErrorCode(err error) (string, bool) {
	if err == nil {
		return "", false
	}

	var remote RemoteError
	if errors.As(err, &remote) && remote.Code != "" {
		return remote.Code, true
	}

	return registeredCode(err)
}

func registeredCode(err error) (string, bool) {
	errorRegistry.mu.RLock()
	defer errorRegistry.mu.RUnlock()

	for _, entry := range errorRegistry.entries {
		if errors.Is(err, entry.target) {
			return entry.code, true
		}
	}
	return "", false
}

func OKResp() []byte {
	b, _ := json.Marshal(statusResp{OK: true})
	return b
}

func ErrResp(err error) []byte {
	r := statusResp{OK: false, Error: err.Error()}
	if code, ok := registeredCode(err); ok {
		r.ErrorCode = code
	}
	b, _ := json.Marshal(r)
	return b
}

func Err(data []byte) error {
	var r statusResp
	if err := json.Unmarshal(data, &r); err != nil {
		return err
	}
	if !r.OK {
		return RemoteError{Code: r.ErrorCode, Message: r.Error}
	}
	return nil
}

func OKDataResp[T any](data T) []byte {
	b, _ := json.Marshal(Resp[T]{OK: true, Data: data})
	return b
}

func Data[T any](raw []byte) (T, error) {
	var r Resp[T]
	if err := json.Unmarshal(raw, &r); err != nil {
		var zero T
		return zero, err
	}
	if !r.OK {
		var zero T
		return zero, RemoteError{Code: r.ErrorCode, Message: r.Error}
	}
	return r.Data, nil
}
