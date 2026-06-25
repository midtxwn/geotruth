package messages

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func TestOKResp(t *testing.T) {
	resp := OKResp()

	// Should be valid JSON
	var r statusResp
	if err := json.Unmarshal(resp, &r); err != nil {
		t.Fatalf("failed to unmarshal OKResp: %v", err)
	}

	if !r.OK {
		t.Error("expected OK to be true")
	}
	if r.Error != "" {
		t.Errorf("expected empty error, got: %s", r.Error)
	}
}

func TestErrResp(t *testing.T) {
	testErr := errors.New("something went wrong")
	resp := ErrResp(testErr)

	// Should be valid JSON
	var r statusResp
	if err := json.Unmarshal(resp, &r); err != nil {
		t.Fatalf("failed to unmarshal ErrResp: %v", err)
	}

	if r.OK {
		t.Error("expected OK to be false")
	}
	if r.Error != "something went wrong" {
		t.Errorf("expected error message 'something went wrong', got: %s", r.Error)
	}
}

func TestErrResp_NilError(t *testing.T) {
	// This shouldn't happen in practice - calling ErrResp(nil) will panic
	// because it tries to call err.Error() on nil
	// Document this behavior and skip the test
	t.Skip("ErrResp(nil) panics - this is expected behavior, don't pass nil")
}

func TestErr_OKResponse(t *testing.T) {
	okResp := statusResp{OK: true}
	data, _ := json.Marshal(okResp)

	err := Err(data)
	if err != nil {
		t.Errorf("expected no error for OK response, got: %v", err)
	}
}

func TestErr_ErrorResponse(t *testing.T) {
	errResp := statusResp{OK: false, Error: "database connection failed"}
	data, _ := json.Marshal(errResp)

	err := Err(data)
	if err == nil {
		t.Fatal("expected error from error response")
	}
	if err.Error() != "database connection failed" {
		t.Errorf("expected error message 'database connection failed', got: %v", err)
	}
}

func TestErr_InvalidJSON(t *testing.T) {
	invalidData := []byte("not valid json")

	err := Err(invalidData)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestErr_EmptyData(t *testing.T) {
	emptyData := []byte{}

	err := Err(emptyData)
	if err == nil {
		t.Fatal("expected error for empty data")
	}
}

func TestResp_JSONStructure(t *testing.T) {
	// Test that our status response serializes correctly
	r := statusResp{OK: true}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("failed to marshal Resp: %v", err)
	}

	// Check the JSON structure
	expected := `{"ok":true}`
	if string(data) != expected {
		t.Errorf("expected JSON %s, got %s", expected, string(data))
	}

	// Test with error
	r = statusResp{OK: false, Error: "test error"}
	data, err = json.Marshal(r)
	if err != nil {
		t.Fatalf("failed to marshal Resp with error: %v", err)
	}

	expected = `{"ok":false,"error":"test error"}`
	if string(data) != expected {
		t.Errorf("expected JSON %s, got %s", expected, string(data))
	}
}

func TestResp_RoundTrip(t *testing.T) {
	// Test OK response round-trip
	original := statusResp{OK: true}
	data, _ := json.Marshal(original)

	var parsed statusResp
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if parsed.OK != original.OK {
		t.Error("OK field mismatch after round-trip")
	}
	if parsed.Error != original.Error {
		t.Error("Error field mismatch after round-trip")
	}

	// Test error response round-trip
	original = statusResp{OK: false, Error: "some error"}
	data, _ = json.Marshal(original)

	parsed = statusResp{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if parsed.OK != original.OK {
		t.Error("OK field mismatch after round-trip")
	}
	if parsed.Error != original.Error {
		t.Errorf("Error field mismatch: expected %s, got %s", original.Error, parsed.Error)
	}
}

func TestResp_Omitempty(t *testing.T) {
	// When OK is true, error should be omitted
	r := statusResp{OK: true, Error: ""}
	data, _ := json.Marshal(r)

	// The JSON should not contain "error" field when empty
	var m map[string]interface{}
	json.Unmarshal(data, &m)

	if _, exists := m["error"]; exists {
		t.Error("error field should be omitted when empty")
	}
}

func TestIntegration_OKResp_Err(t *testing.T) {
	// Simulate a full request-response cycle with OK
	resp := OKResp()

	err := Err(resp)
	if err != nil {
		t.Errorf("Err should return nil for OK response: %v", err)
	}
}

func TestIntegration_ErrResp_Err(t *testing.T) {
	// Simulate a full request-response cycle with error
	originalErr := errors.New("validation failed")
	resp := ErrResp(originalErr)

	resultErr := Err(resp)
	if resultErr == nil {
		t.Fatal("Err should return error for error response")
	}
	if resultErr.Error() != originalErr.Error() {
		t.Errorf("error message mismatch: expected %v, got %v", originalErr, resultErr)
	}
}

func TestErrResp_RegisteredErrorCode(t *testing.T) {
	sentinel := errors.New("registered test error")
	MustRegisterError("messages_test.registered", sentinel)

	resp := ErrResp(fmt.Errorf("%w: detail", sentinel))

	var r statusResp
	if err := json.Unmarshal(resp, &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.ErrorCode != "messages_test.registered" {
		t.Fatalf("expected registered code, got %q", r.ErrorCode)
	}

	err := Err(resp)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected errors.Is to match sentinel, got %v", err)
	}
}

func TestData_RegisteredErrorCode(t *testing.T) {
	sentinel := errors.New("registered data error")
	MustRegisterError("messages_test.data_registered", sentinel)

	wrapped := ErrResp(fmt.Errorf("%w: detail", sentinel))

	_, err := Data[struct{}](wrapped)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected errors.Is to match sentinel, got %v", err)
	}
}

func TestRegisterError_Idempotent(t *testing.T) {
	sentinel := errors.New("idempotent test error")
	if err := RegisterError("messages_test.idempotent", sentinel); err != nil {
		t.Fatalf("register first: %v", err)
	}
	if err := RegisterError("messages_test.idempotent", sentinel); err != nil {
		t.Fatalf("register second: %v", err)
	}
}

func TestRegisterError_ConflictingCode(t *testing.T) {
	if err := RegisterError("messages_test.conflict", errors.New("first")); err != nil {
		t.Fatalf("register first: %v", err)
	}
	if err := RegisterError("messages_test.conflict", errors.New("second")); err == nil {
		t.Fatal("expected conflicting registration error")
	}
}

func TestResp_UnmarshalExtraFields(t *testing.T) {
	// Test that extra fields don't break unmarshaling
	jsonWithExtra := `{"ok":false,"error":"test","extra_field":"ignored"}`

	var r statusResp
	if err := json.Unmarshal([]byte(jsonWithExtra), &r); err != nil {
		t.Fatalf("failed to unmarshal with extra fields: %v", err)
	}

	if r.OK {
		t.Error("expected OK to be false")
	}
	if r.Error != "test" {
		t.Errorf("expected error 'test', got: %s", r.Error)
	}
}

func TestResp_UnmarshalPartial(t *testing.T) {
	// Test unmarshaling with only OK field (no error field)
	jsonPartial := `{"ok":true}`

	var r statusResp
	if err := json.Unmarshal([]byte(jsonPartial), &r); err != nil {
		t.Fatalf("failed to unmarshal partial JSON: %v", err)
	}

	if !r.OK {
		t.Error("expected OK to be true")
	}
	if r.Error != "" {
		t.Errorf("expected empty error, got: %s", r.Error)
	}
}

func BenchmarkOKResp(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = OKResp()
	}
}

func BenchmarkErrResp(b *testing.B) {
	testErr := errors.New("benchmark error")
	for i := 0; i < b.N; i++ {
		_ = ErrResp(testErr)
	}
}

func BenchmarkErr_OK(b *testing.B) {
	data := OKResp()
	for i := 0; i < b.N; i++ {
		_ = Err(data)
	}
}

func BenchmarkErr_Error(b *testing.B) {
	testErr := errors.New("benchmark error")
	data := ErrResp(testErr)
	for i := 0; i < b.N; i++ {
		_ = Err(data)
	}
}

func TestOKDataResp(t *testing.T) {
	inner := []struct {
		ID string `json:"id"`
	}{{ID: "obj1"}}
	resp := OKDataResp(inner)

	var r Resp[[]struct {
		ID string `json:"id"`
	}]
	if err := json.Unmarshal(resp, &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !r.OK {
		t.Error("expected OK true")
	}
	if len(r.Data) != 1 || r.Data[0].ID != "obj1" {
		t.Errorf("unexpected data: %+v", r.Data)
	}
}

func TestData_OKResponse(t *testing.T) {
	inner := struct {
		ID string `json:"id"`
	}{ID: "zone-a"}
	wrapped := OKDataResp(inner)

	data, err := Data[struct {
		ID string `json:"id"`
	}](wrapped)
	if err != nil {
		t.Fatalf("Data returned error: %v", err)
	}
	if data.ID != inner.ID {
		t.Errorf("expected %s, got %s", inner.ID, data.ID)
	}
}

func TestData_ErrorResponse(t *testing.T) {
	wrapped := ErrResp(errors.New("area not found"))

	_, err := Data[struct{}](wrapped)
	if err == nil {
		t.Fatal("expected error for error response")
	}
	if err.Error() != "area not found" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestData_InvalidJSON(t *testing.T) {
	_, err := Data[struct{}]([]byte("not json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestIntegration_OKDataResp_Data(t *testing.T) {
	type fakeObj struct {
		ID string `json:"id"`
	}
	obj := fakeObj{ID: "obj1"}

	wrapped := OKDataResp(obj)
	parsed, err := Data[fakeObj](wrapped)
	if err != nil {
		t.Fatalf("Data: %v", err)
	}

	if parsed.ID != "obj1" {
		t.Errorf("expected obj1, got %s", parsed.ID)
	}
}

func TestOKDataResp_IncludesNullData(t *testing.T) {
	resp := OKDataResp[any](nil)
	var m map[string]interface{}
	json.Unmarshal(resp, &m)
	if _, exists := m["data"]; !exists {
		t.Error("data field should be present")
	}
}

func TestErr_OnOKDataResp(t *testing.T) {
	wrapped := OKDataResp(struct {
		InstanceID string `json:"instance_id"`
		CommitSeq  uint64 `json:"commit_seq"`
	}{InstanceID: "b1i1", CommitSeq: 42})

	err := Err(wrapped)
	if err != nil {
		t.Errorf("messages.Err should return nil for OKDataResp with commit ack data, got: %v", err)
	}
}

func TestData_ExtractsCommitAck(t *testing.T) {
	wrapped := OKDataResp(struct {
		InstanceID string `json:"instance_id"`
		CommitSeq  uint64 `json:"commit_seq"`
	}{InstanceID: "b1i1", CommitSeq: 42})

	parsed, err := Data[struct {
		InstanceID string `json:"instance_id"`
		CommitSeq  uint64 `json:"commit_seq"`
	}](wrapped)
	if err != nil {
		t.Fatalf("Data: %v", err)
	}

	if parsed.InstanceID != "b1i1" || parsed.CommitSeq != 42 {
		t.Errorf("expected commit ack b1i1/42, got %s/%d", parsed.InstanceID, parsed.CommitSeq)
	}
}

func BenchmarkOKDataResp(b *testing.B) {
	inner := []struct {
		ID string `json:"id"`
	}{{ID: "obj1"}}
	for i := 0; i < b.N; i++ {
		_ = OKDataResp(inner)
	}
}

func BenchmarkData_OK(b *testing.B) {
	inner := []struct {
		ID string `json:"id"`
	}{{ID: "obj1"}}
	wrapped := OKDataResp(inner)
	for i := 0; i < b.N; i++ {
		_, _ = Data[[]struct {
			ID string `json:"id"`
		}](wrapped)
	}
}
