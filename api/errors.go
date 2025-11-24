package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/vocdoni/davinci-node/log"
)

// Error is used by handler functions to wrap errors, assigning a unique error code
// and also specifying which HTTP Status should be used.
type Error struct {
	Err        error
	Code       int
	HTTPstatus int
}

// MarshalJSON returns a JSON containing Err.Error() and Code. Field HTTPstatus is ignored.
//
// Example output: {"error":"account not found","code":4003}
func (e Error) MarshalJSON() ([]byte, error) {
	// This anon struct is needed to actually include the error string,
	// since it wouldn't be marshaled otherwise. (json.Marshal doesn't call Err.Error())
	return json.Marshal(
		struct {
			Err  string `json:"error"`
			Code int    `json:"code"`
		}{
			Err:  e.Err.Error(),
			Code: e.Code,
		})
}

// Error returns the Message contained inside the APIerror
func (e Error) Error() string {
	return e.Err.Error()
}

// Write serializes a JSON msg using APIerror.Message and APIerror.Code
// and passes that to ctx.Send()
func (e Error) Write(w http.ResponseWriter) {
	msg, err := json.Marshal(e)
	if err != nil {
		log.Warnw("marshal failed", "error", err)
		http.Error(w, "marshal failed", http.StatusInternalServerError)
		return
	}
	log.Debugw("API error response", "error", e.Err, "code", e.Code, "httpStatus", e.HTTPstatus)
	// set the content type to JSON
	w.Header().Set("Content-Type", "application/json")
	http.Error(w, string(msg), e.HTTPstatus)
}

// Withf returns a copy of APIerror with the Sprintf formatted string appended at the end of e.Err
func (e Error) Withf(format string, args ...any) Error {
	return Error{
		Err:        fmt.Errorf("%w: %v", e.Err, fmt.Sprintf(format, args...)),
		Code:       e.Code,
		HTTPstatus: e.HTTPstatus,
	}
}

// With returns a copy of APIerror with the string appended at the end of e.Err
func (e Error) With(s string) Error {
	return Error{
		Err:        fmt.Errorf("%w: %v", e.Err, s),
		Code:       e.Code,
		HTTPstatus: e.HTTPstatus,
	}
}

// WithErr returns a copy of APIerror with err.Error() appended at the end of e.Err
func (e Error) WithErr(err error) Error {
	return Error{
		Err:        fmt.Errorf("%w: %v", e.Err, err.Error()),
		Code:       e.Code,
		HTTPstatus: e.HTTPstatus,
	}
}

// Error definitions - maintaining exact compatibility with existing API
var (
	ErrResourceNotFound        = Error{Code: 40001, HTTPstatus: http.StatusNotFound, Err: fmt.Errorf("resource not found")}
	ErrMalformedBody           = Error{Code: 40004, HTTPstatus: http.StatusBadRequest, Err: fmt.Errorf("malformed JSON body")}
	ErrInvalidSignature        = Error{Code: 40005, HTTPstatus: http.StatusBadRequest, Err: fmt.Errorf("invalid signature")}
	ErrMalformedProcessID      = Error{Code: 40006, HTTPstatus: http.StatusBadRequest, Err: fmt.Errorf("malformed process ID")}
	ErrProcessNotFound         = Error{Code: 40007, HTTPstatus: http.StatusNotFound, Err: fmt.Errorf("process not found")}
	ErrInvalidCensusProof      = Error{Code: 40008, HTTPstatus: http.StatusBadRequest, Err: fmt.Errorf("invalid census proof")}
	ErrInvalidBallotProof      = Error{Code: 40009, HTTPstatus: http.StatusBadRequest, Err: fmt.Errorf("invalid ballot proof")}
	ErrInvalidCensusID         = Error{Code: 40010, HTTPstatus: http.StatusBadRequest, Err: fmt.Errorf("invalid census ID")}
	ErrCensusNotFound          = Error{Code: 40011, HTTPstatus: http.StatusNotFound, Err: fmt.Errorf("census not found")}
	ErrKeyLengthExceeded       = Error{Code: 40012, HTTPstatus: http.StatusBadRequest, Err: fmt.Errorf("key length exceeded")}
	ErrInvalidBallotInputsHash = Error{Code: 40013, HTTPstatus: http.StatusBadRequest, Err: fmt.Errorf("invalid ballot inputs hash")}
	ErrUnauthorized            = Error{Code: 40014, HTTPstatus: http.StatusForbidden, Err: fmt.Errorf("unauthorized")}
	ErrMalformedParam          = Error{Code: 40015, HTTPstatus: http.StatusBadRequest, Err: fmt.Errorf("malformed parameter")}
	ErrMalformedNullifier      = Error{Code: 40016, HTTPstatus: http.StatusBadRequest, Err: fmt.Errorf("malformed nullifier")}
	ErrMalformedAddress        = Error{Code: 40017, HTTPstatus: http.StatusBadRequest, Err: fmt.Errorf("malformed address")}
	ErrInvalidCensusRoot       = Error{Code: 40018, HTTPstatus: http.StatusBadRequest, Err: fmt.Errorf("invalid census root")}

	ErrMarshalingServerJSONFailed = Error{Code: 50001, HTTPstatus: http.StatusInternalServerError, Err: fmt.Errorf("marshaling (server-side) JSON failed")}
	ErrGenericInternalServerError = Error{Code: 50002, HTTPstatus: http.StatusInternalServerError, Err: fmt.Errorf("internal server error")}
)
