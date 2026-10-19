package gmq

import "errors"

var (
	// ErrNotImplemented identifies an operation unsupported by a broker.
	ErrNotImplemented = errors.New("method not implemented")

	ErrNoMsg = errors.New("no msg")

	ErrInternal = errors.New("internal error")

	ErrMsgIdConflict = errors.New("msg id conflict")

	ErrWaitTimeOut = errors.New("operation times out")
)
