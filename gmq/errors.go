package gmq

import "errors"

var (
	ErrNoMsg = errors.New("no msg")

	ErrInternal = errors.New("internal error")

	ErrMsgIdConflict = errors.New("msg id conflict")

	ErrWaitTimeOut = errors.New("operation times out")

	ErrNotImplemented = errors.New("method not implemented")
)
