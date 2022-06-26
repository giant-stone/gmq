package gmq

import "errors"

var (
	ErrNoMsg = errors.New("no msg")

	ErrIncompatibleVer = errors.New("incompatible version")

	ErrInternal = errors.New("internal error")

	ErrMsgIdConflict = errors.New("msg id conflict")
)
