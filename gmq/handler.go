package gmq

import "context"

// IHandler processes a message. Implementations own any dependencies they need.
type IHandler interface {
	ProcessMsg(ctx context.Context, msg IMsg) error
}

// HandlerFunc adapts a function to IHandler.
type HandlerFunc func(ctx context.Context, msg IMsg) error

// ProcessMsg calls fn with the message and its processing context.
func (fn HandlerFunc) ProcessMsg(ctx context.Context, msg IMsg) error {
	return fn(ctx, msg)
}

var _ IHandler = HandlerFunc(nil)
