package gmq

import "context"

type Handler interface {
	ProcessMsg(ctx context.Context, msg IMsg) error
}

type HandlerFunc func(ctx context.Context, msg IMsg) error

// ProcessMsg calls fn(ctx, task)
func (fn HandlerFunc) ProcessMsg(ctx context.Context, msg IMsg) error {
	return fn(ctx, msg)
}
