package gmq

import (
	"context"

	"github.com/bwmarrin/snowflake"
)

type IHandler interface {
	ProcessMsg(ctx context.Context, msg IMsg) error

	// Implement the following methods to control time and unique identifier changes and implement mocking testing.
	SetClock(c Clock)
	SetIdGenerator(v IdGenerator)
}

type HandlerFunc func(ctx context.Context, msg IMsg) error

// SetClock implements IHandler.
func (fn HandlerFunc) SetClock(c Clock) {
	panic("unimplemented")
}

// SetIdGenerator implements IHandler.
func (fn HandlerFunc) SetIdGenerator(v IdGenerator) {
	panic("unimplemented")
}

// ProcessMsg calls fn(ctx, task)
func (fn HandlerFunc) ProcessMsg(ctx context.Context, msg IMsg) error {
	return fn(ctx, msg)
}

type IdGenerator interface {
	Generate() snowflake.ID
}

type Handler struct {
	Clock       Clock
	IdGenerator IdGenerator
}

func (it *Handler) SetClock(c Clock) {
	it.Clock = c
}

func (it *Handler) SetIdGenerator(v IdGenerator) {
	it.IdGenerator = v
}

func (it *Handler) ProcessMsg(ctx context.Context, msg IMsg) (err error) {
	panic("unimplemented")
}
