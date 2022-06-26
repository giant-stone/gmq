package gmq

import (
	"fmt"
	"time"
)

type OptTypeQueue int

const (
	OptTypeQueueWorkerNum OptTypeQueue = iota
	OptTypeQueueWorkerWorkIntervalFunc
)

type OptionQueue interface {
	// String returns a string representation of the option.
	String() string

	// Type returns the type of the option.
	Type() OptTypeQueue

	// Value returns a value used to create this option.
	Value() interface{}
}

type (
	queueWorkerNumOption     uint16
	workerWorkIntervalOption FuncWorkInterval
)

func OptQueueWorkerNum(n uint16) OptionQueue {
	return queueWorkerNumOption(n)
}

func (it queueWorkerNumOption) String() string     { return fmt.Sprintf("queueWorkerNum(%d)", uint16(it)) }
func (it queueWorkerNumOption) Type() OptTypeQueue { return OptTypeQueueWorkerNum }
func (it queueWorkerNumOption) Value() interface{} { return uint16(it) }

type FuncWorkInterval func() time.Duration

func OptWorkerWorkInterval(f FuncWorkInterval) OptionQueue {
	return workerWorkIntervalOption(f)
}

func (it workerWorkIntervalOption) String() string     { return fmt.Sprintf("workerWorkInterval(%s)", "-") }
func (it workerWorkIntervalOption) Type() OptTypeQueue { return OptTypeQueueWorkerWorkIntervalFunc }
func (it workerWorkIntervalOption) Value() interface{} { return FuncWorkInterval(it) }
