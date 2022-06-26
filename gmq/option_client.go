package gmq

import (
	"fmt"
)

type OptTypeClient int

const (
	OptTypeQueueName OptTypeClient = iota
)

type OptionClient interface {
	// String returns a string representation of the option.
	String() string

	// Type returns the type of the option.
	Type() OptTypeClient

	// Value returns a value used to create this option.
	Value() interface{}
}

type (
	queueNameOption string
)

func OptQueueName(s string) OptionClient {
	return queueNameOption(s)
}

func (it queueNameOption) String() string      { return fmt.Sprintf("queueNameOption(%q)", string(it)) }
func (it queueNameOption) Type() OptTypeClient { return OptTypeQueueName }
func (it queueNameOption) Value() interface{}  { return string(it) }
