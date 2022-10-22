package gmq

import (
	"fmt"
	"time"
)

type OptTypeClient int

const (
	OptTypeQueueName OptTypeClient = iota
	OptTypeUniqueIn
)

type OptionClient interface {
	// String returns a string representation of the option.
	String() string

	// Type returns the type of the option.
	Type() OptTypeClient

	// Value returns a value used to create this option.
	Value() interface{}
}

// OptQueueName client option customs the queue name of enqueue messsage.
type (
	queueNameOption string
)

func OptQueueName(s string) OptionClient {
	return queueNameOption(s)
}
func (it queueNameOption) String() string      { return fmt.Sprintf("OptQueueName(%v)", string(it)) }
func (it queueNameOption) Type() OptTypeClient { return OptTypeQueueName }
func (it queueNameOption) Value() interface{}  { return string(it) }

// OptUniqueIn client option makes enqueue messsage unique in speicfy duration,
//   no matter consume it successfully or failed.
type (
	uniqueInOption time.Duration
)

func OptUniqueIn(a time.Duration) OptionClient {
	return uniqueInOption(a)
}
func (it uniqueInOption) String() string      { return fmt.Sprintf("OptUniqueIn(%v)", time.Duration(it)) }
func (it uniqueInOption) Type() OptTypeClient { return OptTypeUniqueIn }
func (it uniqueInOption) Value() interface{}  { return time.Duration(it) }
