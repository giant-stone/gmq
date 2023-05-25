package gmq

import (
	"fmt"
	"time"
)

type OptTypeClient int

const (
	OptTypeQueueName OptTypeClient = iota
	OptTypeUniqueIn
	OptTypeIgnoreUnique
)

type OptionClient interface {
	// String returns a string representation of the option.
	String() string

	// Type returns the type of the option.
	Type() OptTypeClient

	// Value returns a value used to create this option.
	Value() interface{}
}

// OptQueueName client option customs the queue name of enqueue message.
type (
	queueNameOption string
)

func OptQueueName(s string) OptionClient {
	return queueNameOption(s)
}
func (it queueNameOption) String() string      { return fmt.Sprintf("OptQueueName(%v)", string(it)) }
func (it queueNameOption) Type() OptTypeClient { return OptTypeQueueName }
func (it queueNameOption) Value() interface{}  { return string(it) }

// OptUniqueIn client option makes enqueue message unique in specify duration,
//
//	no matter consume it successfully or failed.
type (
	uniqueInOption time.Duration
)

func OptUniqueIn(a time.Duration) OptionClient {
	return uniqueInOption(a)
}
func (it uniqueInOption) String() string      { return fmt.Sprintf("OptUniqueIn(%v)", time.Duration(it)) }
func (it uniqueInOption) Type() OptTypeClient { return OptTypeUniqueIn }
func (it uniqueInOption) Value() interface{}  { return time.Duration(it) }

// OptTypeIgnoreUnique client option customs enqueue item unique behave.
type (
	ignoreUniqueOption bool
)

func OptIgnoreUnique(s bool) OptionClient {
	return ignoreUniqueOption(s)
}
func (it ignoreUniqueOption) String() string      { return fmt.Sprintf("OptIgnoreUnique(%v)", bool(it)) }
func (it ignoreUniqueOption) Type() OptTypeClient { return OptTypeIgnoreUnique }
func (it ignoreUniqueOption) Value() interface{}  { return bool(it) }
