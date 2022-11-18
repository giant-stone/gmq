package gmq

import "fmt"

const (
	Namespace = "gmq"
)

const (
	DefaultQueueName = "default"
)

const (
	QueueNameList   = "queues"
	QueueNamePaused = "paused"
)

//
// message state list
//
const (
	// message init state
	MsgStatePending = "pending"

	// message take by worker from pending queue, wait for consuming rate restrict
	MsgStateWaiting = "waiting"

	// message is processing by worker
	MsgStateProcessing = "processing"

	// message has been processed and failed
	MsgStateFailed = "failed"

	// message has been processed successfully
	MsgStateCompleted = "completed"
)

func NewKeyQueueList() string {
	return fmt.Sprintf("%s:%s", Namespace, QueueNameList)
}

func NewKeyQueuePending(ns, queueName string) string {
	return fmt.Sprintf("%s:%s:%s", ns, queueName, MsgStatePending)
}

func NewKeyMsgDetail(ns, queueName, msgId string) string {
	return fmt.Sprintf("%s:%s:msg:%s", ns, queueName, msgId)
}

func NewKeyMsgUnique(ns, queueName, msgId string) string {
	return fmt.Sprintf("%s:%s:uniq:%s", ns, queueName, msgId)
}

func NewKeyQueuePaused(ns, queueName string) string {
	return fmt.Sprintf("%s:%s:%s", ns, queueName, QueueNamePaused)
}

func NewKeyQueueProcessing(ns, queueName string) string {
	return fmt.Sprintf("%s:%s:%s", ns, queueName, MsgStateProcessing)
}

func NewKeyQueueWaiting(ns, queueName string) string {
	return fmt.Sprintf("%s:%s:%s", ns, queueName, MsgStateWaiting)
}

func NewKeyQueueCompleted(ns, queueName string) string {
	return fmt.Sprintf("%s:%s:%s", ns, queueName, MsgStateCompleted)
}

func NewKeyQueueFailed(ns, queueName string) string {
	return fmt.Sprintf("%s:%s:%s", ns, queueName, MsgStateFailed)
}

// gmq:<queueName>:completed:<YYYY-MM-DD>
func NewKeyDailyStatCompleted(ns, queueName, YYYYMMDD string) string {
	return fmt.Sprintf("%s:%s:%s:%s", ns, queueName, MsgStateCompleted, YYYYMMDD)
}

// gmq:<queueName>:failed:<YYYY-MM-DD>
func NewKeyDailyStatFailed(ns, queueName, YYYYMMDD string) string {
	return fmt.Sprintf("%s:%s:%s:%s", ns, queueName, MsgStateFailed, YYYYMMDD)
}

func NewKeyQueueState(ns, queueName string, state string) string {
	return fmt.Sprintf("%s:%s:%s", ns, queueName, state)
}

func NewKeyQueuePattern(ns, queueName string) string {
	return fmt.Sprintf("%s:%s:*", ns, queueName)
}
