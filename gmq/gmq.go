package gmq

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

// message state list
const (
	// message init state
	MsgStatePending = "pending"

	// message is processing by worker
	MsgStateProcessing = "processing"

	// message has been processed and failed
	MsgStateFailed = "failed"

	// message has been processed successfully
	MsgStateCompleted = "completed"
)
