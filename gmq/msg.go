package gmq

type IMsg interface {
	// The message id is global unique, and *contains* the MQ namespace prefix.
	GetId() string
	// The payload should be encoding in JSON.
	GetPayload() []byte
	GetQueue() string

	SetId(v string)
	SetPayload(v []byte)
	SetQueue(v string)

	String() string
}
