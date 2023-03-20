package gmq

type IMsg interface {
	GetId() string

	GetPayload() []byte
	GetQueue() string
	String() string
}
