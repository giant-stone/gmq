package gmq

type IMsg interface {
	GetPayload() []byte
	GetId() string
	GetQueue() string
	String() string
}
