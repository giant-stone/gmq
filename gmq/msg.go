package gmq

type IMsg interface {
	GetPayload() map[string]interface{}
	GetId() string
	GetQueue() string
	String() string
}
