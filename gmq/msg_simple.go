package gmq

import (
	"encoding/json"
	"fmt"

	"github.com/giant-stone/go/gstr"
)

type Msg struct {
	Payload map[string]interface{}
	Id      string
	Queue   string

	State string

	Created   int64
	Processed int64
}

func (it *Msg) GetPayload() map[string]interface{} {
	return it.Payload
}

func (it *Msg) GetId() string {
	return it.Id
}

func (it *Msg) GetQueue() string {
	return it.Queue
}

func (it *Msg) String() string {
	payload, _ := json.Marshal(it.Payload)
	shorten := gstr.ShortenWith(string(payload), 50, gstr.DefaultShortenSuffix)
	return fmt.Sprintf("<Msg id=%s payload=%s>", it.Id, shorten)
}
