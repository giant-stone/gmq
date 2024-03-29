package gmq

import (
	"fmt"

	"github.com/giant-stone/go/gstr"
)

type Msg struct {
	Payload []byte `json:"payload"`
	Id      string `json:"id"`
	Queue   string `json:"queue"`

	// message created at timestamp in Unix milliseconds
	Created int64 `json:"created"`

	// expired timestamp in Unix milliseconds
	Expireat int64 `json:"expireat"`

	Err   string `json:"err"`
	State string `json:"state"`

	// state last changed timestamp in Unix milliseconds
	Updated int64 `json:"updated"`
}

func (it *Msg) GetPayload() []byte {
	return it.Payload
}

func (it *Msg) GetId() string {
	return it.Id
}

func (it *Msg) GetQueue() string {
	return it.Queue
}

func (it *Msg) String() string {
	shorten := gstr.ShortenWith(string(it.Payload), 200, gstr.DefaultShortenSuffix)
	return fmt.Sprintf("<Msg queue=%s id=%s payload=%s>", it.Queue, it.Id, shorten)
}
