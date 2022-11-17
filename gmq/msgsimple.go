package gmq

import (
	"fmt"

	"github.com/giant-stone/go/gstr"
)

type Msg struct {
	Payload []byte `json:"payload"`
	Id      string `json:"id"`
	Queue   string `json:"queue"`

	Created     int64  `json:"created"`
	Dieat       int64  `json:"dieat"`
	Expiredat   int64  `json:"expiredat"`
	Err         string `json:"err"`
	Processedat int64  `json:"processedat"`
	State       string `json:"state"`
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
	shorten := gstr.ShortenWith(string(it.Payload), 50, gstr.DefaultShortenSuffix)
	return fmt.Sprintf("<Msg queue=%s id=%s payload=%s>", it.Queue, it.Id, shorten)
}
