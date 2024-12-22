package gmq

import (
	"fmt"

	"github.com/giant-stone/go/gstr"
)

type Msg struct {
	// Global unique id in MQ, it contains namespace prefix.
	Id string `json:"id"`

	Payload []byte `json:"payload"`
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

// SetId implements IMsg.
func (it *Msg) SetId(v string) {
	it.Id = v
}

// SetPayload implements IMsg.
func (it *Msg) SetPayload(v []byte) {
	it.Payload = v
}

// SetQueue implements IMsg.
func (it *Msg) SetQueue(v string) {
	it.Queue = v
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
