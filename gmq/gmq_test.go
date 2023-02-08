package gmq_test

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/go/grand"
)

func GenerateNewMsg() *gmq.Msg {
	queue := grand.String(5)
	id := fmt.Sprintf("%s.%s.%d", queue, grand.String(10), time.Now().UnixMilli())
	type Payload struct {
		Data string
	}
	p := Payload{Data: grand.String(20)}
	dat, _ := json.Marshal(p)
	return &gmq.Msg{Payload: dat, Id: id, Queue: queue}
}
