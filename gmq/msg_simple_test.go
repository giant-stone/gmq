package gmq

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMsgExpirationJSONField(t *testing.T) {
	const expiration = int64(123456789)
	data, err := json.Marshal(Msg{ExpiresAt: expiration})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"expire_at":123456789`) {
		t.Fatalf("unexpected expiration JSON field: %s", data)
	}
	var message Msg
	if err := json.Unmarshal(data, &message); err != nil {
		t.Fatal(err)
	}
	if message.ExpiresAt != expiration {
		t.Fatalf("expiration = %d, want %d", message.ExpiresAt, expiration)
	}
}
