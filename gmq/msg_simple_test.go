package gmq

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMsgExpirationJSONCompatibility(t *testing.T) {
	const expiration = int64(123456789)
	data, err := json.Marshal(Msg{ExpiresAt: expiration})
	if err != nil {
		t.Fatal(err)
	}
	// cspell:ignore expireat
	if !strings.Contains(string(data), `"expireat":123456789`) {
		t.Fatalf("expiration storage key changed: %s", data)
	}
	var message Msg
	if err := json.Unmarshal(data, &message); err != nil {
		t.Fatal(err)
	}
	if message.ExpiresAt != expiration {
		t.Fatalf("expiration = %d, want %d", message.ExpiresAt, expiration)
	}
}
