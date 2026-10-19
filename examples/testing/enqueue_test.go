package testing_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/giant-stone/gmq/gmq"
	"github.com/golang/mock/gomock"
)

const reportUniquenessWindow = 12 * time.Hour

// enqueueReport represents application code with an injected storage dependency.
func enqueueReport(ctx context.Context, broker gmq.Broker, date string) error {
	client, err := gmq.NewClientFromBroker(broker)
	if err != nil {
		return err
	}
	_, err = client.Enqueue(ctx, &gmq.Msg{
		Id:      "report-" + date,
		Queue:   "reports",
		Payload: []byte(fmt.Sprintf(`{"date":%q}`, date)),
	}, gmq.OptUniqueIn(reportUniquenessWindow))
	if err != nil {
		return fmt.Errorf("enqueue report: %w", err)
	}
	return nil
}

func TestEnqueueReport(t *testing.T) {
	backendError := errors.New("storage unavailable")
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "duplicate", err: gmq.ErrMsgIdConflict},
		{name: "backend failure", err: backendError},
	} {
		t.Run(test.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			broker := gmq.NewMockBroker(controller)
			ctx := context.Background()
			const date = "2026-10-19"
			broker.EXPECT().Enqueue(ctx, gomock.Any(), gmq.OptUniqueIn(reportUniquenessWindow)).
				DoAndReturn(func(_ context.Context, msg gmq.IMsg, opts ...gmq.OptionClient) (*gmq.Msg, error) {
					if msg.GetId() != "report-"+date || msg.GetQueue() != "reports" {
						t.Fatalf("unexpected message identity: %s", msg)
					}
					if string(msg.GetPayload()) != `{"date":"2026-10-19"}` {
						t.Fatalf("unexpected payload: %s", msg.GetPayload())
					}
					if len(opts) != 1 || opts[0].Value() != reportUniquenessWindow {
						t.Fatalf("unexpected uniqueness options: %v", opts)
					}
					if test.err != nil {
						return nil, test.err
					}
					return &gmq.Msg{Id: msg.GetId(), Queue: msg.GetQueue(), Payload: msg.GetPayload()}, nil
				})
			if err := enqueueReport(ctx, broker, date); !errors.Is(err, test.err) {
				t.Fatalf("enqueueReport error = %v, want %v", err, test.err)
			}
		})
	}
}
