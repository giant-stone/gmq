package gmq

import "context"

type Client struct {
	broker Broker
}

func NewClient(dsn string, ns string) (rs *Client, err error) {
	broker, err := NewBrokerRedis(dsn, ns)
	if err != nil {
		return
	}

	return &Client{broker: broker}, nil
}

// Close closes the redis connection.
func (it *Client) Close() error {
	return it.broker.Close()
}

// Enqueue enqueues a message into a queue.
func (it *Client) Enqueue(ctx context.Context, msg IMsg, opts ...OptionClient) (rs *Msg, err error) {
	return it.broker.Enqueue(ctx, msg, opts...)
}
