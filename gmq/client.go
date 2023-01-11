package gmq

import "context"

type Client struct {
	broker Broker
}

func NewClientFromBroker(broker Broker) (rs *Client, err error) {
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
