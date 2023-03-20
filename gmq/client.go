package gmq

import "context"

type Client struct {
	Broker Broker
}

func NewClientFromBroker(broker Broker) (rs *Client, err error) {
	return &Client{Broker: broker}, nil
}

// Close closes the redis connection.
func (it *Client) Close() error {
	return it.Broker.Close()
}

// Enqueue enqueues a message into a queue.
func (it *Client) Enqueue(ctx context.Context, msg IMsg, opts ...OptionClient) (rs *Msg, err error) {
	return it.Broker.Enqueue(ctx, msg, opts...)
}
