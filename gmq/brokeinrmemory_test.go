package gmq_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
)

var (
	universalBrokerInMemory gmq.Broker
)

func setupBrokerInMemory(t testing.TB) gmq.Broker {
	broker, err := gmq.NewBrokerInMemory(&gmq.BrokerInMemoryOpts{
		MaxBytes: 1024 * 1024,
	})
	require.NoError(t, err)
	universalBrokerInMemory = broker
	return universalBrokerInMemory
}

func getTestBrokerInMemory(t testing.TB) gmq.Broker {
	return setupBrokerInMemory(t)
}

func getTestClientInMemory(t *testing.T) *gmq.Client {
	broker := getTestBrokerInMemory(t)
	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err)
	return cli
}

func TestBrokerInMemory_Enqueue(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_Enqueue(t, broker)
}

func TestBrokerInMemory_GetMsg(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_GetMsg(t, broker)
}

func TestBrokerInMemory_Dequeue(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_Dequeue(t, broker)
}

func TestBrokerInMemory_DeleteMsg(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_DeleteMsg(t, broker)
}

func TestBrokerInMemory_DeleteQueue(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_DeleteQueue(t, broker)
}

func TestBrokerInMemory_DeleteAgo(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_DeleteAgo(t, broker)
}

func TestBrokerInMemory_Complete(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_Complete(t, broker)
}

func TestBrokerInMemory_Fail(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_Fail(t, broker)
}

func TestBrokerInMemory_ListQueue(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_ListQueue(t, broker)
}

func TestBrokerInMemory_ListFailed(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_ListFailed(t, broker)
}

func TestBrokerInMemory_GetStats(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_GetStats(t, broker)
}

func TestBrokerInMemory_GetStatsByDate(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_GetStatsByDate(t, broker)
}

func TestBrokerInMemory_AutoDeduplicateMsgByDefault(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_AutoDeduplicateMsgByDefault(t, broker)
}

func TestBrokerInMemory_AutoDeduplicateFailedMsg(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_AutoDeduplicateFailedMsg(t, broker)
}

func TestBrokerInMemory_AutoDeduplicateCompletedMsg(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testBroker_AutoDeduplicateCompletedMsg(t, broker)
}
