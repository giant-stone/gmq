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
	gmq.TestBroker_Enqueue(t, broker)
}

func TestBrokerInMemory_GetMsg(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_GetMsg(t, broker)
}

func TestBrokerInMemory_Dequeue(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_Dequeue(t, broker)
}

func TestBrokerInMemory_DeleteMsg(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_DeleteMsg(t, broker)
}

func TestBrokerInMemory_DeleteQueue(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_DeleteQueue(t, broker)
}

func TestBrokerInMemory_DeleteAgo(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_DeleteAgo(t, broker)
}

func TestBrokerInMemory_Complete(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_Complete(t, broker)
}

func TestBrokerInMemory_Fail(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_Fail(t, broker)
}

func TestBrokerInMemory_ListQueue(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_ListQueue(t, broker)
}

func TestBrokerInMemory_ListMsg(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_ListMsg(t, broker)
}

func TestBrokerInMemory_ListFailed(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_ListFailed(t, broker)
}

func TestBrokerInMemory_ListFailedMaxItems(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_ListFailedMaxItems(t, broker)
}

func TestBrokerInMemory_GetStats(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_GetStats(t, broker)
}

func TestBrokerInMemory_GetStatsByDate(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_GetStatsByDate(t, broker)
}

func TestBrokerInMemory_AutoDeduplicateMsgByDefault(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_AutoDeduplicateMsgByDefault(t, broker)
}

func TestBrokerInMemory_AutoDeduplicateFailedMsg(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_AutoDeduplicateFailedMsg(t, broker)
}

func TestBrokerInMemory_AutoDeduplicateCompletedMsg(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	gmq.TestBroker_AutoDeduplicateCompletedMsg(t, broker)
}

func TestBrokerInMemory_ClientEnqueue(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testClient_Enqueue(t, broker)
}

func TestBrokerInMemory_ClientDequeue(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testClient_Dequeue(t, broker)
}

func TestBrokerInMemory_ClientEnqueueOptQueueName(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testClient_EnqueueOptQueueName(t, broker)
}

func TestBrokerInMemory_ClientEnqueueDuplicatedMsg(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testClient_EnqueueDuplicatedMsg(t, broker)
}

func TestBrokerInMemory_ClientEnqueueOptUniqueIn(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testClient_EnqueueOptUniqueIn(t, broker)
}

func TestBrokerInMemory_ClientEnqueueOptTypeIgnoreUnique(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	testClient_EnqueueOptTypeIgnoreUnique(t, broker)
}
