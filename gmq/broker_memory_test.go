package gmq_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/gmq/internal/test_util"
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
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerEnqueue(t, broker)
}

func TestBrokerInMemory_GetMsg(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerGetMsg(t, broker)
}

func TestBrokerInMemory_Dequeue(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerDequeue(t, broker)
}

func TestBrokerInMemory_DeleteMsg(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerDeleteMsg(t, broker)
}

func TestBrokerInMemory_DeleteQueue(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerDeleteQueue(t, broker)
}

func TestBrokerInMemory_DeleteAgo(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerDeleteAgo(t, broker)
}

func TestBrokerInMemory_Complete(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerComplete(t, broker)
}

func TestBrokerInMemory_Fail(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerFail(t, broker)
}

func TestBrokerInMemory_ListQueue(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerListQueue(t, broker)
}

func TestBrokerInMemory_ListMsg(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerListMsg(t, broker)
}

func TestBrokerInMemory_ListFailed(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerListFailed(t, broker)
}

func TestBrokerInMemory_ListFailedMaxItems(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerListFailedMaxItems(t, broker)
}

func TestBrokerInMemory_GetStats(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerGetStats(t, broker)
}

func TestBrokerInMemory_GetStatsByDate(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerGetStatsByDate(t, broker)
}

func TestBrokerInMemory_AutoDeduplicateMsgByDefault(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerAutoDeduplicateMsgByDefault(t, broker)
}

func TestBrokerInMemory_AutoDeduplicateFailedMsg(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerAutoDeduplicateFailedMsg(t, broker)
}

func TestBrokerInMemory_AutoDeduplicateCompletedMsg(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	gmq.CheckBrokerAutoDeduplicateCompletedMsg(t, broker)
}

func TestBrokerInMemory_ClientEnqueue(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	testClient_Enqueue(t, broker)
}

func TestBrokerInMemory_ClientDequeue(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	testClient_Dequeue(t, broker)
}

func TestBrokerInMemory_ClientEnqueueOptQueueName(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	testClient_EnqueueOptQueueName(t, broker)
}

func TestBrokerInMemory_ClientEnqueueDuplicatedMsg(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	testClient_EnqueueDuplicatedMsg(t, broker)
}

func TestBrokerInMemory_ClientEnqueueOptUniqueIn(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerInMemory(t)
	testClient_EnqueueOptUniqueIn(t, broker)
}
