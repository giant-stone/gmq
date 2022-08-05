package gmq_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/go/gtime"
	"github.com/stretchr/testify/require"
)

func TestMonitor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lastRecord := 15
	now := time.Now().AddDate(0, 0, -lastRecord)
	broker := getTestBroker(t)
	rdb := getTestClient(t)
	broker.SetClock(gmq.NewSimulatedClock(now))
	var err error
	// 生成记录
	queueList := []string{"default", "TestMonitor"}
	for _, queue := range queueList {
		_, err := rdb.SAdd(ctx, gmq.NewKeyQueueList(), queue).Result()
		require.NoError(t, err)
	}

	msgFailed, msgProcessed := make([]int64, (lastRecord+1)*len(queueList)), make([]int64, (lastRecord+1)*len(queueList))
	periods := []int{7, 14}

	for i := 0; i < (lastRecord+1)*len(queueList); {
		for _, queue := range queueList {
			msgFailed[i] = int64(rand.Intn(1000))
			msgProcessed[i] = int64(rand.Intn(1000))
			_, err = rdb.Set(ctx, gmq.NewKeyDailyStatFailed(gmq.Namespace, queue, gtime.UnixTime2YyyymmddUtc(now.Unix())), msgFailed[i], 0).Result()
			require.NoError(t, err)
			_, err = rdb.Set(ctx, gmq.NewKeyDailyStatProcessed(gmq.Namespace, queue, gtime.UnixTime2YyyymmddUtc(now.Unix())), msgProcessed[i], 0).Result()
			require.NoError(t, err)
			i++
		}
		now = now.AddDate(0, 0, 1)
	}

	for i := range periods {
		var sumFailed, sumProcessed int64 = 0, 0
		k := 0
		for idx := k; idx < periods[i]*2; idx += 2 {
			sumFailed += msgFailed[idx] + msgFailed[idx+1]
			sumProcessed += msgProcessed[idx] + msgProcessed[idx+1]
		}
		for j := periods[i] - lastRecord; j <= 0; j++ {
			now := time.Now().AddDate(0, 0, j)
			broker.SetClock(gmq.NewSimulatedClock(now))
			_, err = broker.Monitor(ctx, periods[i])
			require.NoError(t, err)
			key := gmq.NewQueueKeyMonitor(gmq.Namespace, gtime.UnixTime2YyyymmddUtc(now.Unix()), periods[i])
			echoFailed, err := rdb.HGet(ctx,
				key,
				"failed").
				Result()
			info := fmt.Sprintf("brokder.monitor date:%s period:%d", gtime.UnixTime2YyyymmddUtc(now.Unix()), periods[i])
			require.NoError(t, err, info)
			require.Equal(t, strconv.Itoa(int(sumFailed)), echoFailed, info)

			echoProcessed, err := rdb.HGet(ctx,
				key,
				"processed").
				Result()
			require.NoError(t, err, info)
			require.Equal(t, strconv.Itoa(int(sumProcessed)), echoProcessed, info)

			// iter
			for idx := range queueList {
				sumFailed -= msgFailed[idx+k]
				sumFailed += msgFailed[idx+k+periods[i]*2]
				sumProcessed -= msgProcessed[idx+k]
				sumProcessed += msgProcessed[idx+k+periods[i]*2]
			}
			k += 2
		}
	}
}
