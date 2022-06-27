package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/gtime"
	"github.com/giant-stone/go/gutil"

	"github.com/giant-stone/gmq/gmq"
)

var (
	cmdPrintStats bool
	cmdAddMsg     bool
	cmdGetMsg     bool
	cmdDelMsg     bool

	dsnRedis   string
	msgId      string
	payloadStr string
	queueName  string

	loglevel string
)

func main() {
	flag.StringVar(&loglevel, "l", "", "loglevel debug,info,warn,error")
	flag.StringVar(&dsnRedis, "d", "redis://127.0.0.1:6379", "redis DSN")

	flag.BoolVar(&cmdPrintStats, "stats", false, "print queue stats")
	flag.BoolVar(&cmdAddMsg, "add", false, "append a message into queue")
	flag.BoolVar(&cmdGetMsg, "get", false, "get a message detail")
	flag.BoolVar(&cmdDelMsg, "del", false, "delete a message from queue")

	flag.StringVar(&queueName, "q", gmq.DefaultQueueName, "queue name")
	flag.StringVar(&payloadStr, "p", "", "message payload in JSON")
	flag.StringVar(&msgId, "i", "", "message id, it is auto-generated by default")

	flag.Parse()

	glogging.Init([]string{"stdout"}, loglevel)

	if !cmdPrintStats && !cmdAddMsg && !cmdGetMsg && !cmdDelMsg {
		flag.PrintDefaults()
		os.Exit(1)
	}

	broker, err := gmq.NewBrokerRedis(dsnRedis)
	gutil.ExitOnErr(err)
	ctx := context.Background()

	if cmdPrintStats {
		printStats(ctx, broker)
	} else if cmdAddMsg {
		addMsg(ctx, broker, queueName, payloadStr, msgId)
	} else if cmdGetMsg {
		getMsg(ctx, broker, queueName, msgId)
	} else if cmdDelMsg {
		delMsg(ctx, broker, queueName, msgId)
	} else {
		flag.PrintDefaults()
		os.Exit(1)
	}

	fmt.Print("\n")
}

func addMsg(ctx context.Context, broker gmq.Broker, queueName, payloadStr, id string) {
	if payloadStr == "" && id == "" {
		fmt.Println("payload or id is required")
		return
	}

	rs, err := broker.Enqueue(ctx, &gmq.Msg{
		Payload: []byte(payloadStr),
		Id:      id,
	}, gmq.OptQueueName(queueName))
	gutil.ExitOnErr(err)

	dat, _ := json.Marshal(rs)
	fmt.Println("reply", string(dat))
}

func printStats(ctx context.Context, broker gmq.Broker) {
	queues, err := broker.GetStats(ctx)
	gutil.ExitOnErr(err)

	fmt.Println("")
	fmt.Print("# gmq stats \n\n")
	if len(queues) == 0 {
		fmt.Println("Related info not found. Do consumer(s) have not start yet?")
	} else {
		for _, rsStat := range queues {
			fmt.Printf("queue=%s total=%d pending=%d waiting=%d processing=%d failed=%d \n\n",
				rsStat.Name,
				rsStat.Total,
				rsStat.Pending,
				rsStat.Waiting,
				rsStat.Processing,
				rsStat.Failed,
			)
		}

		fmt.Print("## daily stats \n\n")
		date := gtime.UnixTime2YyyymmddUtc(time.Now().Unix())
		dailyStats, err := broker.GetStatsByDate(ctx, date)
		gutil.ExitOnErr(err)
		fmt.Printf("date=%s(UTC) processed=%d failed=%d \n\n", dailyStats.Date, dailyStats.Processed, dailyStats.Failed)
	}
}

func getMsg(ctx context.Context, broker gmq.Broker, queueName, msgId string) {
	msg, err := broker.Get(ctx, queueName, msgId)
	if err != nil {
		if err == gmq.ErrNoMsg {
			fmt.Printf("message matched queue=%s id=%s not found", queueName, msgId)
			return
		}
		gutil.ExitOnErr(err)
	}

	fmt.Println("RAW\n", msg)

	dat, err := json.MarshalIndent(msg, "", "  ")
	gutil.ExitOnErr(err)
	fmt.Println("INTERNAL\n", string(dat))
}

func delMsg(ctx context.Context, broker gmq.Broker, queueName, msgId string) {
	err := broker.Delete(ctx, queueName, msgId)
	gutil.ExitOnErr(err)
	fmt.Printf("queue=%s msgId=%s deleted \n", queueName, msgId)
}
