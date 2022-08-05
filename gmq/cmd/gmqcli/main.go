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
	cmdMonitor    bool
	cmdPauseq     string
	cmdResumeq    string

	dsnRedis   string
	msgId      string
	payloadStr string
	queueName  string
	period     int

	loglevel string
)

func main() {
	flag.StringVar(&loglevel, "l", "", "loglevel debug,info,warn,error")
	flag.StringVar(&dsnRedis, "d", "redis://127.0.0.1:6379/0", "redis DSN")

	flag.BoolVar(&cmdPrintStats, "stats", false, "print queue stats")
	flag.BoolVar(&cmdAddMsg, "add", false, "append a message into queue")
	flag.BoolVar(&cmdGetMsg, "get", false, "get a message detail")
	flag.BoolVar(&cmdDelMsg, "del", false, "delete a message from queue")
	flag.BoolVar(&cmdMonitor, "m", false,
		`collect the amount of msg consumed for the past few days
			-t indicate days, 7+1(include today) as default`)

	flag.StringVar(&queueName, "q", gmq.DefaultQueueName, "queue name")
	flag.StringVar(&payloadStr, "p", "", "message payload in JSON")
	flag.StringVar(&msgId, "i", "", "message id, it is auto-generated by default")
	flag.StringVar(&cmdPauseq, "pause", "", "queuename to pause")
	flag.StringVar(&cmdResumeq, "resume", "", "queuename to resume")
	flag.IntVar(&period, "t", 7, "display last n+1(include today)'s statistic")

	flag.Parse()
	glogging.Init([]string{"stdout"}, loglevel)
	if !cmdPrintStats && !cmdAddMsg && !cmdGetMsg && !cmdDelMsg && (cmdPauseq != "") && (cmdResumeq != "") && (!cmdMonitor) {
		flag.PrintDefaults()
		os.Exit(1)
	}

	broker, err := gmq.NewBrokerRedis(dsnRedis)
	gutil.ExitOnErr(err)
	ctx := context.Background()

	if cmdPauseq != "" {
		pauseQueue(ctx, broker, cmdPauseq)
		os.Exit(1)
	} else if cmdResumeq != "" {
		resumeQueue(ctx, broker, cmdResumeq)
		os.Exit(1)
	}

	if cmdPrintStats {
		printStats(ctx, broker)
	} else if cmdAddMsg {
		addMsg(ctx, broker, queueName, payloadStr, msgId)
	} else if cmdGetMsg {
		getMsg(ctx, broker, queueName, msgId)
	} else if cmdDelMsg {
		delMsg(ctx, broker, queueName, msgId)
	} else if cmdMonitor {
		monitor(ctx, broker, period)
	} else {
		flag.PrintDefaults()
		os.Exit(1)
	}

	fmt.Print("\n")
}

func pauseQueue(ctx context.Context, broker gmq.Broker, queuename string) {
	if err := broker.Pause(queuename); err != nil {
		fmt.Printf("Pausing queue %s  failed. errer(%s) \n", queuename, err.Error())
	} else {
		fmt.Printf("Pause queue %s \n", queuename)
	}
}
func resumeQueue(ctx context.Context, broker gmq.Broker, queuename string) {
	if err := broker.Resume(queuename); err != nil {
		fmt.Printf("Resuming queue %s failed. errer(%s) \n", cmdPauseq, err.Error())
	} else {
		fmt.Printf("Resume queue %s \n", queuename)
	}
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

func monitor(ctx context.Context, broker gmq.Broker, period int) {
	info, err := broker.Monitor(ctx, period)
	gutil.ExitOnErr(err)
	now := time.Now()
	fmt.Printf("## Consume Statistic: %s ~ %s \n\n",
		gtime.UnixTime2YyyymmddUtc(now.AddDate(0, 0, -period).Unix()),
		gtime.UnixTime2YyyymmddUtc(now.Unix()))
	fmt.Printf("total processed: %d, total failed: %d, total: %d \n", info.TotalFailed, info.TotalProcessed, info.Total)
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
	if err != nil {
		if err != gmq.ErrNoMsg {
			gutil.ExitOnErr(err)
		}
	}
	fmt.Printf("queue=%s msgId=%s deleted \n", queueName, msgId)
}
