package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/gutil"

	"github.com/giant-stone/gmq/gmq"
)

var (
	cmdAddMsg     bool
	cmdGetMsg     bool
	cmdListMsg    bool
	cmdListFailed bool
	cmdDelMsg     bool

	cmdDelQueue  bool
	cmdListQueue bool
	cmdPauseq    string
	cmdResumeq   string

	cmdPrintStats bool

	dsnRedis string

	msgId      string
	payloadStr string
	queueName  string
	state      string

	optLimit  int64
	optOffset int64

	loglevel string

	useUTC bool
)

var (
	msgStatList = []string{
		gmq.MsgStatePending,
		gmq.MsgStateProcessing,
		gmq.MsgStateFailed,
	}
)

func main() {
	flag.StringVar(&loglevel, "l", "debug", "loglevel debug,info,warn,error")
	flag.StringVar(&dsnRedis, "d", "redis://127.0.0.1:6379/0", "redis DSN")

	// commands
	flag.BoolVar(&cmdPrintStats, "stat", false, "print queue stats")
	flag.BoolVar(&cmdAddMsg, "add", false, "append a message into queue")

	flag.BoolVar(&cmdGetMsg, "get", false, "get a message detail")
	flag.BoolVar(&cmdListMsg, "list", false, "list messages from a queue ")
	flag.BoolVar(&cmdListFailed, "listfailed", false, "list history records of the failed message")
	flag.BoolVar(&cmdDelMsg, "del", false, "delete a message from queue")

	flag.BoolVar(&cmdListQueue, "listqueue", false, "list all queue names")
	flag.BoolVar(&cmdDelQueue, "delqueue", false, "delete a message from queue")
	flag.StringVar(&cmdPauseq, "pause", "", "queuename to pause")
	flag.StringVar(&cmdResumeq, "resume", "", "queuename to resume")

	// options
	flag.StringVar(&queueName, "q", gmq.DefaultQueueName, "queue name")
	flag.StringVar(&payloadStr, "p", "", "message payload in JSON")
	flag.StringVar(&msgId, "i", "", "message id, it is auto-generated by default")
	stateList := strings.Join(msgStatList, ",")
	flag.StringVar(&state, "s", "failed", fmt.Sprintf("must be one of %s, required for -list, queue state to search", stateList))

	flag.Int64Var(&optLimit, "n", gmq.DefaultLimit, fmt.Sprintf("use with -list, maximum number of messages to display, default is %d", gmq.DefaultLimit))
	flag.Int64Var(&optOffset, "o", 0, "use with -list, first messages offset to display, start with 0")

	flag.BoolVar(&useUTC, "u", false, "process time in UTC instead of local")

	flag.Parse()
	flag.Usage = mdbcliUsage

	glogging.Init([]string{"stdout"}, glogging.Loglevel(loglevel))

	if !cmdPrintStats &&
		!cmdAddMsg && !cmdGetMsg && !cmdListMsg && !cmdListFailed && !cmdDelMsg &&
		!cmdDelQueue && !cmdListQueue &&
		cmdPauseq != "" && cmdResumeq != "" {
		flag.Usage()
		os.Exit(1)
	}

	broker, err := gmq.NewBrokerRedis(dsnRedis)
	broker.UTC(useUTC)
	gutil.ExitOnErr(err)
	ctx := context.Background()
	if cmdPauseq != "" {
		pauseQueue(ctx, broker, cmdPauseq)
		os.Exit(0)
	} else if cmdResumeq != "" {
		resumeQueue(ctx, broker, cmdResumeq)
		os.Exit(0)
	}

	if cmdPrintStats {
		printStats(ctx, broker)
	} else if cmdAddMsg {
		addMsg(ctx, broker, queueName, payloadStr, msgId)
	} else if cmdGetMsg {
		getMsg(ctx, broker, queueName, msgId)
	} else if cmdListMsg {
		if state == "" {
			flag.Usage()
			os.Exit(1)
		}
		listMsg(ctx, broker, queueName, optLimit, optOffset)

	} else if cmdListFailed {
		listFailed(ctx, broker, queueName, msgId, optLimit, optOffset)

	} else if cmdDelMsg {
		delMsg(ctx, broker, queueName, msgId)
	} else if cmdListQueue {
		listQueue(ctx, broker)

	} else if cmdDelQueue {
		delQueue(ctx, broker, queueName)
	} else {
		flag.Usage()
		os.Exit(1)
	}

	fmt.Print("\n")
}

func listQueue(ctx context.Context, broker gmq.Broker) {
	names, err := broker.ListQueue(ctx)
	gutil.ExitOnErr(err)
	for _, name := range names {
		fmt.Println(name)
	}
}

func listFailed(ctx context.Context, broker gmq.Broker, queueName, msgId string, limit, offset int64) {
	if msgId == "" {
		fmt.Println("the option -i / message id is required")
		os.Exit(1)
	}

	msgs, err := broker.ListFailed(ctx, queueName, msgId, limit, offset)
	gutil.ExitOnErr(err)

	if len(msgs) == 0 {
		fmt.Printf("history of failed message matched queue=%s id=%s not found", queueName, msgId)
		os.Exit(0)
	}
	for _, msg := range msgs {
		fmt.Println(msg)
	}
}

func pauseQueue(ctx context.Context, broker gmq.Broker, queuename string) {
	if err := broker.Pause(ctx, queuename); err != nil {
		fmt.Printf("Pausing queue %s  failed. errer(%s) \n", queuename, err.Error())
	} else {
		fmt.Printf("Pause queue %s \n", queuename)
	}
}
func resumeQueue(ctx context.Context, broker gmq.Broker, queuename string) {
	if err := broker.Resume(ctx, queuename); err != nil {
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

func printStats(ctx context.Context, broker gmq.Broker) {
	queues, err := broker.GetStats(ctx)
	gutil.ExitOnErr(err)

	fmt.Print("\n# gmq Statistic \n\n")
	fmt.Print("## Daily Statistic \n\n")
	if len(queues) == 0 {
		fmt.Println("Related info not found. Do consumer(s) have not start yet?")
	} else {
		for _, rsStat := range queues {
			fmt.Printf("queue=%s total=%d pending=%d processing=%d failed=%d \n",
				rsStat.Name,
				rsStat.Total,
				rsStat.Pending,
				rsStat.Processing,
				rsStat.Failed,
			)
		}
		printStatsWeekly(ctx, broker)
	}
}

func printStatsWeekly(ctx context.Context, broker gmq.Broker) {
	rsStat, err := broker.GetStatsWeekly(ctx)
	gutil.ExitOnErr(err)

	now := time.Now()
	if useUTC {
		now = now.UTC()
	} else {
		now = now.Local()
	}

	fmt.Printf("\n## Weekly Statistic: %s ~ %s \n\n", now.AddDate(0, 0, -7).Format("2006-01-02"), now.Format("2006-01-02"))

	totalCompleted := int64(0)
	totalFailed := int64(0)
	for _, item := range rsStat {
		totalCompleted += item.Completed
		totalFailed += item.Failed
		fmt.Printf("date=%s completed=%d, failed=%d, total=%d \n", item.Date, item.Completed, item.Failed, item.Completed+item.Failed)
	}
	fmt.Printf("\ncompleted=%d, failed=%d, total=%d \n", totalCompleted, totalFailed, totalCompleted+totalFailed)
}

func getMsg(ctx context.Context, broker gmq.Broker, queueName, msgId string) {
	msg, err := broker.GetMsg(ctx, queueName, msgId)
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

	if msg.Created > 0 {
		t := time.UnixMilli(msg.Created)
		if useUTC {
			t = t.UTC()
		} else {
			t = t.Local()
		}
		fmt.Printf("  Created=%d (%s) \n", msg.Created, t.Format(time.RFC3339))
	}

	if msg.Updated > 0 {
		t := time.UnixMilli(msg.Updated)
		if useUTC {
			t = t.UTC()
		} else {
			t = t.Local()
		}
		fmt.Printf("  Updated=%d (%s) \n", msg.Updated, t.Format(time.RFC3339))
	}

	if msg.Expiredat > 0 {
		t := time.UnixMilli(msg.Expiredat)
		if useUTC {
			t = t.UTC()
		} else {
			t = t.Local()
		}
		fmt.Printf("  Expiredat=%d (%s) \n", msg.Expiredat, t.Format(time.RFC3339))
	}
}

func listMsg(ctx context.Context, broker gmq.Broker, queueName string, limit, offset int64) {
	msgs, err := broker.ListMsg(ctx, queueName, state, limit, offset)
	if err != nil {
		if err == gmq.ErrNoMsg {
			fmt.Printf("message matched queue=%s state=%s not found", queueName, state)
			return
		}
		gutil.ExitOnErr(err)
	}

	fmt.Printf("list messages of queue=%s state=%s \n\n", queueName, state)
	for _, msg := range msgs {
		fmt.Println(msg)
	}
}

func delMsg(ctx context.Context, broker gmq.Broker, queueName, msgId string) {
	err := broker.DeleteMsg(ctx, queueName, msgId)
	if err != nil {
		if err != gmq.ErrNoMsg {
			gutil.ExitOnErr(err)
		}
	}
	fmt.Printf("queue=%s msgId=%s deleted \n", queueName, msgId)
}

func delQueue(ctx context.Context, broker gmq.Broker, queueName string) {
	err := broker.DeleteQueue(ctx, queueName)
	if err != nil {
		if err != gmq.ErrNoMsg {
			gutil.ExitOnErr(err)
		}
	}
	fmt.Printf("queue %s cleared", queueName)
}

func mdbcliUsage() {
	fmt.Printf("\nmdbcli is a terminal supports gmq queue management \n\n")
	fmt.Printf("Usage: %s <one of following>", os.Args[0])
	fmt.Printf("  -stat print snapshots of all queues and statistics for the last 8 days\n\n")
	fmt.Printf("  -list list all or part of messages of a queue \n\n\t -list -q <queueName> -s <failed|pending|processing> [-n n] [-o m]\n\n")
	fmt.Printf("  -listfailed list history records of the failed messages \n\n\t -listfailed -q <queueName> -i <msgId> [-n n] [-o m]\n\n")
	fmt.Printf("  -get print detail of certain message by offering its queue and id \n\n\t -get -i <msgId> -q <queueName> \n\n")
	fmt.Printf("  -add add certain message by offering its queue, id and payload \n\n\t -add -i <msgId> -q <queueName> -p <payload> \n\n")
	fmt.Printf("  -del delete a certain message with by offering its queue and id \n\n\t -del -i <msgId> -q <queueName> \n\n")
	fmt.Printf("  -delqueue delete queue \n\n\t -delqueue -q <queueName>\n\n")
	fmt.Printf("  -listqueue list all queue names \n\n")
	fmt.Printf("  -pause pause queue consumption \n\n\t -pause -q <queueName>\n\n")
	fmt.Printf("  -resume resume queue consumption \n\n\t -resume -q <queueName>\n\n")
}
