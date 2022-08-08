package impl

// 定义错误类型，gmq的服务器（消息处理）失败时，会在Broker（如redis）中写入错误信息，方便后续检查调试。
import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/giant-stone/go/glogging"

	"github.com/giant-stone/gmq/gmq"
)

type ErrCrawlIMDB struct {
	TopIdx string
	Op     string
	Detail string
}

func (e *ErrCrawlIMDB) Error() string {
	return fmt.Sprintf("while crawl %s movie on IMDB, error(%s), op: %s", e.TopIdx, e.Detail, e.Op)
}

// 定义自定义的消费速率
func workIntervalFunc() time.Duration {
	return time.Second * time.Duration(10)
}

func RunCrawlServer(ctx context.Context, broker gmq.Broker) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := gmq.NewServer(ctx, broker, &gmq.Config{Logger: glogging.Sugared,
		QueueCfgs: map[string]*gmq.QueueCfg{
			// 队列名 - 队列配置
			queueName: gmq.NewQueueCfg(
				gmq.OptQueueWorkerNum(1),                    // 配置限制队列只有一个 worker
				gmq.OptWorkerWorkInterval(workIntervalFunc), // 配置限制队列消费间隔
			),
		},
	})
	mux := gmq.NewMux()

	mux.Handle(queueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) error {
		msgId := msg.GetId()
		link := strings.Split(string(msg.GetPayload()), ":")[1]
		link = strings.Trim(link, "\"{} ")

		resp, err := http.Get(IMDBPrefix + link)
		if err != nil {
			return &ErrCrawlIMDB{msgId, "http.Get", err.Error()}
		}
		buf, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return &ErrCrawlIMDB{msgId, "http.Get", err.Error()}
		}
		ms, err := parseDetail(buf)
		if err != nil {
			return &ErrCrawlIMDB{msgId, "parseDetail", err.Error()}
		}
		// 持久化
		err = persistence(ms)
		if err != nil {
			return &ErrCrawlIMDB{msgId, "persistence", err.Error()}
		}
		title, year := ms.Title, ms.Year
		log.Printf("%s: Title: %s, Year: %s", msgId, title, year)
		return nil
	}))

	go srv.Run(mux)
	select {}
}

type movie struct {
	Title string
	Year  string
	// other field ...
}

var re = regexp.MustCompile(`<title>(.*)\((.*)\).*</title>`)

func parseDetail(buf []byte) (*movie, error) {
	tmp := re.FindStringSubmatch(string(buf))

	title, err := url.PathUnescape(tmp[1])
	if err != nil {
		return nil, err
	}
	year := tmp[2]

	return &movie{Title: title, Year: year}, nil
}

func persistence(ms *movie) error {
	// 写入文件或数据库
	return nil
}
