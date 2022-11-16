package impl

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/go/gutil"
	"golang.org/x/net/html"
)

const (
	queueName        = "crawlIMDB"
	IMDBPrefix       = "https://imdb.com"
	Source           = "IMDB"
	UniqueIn         = 60 * 60 * 24 // seconds ， 防止重复入队
	ProcessdInterval = 10           // seconds ， 处理间隔，避免IP被封
)

func RunCrawlClient(ctx context.Context, broker gmq.Broker) {
	// 创建客户端
	cli, err := gmq.NewClientFromBroker(broker)
	gutil.ExitOnErr(err)

	// 爬取数据
	resp, err := http.Get("http://imdb.com/chart/top")
	gutil.ExitOnErr(err)
	doc, err := html.Parse(resp.Body)
	gutil.ExitOnErr(err)
	resp.Body.Close()

	// 解析链接
	re := regexp.MustCompile(`ref_=chttp_tt_.*`)
	var visitNodes func(*html.Node)
	detailsPages := map[int]string{}
	visitNodes = func(n *html.Node) {
		for _, a := range n.Attr {
			if a.Key == "href" && strings.HasPrefix(a.Val, "/title/tt") {
				topIdx := re.Find([]byte(a.Val))
				idx, err := strconv.Atoi(string(topIdx[14:]))
				if err != nil {
					log.Printf("parseURL failed: %s", a.Val)
				}
				detailsPages[idx] = a.Val
			}
		}
	}

	forEachNode(doc, visitNodes)

	// 消息入队
	for i := 1; i <= 250; i++ {
		dp, has := detailsPages[i]
		if !has {
			log.Printf("Enqueue Top %d  movie failed", i)
		}
		cli.Enqueue(ctx, imdbMsg(dp, i), gmq.OptUniqueIn(UniqueIn*time.Second))
	}
}

// 创建消息
func imdbMsg(url string, TopIndx int) *gmq.Msg {
	return &gmq.Msg{
		Payload: []byte(fmt.Sprintf("{%s: %s}", strconv.Quote("url"), strconv.Quote(url))),
		Id:      "Top" + strconv.Itoa(TopIndx),
		Queue:   queueName,
		State:   "pending",
		Created: time.Now().UnixMilli(),
	}
}

func forEachNode(n *html.Node, next func(n *html.Node)) {
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		forEachNode(c, next)
	}
	if next != nil {
		next(n)
	}
}
