package tailfile

import (
	"context"
	"fmt"
	"logagent/kafka"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
)

type tailTask struct {
	path   string
	topic  string
	tObj   *tail.Tail
	ctx    context.Context
	cancel context.CancelFunc
}

func newTailTask(path string, topic string, ctx context.Context, cancel context.CancelFunc) *tailTask {
	tt := &tailTask{
		path:   path,
		topic:  topic,
		ctx:    ctx,
		cancel: cancel,
	}
	return tt
}

func (tt *tailTask) Init() (err error) {
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	tt.tObj, err = tail.TailFile(tt.path, config)
	return
}

func (tt *tailTask) run() {
	//读取日志，发送到kafka
	for {
		//循环读取一行日志

		//等待退出
		select {
		case line, ok := <-tt.tObj.Lines:
			if !ok {
				fmt.Println("error read ok ", tt.tObj)
				time.Sleep(1 * time.Second)
				continue
			}
			if len(strings.Trim(line.Text, "\r")) == 0 {
				continue
			}
			fmt.Println("read line ", line.Text)
			//包装消息对象
			msg := &sarama.ProducerMessage{}
			msg.Topic = tt.topic
			msg.Value = sarama.StringEncoder(line.Text)
			//放入通道
			kafka.MsgChan <- msg
		case <-tt.ctx.Done():
			return
		}
	}
}
