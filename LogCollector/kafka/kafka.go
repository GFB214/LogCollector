package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

var (
	Client  sarama.SyncProducer
	MsgChan chan *sarama.ProducerMessage
)

func Init(address []string, chanSize int64) (err error) {
	config := sarama.NewConfig()
	// 全部ACK
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 随机分区
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	// 确认
	config.Producer.Return.Successes = true

	Client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		fmt.Println("connect kafka error")
		return err
	}

	MsgChan = make(chan *sarama.ProducerMessage, chanSize)

	go snedMsg()

	return err
}

//从MsgChan读出消息并发送
func snedMsg() {
	for {
		select {
		case msg := <-MsgChan:
			partition, offset, err := Client.SendMessage(msg)
			if err != nil {
				fmt.Println("sned to kafka error")
				return
			}
			fmt.Println("sned to kafka success ", partition, offset)

		}
	}
}
