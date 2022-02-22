package es

import (
	"context"
	"fmt"

	"github.com/olivere/elastic/v7"
)

// 将日志数据写入Elasticsearch
type ESClient struct {
	client      *elastic.Client
	index       string
	logDataChan chan interface{}
}

var (
	esClient *ESClient
)

func Init(addr, index string, goroutineNum, maxSize int) (err error) {
	client, err := elastic.NewClient(elastic.SetURL("http://" + addr))
	if err != nil {
		// Handle error
		panic(err)
	}
	esClient = &ESClient{
		client:      client,
		index:       index,
		logDataChan: make(chan interface{}, maxSize),
	}
	fmt.Println("connect to es success")
	// 从通道中取出数据,写入到kafka中去
	for i := 0; i < goroutineNum; i++ {
		go sendToES()
	}
	return
}

func sendToES() {
	for mt := range esClient.logDataChan {
		put1, err := esClient.client.Index().
			Index(esClient.index).
			BodyJson(mt).
			Do(context.Background())
		if err != nil {
			panic(err)
		}
		fmt.Printf("Indexed user %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
	}
}

// 供其他代码将数据发送到chan中
func PutLogData(msg interface{}) {
	esClient.logDataChan <- msg
}
