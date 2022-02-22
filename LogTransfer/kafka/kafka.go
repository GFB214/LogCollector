package kafka

import (
	"encoding/json"
	"fmt"
	"logtransfer/es"

	"github.com/Shopify/sarama"
)

func Init(addr []string, topic string) (err error) {
	consumer, err := sarama.NewConsumer(addr, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	// 获取topic下的分区
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return
	}
	fmt.Println(partitionList)
	for partition := range partitionList {
		// 每个分区一个消费者
		var pc sarama.PartitionConsumer
		pc, err = consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
			return
		}
		// 异步从每个分区消费信息
		fmt.Println("start to consume...")
		go func(sarama.PartitionConsumer) {
			fmt.Println("in sarama.PartitionConsumer")
			for msg := range pc.Messages() {
				fmt.Println(msg.Topic, string(msg.Value))
				var mt map[string]interface{}
				err = json.Unmarshal(msg.Value, &mt)
				if err != nil {
					fmt.Printf("unmarshal msg failed, err:%v\n", err)
					continue
				}
				es.PutLogData(mt)
			}
		}(pc)
	}
	return
}
