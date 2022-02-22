package main

import (
	"fmt"
	"logtransfer/es"
	"logtransfer/kafka"
	"logtransfer/model"

	"github.com/go-ini/ini"
)

// kafka取出数据写入ES
func main() {
	// 加载配置文件
	var cfg = new(model.Config)
	err := ini.MapTo(cfg, "./config/logtransfer.ini")
	if err != nil {
		fmt.Printf("load config failed,err:%v\n", err)
		panic(err)
	}
	fmt.Println("load config success")
	// 连接ES
	err = es.Init(cfg.ESConf.Address, cfg.ESConf.Index, cfg.ESConf.GoNum, cfg.ESConf.MaxSize)
	if err != nil {
		fmt.Printf("Init es failed,err:%v\n", err)
		panic(err)
	}
	fmt.Println("Init ES success")
	// 连接kafka
	err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.Topic)
	if err != nil {
		fmt.Printf("connect to kafka failed,err:%v\n", err)
		panic(err)
	}
	fmt.Println("Init kafka success")
	// 在这儿停顿!
	select {}
}
