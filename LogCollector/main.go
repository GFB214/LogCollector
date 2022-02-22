package main

import (
	"fmt"
	"logagent/etcd"
	"logagent/kafka"
	"logagent/tailfile"

	"github.com/go-ini/ini"
)

// 日志收集客户端
// 类似filebeat
// 收集日志发送到kafka

type Config struct {
	KafkaConfig `ini:"kafka"`
	EtcdConfig  `ini:"etcd"`
}

type EtcdConfig struct {
	Address        string `ini:"address"`
	CollectConfKey string `ini:"collect_conf_key"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	ChanSize int64  `init:"chan_size"`
}

func main() {
	// 读配置文件
	var configObj = new(Config)
	cfg, err := ini.Load("./conf/config.ini")
	if err != nil {
		fmt.Println("load config error")
		return
	}
	kafkaAddr := cfg.Section("kafka").Key("address").String()
	fmt.Println(kafkaAddr)

	cfg.MapTo(configObj)
	fmt.Println(configObj.KafkaConfig.Address)

	// 初始化etcd
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		fmt.Println("init etcd error")
		return
	}
	fmt.Println("init etcd success")

	// 获取配置
	allConf, err := etcd.GetConf(configObj.EtcdConfig.CollectConfKey)
	if err != nil {
		fmt.Println("etcd get conf error")
		return
	}
	fmt.Println(allConf)
	go etcd.WatchConf(configObj.EtcdConfig.CollectConfKey)

	// 初始化kafka
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		fmt.Println("init kafka error")
		return
	}
	fmt.Println("init kafka success")

	// 初始化tail
	err = tailfile.Init(allConf)
	if err != nil {
		fmt.Println("init tail error")
		return
	}
	fmt.Println("init tailfile success")

	select {}
}
