package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"logagent/common"
	"logagent/tailfile"
	"time"

	"go.etcd.io/etcd/clientv3"
)

var (
	client *clientv3.Client
)

func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("init etcd error")
		return err
	}
	return err
}

func GetConf(key string) (collectEntryList []common.CollectEntry, err error) {
	collectEntryList = make([]common.CollectEntry, 0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	// client.Put(ctx, key, `[{"path":"/tmp/test.log","topic":"test_log"},{"p	ath":"/tmp/dev","topic":"dev_log"}]`)
	resp, err := client.Get(ctx, key)
	if err != nil {
		fmt.Println("etcd get key error")
		return
	}
	confJson := resp.Kvs[0]
	err = json.Unmarshal(confJson.Value, &collectEntryList)
	if err != nil {
		fmt.Println("etcd conf unmarshal error")
		return
	}
	return
}

func WatchConf(key string) {
	//watch
	watchChan := client.Watch(context.Background(), key)
	for wResp := range watchChan {
		fmt.Println("etcd get new conf update")
		for _, event := range wResp.Events {
			var newConf []common.CollectEntry
			fmt.Println(event.Type, event.Kv.Key, event.Kv.Value)
			// 删除key，避免Unmarshal空对象
			if event.Type == clientv3.EventTypeDelete {
				tailfile.SendNewConf(newConf)
				continue
			}
			err := json.Unmarshal(event.Kv.Value, &newConf)
			if err != nil {
				fmt.Println("etcd conf unmarshal error")
				continue
			}
			//通知tailfile应用新配置
			tailfile.SendNewConf(newConf)
		}

	}
}
