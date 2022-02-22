package etcd

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/clientv3"
)

func main2() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("init etcd error")
	}
	client.Put(context.Background(), "conf_path", `[{"path":"/tmp/test.log","topic":"test_log"}]`)
}
