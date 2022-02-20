package tailfile

import (
	"context"
	"fmt"
	"logagent/common"
)

type tailTaskMgr struct {
	tailTaskMap      map[string]*tailTask
	collectEntryList []common.CollectEntry
	confChan         chan []common.CollectEntry
}

var (
	ttMgr *tailTaskMgr
)

func Init(allConf []common.CollectEntry) (err error) {
	ttMgr = &tailTaskMgr{
		tailTaskMap:      make(map[string]*tailTask, 20),
		collectEntryList: allConf,
		confChan:         make(chan []common.CollectEntry),
	}
	//开始监听日志
	for _, conf := range allConf {
		ctx, cancel := context.WithCancel(context.Background())
		tt := newTailTask(conf.Path, conf.Topic, ctx, cancel)
		err = tt.Init()
		if err != nil {
			fmt.Println("init tailfile error ", tt.path, tt.topic)
			continue
		}
		fmt.Println("init tailfile success ", tt.path, tt.topic)
		ttMgr.tailTaskMap[tt.path] = tt
		go tt.run()
	}

	//监听配置更新消息
	go ttMgr.watch()
	return
}

func (ttMgr *tailTaskMgr) watch() {
	for {
		newConf := <-ttMgr.confChan
		fmt.Println("tailfile get new conf from etcd ", newConf)
		for _, conf := range newConf {
			//不变的任务
			if ttMgr.isExist(conf) {
				continue
			}
			//新建的任务
			ctx, cancel := context.WithCancel(context.Background())
			tt := newTailTask(conf.Path, conf.Topic, ctx, cancel)
			err := tt.Init()
			if err != nil {
				fmt.Println("init tailfile error ", tt.path, tt.topic)
				continue
			}
			fmt.Println("init tailfile success ", tt.path, tt.topic)
			ttMgr.tailTaskMap[tt.path] = tt
			go tt.run()

		}
		//删除的任务
		for key, task := range ttMgr.tailTaskMap {
			found := false
			for _, conf := range newConf {
				if key == conf.Path {
					found = true
					break
				}
			}
			if found == false {
				//停止并删除
				task.cancel()
				delete(ttMgr.tailTaskMap, key)
				fmt.Println("delete tailtask success ", task.path, task.topic)
			}
		}
	}

}

func SendNewConf(newConf []common.CollectEntry) {
	ttMgr.confChan <- newConf
}

func (ttMgr *tailTaskMgr) isExist(conf common.CollectEntry) bool {
	_, ok := ttMgr.tailTaskMap[conf.Path]
	return ok
}
