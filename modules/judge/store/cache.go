package store

import (
	"github.com/open-falcon/falcon-plus/common/model"
	"sync"
)

var TmpJudgeItem *TmpItemMap

type TmpItemMap struct {
	sync.RWMutex
	B map[string]*model.JudgeItem
}

func (this *TmpItemMap) Get(key string) (*model.JudgeItem, bool) {
	this.RLock()
	defer this.RUnlock()
	val, ok := this.B[key]
	return val, ok
}

func (this *TmpItemMap) Set(key string, item *model.JudgeItem) (*model.JudgeItem, bool) {
	this.Lock()
	defer this.Unlock()
	old, ok := this.B[key]
	if ok {
		//同一个step数据进行累加
		if old.Timestamp-item.Timestamp == 0 {
			old.Value += item.Value
		} else {
			this.B[key] = item
			return old, true
		}
	} else {
		this.B[key] = item
	}
	return nil, false
}

func init() {
	TmpJudgeItem = &TmpItemMap{
		B: make(map[string]*model.JudgeItem),
	}
}
