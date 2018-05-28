package cron

import (
	"encoding/json"
	"fmt"
	"github.com/open-falcon/falcon-plus/common/model"
	cmodel "github.com/open-falcon/falcon-plus/common/model"
	cutils "github.com/open-falcon/falcon-plus/common/utils"
	"github.com/open-falcon/falcon-plus/modules/judge/g"
	"github.com/open-falcon/falcon-plus/modules/judge/judge"
	nsema "github.com/toolkits/concurrent/semaphore"
	"log"
	"strings"
	"time"
)

const (
	ConcurrentOfJudge = 100
)

var semaJudge = nsema.NewSemaphore(ConcurrentOfJudge) //全量同步任务 并发控制器

func PullAndJudge() {

	for {
		//get strategy info
		strategyMap := g.StrategyMap.M
		now1 := time.Now().Unix()
		for key, val := range strategyMap {
			semaJudge.Acquire()
			go judgeItemWithStrategy(key, val)
			fmt.Println("%s", key)
		}
		now2 := time.Now().Unix()
		d := time.Duration(now2 - now1)
		if d <= 0 {
			time.Sleep(time.Second * 60)
		} else if d < 60 {
			time.Sleep(time.Second * d)
		} else {
			log.Printf("metric polling exceed 60s")
		}
	}
}

func judgeItemWithStrategy(key string, val []model.Strategy) {
	defer semaJudge.Release()
	etime := time.Now().Unix() - 60*1
	for _, strategy := range val {
		//get history data from graph
		s := strings.Split(key, "/")
		if len(s) != 2 {
			return
		}
		var counter string
		tags := cutils.SortedTags(strategy.Tags)
		if tags == "" {
			counter = s[1]
		} else {
			counter = fmt.Sprintf("%s/%s", s[1], cutils.SortedTags(strategy.Tags))
		}
		//解析
		sfunc := strings.Split(strategy.Func[:len(strategy.Func)-1], "#")
		if len(sfunc) < 1 {
			return
		}
		args, err := judge.Atois(sfunc[1])
		if err != nil || len(args) < 1 {
			return
		}
		queryParam := GenQParam(s[0], counter, "AVERAGE", -1, etime, args[0])
		var resp = &cmodel.GraphQueryResponse{}
		queryHistoryData(queryParam, resp)
		if resp != nil && len(resp.Values) > 0 {
			fn, err := judge.ParseFuncFromString(strategy.Func, strategy.Operator, strategy.RightValue)
			if err != nil {
				log.Printf("[ERROR] parse func %s fail: %v. strategy id: %d", strategy.Func, err, strategy.Id)
				return
			}
			var (
				leftValue   float64
				isTriggered bool
				isEnough    bool
			)
			//判断是否是同环比报警,是则获取同比数据
			if strings.Contains(strategy.Func, "c_avg_rate_abs") {
				if len(args) < 2 {
					log.Printf("[ERROR] c_avg_rate_abs pattern error,pattern:%s", strategy.Func)
					return
				}
				etime = etime - 60*60*24*int64(args[1])
				var resp1 = &cmodel.GraphQueryResponse{}
				queryParam.End = etime
				queryHistoryData(queryParam, resp1)
				if resp1 != nil && len(resp1.Values) > 0 {
					leftValue, isTriggered, isEnough = fn.RelativeCompute(resp.Values, resp1.Values)
				} else {
					isEnough = false
					log.Printf("query:%v ring ratio data is nil", queryParam)
				}
			} else {
				leftValue, isTriggered, isEnough = fn.Compute(resp.Values)
			}
			if !isEnough {
				return
			}
			fmt.Printf("strategy info :%s.Trigger info:leftValue:%f,isTriggered:%t", strategy, leftValue, isTriggered)
			now := time.Now().Unix()
			key := cutils.Md5(cutils.PK(s[0], s[1], strategy.Tags))
			event := &model.Event{
				Id:         fmt.Sprintf("s_%d_%s", strategy.Id, key),
				Strategy:   &strategy,
				Endpoint:   s[0],
				LeftValue:  leftValue,
				EventTime:  now,
				PushedTags: strategy.Tags,
			}
			sendEventIfNeed(resp.Values, isTriggered, now, event, strategy.MaxStep)
		}
	}
}

func queryHistoryData(queryParam cmodel.GraphQueryParam, resp *cmodel.GraphQueryResponse) {
	fmt.Println(queryParam)
	err := g.ApiClient.Call("GraphRpc.QueryOne", queryParam, resp)
	fmt.Println(resp)
	if err != nil {
		return
	}
	log.Println("client\t-", "receive remote return ", resp)
}

func GenQParam(endpoint string, counter string, consolFun string, stime int64, etime int64, step int) cmodel.GraphQueryParam {
	return cmodel.GraphQueryParam{
		Start:     stime,
		End:       etime,
		ConsolFun: consolFun,
		Endpoint:  endpoint,
		Counter:   counter,
		Step:      step,
	}
}

func sendEventIfNeed(historyData []*model.RRDData, isTriggered bool, now int64, event *model.Event, maxStep int) {
	lastEvent, exists := g.LastEvents.Get(event.Id)
	if isTriggered {
		event.Status = "PROBLEM"
		if !exists || lastEvent.Status[0] == 'O' {
			// 本次触发了阈值，之前又没报过警，得产生一个报警Event
			event.CurrentStep = 1

			// 但是有些用户把最大报警次数配置成了0，相当于屏蔽了，要检查一下
			if maxStep == 0 {
				return
			}

			sendEvent(event)
			return
		}

		// 逻辑走到这里，说明之前Event是PROBLEM状态
		if lastEvent.CurrentStep >= maxStep {
			// 报警次数已经足够多，到达了最多报警次数了，不再报警
			return
		}

		if historyData[len(historyData)-1].Timestamp <= lastEvent.EventTime {
			// 产生过报警的点，就不能再使用来判断了，否则容易出现一分钟报一次的情况
			// 只需要拿最后一个historyData来做判断即可，因为它的时间最老
			return
		}

		if now-lastEvent.EventTime < g.Config().Alarm.MinInterval {
			// 报警不能太频繁，两次报警之间至少要间隔MinInterval秒，否则就不能报警
			return
		}

		event.CurrentStep = lastEvent.CurrentStep + 1
		sendEvent(event)
	} else {
		// 如果LastEvent是Problem，报OK，否则啥都不做
		if exists && lastEvent.Status[0] == 'P' {
			event.Status = "OK"
			event.CurrentStep = 1
			sendEvent(event)
		}
	}
}

func sendEvent(event *model.Event) {
	// update last event
	g.LastEvents.Set(event.Id, event)

	bs, err := json.Marshal(event)
	if err != nil {
		log.Printf("json marshal event %v fail: %v", event, err)
		return
	}

	// send to redis
	redisKey := fmt.Sprintf(g.Config().Alarm.QueuePattern, event.Priority())
	rc := g.RedisConnPool.Get()
	defer rc.Close()
	rc.Do("LPUSH", redisKey, string(bs))
}
