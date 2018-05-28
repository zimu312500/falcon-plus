// Copyright 2017 Xiaomi, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package judge

import (
	"fmt"
	"github.com/open-falcon/falcon-plus/common/model"
	"math"
	"strconv"
	"strings"
)

type Function interface {
	Compute(vs []*model.RRDData) (leftValue float64, isTriggered bool, isEnough bool)
	RelativeCompute(current []*model.RRDData, relative []*model.RRDData) (rate float64, isTriggered bool, isEnough bool)
}

type MaxFunction struct {
	Function
	Limit      int
	Operator   string
	RightValue float64
}

func (this MaxFunction) Compute(vs []*model.RRDData) (leftValue float64, isTriggered bool, isEnough bool) {
	if len(vs) >= this.Limit {
		isEnough = true
	}
	if !isEnough {
		return
	}

	max := vs[0].Value
	for i := 1; i < this.Limit; i++ {
		if max < vs[i].Value {
			max = vs[i].Value
		}
	}

	leftValue = float64(max)
	isTriggered = checkIsTriggered(leftValue, this.Operator, this.RightValue)
	return
}

type MinFunction struct {
	Function
	Limit      int
	Operator   string
	RightValue float64
}

func (this MinFunction) Compute(vs []*model.RRDData) (leftValue float64, isTriggered bool, isEnough bool) {
	if len(vs) >= this.Limit {
		isEnough = true
	}
	if !isEnough {
		return
	}

	min := vs[0].Value
	for i := 1; i < this.Limit; i++ {
		if min > vs[i].Value {
			min = vs[i].Value
		}
	}

	leftValue = float64(min)
	isTriggered = checkIsTriggered(leftValue, this.Operator, this.RightValue)
	return
}

type AllFunction struct {
	Function
	Limit      int
	Operator   string
	RightValue float64
}

func (this AllFunction) Compute(vs []*model.RRDData) (leftValue float64, isTriggered bool, isEnough bool) {
	if len(vs) >= this.Limit {
		isEnough = true
	}
	if !isEnough {
		return
	}

	isTriggered = true
	for i := 0; i < this.Limit; i++ {
		isTriggered = checkIsTriggered(float64(vs[i].Value), this.Operator, this.RightValue)
		if !isTriggered {
			break
		}
	}

	leftValue = float64(vs[0].Value)
	return
}

type LookupFunction struct {
	Function
	Num        int
	Limit      int
	Operator   string
	RightValue float64
}

func (this LookupFunction) Compute(vs []*model.RRDData) (leftValue float64, isTriggered bool, isEnough bool) {
	if len(vs) >= this.Limit {
		isEnough = true
	}
	if !isEnough {
		return
	}

	leftValue = float64(vs[0].Value)

	for n, i := 0, 0; i < this.Limit; i++ {
		if checkIsTriggered(float64(vs[i].Value), this.Operator, this.RightValue) {
			n++
			if n == this.Num {
				isTriggered = true
				return
			}
		}
	}

	return
}

type SumFunction struct {
	Function
	Limit      int
	Operator   string
	RightValue float64
}

func (this SumFunction) Compute(vs []*model.RRDData) (leftValue float64, isTriggered bool, isEnough bool) {
	if len(vs) >= this.Limit {
		isEnough = true
	}
	if !isEnough {
		return
	}

	sum := 0.0
	for i := 0; i < this.Limit; i++ {
		sum += float64(vs[i].Value)
	}

	leftValue = sum
	isTriggered = checkIsTriggered(leftValue, this.Operator, this.RightValue)
	return
}

type AvgFunction struct {
	Function
	Limit      int
	Operator   string
	RightValue float64
}

func (this AvgFunction) Compute(vs []*model.RRDData) (leftValue float64, isTriggered bool, isEnough bool) {
	if len(vs) >= this.Limit {
		isEnough = true
	}
	if !isEnough {
		return
	}

	sum := 0.0
	for i := 0; i < this.Limit; i++ {
		sum += float64(vs[i].Value)
	}

	leftValue = sum / float64(this.Limit)
	isTriggered = checkIsTriggered(leftValue, this.Operator, this.RightValue)
	return
}

type DiffFunction struct {
	Function
	Limit      int
	Operator   string
	RightValue float64
}

// 只要有一个点的diff触发阈值，就报警
func (this DiffFunction) Compute(vs []*model.RRDData) (leftValue float64, isTriggered bool, isEnough bool) {
	// 此处this.Limit要+1，因为通常说diff(#3)，是当前点与历史的3个点相比较
	// 然而最新点已经在linkedlist的第一个位置，所以……
	if len(vs) >= this.Limit {
		isEnough = true
	}
	if !isEnough {
		return
	}

	if len(vs) == 0 {
		isEnough = false
		return
	}

	first := vs[0].Value

	isTriggered = false
	for i := 1; i < this.Limit+1; i++ {
		// diff是当前值减去历史值
		leftValue = float64(first - vs[i].Value)
		isTriggered = checkIsTriggered(leftValue, this.Operator, this.RightValue)
		if isTriggered {
			break
		}
	}

	return
}

// pdiff(#3)
type PDiffFunction struct {
	Function
	Limit      int
	Operator   string
	RightValue float64
}

func (this PDiffFunction) Compute(vs []*model.RRDData) (leftValue float64, isTriggered bool, isEnough bool) {
	if len(vs) >= this.Limit {
		isEnough = true
	}
	if !isEnough {
		return
	}

	if len(vs) == 0 {
		isEnough = false
		return
	}

	first := vs[0].Value

	isTriggered = false
	for i := 1; i < this.Limit+1; i++ {
		if vs[i].Value == 0 {
			continue
		}

		leftValue = float64((first - vs[i].Value) / vs[i].Value * 100.0)
		isTriggered = checkIsTriggered(leftValue, this.Operator, this.RightValue)
		if isTriggered {
			break
		}
	}

	return
}

type CAvgRateAbs struct {
	Function
	Operator   string
	RightValue float64
}

func (this CAvgRateAbs) RelativeCompute(current []*model.RRDData, relative []*model.RRDData) (leftValue float64, isTriggered bool, isEnough bool) {
	if len(current) > 0 && len(relative) > 0 {
		//cal avg
		var (
			csum   float64
			ccount int
			rsum   float64
			rcount int
		)
		for i := 0; i < len(current); i++ {
			if !math.IsNaN(float64(current[i].Value)) && current[i].Value != 0 {
				ccount++
				csum += float64(current[i].Value)
			}
		}
		for i := 0; i < len(relative); i++ {
			if !math.IsNaN(float64(relative[i].Value)) && relative[i].Value != 0 {
				rcount++
				rsum += float64(relative[i].Value)
			}
		}
		if ccount != 0 && rcount != 0 {
			isEnough = true
			cv := csum / float64(ccount)
			rv := rsum / float64(rcount)
			rate := math.Abs(cv-rv) / rv * 100
			isTriggered = checkIsTriggered(rate, this.Operator, this.RightValue)
			leftValue = rate
		}
	}
	return
}

func Atois(s string) (ret []int, err error) {
	a := strings.Split(s, ",")
	ret = make([]int, len(a))
	for i, v := range a {
		ret[i], err = strconv.Atoi(v)
		if err != nil {
			return
		}
	}
	return
}

// @str: e.g. all(#3) sum(#3) avg(#10) diff(#10)
//ext str,e,g, c_vag_rate_abs(#3#7)
func ParseFuncFromString(str string, operator string, rightValue float64) (fn Function, err error) {
	if str == "" {
		return nil, fmt.Errorf("func can not be null!")
	}
	idx := strings.Index(str, "#")
	args, err := Atois(str[idx+1 : len(str)-1])
	if err != nil {
		return nil, err
	}

	switch str[:idx-1] {
	case "max":
		fn = &MaxFunction{Limit: args[0], Operator: operator, RightValue: rightValue}
	case "min":
		fn = &MinFunction{Limit: args[0], Operator: operator, RightValue: rightValue}
	case "all":
		fn = &AllFunction{Limit: args[0], Operator: operator, RightValue: rightValue}
	case "sum":
		fn = &SumFunction{Limit: args[0], Operator: operator, RightValue: rightValue}
	case "avg":
		fn = &AvgFunction{Limit: args[0], Operator: operator, RightValue: rightValue}
	case "diff":
		fn = &DiffFunction{Limit: args[0], Operator: operator, RightValue: rightValue}
	case "pdiff":
		fn = &PDiffFunction{Limit: args[0], Operator: operator, RightValue: rightValue}
	case "lookup":
		fn = &LookupFunction{Num: args[0], Limit: args[1], Operator: operator, RightValue: rightValue}
	case "c_avg_rate_abs":
		fn = &CAvgRateAbs{Operator: operator, RightValue: rightValue}
	default:
		err = fmt.Errorf("not_supported_method")
	}

	return
}

func checkIsTriggered(leftValue float64, operator string, rightValue float64) (isTriggered bool) {
	switch operator {
	case "=", "==":
		isTriggered = math.Abs(leftValue-rightValue) < 0.0001
	case "!=":
		isTriggered = math.Abs(leftValue-rightValue) > 0.0001
	case "<":
		isTriggered = leftValue < rightValue
	case "<=":
		isTriggered = leftValue <= rightValue
	case ">":
		isTriggered = leftValue > rightValue
	case ">=":
		isTriggered = leftValue >= rightValue
	}

	return
}
