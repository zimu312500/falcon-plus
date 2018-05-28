package rpc

import (
	"fmt"
	cmodel "github.com/open-falcon/falcon-plus/common/model"
	grh "github.com/open-falcon/falcon-plus/modules/api/graph"
)

type GraphRpc int

func (grprpc *GraphRpc) QueryOne(para cmodel.GraphQueryParam, resp *cmodel.GraphQueryResponse) error {
	r, _ := grh.QueryOne(para)
	if r != nil {
		resp.Values = r.Values
		resp.Counter = r.Counter
		resp.DsType = r.DsType
		resp.Endpoint = r.Endpoint
		resp.Step = r.Step
		fmt.Println(resp)
	}
	return nil
}
