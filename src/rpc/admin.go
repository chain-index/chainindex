package rpc

import (
	"fmt"
	"github.com/chain-index/chainindex/src/common"
	"runtime"
	//"encoding/hex"
	//"errors"
	//"github.com/chain-index/chainindex/src/blockchain"
	//ci_pb "github.com/chain-index/chainindex/src/protos"
	//"go.uber.org/zap"
)

type AdminService struct {
}

func (s *AdminService) NodeInfo() (interface{}, error) {
	return map[string]interface{}{
		"version": fmt.Sprintf("%v/%v/%v/%v", common.GetVersion(), runtime.GOOS, runtime.GOARCH, runtime.Version()),
	}, nil
}
