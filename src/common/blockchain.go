package common

import (
	ci_pb "github.com/chain-index/chainindex/src/protos"
	"math/big"
)

func BlockOrderingRule(iBlock, jBlock *ci_pb.BcBlock) bool {
	if iBlock.GetHeight() == jBlock.GetHeight() {
		iDist, _ := new(big.Int).SetString(iBlock.GetTotalDistance(), 10)
		jDist, _ := new(big.Int).SetString(jBlock.GetTotalDistance(), 10)
		compare := iDist.Cmp(jDist)
		if compare == 0 {
			return jBlock.GetTimestamp() < iBlock.GetTimestamp()
		}
		return compare < 0
	}
	return iBlock.GetHeight() < jBlock.GetHeight()
}
