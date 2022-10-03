package rpc

import (
	//"encoding/hex"
	//"errors"
	"github.com/chain-index/chainindex/src/blockchain"
	ci_pb "github.com/chain-index/chainindex/src/protos"
	//"go.uber.org/zap"
)

type BlockchainService struct {
	Chain *blockchain.ChainindexBlockchain
}

func (s *BlockchainService) GetBlockByHash(hash string) (*ci_pb.BcBlock, error) {
	block, err := s.Chain.GetBlockByHash(hash)
	if err != nil {
		block = new(ci_pb.BcBlock)
	}
	return block, err
}

func (s *BlockchainService) GetBlockByHeight(height uint64) (*ci_pb.BcBlock, error) {
	block, err := s.Chain.GetBlockByHeight(height)
	if err != nil {
		block = new(ci_pb.BcBlock)
	}
	return block, err
}

func (s *BlockchainService) GetBlockByTx(txHash string) (*ci_pb.BcBlock, error) {
	block, err := s.Chain.GetBlockByTx(txHash)
	if err != nil {
		block = new(ci_pb.BcBlock)
	}
	return block, err
}

func (s *BlockchainService) GetTxByHash(txHash string) (*ci_pb.Transaction, error) {
	var tx *ci_pb.Transaction
	block, err := s.Chain.GetBlockByTx(txHash)
	if err != nil {
		tx = new(ci_pb.Transaction)
	} else {
		for _, btx := range block.GetTxs() {
			if txHash == btx.GetHash() {
				tx = btx
				break
			}
		}
	}
	return tx, err
}

func (s *BlockchainService) Syncing() (interface{}, error) {
	progress := s.Chain.SyncProgress()

	if progress.Done {
		return false, nil
	}

	return map[string]interface{}{
		"startingBlock": progress.StartingBlock.GetHeight(),
		"currentBlock":  progress.CurrentBlock.GetHeight(),
		"highestBlock":  progress.HighestPeerBlockHeight,
	}, nil
}

func (s *BlockchainService) GetHighestBlock() (*ci_pb.BcBlock, error) {
	block, err := s.Chain.GetHighestBlock()
	if err != nil {
		block = new(ci_pb.BcBlock)
	}
	return block, err
}
