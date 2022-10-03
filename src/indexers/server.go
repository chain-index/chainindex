package indexers

import (
	"context"
	"errors"
	"fmt"
	"io"

	"sync"

	ci_pb "github.com/chain-index/chainindex/src/protos"
	"go.uber.org/zap"
)

func NewServer() *Server {
	out := new(Server)
	out.incomingBlocks = make(chan *ci_pb.Block, 10)
	out.requestLock.Lock()
	out.roverRequests = make(map[string]chan *ci_pb.RoverMessage)
	out.requestLock.Unlock()
	out.syncLock.Lock()
	out.syncStatus = make(map[string]bool)
	out.syncLock.Unlock()
	return out
}

type Server struct {
	ci_pb.UnimplementedRoverServer

	requestLock, blockLock, syncLock sync.Mutex

	incomingBlocks chan *ci_pb.Block
	roverRequests  map[string]chan *ci_pb.RoverMessage

	syncStatus map[string]bool
}

func (s *Server) Join(joinReq *ci_pb.RoverIdent, stream ci_pb.Rover_JoinServer) error {
	name := joinReq.GetRoverName()
	zap.S().Infof("Got join request from %v", name)
	var requests chan *ci_pb.RoverMessage
	s.requestLock.Lock()
	if _, ok := s.roverRequests[name]; !ok {
		requests = make(chan *ci_pb.RoverMessage, 10)
		s.roverRequests[name] = requests
	} else {
		zap.S().Errorf("Rover %v is already in list of joined rovers, dropping join request!", name)
		s.requestLock.Unlock()
		return nil
	}
	s.requestLock.Unlock()
	s.syncLock.Lock()
	s.syncStatus[name] = false
	s.syncLock.Unlock()

	zap.S().Infof("Joined %v to rovers!", name)

	for {
		oneRequest, more := <-requests
		zap.S().Infof("Got rover request: %v", oneRequest)
		if err := stream.Send(oneRequest); err != nil {
			zap.S().Error(err)
			close(requests)
			return err
		}
		if more {
			zap.S().Warnf("Rover message request channel for %v has been closed!", name)
			return nil
		}
	}
	return nil
}

func (s *Server) CollectBlock(stream ci_pb.Rover_CollectBlockServer) error {
	for {
		block, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(new(ci_pb.Null))
		}
		if err != nil {
			return err
		}
		zap.S().Infof("Got a new Block -> %v", block)
		s.incomingBlocks <- block
	}
	return nil
}

func (s *Server) ReportSyncStatus(ctx context.Context, status *ci_pb.RoverSyncStatus) (*ci_pb.Null, error) {
	name := status.GetRoverName()
	if _, ok := s.syncStatus[name]; ok {
		s.syncLock.Lock()
		s.syncStatus[name] = status.GetStatus()
		s.syncLock.Unlock()
	} else {
		zap.S().Errorf("Unknown rover: %v", name)
		return nil, errors.New(fmt.Sprintf("Unknown rover: %v", name))
	}
	return &ci_pb.Null{}, nil
}

func (s *Server) ReportBlockRange(context.Context, *ci_pb.RoverMessage_RoverBlockRange) (*ci_pb.Null, error) {
	return nil, nil
}

func (s *Server) IsBeforeSettleHeight(context.Context, *ci_pb.SettleTxCheckReq) (*ci_pb.SettleTxCheckResponse, error) {
	return nil, nil
}

func (s *Server) PushRoverRequest(rover string, req *ci_pb.RoverMessage) {
	s.requestLock.Lock()
	if reqs, ok := s.roverRequests[rover]; ok {
		reqs <- req
	}
	s.requestLock.Unlock()
}
