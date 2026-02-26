package servers

import (
	"context"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"

	pbgs "github.com/iggydv12/gomad/gen/proto/groupstorage"
	pbm "github.com/iggydv12/gomad/gen/proto/models"
)

// GroupStorageServiceServer implements the GroupStorageService gRPC server.
// Maps to GroupStorageService.java + GroupStorageServer.java.
type GroupStorageServiceServer struct {
	pbgs.UnimplementedGroupStorageServiceServer
	logger  *zap.Logger
	handler GroupStorageHandler
}

// GroupStorageHandler is the interface the node's GroupStorage component must implement.
type GroupStorageHandler interface {
	Get(id string) (*pbm.GameObjectGrpc, error)
	Put(obj *pbm.GameObjectGrpc) (bool, error)
	Update(obj *pbm.GameObjectGrpc) (bool, error)
	Delete(id string) (bool, error)
	AddToGroupLedger(objectID, peerID string, ttl int64) bool
	RemovePeerGroupLedger(peerID string) bool
}

// NewGroupStorageServiceServer creates a GroupStorageServiceServer.
func NewGroupStorageServiceServer(handler GroupStorageHandler, logger *zap.Logger) *GroupStorageServiceServer {
	return &GroupStorageServiceServer{handler: handler, logger: logger}
}

// Serve starts the gRPC listener.
func (s *GroupStorageServiceServer) Serve(addr string) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*1024),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 300 * time.Second}),
	)
	pbgs.RegisterGroupStorageServiceServer(srv, s)
	go func() {
		if err := srv.Serve(lis); err != nil {
			s.logger.Error("GroupStorageService gRPC server stopped", zap.Error(err))
		}
	}()
	s.logger.Info("GroupStorageService gRPC listening", zap.String("addr", addr))
	return srv, nil
}

func (s *GroupStorageServiceServer) Get(ctx context.Context, req *pbgs.GetObjectRequest) (*pbgs.GetObjectResponse, error) {
	obj, err := s.handler.Get(req.Id)
	if err != nil {
		return &pbgs.GetObjectResponse{Result: false}, nil
	}
	return &pbgs.GetObjectResponse{Object: obj, Result: true}, nil
}

func (s *GroupStorageServiceServer) Put(ctx context.Context, req *pbgs.PutObjectRequest) (*pbgs.PutObjectResponse, error) {
	result, err := s.handler.Put(req.Object)
	if err != nil {
		s.logger.Warn("group storage put failed", zap.Error(err))
		return &pbgs.PutObjectResponse{Result: false}, nil
	}
	return &pbgs.PutObjectResponse{Result: result}, nil
}

func (s *GroupStorageServiceServer) Update(ctx context.Context, req *pbgs.UpdateObjectRequest) (*pbgs.UpdateObjectResponse, error) {
	result, err := s.handler.Update(req.Object)
	if err != nil {
		return &pbgs.UpdateObjectResponse{Result: false}, nil
	}
	return &pbgs.UpdateObjectResponse{Result: result}, nil
}

func (s *GroupStorageServiceServer) Delete(ctx context.Context, req *pbgs.DeleteObjectRequest) (*pbgs.DeleteObjectResponse, error) {
	result, err := s.handler.Delete(req.Id)
	if err != nil {
		return &pbgs.DeleteObjectResponse{Result: false}, nil
	}
	return &pbgs.DeleteObjectResponse{Result: result}, nil
}

func (s *GroupStorageServiceServer) AddToGroupLedger(ctx context.Context, req *pbgs.AddToGroupLedgerRequest) (*pbgs.AddToGroupLedgerResponse, error) {
	result := s.handler.AddToGroupLedger(req.ObjectId, req.PeerId, req.Ttl)
	return &pbgs.AddToGroupLedgerResponse{Result: result}, nil
}

func (s *GroupStorageServiceServer) RemovePeerGroupLedger(ctx context.Context, req *pbgs.RemovePeerGroupLedgerRequest) (*pbgs.RemovePeerGroupLedgerResponse, error) {
	result := s.handler.RemovePeerGroupLedger(req.PeerId)
	return &pbgs.RemovePeerGroupLedgerResponse{Result: result}, nil
}

func (s *GroupStorageServiceServer) HealthCheck(ctx context.Context, _ *emptypb.Empty) (*pbgs.HealthCheckResponse, error) {
	return &pbgs.HealthCheckResponse{Status: pbgs.HealthCheckResponse_SERVING}, nil
}

// HealthCheckWatch streams health status until the client disconnects.
func (s *GroupStorageServiceServer) HealthCheckWatch(_ *emptypb.Empty, stream pbgs.GroupStorageService_HealthCheckWatchServer) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			if err := stream.Send(&pbgs.HealthCheckResponse{Status: pbgs.HealthCheckResponse_SERVING}); err != nil {
				return err
			}
		}
	}
}
