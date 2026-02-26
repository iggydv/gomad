package servers

import (
	"context"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pbm "github.com/iggydv12/gomad/gen/proto/models"
	pbps "github.com/iggydv12/gomad/gen/proto/peerstorage"
)

// PeerStorageServiceServer implements the PeerStorageService gRPC server.
// Maps to PeerStorageService.java + PeerStorageServer.java.
type PeerStorageServiceServer struct {
	pbps.UnimplementedPeerStorageServiceServer
	logger  *zap.Logger
	handler PeerStorageHandler
}

// PeerStorageHandler is the interface the node's PeerStorage component must implement.
type PeerStorageHandler interface {
	Get(id string) (*pbm.GameObjectGrpc, error)
	Put(obj *pbm.GameObjectGrpc) (bool, error)
	Update(obj *pbm.GameObjectGrpc) (bool, error)
	Delete(id string) (bool, error)
}

// NewPeerStorageServiceServer creates a PeerStorageServiceServer.
func NewPeerStorageServiceServer(handler PeerStorageHandler, logger *zap.Logger) *PeerStorageServiceServer {
	return &PeerStorageServiceServer{handler: handler, logger: logger}
}

// Serve starts the gRPC listener.
func (s *PeerStorageServiceServer) Serve(addr string) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*1024),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 300 * time.Second}),
	)
	pbps.RegisterPeerStorageServiceServer(srv, s)
	go func() {
		if err := srv.Serve(lis); err != nil {
			s.logger.Error("PeerStorageService gRPC server stopped", zap.Error(err))
		}
	}()
	s.logger.Info("PeerStorageService gRPC listening", zap.String("addr", addr))
	return srv, nil
}

func (s *PeerStorageServiceServer) Get(ctx context.Context, req *pbps.GetObjectRequest) (*pbps.GetObjectResponse, error) {
	obj, err := s.handler.Get(req.Id)
	if err != nil {
		return &pbps.GetObjectResponse{Result: false}, nil
	}
	return &pbps.GetObjectResponse{Object: obj, Result: true}, nil
}

func (s *PeerStorageServiceServer) Put(ctx context.Context, req *pbps.PutObjectRequest) (*pbps.PutObjectResponse, error) {
	result, err := s.handler.Put(req.Object)
	if err != nil {
		return &pbps.PutObjectResponse{Result: false}, nil
	}
	return &pbps.PutObjectResponse{Result: result}, nil
}

func (s *PeerStorageServiceServer) Update(ctx context.Context, req *pbps.UpdateObjectRequest) (*pbps.UpdateObjectResponse, error) {
	result, err := s.handler.Update(req.Object)
	if err != nil {
		return &pbps.UpdateObjectResponse{Result: false}, nil
	}
	return &pbps.UpdateObjectResponse{Result: result}, nil
}

func (s *PeerStorageServiceServer) Delete(ctx context.Context, req *pbps.DeleteObjectRequest) (*pbps.DeleteObjectResponse, error) {
	result, err := s.handler.Delete(req.Id)
	if err != nil {
		return &pbps.DeleteObjectResponse{Result: false}, nil
	}
	return &pbps.DeleteObjectResponse{Result: result}, nil
}
