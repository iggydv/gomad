package servers

import (
	"context"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/iggydv12/gomad/gen/proto/peerservice"
)

// PeerServiceServer implements the PeerService gRPC server.
// Maps to PeerService.java + PeerServer.java.
type PeerServiceServer struct {
	pb.UnimplementedPeerServiceServer
	logger  *zap.Logger
	handler PeerHandler
}

// PeerHandler is the interface the node's Peer component must implement.
type PeerHandler interface {
	AddGroupStoragePeer(host string) bool
	RemoveGroupStoragePeer(host string) bool
	HandleSuperPeerLeave() bool
	RepairObjects(objectIDs []string) bool
}

// NewPeerServiceServer creates a PeerServiceServer.
func NewPeerServiceServer(handler PeerHandler, logger *zap.Logger) *PeerServiceServer {
	return &PeerServiceServer{handler: handler, logger: logger}
}

// Serve starts the gRPC listener.
func (s *PeerServiceServer) Serve(addr string) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*1024),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 300 * time.Second}),
	)
	pb.RegisterPeerServiceServer(srv, s)
	go func() {
		if err := srv.Serve(lis); err != nil {
			s.logger.Error("PeerService gRPC server stopped", zap.Error(err))
		}
	}()
	s.logger.Info("PeerService gRPC listening", zap.String("addr", addr))
	return srv, nil
}

func (s *PeerServiceServer) AddPeer(ctx context.Context, req *pb.AddPeerRequest) (*pb.AddPeerResponse, error) {
	s.logger.Info("addPeer", zap.String("host", req.Host))
	result := s.handler.AddGroupStoragePeer(req.Host)
	return &pb.AddPeerResponse{Response: result}, nil
}

func (s *PeerServiceServer) RemovePeer(ctx context.Context, req *pb.RemovePeerRequest) (*pb.RemovePeerResponse, error) {
	s.logger.Info("removePeer", zap.String("host", req.Host))
	result := s.handler.RemoveGroupStoragePeer(req.Host)
	return &pb.RemovePeerResponse{Response: result}, nil
}

func (s *PeerServiceServer) HandleSuperPeerLeave(ctx context.Context, _ *emptypb.Empty) (*pb.SuperPeerLeaveResponse, error) {
	s.logger.Info("handleSuperPeerLeave")
	result := s.handler.HandleSuperPeerLeave()
	return &pb.SuperPeerLeaveResponse{Response: result}, nil
}

func (s *PeerServiceServer) RepairObjects(ctx context.Context, req *pb.RepairObjectRequest) (*pb.RepairObjectResponse, error) {
	s.logger.Info("repairObjects", zap.Strings("ids", req.ObjectIds))
	result := s.handler.RepairObjects(req.ObjectIds)
	return &pb.RepairObjectResponse{Response: result}, nil
}
