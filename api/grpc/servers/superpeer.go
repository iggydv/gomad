// Package servers provides gRPC server implementations.
package servers

import (
	"context"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/iggydv12/nomad-go/internal/ledger"
	pb "github.com/iggydv12/nomad-go/gen/proto/superpeerservice"
)

// SuperPeerServiceServer implements the SuperPeerService gRPC server.
// Maps to SuperPeerService.java + SuperPeerServer.java in the Java implementation.
type SuperPeerServiceServer struct {
	pb.UnimplementedSuperPeerServiceServer
	logger     *zap.Logger
	groupLedger *ledger.GroupLedger
	handler    SuperPeerHandler
}

// SuperPeerHandler is the interface the node's SuperPeer component must implement
// to process incoming gRPC requests.
type SuperPeerHandler interface {
	HandleJoin(peerServer, groupStorageServer string, pos Position) (bool, map[string][]ledger.MetaData, map[string][]ledger.MetaData, int32)
	HandleLeave(peerServer, groupStorageServer string) bool
	Repair(objectID string) bool
	AddObjectReference(objectID, peerID string, ttl int64) bool
	RemovePeerGroupLedger(peerID string) bool
	NotifyPeers(host string) bool
	PingPeer(hostname string) bool
	UpdatePositionReference(pos Position, peerID string) (bool, string, string)
	GetReplicationFactor() int32
}

// Position is a simple value type to avoid proto import cycles.
type Position struct{ X, Y, Z float64 }

// NewSuperPeerServiceServer creates the gRPC server wrapper.
func NewSuperPeerServiceServer(handler SuperPeerHandler, gl *ledger.GroupLedger, logger *zap.Logger) *SuperPeerServiceServer {
	return &SuperPeerServiceServer{
		handler:     handler,
		groupLedger: gl,
		logger:      logger,
	}
}

// Serve starts the gRPC listener on addr.
func (s *SuperPeerServiceServer) Serve(addr string) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*1024),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 300 * time.Second}),
	)
	pb.RegisterSuperPeerServiceServer(srv, s)
	go func() {
		if err := srv.Serve(lis); err != nil {
			s.logger.Error("SuperPeerService gRPC server stopped", zap.Error(err))
		}
	}()
	s.logger.Info("SuperPeerService gRPC listening", zap.String("addr", addr))
	return srv, nil
}

// HandleJoin processes a peer join request.
func (s *SuperPeerServiceServer) HandleJoin(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponseWithLedger, error) {
	s.logger.Info("handleJoin", zap.String("peer", req.PeerServerHost))
	pos := Position{X: req.Position.X, Y: req.Position.Y, Z: req.Position.Z}
	accepted, objLedger, peerLedger, rf := s.handler.HandleJoin(req.PeerServerHost, req.GroupStorageServerHost, pos)

	objPairs := ledgerToProto(objLedger)
	peerPairs := ledgerToProto(peerLedger)

	return &pb.JoinResponseWithLedger{
		Accepted:          accepted,
		ObjectLedger:      &pb.MultiMapPair{KeyPair: objPairs},
		PeerLedger:        &pb.MultiMapPair{KeyPair: peerPairs},
		ReplicationFactor: rf,
	}, nil
}

// HandleLeave processes a peer leave request.
func (s *SuperPeerServiceServer) HandleLeave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	s.logger.Info("handleLeave", zap.String("peer", req.PeerServerHostname))
	result := s.handler.HandleLeave(req.PeerServerHostname, req.GroupStorageServerHostname)
	return &pb.LeaveResponse{Success: result}, nil
}

// Repair initiates repair for a single object.
func (s *SuperPeerServiceServer) Repair(ctx context.Context, req *pb.RepairObjectRequest) (*pb.RepairObjectResponse, error) {
	result := s.handler.Repair(req.ObjectId)
	return &pb.RepairObjectResponse{Succeed: result}, nil
}

// Migrate handles peer migration.
func (s *SuperPeerServiceServer) Migrate(ctx context.Context, req *pb.MigrationRequest) (*pb.MigrationResponse, error) {
	// Migration is accepted (peer has already relocated by the time this is called)
	return &pb.MigrationResponse{Succeed: true}, nil
}

// AddObjectReference registers an object→peer mapping in the group ledger.
func (s *SuperPeerServiceServer) AddObjectReference(ctx context.Context, req *pb.AddObjectRequest) (*pb.AddObjectResponse, error) {
	result := s.handler.AddObjectReference(req.ObjectId, req.PeerId, req.Ttl)
	return &pb.AddObjectResponse{Succeed: result}, nil
}

// RemovePeerGroupLedger removes a peer from the group ledger.
func (s *SuperPeerServiceServer) RemovePeerGroupLedger(ctx context.Context, req *pb.RemovePeerGroupLedgerRequest) (*pb.RemovePeerGroupLedgerResponse, error) {
	result := s.handler.RemovePeerGroupLedger(req.PeerId)
	return &pb.RemovePeerGroupLedgerResponse{Result: result}, nil
}

// NotifyPeers fans out an addPeer notification.
func (s *SuperPeerServiceServer) NotifyPeers(ctx context.Context, req *pb.NotifyPeersRequest) (*pb.NotifyPeersResponse, error) {
	result := s.handler.NotifyPeers(req.Host)
	return &pb.NotifyPeersResponse{Result: result}, nil
}

// PingPeer checks if a peer is reachable.
func (s *SuperPeerServiceServer) PingPeer(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	result := s.handler.PingPeer(req.Hostname)
	return &pb.PingResponse{Result: result}, nil
}

// UpdatePositionReference updates a peer's virtual position and returns migration info.
func (s *SuperPeerServiceServer) UpdatePositionReference(ctx context.Context, req *pb.PositionUpdateRequest) (*pb.PositionUpdateResponse, error) {
	pos := Position{X: req.Position.X, Y: req.Position.Y, Z: req.Position.Z}
	ack, newSP, newGroup := s.handler.UpdatePositionReference(pos, req.PeerId)
	return &pb.PositionUpdateResponse{
		Acknowledge:  ack,
		NewSuperPeer: newSP,
		NewGroupName: newGroup,
	}, nil
}

// ledgerToProto converts an internal ledger map to the proto MultiMapPair format.
func ledgerToProto(m map[string][]ledger.MetaData) map[string]*pb.MetaDataCollection {
	result := make(map[string]*pb.MetaDataCollection, len(m))
	for k, entries := range m {
		coll := &pb.MetaDataCollection{Values: make([]*pb.MetaData, len(entries))}
		for i, e := range entries {
			coll.Values[i] = &pb.MetaData{Id: e.ID, Ttl: e.TTL}
		}
		result[k] = coll
	}
	return result
}
