package clients

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/iggydv12/nomad-go/gen/proto/peerservice"
)

// PeerClient is a gRPC client for the PeerService.
// Maps to PeerClient.java in the Java implementation.
type PeerClient struct {
	conn   *grpc.ClientConn
	client pb.PeerServiceClient
	logger *zap.Logger
	target string
}

// NewPeerClient dials the target peer and returns a client.
func NewPeerClient(target string, logger *zap.Logger) (*PeerClient, error) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 300 * time.Second}),
	)
	if err != nil {
		return nil, err
	}
	return &PeerClient{
		conn:   conn,
		client: pb.NewPeerServiceClient(conn),
		logger: logger,
		target: target,
	}, nil
}

func (c *PeerClient) Close() error { return c.conn.Close() }

func (c *PeerClient) Target() string { return c.target }

// AddPeer notifies a peer to add a new group storage peer.
func (c *PeerClient) AddPeer(host string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	resp, err := c.client.AddPeer(ctx, &pb.AddPeerRequest{Host: host})
	if err != nil {
		return false, err
	}
	return resp.Response, nil
}

// RemovePeer notifies a peer to remove a group storage peer.
func (c *PeerClient) RemovePeer(host string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	resp, err := c.client.RemovePeer(ctx, &pb.RemovePeerRequest{Host: host})
	if err != nil {
		return false, err
	}
	return resp.Response, nil
}

// HandleSuperPeerLeave notifies the peer that the super-peer has left.
func (c *PeerClient) HandleSuperPeerLeave() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.client.HandleSuperPeerLeave(ctx, &emptypb.Empty{})
	if err != nil {
		return false, err
	}
	return resp.Response, nil
}

// RepairObjects instructs the peer to re-replicate listed objects.
func (c *PeerClient) RepairObjects(objectIDs []string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := c.client.RepairObjects(ctx, &pb.RepairObjectRequest{ObjectIds: objectIDs})
	if err != nil {
		return false, err
	}
	return resp.Response, nil
}
