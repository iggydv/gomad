// Package clients provides gRPC client wrappers.
package clients

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pb "github.com/iggydv12/gomad/gen/proto/superpeerservice"
	"github.com/iggydv12/gomad/internal/ledger"
)

const superPeerDeadline = 350 * time.Millisecond

// SuperPeerClient is a gRPC client for the SuperPeerService.
// Maps to SuperPeerClient.java in the Java implementation.
type SuperPeerClient struct {
	conn   *grpc.ClientConn
	client pb.SuperPeerServiceClient
	logger *zap.Logger
	target string
	active bool
}

// NewSuperPeerClient dials the super-peer and returns a client.
func NewSuperPeerClient(target string, logger *zap.Logger) (*SuperPeerClient, error) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 300 * time.Second}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*1024)),
	)
	if err != nil {
		return nil, err
	}
	return &SuperPeerClient{
		conn:   conn,
		client: pb.NewSuperPeerServiceClient(conn),
		logger: logger,
		target: target,
		active: true,
	}, nil
}

// Close shuts down the client connection.
func (c *SuperPeerClient) Close() error {
	c.active = false
	return c.conn.Close()
}

func (c *SuperPeerClient) IsActive() bool { return c.active }

// JoinGroup calls handleJoin on the super-peer.
// Returns accepted flag, object ledger, peer ledger, replication factor.
func (c *SuperPeerClient) JoinGroup(peerServer, groupStorageServer string, pos Position) (bool, map[string][]ledger.MetaData, map[string][]ledger.MetaData, int32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.HandleJoin(ctx, &pb.JoinRequest{
		PeerServerHost:        peerServer,
		GroupStorageServerHost: groupStorageServer,
		Position: &pb.VirtualPosition{X: pos.X, Y: pos.Y, Z: pos.Z},
	})
	if err != nil {
		return false, nil, nil, 0, err
	}

	objLedger := protoToLedger(resp.ObjectLedger)
	peerLedger := protoToLedger(resp.PeerLedger)
	return resp.Accepted, objLedger, peerLedger, resp.ReplicationFactor, nil
}

// LeaveGroup calls handleLeave on the super-peer.
func (c *SuperPeerClient) LeaveGroup(peerServer, groupStorageServer string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), superPeerDeadline*10)
	defer cancel()
	resp, err := c.client.HandleLeave(ctx, &pb.LeaveRequest{
		PeerServerHostname:        peerServer,
		GroupStorageServerHostname: groupStorageServer,
	})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

// NotifyPeers calls notifyPeers on the super-peer.
func (c *SuperPeerClient) NotifyPeers(host string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), superPeerDeadline*10)
	defer cancel()
	resp, err := c.client.NotifyPeers(ctx, &pb.NotifyPeersRequest{Host: host})
	if err != nil {
		return false, err
	}
	return resp.Result, nil
}

// PingPeer asks the super-peer to ping a given peer.
func (c *SuperPeerClient) PingPeer(hostname string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), superPeerDeadline)
	defer cancel()
	resp, err := c.client.PingPeer(ctx, &pb.PingRequest{Hostname: hostname})
	if err != nil {
		return false, err
	}
	return resp.Result, nil
}

// UpdatePosition sends a position update to the super-peer.
// Returns (ack, newSuperPeer, newGroup, error).
func (c *SuperPeerClient) UpdatePosition(peerID string, pos Position) (bool, string, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), superPeerDeadline*5)
	defer cancel()
	resp, err := c.client.UpdatePositionReference(ctx, &pb.PositionUpdateRequest{
		PeerId:   peerID,
		Position: &pb.VirtualPosition{X: pos.X, Y: pos.Y, Z: pos.Z},
	})
	if err != nil {
		return false, "", "", err
	}
	return resp.Acknowledge, resp.NewSuperPeer, resp.NewGroupName, nil
}

// AddObjectReference notifies the super-peer of a newly stored object.
func (c *SuperPeerClient) AddObjectReference(objectID, peerID string, ttl int64) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), superPeerDeadline)
	defer cancel()
	resp, err := c.client.AddObjectReference(ctx, &pb.AddObjectRequest{
		ObjectId: objectID,
		PeerId:   peerID,
		Ttl:      ttl,
	})
	if err != nil {
		return false, err
	}
	return resp.Succeed, nil
}

// Position is a value type for passing virtual positions without proto imports.
type Position struct{ X, Y, Z float64 }

// protoToLedger converts a proto MultiMapPair to an internal ledger map.
func protoToLedger(pair *pb.MultiMapPair) map[string][]ledger.MetaData {
	if pair == nil {
		return nil
	}
	result := make(map[string][]ledger.MetaData, len(pair.KeyPair))
	for k, coll := range pair.KeyPair {
		entries := make([]ledger.MetaData, len(coll.Values))
		for i, v := range coll.Values {
			entries[i] = ledger.MetaData{ID: v.Id, TTL: v.Ttl}
		}
		result[k] = entries
	}
	return result
}
