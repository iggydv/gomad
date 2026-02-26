package clients

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"

	pbgs "github.com/iggydv12/nomad-go/gen/proto/groupstorage"
	pbm "github.com/iggydv12/nomad-go/gen/proto/models"
)

// GroupStorageClient is a gRPC client for the GroupStorageService.
// Maps to GroupStorageClient.java in the Java implementation.
type GroupStorageClient struct {
	conn   *grpc.ClientConn
	client pbgs.GroupStorageServiceClient
	logger *zap.Logger
	target string
	active bool
}

// NewGroupStorageClient dials the target and returns a client.
func NewGroupStorageClient(target string, logger *zap.Logger) (*GroupStorageClient, error) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 300 * time.Second}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*1024)),
	)
	if err != nil {
		return nil, err
	}
	return &GroupStorageClient{
		conn:   conn,
		client: pbgs.NewGroupStorageServiceClient(conn),
		logger: logger,
		target: target,
		active: true,
	}, nil
}

func (c *GroupStorageClient) Close() error {
	c.active = false
	return c.conn.Close()
}

func (c *GroupStorageClient) IsActive() bool  { return c.active }
func (c *GroupStorageClient) Target() string  { return c.target }

// HealthCheck sends a single health check.
func (c *GroupStorageClient) HealthCheck() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	resp, err := c.client.HealthCheck(ctx, &emptypb.Empty{})
	if err != nil {
		return false, err
	}
	return resp.Status == pbgs.HealthCheckResponse_SERVING, nil
}

// Get retrieves an object.
func (c *GroupStorageClient) Get(id string) (*pbm.GameObjectGrpc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	resp, err := c.client.Get(ctx, &pbgs.GetObjectRequest{Id: id})
	if err != nil || !resp.Result {
		return nil, err
	}
	return resp.Object, nil
}

// Put stores an object on the remote peer.
func (c *GroupStorageClient) Put(obj *pbm.GameObjectGrpc) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	resp, err := c.client.Put(ctx, &pbgs.PutObjectRequest{Object: obj})
	if err != nil {
		return false, err
	}
	return resp.Result, nil
}

// Update overwrites an object on the remote peer.
func (c *GroupStorageClient) Update(obj *pbm.GameObjectGrpc) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	resp, err := c.client.Update(ctx, &pbgs.UpdateObjectRequest{Object: obj})
	if err != nil {
		return false, err
	}
	return resp.Result, nil
}

// Delete removes an object from the remote peer.
func (c *GroupStorageClient) Delete(id string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	resp, err := c.client.Delete(ctx, &pbgs.DeleteObjectRequest{Id: id})
	if err != nil {
		return false, err
	}
	return resp.Result, nil
}

// AddToGroupLedger registers object→peer mapping on the remote peer.
func (c *GroupStorageClient) AddToGroupLedger(objectID, peerID string, ttl int64) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	resp, err := c.client.AddToGroupLedger(ctx, &pbgs.AddToGroupLedgerRequest{
		ObjectId: objectID,
		PeerId:   peerID,
		Ttl:      ttl,
	})
	if err != nil {
		return false, err
	}
	return resp.Result, nil
}

// RemovePeerGroupLedger tells the remote peer to remove peerID from its ledger.
func (c *GroupStorageClient) RemovePeerGroupLedger(peerID string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	resp, err := c.client.RemovePeerGroupLedger(ctx, &pbgs.RemovePeerGroupLedgerRequest{PeerId: peerID})
	if err != nil {
		return false, err
	}
	return resp.Result, nil
}
