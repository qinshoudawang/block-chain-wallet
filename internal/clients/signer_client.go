package clients

import (
	signpb "wallet-system/proto/signer"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type SignerClient struct {
	Conn   *grpc.ClientConn
	Client signpb.SignerServiceClient
}

func (c *SignerClient) Close() error {
	if c == nil || c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

func NewSignerClient(addr string) (*SignerClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &SignerClient{
		Conn:   conn,
		Client: signpb.NewSignerServiceClient(conn),
	}, nil
}
