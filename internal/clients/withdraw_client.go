package clients

import (
	withdrawpb "wallet-system/proto/withdraw"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WithdrawClient struct {
	Conn   *grpc.ClientConn
	Client withdrawpb.WithdrawServiceClient
}

func (c *WithdrawClient) Close() error {
	if c == nil || c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

func NewWithdrawClient(addr string) (*WithdrawClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &WithdrawClient{
		Conn:   conn,
		Client: withdrawpb.NewWithdrawServiceClient(conn),
	}, nil
}
