package clients

import (
	"fmt"

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
	cfg := loadOIDCClientCredentialsConfig("WITHDRAW")
	tokenSource, err := newBearerTokenSource(cfg)
	if err != nil {
		return nil, fmt.Errorf("init withdraw oidc auth failed: %w", err)
	}

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if tokenSource != nil {
		opts = append(opts, grpc.WithUnaryInterceptor(tokenSource.unaryClientInterceptor()))
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &WithdrawClient{
		Conn:   conn,
		Client: withdrawpb.NewWithdrawServiceClient(conn),
	}, nil
}
