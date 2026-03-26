package clients

import (
	"fmt"

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
	cfg := loadOIDCClientCredentialsConfig("SIGNER")
	tokenSource, err := newBearerTokenSource(cfg)
	if err != nil {
		return nil, fmt.Errorf("init signer oidc auth failed: %w", err)
	}

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if tokenSource != nil {
		opts = append(opts, grpc.WithUnaryInterceptor(tokenSource.unaryClientInterceptor()))
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &SignerClient{
		Conn:   conn,
		Client: signpb.NewSignerServiceClient(conn),
	}, nil
}
