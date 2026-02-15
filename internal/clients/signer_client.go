package clients

import (
	signpb "wallet-system/proto/signer"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewSignerClient(addr string) (signpb.SignerServiceClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return signpb.NewSignerServiceClient(conn), nil
}
