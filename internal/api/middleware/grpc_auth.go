package middleware

import (
	"context"
	"log"

	"github.com/golang-jwt/jwt/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func (m *JWTAuthMiddleware) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if m == nil || !m.cfg.Enabled {
			return handler(ctx, req)
		}

		tokenString := bearerTokenFromMetadata(ctx)
		if tokenString == "" {
			return nil, status.Error(codes.Unauthenticated, "missing bearer token")
		}

		claims, err := m.ValidateToken(tokenString)
		if err != nil {
			log.Printf("[grpc-auth] jwt validation failed method=%s err=%v", info.FullMethod, err)
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}

		ctx = context.WithValue(ctx, authClaimsContextKey{}, claims)
		return handler(ctx, req)
	}
}

func bearerTokenFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return ""
	}
	return bearerToken(values[0])
}

type authClaimsContextKey struct{}

func AuthClaimsFromContext(ctx context.Context) (jwt.MapClaims, bool) {
	claims, ok := ctx.Value(authClaimsContextKey{}).(jwt.MapClaims)
	return claims, ok
}
