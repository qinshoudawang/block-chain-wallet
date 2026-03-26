package api

import "wallet-system/internal/api/middleware"

type RouterConfig struct {
	Auth *middleware.JWTAuthMiddleware
}
