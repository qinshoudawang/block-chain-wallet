package api

import (
	"wallet-system/internal/api/handler"
	"wallet-system/internal/withdraw"

	"github.com/gin-gonic/gin"
)

func NewRouter(wsvc *withdraw.Service) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())

	h := handler.NewWithdrawHandler(wsvc)

	v1 := r.Group("/v1")
	{
		v1.POST("/withdraw", h.Withdraw)
	}
	return r
}
