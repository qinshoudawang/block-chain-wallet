package api

import (
	"wallet-system/internal/api/handler"

	"github.com/gin-gonic/gin"
)

func NewRouter(wsvc handler.WithdrawService, asvc handler.AddressService) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())

	wh := handler.NewWithdrawHandler(wsvc)
	ah := handler.NewAddressHandler(asvc)

	v1 := r.Group("/v1")
	{
		v1.POST("/withdraw", wh.Withdraw)
		v1.POST("/address", ah.CreateAddress)
	}
	return r
}
