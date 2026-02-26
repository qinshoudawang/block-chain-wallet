package api

import (
	"wallet-system/internal/address"
	"wallet-system/internal/api/handler"
	"wallet-system/internal/withdraw"

	"github.com/gin-gonic/gin"
)

func NewRouter(wsvc *withdraw.Service, asvc *address.AddressService) *gin.Engine {
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
