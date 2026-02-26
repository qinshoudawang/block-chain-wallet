package handler

import (
	"log"
	"net/http"

	"wallet-system/internal/address"
	"wallet-system/internal/api/dto"

	"github.com/gin-gonic/gin"
)

type AddressHandler struct {
	svc *address.AddressService
}

func NewAddressHandler(svc *address.AddressService) *AddressHandler {
	return &AddressHandler{svc: svc}
}

func (h *AddressHandler) CreateAddress(c *gin.Context) {
	var req dto.CreateAddressRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}

	addr, err := h.svc.CreateUserAddress(c.Request.Context(), req.UserID, req.Chain)
	if err != nil {
		log.Printf("[address-handler] create address failed user_id=%s chain=%s err=%v", req.UserID, req.Chain, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, dto.CreateAddressResponse{
		UserID:         addr.UserID,
		Chain:          addr.Chain,
		Address:        addr.Address,
		AddressIndex:   addr.AddressIndex,
		DerivationPath: addr.DerivationPath,
	})
}
