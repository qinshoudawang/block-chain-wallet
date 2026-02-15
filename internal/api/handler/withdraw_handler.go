package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"wallet-system/internal/api/dto"
	"wallet-system/internal/withdraw"

	"github.com/gin-gonic/gin"
)

type WithdrawHandler struct {
	svc *withdraw.Service
}

func NewWithdrawHandler(svc *withdraw.Service) *WithdrawHandler {
	return &WithdrawHandler{svc: svc}
}

func (h *WithdrawHandler) Withdraw(c *gin.Context) {
	log.Printf("[withdraw-handler] request received path=%s", c.FullPath())

	var req dto.WithdrawRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Printf("[withdraw-handler] bind json failed err=%v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	log.Printf("[withdraw-handler] request parsed chain_id=%s to=%s amount=%s", req.ChainId, req.To, req.Amount)

	// 获取签名
	resp, err := h.svc.CreateAndSignWithdraw(c.Request.Context(), withdraw.WithdrawInput{
		To:     req.To,
		Amount: req.Amount,
	})
	if err != nil {
		log.Printf("[withdraw-handler] create/sign failed chain_id=%s to=%s amount=%s err=%v", req.ChainId, req.To, req.Amount, err)
		// 简化：生产会分错误码（policy拒绝/忙/系统错）
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	log.Printf("[withdraw-handler] create/sign success withdraw_id=%s request_id=%s nonce=%d", resp.WithdrawID, resp.RequestID, resp.Nonce)

	// 异步广播
	key := req.ChainId + ":" + resp.From
	taskBytes, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[withdraw-handler] marshal task failed withdraw_id=%s request_id=%s err=%v", resp.WithdrawID, resp.RequestID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	err = h.svc.Producer.Publish(c.Request.Context(), key, taskBytes)
	if err != nil {
		log.Printf("[withdraw-handler] publish failed withdraw_id=%s request_id=%s key=%s err=%v", resp.WithdrawID, resp.RequestID, key, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	log.Printf("[withdraw-handler] publish success withdraw_id=%s request_id=%s key=%s", resp.WithdrawID, resp.RequestID, key)

	c.JSON(http.StatusOK, dto.WithdrawResponse{
		WithdrawID: resp.WithdrawID,
		RequestID:  resp.RequestID,
		Nonce:      resp.Nonce,
		Status:     dto.WithdrawStatusSignedEnqueued,
	})
}
