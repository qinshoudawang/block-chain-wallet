package handler

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"wallet-system/internal/api/dto"
	"wallet-system/internal/broadcaster"
	"wallet-system/internal/withdraw"

	"github.com/gin-gonic/gin"
)

type WithdrawService interface {
	MatchRequestChain(chain string) (string, error)
	CreateAndSignWithdraw(ctx context.Context, in withdraw.WithdrawInput) (*broadcaster.BroadcastTask, error)
	EnqueueBroadcastTask(ctx context.Context, canonicalChain string, task *broadcaster.BroadcastTask) error
}

type WithdrawHandler struct {
	svc WithdrawService
}

func NewWithdrawHandler(svc WithdrawService) *WithdrawHandler {
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
	log.Printf("[withdraw-handler] request parsed chain=%s to=%s amount=%s token=%s", req.Chain, req.To, req.Amount, req.Token)

	canonicalChain, err := h.svc.MatchRequestChain(req.Chain)
	if err != nil {
		log.Printf("[withdraw-handler] invalid chain_id req_chain=%s err=%v", req.Chain, err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid chain_id"})
		return
	}

	// 获取签名
	resp, err := h.svc.CreateAndSignWithdraw(c.Request.Context(), withdraw.WithdrawInput{
		Chain:  canonicalChain,
		To:     req.To,
		Amount: req.Amount,
		Token:  req.Token,
	})
	if err != nil {
		log.Printf("[withdraw-handler] create/sign failed chain=%s to=%s amount=%s err=%v", req.Chain, req.To, req.Amount, err)
		// 简化：生产会分错误码（policy拒绝/忙/系统错）
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	log.Printf("[withdraw-handler] create/sign success withdraw_id=%s request_id=%s sequence=%s", resp.WithdrawID, resp.RequestID, sequenceForLog(resp.Sequence))

	// 异步广播
	err = h.svc.EnqueueBroadcastTask(c.Request.Context(), canonicalChain, resp)
	if err != nil {
		key := canonicalChain + ":" + resp.From
		log.Printf("[withdraw-handler] publish failed withdraw_id=%s request_id=%s key=%s err=%v", resp.WithdrawID, resp.RequestID, key, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	log.Printf("[withdraw-handler] publish success withdraw_id=%s request_id=%s key=%s", resp.WithdrawID, resp.RequestID, canonicalChain+":"+resp.From)

	c.JSON(http.StatusOK, dto.WithdrawResponse{
		WithdrawID: resp.WithdrawID,
		RequestID:  resp.RequestID,
		Sequence:   resp.Sequence,
		Status:     dto.WithdrawStatusSignedEnqueued,
	})
}

func sequenceForLog(v uint64) string {
	return fmt.Sprintf("%d", v)
}
