package dto

type WithdrawStatus string

const (
	WithdrawStatusSignedEnqueued WithdrawStatus = "SIGNED_ENQUEUED"
)

type WithdrawRequest struct {
	Chain  string `json:"chain" binding:"required"`
	To     string `json:"to" binding:"required"`
	Amount string `json:"amount" binding:"required"` // wei decimal
}

type WithdrawResponse struct {
	WithdrawID string         `json:"withdraw_id"`
	RequestID  string         `json:"request_id"`
	Nonce      uint64         `json:"nonce"`
	Status     WithdrawStatus `json:"status"`
}
