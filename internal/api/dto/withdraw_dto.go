package dto

type WithdrawStatus string

const (
	WithdrawStatusSignedEnqueued WithdrawStatus = "SIGNED_ENQUEUED"
)

type WithdrawRequest struct {
	Chain  string `json:"chain" binding:"required"`
	To     string `json:"to" binding:"required"`
	Amount string `json:"amount" binding:"required"` // atomic-unit decimal
	Token  string `json:"token"`                     // optional token contract address (EVM)
}

type WithdrawResponse struct {
	WithdrawID string         `json:"withdraw_id"`
	RequestID  string         `json:"request_id"`
	Sequence   uint64         `json:"sequence"`
	Status     WithdrawStatus `json:"status"`
}
