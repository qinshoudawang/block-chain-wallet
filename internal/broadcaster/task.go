package broadcaster

const (
	SignedPayloadEncodingHex    = "hex"
	SignedPayloadEncodingBase64 = "base64"

	TaskTypeWithdraw = "WITHDRAW"
	TaskTypeSweep    = "SWEEP"
	TaskTypeTopUp    = "TOPUP"
)

type BroadcastTask struct {
	TaskType              string `json:"task_type,omitempty"`
	Version               int    `json:"version"`
	WithdrawID            string `json:"withdraw_id"`
	SweepID               string `json:"sweep_id,omitempty"`
	RequestID             string `json:"request_id"`
	Chain                 string `json:"chain"`
	From                  string `json:"from"`
	To                    string `json:"to"`
	Amount                string `json:"amount"`
	Sequence              uint64 `json:"sequence"`
	SignedPayload         string `json:"signed_payload"`
	SignedPayloadEncoding string `json:"signed_payload_encoding"`
	TokenContractAddress  string `json:"token_contract_address,omitempty"`
	CreatedAt             int64  `json:"created_at"`
	Attempt               int    `json:"attempt"`
}
