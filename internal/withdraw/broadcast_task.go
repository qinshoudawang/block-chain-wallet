package withdraw

type BroadcastTask struct {
	Version     int    `json:"version"`
	WithdrawID  string `json:"withdraw_id"`
	RequestID   string `json:"request_id"`
	Chain       string `json:"chain"`
	From        string `json:"from"`
	To          string `json:"to"`
	Amount      string `json:"amount"`
	Nonce       uint64 `json:"nonce"`
	SignedTxHex string `json:"signed_tx_hex"`
	CreatedAt   int64  `json:"created_at"`
	Attempt     int    `json:"attempt"`
}
