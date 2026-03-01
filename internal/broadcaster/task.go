package broadcaster

const (
	SignedPayloadEncodingHex    = "hex"
	SignedPayloadEncodingBase64 = "base64"
)

type BroadcastTask struct {
	Version               int    `json:"version"`
	WithdrawID            string `json:"withdraw_id"`
	RequestID             string `json:"request_id"`
	Chain                 string `json:"chain"`
	From                  string `json:"from"`
	To                    string `json:"to"`
	Amount                string `json:"amount"`
	Sequence              uint64 `json:"sequence"`
	ReservationType       string `json:"reservation_type,omitempty"`
	SignedPayload         string `json:"signed_payload"`
	SignedPayloadEncoding string `json:"signed_payload_encoding"`
	ChainMetaJSON         string `json:"chain_meta_json,omitempty"`
	CreatedAt             int64  `json:"created_at"`
	Attempt               int    `json:"attempt"`
}
