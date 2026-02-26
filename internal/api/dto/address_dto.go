package dto

type CreateAddressRequest struct {
	UserID string `json:"user_id" binding:"required"`
	Chain  string `json:"chain" binding:"required"`
}

type CreateAddressResponse struct {
	UserID         string `json:"user_id"`
	Chain          string `json:"chain"`
	Address        string `json:"address"`
	AddressIndex   uint32 `json:"address_index"`
	DerivationPath string `json:"derivation_path"`
}
