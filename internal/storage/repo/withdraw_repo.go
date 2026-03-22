package repo

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"strings"
	"time"

	addressmodel "wallet-system/internal/storage/model/address"
	withdrawmodel "wallet-system/internal/storage/model/withdraw"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type WithdrawRepo struct {
	db *gorm.DB
}

func NewWithdrawRepo(db *gorm.DB) *WithdrawRepo {
	return &WithdrawRepo{db: db}
}

func (r *WithdrawRepo) GetByWithdrawID(ctx context.Context, withdrawID string) (*withdrawmodel.WithdrawOrder, error) {
	var out withdrawmodel.WithdrawOrder
	if err := r.db.WithContext(ctx).
		Where("withdraw_id = ?", withdrawID).
		First(&out).Error; err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *WithdrawRepo) GetBroadcastedByTxHash(ctx context.Context, chain string, txHash string) (*withdrawmodel.WithdrawOrder, bool, error) {
	if r == nil || r.db == nil {
		return nil, false, errors.New("withdraw repo not configured")
	}
	var out withdrawmodel.WithdrawOrder
	err := r.db.WithContext(ctx).
		Where("chain = ? AND tx_hash = ? AND status = ?",
			strings.ToLower(strings.TrimSpace(chain)),
			strings.TrimSpace(txHash),
			withdrawmodel.StatusBROADCASTED,
		).
		First(&out).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &out, true, nil
}

func (r *WithdrawRepo) GetTrackedByTxHash(ctx context.Context, chain string, txHash string) (*withdrawmodel.WithdrawOrder, bool, error) {
	if r == nil || r.db == nil {
		return nil, false, errors.New("withdraw repo not configured")
	}
	var out withdrawmodel.WithdrawOrder
	err := r.db.WithContext(ctx).
		Where("chain = ? AND tx_hash = ? AND status IN ?",
			strings.ToLower(strings.TrimSpace(chain)),
			strings.TrimSpace(txHash),
			[]withdrawmodel.WithdrawStatus{withdrawmodel.StatusBROADCASTED, withdrawmodel.StatusCONFIRMED, withdrawmodel.StatusFAILED},
		).
		Order("id DESC").
		First(&out).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &out, true, nil
}

func (r *WithdrawRepo) ExistsTrackedTxHash(ctx context.Context, chain string, txHash string) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("withdraw repo not configured")
	}
	var cnt int64
	err := r.db.WithContext(ctx).
		Model(&withdrawmodel.WithdrawOrder{}).
		Where("chain = ? AND tx_hash = ? AND status IN ?",
			strings.ToLower(strings.TrimSpace(chain)),
			strings.TrimSpace(txHash),
			[]withdrawmodel.WithdrawStatus{withdrawmodel.StatusBROADCASTED, withdrawmodel.StatusCONFIRMED, withdrawmodel.StatusFAILED},
		).
		Count(&cnt).Error
	return cnt > 0, err
}

func (r *WithdrawRepo) ListTrackedTxHashes(ctx context.Context, chain string, limit int) ([]string, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("withdraw repo not configured")
	}
	if limit <= 0 {
		limit = 500
	}
	var out []string
	err := r.db.WithContext(ctx).
		Model(&withdrawmodel.WithdrawOrder{}).
		Distinct("tx_hash").
		Where("chain = ? AND tx_hash <> '' AND status IN ?",
			strings.ToLower(strings.TrimSpace(chain)),
			[]withdrawmodel.WithdrawStatus{withdrawmodel.StatusBROADCASTED, withdrawmodel.StatusCONFIRMED, withdrawmodel.StatusFAILED},
		).
		Limit(limit).
		Pluck("tx_hash", &out).Error
	return out, err
}

func (r *WithdrawRepo) FailWithSettlement(
	ctx context.Context,
	lr *LedgerRepo,
	withdrawID string,
	transferAssetContractAddress string,
	transferSpentAmount *big.Int,
	networkFeeAssetContractAddress string,
	networkFeeAmount *big.Int,
) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("withdraw repo not configured")
	}
	var updated bool
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rec withdrawmodel.WithdrawOrder
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("withdraw_id = ?", strings.TrimSpace(withdrawID)).
			First(&rec).Error; err != nil {
			return err
		}
		if rec.Status == withdrawmodel.StatusFAILED {
			updated = false
			return nil
		}
		if rec.Status != withdrawmodel.StatusBROADCASTED {
			updated = false
			return nil
		}

		if lr != nil && !isTopUpRequestID(rec.RequestID) {
			if transferAssetContractAddress == networkFeeAssetContractAddress {
				totalSpent := new(big.Int)
				if transferSpentAmount != nil {
					totalSpent.Add(totalSpent, transferSpentAmount)
				}
				if networkFeeAmount != nil {
					totalSpent.Add(totalSpent, networkFeeAmount)
				}
				if err := lr.settleWithdrawFreezeWithDB(tx, withdrawID, transferAssetContractAddress, totalSpent); err != nil {
					return err
				}
			} else {
				if err := lr.settleWithdrawFreezeWithDB(tx, withdrawID, transferAssetContractAddress, transferSpentAmount); err != nil {
					return err
				}
				if err := lr.settleWithdrawFreezeWithDB(tx, withdrawID, networkFeeAssetContractAddress, networkFeeAmount); err != nil {
					return err
				}
			}
		}

		if _, err := r.saveSettlementWithDB(tx, withdrawID, networkFeeAssetContractAddress, networkFeeAmount, transferSpentAmount); err != nil {
			return err
		}
		res := tx.Model(&withdrawmodel.WithdrawOrder{}).
			Where("id = ? AND status = ?", rec.ID, withdrawmodel.StatusBROADCASTED).
			Updates(map[string]any{
				"status":     withdrawmodel.StatusFAILED,
				"last_error": "onchain execution failed",
				"updated_at": time.Now(),
			})
		if res.Error != nil {
			return res.Error
		}
		updated = res.RowsAffected > 0
		return nil
	})
	return updated, err
}

// InsertSigned: withdraw-api 在签名成功后落库 SIGNED，然后再投 Kafka
func (r *WithdrawRepo) InsertSigned(ctx context.Context, o *withdrawmodel.WithdrawOrder) error {
	o.Status = withdrawmodel.StatusSIGNED
	return r.db.WithContext(ctx).Create(o).Error
}

// NextSequenceFloor returns max(sequence)+1 for a (chain, from) pair from persisted withdraw orders.
// It is used to prevent sequence allocators from reusing a sequence after cache loss.
func (r *WithdrawRepo) NextSequenceFloor(ctx context.Context, chain, fromAddr string) (uint64, error) {
	if r == nil || r.db == nil {
		return 0, errors.New("withdraw repo not configured")
	}
	var maxSequence sql.NullInt64
	row := r.db.WithContext(ctx).
		Model(&withdrawmodel.WithdrawOrder{}).
		Where("chain = ? AND from_addr = ?", chain, fromAddr).
		Select("MAX(sequence)").
		Row()
	if err := row.Scan(&maxSequence); err != nil {
		return 0, err
	}
	if !maxSequence.Valid {
		return 0, nil
	}
	if maxSequence.Int64 < 0 {
		return 0, errors.New("invalid max sequence in db")
	}
	return uint64(maxSequence.Int64) + 1, nil
}

// MarkBroadcasted: broadcaster 广播成功后调用（幂等）
// 只允许 SIGNED/BROADCASTING -> BROADCASTED
func (r *WithdrawRepo) MarkBroadcasted(ctx context.Context, withdrawID, txHash string) (bool, error) {
	now := time.Now()
	res := r.db.WithContext(ctx).Model(&withdrawmodel.WithdrawOrder{}).
		Where("withdraw_id = ? AND status IN ?", withdrawID, []withdrawmodel.WithdrawStatus{
			withdrawmodel.StatusSIGNED, withdrawmodel.StatusBROADCASTING, withdrawmodel.StatusBROADCASTED,
		}).
		Updates(map[string]any{
			"status":         withdrawmodel.StatusBROADCASTED,
			"tx_hash":        txHash,
			"broadcasted_at": now,
			"last_error":     "",
			"next_retry_at":  nil,
		})
	return res.RowsAffected > 0, res.Error
}

// MarkRetry: broadcaster 广播失败（可重试）
// 只允许 SIGNED/BROADCASTING 状态更新，避免已终态被回滚
func (r *WithdrawRepo) MarkRetry(ctx context.Context, withdrawID string, next time.Time, lastErr string) (bool, error) {
	res := r.db.WithContext(ctx).Model(&withdrawmodel.WithdrawOrder{}).
		Where("withdraw_id = ? AND status IN ?", withdrawID, []withdrawmodel.WithdrawStatus{withdrawmodel.StatusSIGNED, withdrawmodel.StatusBROADCASTING}).
		Updates(map[string]any{
			"status":        withdrawmodel.StatusBROADCASTING,
			"retry_count":   gorm.Expr("retry_count + 1"),
			"next_retry_at": next,
			"last_error":    lastErr,
		})
	return res.RowsAffected > 0, res.Error
}

// MarkFailed: 达到最大重试或不可重试错误
func (r *WithdrawRepo) MarkFailed(ctx context.Context, withdrawID string, lastErr string) (bool, error) {
	res := r.db.WithContext(ctx).Model(&withdrawmodel.WithdrawOrder{}).
		Where("withdraw_id = ? AND status IN ?", withdrawID, []withdrawmodel.WithdrawStatus{
			withdrawmodel.StatusSIGNED, withdrawmodel.StatusBROADCASTING, withdrawmodel.StatusBROADCASTED,
		}).
		Updates(map[string]any{
			"status":     withdrawmodel.StatusFAILED,
			"last_error": lastErr,
		})
	return res.RowsAffected > 0, res.Error
}

// GetForReplay: replayer 扫描到点要重投的订单
func (r *WithdrawRepo) ListDueRetries(ctx context.Context, limit int) ([]withdrawmodel.WithdrawOrder, error) {
	var out []withdrawmodel.WithdrawOrder
	err := r.db.WithContext(ctx).
		Where("status = ? AND next_retry_at IS NOT NULL AND next_retry_at <= ?", withdrawmodel.StatusBROADCASTING, time.Now()).
		Order("next_retry_at ASC").
		Limit(limit).
		Find(&out).Error
	return out, err
}

// MarkReplayScheduled: 避免同一条 due 被多个 replayer 重复投递
// 简化做法：把 next_retry_at 往后推一小段，作为“占位”
func (r *WithdrawRepo) MarkReplayScheduled(ctx context.Context, withdrawID string, bump time.Duration) (bool, error) {
	next := time.Now().Add(bump)
	res := r.db.WithContext(ctx).Model(&withdrawmodel.WithdrawOrder{}).
		Where("withdraw_id = ? AND status = ? AND next_retry_at IS NOT NULL AND next_retry_at <= ?",
			withdrawID, withdrawmodel.StatusBROADCASTING, time.Now()).
		Update("next_retry_at", next)
	return res.RowsAffected > 0, res.Error
}

// Confirm 扫描：取 BROADCASTED 的订单
func (r *WithdrawRepo) ListBroadcastedToConfirm(ctx context.Context, limit int) ([]withdrawmodel.WithdrawOrder, error) {
	var out []withdrawmodel.WithdrawOrder
	err := r.db.WithContext(ctx).
		Where("status = ? AND tx_hash <> ''", withdrawmodel.StatusBROADCASTED).
		Order("updated_at ASC").
		Limit(limit).
		Find(&out).Error
	return out, err
}

func (r *WithdrawRepo) ListFinalizedAfterID(ctx context.Context, chain string, lastID uint64, limit int) ([]withdrawmodel.WithdrawOrder, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("withdraw repo not configured")
	}
	if limit <= 0 {
		limit = 500
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	var out []withdrawmodel.WithdrawOrder
	err := r.db.WithContext(ctx).
		Where("chain = ? AND id > ? AND status IN ?", chain, lastID, []withdrawmodel.WithdrawStatus{
			withdrawmodel.StatusCONFIRMED,
			withdrawmodel.StatusFAILED,
		}).
		Order("id ASC").
		Limit(limit).
		Find(&out).Error
	return out, err
}

func (r *WithdrawRepo) ListConfirmedBetween(ctx context.Context, chain string, from time.Time, to time.Time) ([]withdrawmodel.WithdrawOrder, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("withdraw repo not configured")
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	var out []withdrawmodel.WithdrawOrder
	err := r.db.WithContext(ctx).
		Where("chain = ? AND status = ? AND confirmed_at IS NOT NULL AND confirmed_at > ? AND confirmed_at <= ?",
			chain, withdrawmodel.StatusCONFIRMED, from, to).
		Order("confirmed_at ASC, id ASC").
		Find(&out).Error
	return out, err
}

func (r *WithdrawRepo) ListReorgAffectedAfterID(ctx context.Context, chain string, fromBlock uint64, lastID uint64, limit int) ([]withdrawmodel.WithdrawOrder, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("withdraw repo not configured")
	}
	if limit <= 0 {
		limit = 200
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	var out []withdrawmodel.WithdrawOrder
	err := r.db.WithContext(ctx).
		Where("chain = ? AND id > ? AND tx_hash <> '' AND block_number IS NOT NULL AND block_number >= ? AND status IN ?", chain, lastID, fromBlock, []withdrawmodel.WithdrawStatus{
			withdrawmodel.StatusCONFIRMED,
			withdrawmodel.StatusFAILED,
		}).
		Order("id ASC").
		Limit(limit).
		Find(&out).Error
	return out, err
}

// UpdateConfirmations: 更新确认数，达到阈值则置 CONFIRMED
func (r *WithdrawRepo) UpdateConfirmations(ctx context.Context, withdrawID string, blockNum uint64, conf int, threshold int) (bool, error) {
	return r.updateConfirmationsWithDB(r.db.WithContext(ctx), withdrawID, blockNum, conf, threshold)
}

func (r *WithdrawRepo) updateConfirmationsWithDB(db *gorm.DB, withdrawID string, blockNum uint64, conf int, threshold int) (bool, error) {
	updates := map[string]any{
		"block_number":  blockNum,
		"confirmations": conf,
		"last_error":    "",
	}
	// 达标则确认
	if conf >= threshold {
		now := time.Now()
		updates["status"] = withdrawmodel.StatusCONFIRMED
		updates["confirmed_at"] = &now
	}
	res := db.Model(&withdrawmodel.WithdrawOrder{}).
		Where("withdraw_id = ? AND status = ?", withdrawID, withdrawmodel.StatusBROADCASTED).
		Updates(updates)
	return res.RowsAffected > 0, res.Error
}

func (r *WithdrawRepo) SaveSettlement(
	ctx context.Context,
	withdrawID string,
	networkFeeAssetContractAddress string,
	networkFeeAmount *big.Int,
	actualSpentAmount *big.Int,
) (bool, error) {
	return r.saveSettlementWithDB(
		r.db.WithContext(ctx),
		withdrawID,
		networkFeeAssetContractAddress,
		networkFeeAmount,
		actualSpentAmount,
	)
}

func (r *WithdrawRepo) saveSettlementWithDB(
	db *gorm.DB,
	withdrawID string,
	networkFeeAssetContractAddress string,
	networkFeeAmount *big.Int,
	actualSpentAmount *big.Int,
) (bool, error) {
	updates := map[string]any{}
	updates["network_fee_asset_contract_address"] = networkFeeAssetContractAddress
	if networkFeeAmount != nil {
		updates["network_fee_amount"] = networkFeeAmount.String()
	}
	if actualSpentAmount != nil {
		updates["actual_spent_amount"] = actualSpentAmount.String()
	}
	res := db.Model(&withdrawmodel.WithdrawOrder{}).
		Where("withdraw_id = ?", withdrawID).
		Updates(updates)
	return res.RowsAffected > 0, res.Error
}

func (r *WithdrawRepo) ConfirmWithSettlement(
	ctx context.Context,
	lr *LedgerRepo,
	withdrawID string,
	blockNum uint64,
	conf int,
	threshold int,
	transferAssetContractAddress string,
	transferSpentAmount *big.Int,
	networkFeeAssetContractAddress string,
	networkFeeAmount *big.Int,
) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("withdraw repo not configured")
	}
	var updated bool
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rec withdrawmodel.WithdrawOrder
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("withdraw_id = ?", strings.TrimSpace(withdrawID)).
			First(&rec).Error; err != nil {
			return err
		}
		if rec.Status == withdrawmodel.StatusCONFIRMED {
			updated = false
			return nil
		}
		if rec.Status != withdrawmodel.StatusBROADCASTED {
			updated = false
			return nil
		}

		if lr != nil {
			if isTopUpRequestID(rec.RequestID) {
				spent := big.NewInt(0)
				if transferSpentAmount != nil && transferSpentAmount.Sign() > 0 {
					spent.Set(transferSpentAmount)
				} else if v, ok := new(big.Int).SetString(strings.TrimSpace(rec.Amount), 10); ok && v.Sign() > 0 {
					spent.Set(v)
				}
				if spent.Sign() > 0 {
					toUserID, err := resolveUserIDByChainAddress(tx, rec.Chain, rec.ToAddr)
					if err != nil {
						return err
					}
					if err := lr.settleSystemTransferWithDB(tx, rec.Chain, rec.FromAddr, rec.ToAddr, "", spent, toUserID); err != nil {
						return err
					}
				}
			} else {
				if transferAssetContractAddress == networkFeeAssetContractAddress {
					totalSpent := new(big.Int)
					if transferSpentAmount != nil {
						totalSpent = totalSpent.Add(totalSpent, transferSpentAmount)
					}
					if networkFeeAmount != nil {
						totalSpent = totalSpent.Add(totalSpent, networkFeeAmount)
					}
					if totalSpent.Sign() > 0 {
						if err := lr.settleWithdrawFreezeWithDB(tx, withdrawID, transferAssetContractAddress, totalSpent); err != nil {
							return err
						}
					}
				} else {
					if err := lr.settleWithdrawFreezeWithDB(tx, withdrawID, transferAssetContractAddress, transferSpentAmount); err != nil {
						return err
					}
					if err := lr.settleWithdrawFreezeWithDB(tx, withdrawID, networkFeeAssetContractAddress, networkFeeAmount); err != nil {
						return err
					}
				}
			}
		}
		ok, err := r.saveSettlementWithDB(
			tx,
			withdrawID,
			networkFeeAssetContractAddress,
			networkFeeAmount,
			transferSpentAmount,
		)
		if err != nil {
			return err
		}
		if !ok {
			updated = false
			return nil
		}
		ok, err = r.updateConfirmationsWithDB(tx, withdrawID, blockNum, conf, threshold)
		if err != nil {
			return err
		}
		updated = ok
		return nil
	})
	return updated, err
}

func (r *WithdrawRepo) RevertConfirmationWithSettlement(
	ctx context.Context,
	lr *LedgerRepo,
	withdrawID string,
) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("withdraw repo not configured")
	}
	var updated bool
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rec withdrawmodel.WithdrawOrder
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("withdraw_id = ?", strings.TrimSpace(withdrawID)).
			First(&rec).Error; err != nil {
			return err
		}
		if rec.Status == withdrawmodel.StatusBROADCASTED {
			updated = false
			return nil
		}
		if rec.Status != withdrawmodel.StatusCONFIRMED && rec.Status != withdrawmodel.StatusFAILED {
			updated = false
			return nil
		}

		if lr != nil {
			if isTopUpRequestID(rec.RequestID) {
				spent := big.NewInt(0)
				if v, ok := new(big.Int).SetString(strings.TrimSpace(rec.ActualSpentAmount), 10); ok && v.Sign() > 0 {
					spent.Set(v)
				} else if v, ok := new(big.Int).SetString(strings.TrimSpace(rec.Amount), 10); ok && v.Sign() > 0 {
					spent.Set(v)
				}
				if spent.Sign() > 0 {
					if err := lr.revertSystemTransferWithDB(tx, rec.Chain, rec.FromAddr, rec.ToAddr, "", spent); err != nil {
						return err
					}
				}
			} else {
				transferSpent := big.NewInt(0)
				if v, ok := new(big.Int).SetString(strings.TrimSpace(rec.ActualSpentAmount), 10); ok && v.Sign() >= 0 {
					transferSpent = v
				}
				feeSpent := big.NewInt(0)
				if v, ok := new(big.Int).SetString(strings.TrimSpace(rec.NetworkFeeAmount), 10); ok && v.Sign() >= 0 {
					feeSpent = v
				}
				transferAsset := strings.TrimSpace(rec.TokenContractAddress)
				feeAsset := strings.TrimSpace(rec.NetworkFeeAssetContractAddress)
				if transferAsset == feeAsset {
					total := new(big.Int).Add(new(big.Int).Set(transferSpent), feeSpent)
					if err := lr.revertWithdrawFreezeSettlementWithDB(tx, rec.WithdrawID, transferAsset, total); err != nil {
						return err
					}
				} else {
					if err := lr.revertWithdrawFreezeSettlementWithDB(tx, rec.WithdrawID, transferAsset, transferSpent); err != nil {
						return err
					}
					if err := lr.revertWithdrawFreezeSettlementWithDB(tx, rec.WithdrawID, feeAsset, feeSpent); err != nil {
						return err
					}
				}
			}
		}

		res := tx.Model(&withdrawmodel.WithdrawOrder{}).
			Where("id = ? AND status IN ?", rec.ID, []withdrawmodel.WithdrawStatus{withdrawmodel.StatusCONFIRMED, withdrawmodel.StatusFAILED}).
			Updates(map[string]any{
				"status":                             withdrawmodel.StatusBROADCASTED,
				"block_number":                       nil,
				"confirmations":                      nil,
				"confirmed_at":                       nil,
				"network_fee_asset_contract_address": "",
				"network_fee_amount":                 "",
				"actual_spent_amount":                "",
				"last_error":                         "",
				"updated_at":                         time.Now(),
			})
		if res.Error != nil {
			return res.Error
		}
		updated = res.RowsAffected > 0
		return nil
	})
	return updated, err
}

func isTopUpRequestID(requestID string) bool {
	return strings.HasPrefix(strings.TrimSpace(requestID), "sweeper-topup:")
}

func resolveUserIDByChainAddress(tx *gorm.DB, chain string, address string) (string, error) {
	var ua addressmodel.UserAddress
	err := tx.Select("user_id").
		Where("chain = ? AND address = ?", strings.ToLower(strings.TrimSpace(chain)), strings.TrimSpace(address)).
		First(&ua).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(ua.UserID), nil
}

func (r *WithdrawRepo) ReplaceBroadcastedSignedTx(
	ctx context.Context,
	withdrawID string,
	prevTxHash string,
	newSignedPayload string,
	newTxHash string,
) (bool, error) {
	res := r.db.WithContext(ctx).Model(&withdrawmodel.WithdrawOrder{}).
		Where("withdraw_id = ? AND tx_hash = ? AND status = ?", withdrawID, prevTxHash, withdrawmodel.StatusBROADCASTED).
		Updates(map[string]any{
			"signed_payload":          newSignedPayload,
			"signed_payload_encoding": "hex",
			"tx_hash":                 newTxHash,
			"last_error":              "",
			"next_retry_at":           nil,
			"updated_at":              time.Now(),
		})
	return res.RowsAffected > 0, res.Error
}

func (r *WithdrawRepo) SaveRBFSignedPayload(
	ctx context.Context,
	withdrawID string,
	prevTxHash string,
	newSignedPayload string,
	encoding string,
) (bool, error) {
	res := r.db.WithContext(ctx).Model(&withdrawmodel.WithdrawOrder{}).
		Where("withdraw_id = ? AND tx_hash = ? AND status = ?", withdrawID, prevTxHash, withdrawmodel.StatusBROADCASTED).
		Updates(map[string]any{
			"signed_payload":          newSignedPayload,
			"signed_payload_encoding": encoding,
			"updated_at":              time.Now(),
		})
	return res.RowsAffected > 0, res.Error
}

func (r *WithdrawRepo) StartRBFAttempt(ctx context.Context, withdrawID string, prevTxHash string, minInterval time.Duration) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("withdraw repo not configured")
	}
	now := time.Now()
	cutoff := now.Add(-minInterval)
	res := r.db.WithContext(ctx).Model(&withdrawmodel.WithdrawOrder{}).
		Where("withdraw_id = ? AND tx_hash = ? AND status = ?", strings.TrimSpace(withdrawID), strings.TrimSpace(prevTxHash), withdrawmodel.StatusBROADCASTED).
		Where("(last_rbf_attempt_at IS NULL OR last_rbf_attempt_at <= ?)", cutoff).
		Updates(map[string]any{
			"rbf_count":           gorm.Expr("rbf_count + 1"),
			"last_rbf_attempt_at": now,
			"last_error":          "",
			"updated_at":          now,
		})
	return res.RowsAffected > 0, res.Error
}
