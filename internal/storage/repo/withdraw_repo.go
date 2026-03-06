package repo

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"time"

	withdrawmodel "wallet-system/internal/storage/model/withdraw"

	"gorm.io/gorm"
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
	res := r.db.WithContext(ctx).Model(&withdrawmodel.WithdrawOrder{}).
		Where("withdraw_id = ? AND status IN ?", withdrawID, []withdrawmodel.WithdrawStatus{
			withdrawmodel.StatusSIGNED, withdrawmodel.StatusBROADCASTING, withdrawmodel.StatusBROADCASTED,
		}).
		Updates(map[string]any{
			"status":        withdrawmodel.StatusBROADCASTED,
			"tx_hash":       txHash,
			"last_error":    "",
			"next_retry_at": nil,
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
		if lr != nil {
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
