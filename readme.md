# TODO
1. 地址生成（HD Wallet）
2. 充值监听（扫描区块）
3. 账本系统（余额/冻结）
4. 提现系统（风控）
5. 签名系统（我们现在做的）
6. 交易广播
7. 多链支持 solana

# 调用链
withdraw-api
  1) 冻结账本
  2) NonceManager.allocate(chain, from)
  3) TxBuilder.buildUnsignedTx(nonce,...)
  4) msg = CanonicalMessage(withdraw_id, chain, from, to, amount, nonce, hash(unsignedTx), request_id)
  5) auth_token = KMS.GenerateMac(key_id, msg)
  6) 调 signer.Sign(unsignedTx, msg, auth_token, ...)
  7) sender.Broadcast(signedTx)
  8) 更新提现状态

signer（窄职责）
  1) Redis 幂等 request_id
  2) policy 校验（白名单/限额）
  3) KMS.VerifyMac(key_id, msg, auth_token)  ✅
  4) provider.Sign(unsignedTx)（本地私钥/未来换 HSM/MPC）
  5) 审计日志
  6) 返回 signedTx