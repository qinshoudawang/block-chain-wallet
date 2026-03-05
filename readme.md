### Address Service（用户钱包入口）

HD 地址分配（记录 user_id -> derivation index -> address）

### Deposit Scanner（充值监听）

扫块/订阅 logs → 匹配用户地址 → 写 deposits → 调 Ledger 入账

### Ledger Service（账本）

余额、冻结、流水、幂等入账

### Withdraw Orchestrator（提现编排）

风控/审批、冻结余额、分配 nonce、构造 unsigned tx、调用 signer、落库、发 Kafka

### Signer Service（安全域：窄职责）

幂等、防重放、policy 校验、HMAC 授权验真、签名

可插拔后端：Local → KMS/HSM/MPC/TEE

### Tx Broadcaster / Replayer / Confirmer（异步执行器）

消费 tx.broadcast.v1、广播、多 RPC fallback、错误分类、写重试计划

### Sweep Service（归集）

把用户地址余额归集到热钱包（复用 TxBuilder + Signer + Kafka）

### Key Manager（KMS/HSM/MPC/TEE）

密钥材料/派生/签名在安全硬件或受控环境

## Withdraw API 示例

EVM 原生币转账（`token` 不传）：

```json
{
  "chain": "ethereum",
  "to": "0x1111111111111111111111111111111111111111",
  "amount": "10000000000000000"
}
```

EVM ERC20 转账（传 token 合约地址）：

```json
{
  "chain": "ethereum",
  "to": "0x1111111111111111111111111111111111111111",
  "amount": "1000000",
  "token": "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
}
```
