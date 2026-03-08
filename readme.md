# Wallet System

多链钱包后端项目，覆盖地址生成、充值监听、账本记账、提现编排、签名、广播、归集与对账，支持 EVM / BTC / Solana 三类链路。

## Core Modules

- Address: HD 地址分配与地址索引管理。
- Deposit: 多链充值监听、确认推进、reorg 回滚。
- Ledger: 账户余额、冻结、流水、幂等记账。
- Withdraw: 提现编排，包含风控、冻结、构造交易、重试与状态流转。
- Signer: 独立签名服务，支持本地私钥与 AWS KMS。
- Broadcaster: 异步广播、失败重试、消息重放。
- Sweep: 用户地址余额归集与热钱包 top-up。
- Reconciler: 链上余额对账、账本增量对账、业务流对账。

## Tech Stack

- Language: Go
- Storage: PostgreSQL, Redis
- Messaging: Kafka
- RPC / Chain: Ethereum JSON-RPC, Esplora API, Solana RPC
- Chain SDK: go-ethereum, btcd, solana-go
- Key Management: AWS KMS

## Highlights

- 多链统一抽象：EVM、BTC、Solana 共用地址、账本、提现、对账主流程。
- 安全隔离：Signer 独立部署，支持请求 HMAC 验真、幂等与 KMS 托管签名。
- 一致性处理：支持确认数、链回滚检测、充值/提现状态恢复、账本补偿。
- 工程化能力：异步广播、重试重放、归集、热钱包管理、对账告警。 
