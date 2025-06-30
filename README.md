# 空投合约项目

这个项目包含一个用于空投代币的智能合约系统，包括ERC20代币合约和使用Merkle树进行空投验证的分发合约。

## 项目结构

- `contracts/`：智能合约代码
  - `AirdropToken.sol`：ERC20代币合约（支持铸造、暂停等功能）
  - `MerkleDistributor.sol`：使用Merkle树的代币分发合约（支持批次验证）
- `scripts/`：部署和功能脚本
  - `deploy_token.js`：部署代币合约的脚本
  - `deploy.js`：部署空投分发合约的脚本
  - `generate_merkle_tree.js`：生成Merkle树脚本
  - `generate_merkle_proof.js`：获取地址证明脚本
- `csv/`：存放空投用户数据的CSV文件
- `merkle-data/`：生成的Merkle树数据存储目录

## 基础使用指南

### 安装依赖

```shell
pnpm install
```

### 环境变量配置

将`.env.example`文件复制为`.env`，并填写以下配置：

```
# 部署账户的私钥（不要暴露真实私钥）
PRIVATE_KEY=0xabc123...

# Alchemy API密钥
ALCHEMY_KEY=your_alchemy_api_key

# 已部署的空投代币合约地址
AIRDROP_TOKEN=0x1234567890123456789012345678901234567890

# Merkle树根哈希（从生成的merkle_data.json获取）
MERKLE_ROOT=0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
```

> 注意：`AIRDROP_TOKEN`在部署代币合约后获取，`MERKLE_ROOT`在生成Merkle树后从`merkle-data/merkle_data.json`文件中获取。

### 编译合约

```shell
npx hardhat compile
```

### 运行测试

```shell
npx hardhat test
```

### 部署代币合约

```shell
npx hardhat run scripts/deploy_token.js --network <network_name>
```

部署后请记录输出的合约地址，并更新`.env`文件中的`AIRDROP_TOKEN`变量。

### 部署空投分发合约

确保已设置`.env`文件中的`AIRDROP_TOKEN`和`MERKLE_ROOT`后，运行：

```shell
npx hardhat run scripts/deploy.js --network <network_name>
```

## Merkle树生成和证明获取流程

### 1. 准备CSV文件

将空投用户数据CSV文件放置在项目根目录的`csv/`文件夹中。CSV文件格式要求：
- 第2列（索引1）必须是用户地址
- 第9列（索引8）必须是用户得分（用于计算空投数量）
- 第一行为表头，将被自动忽略
- 可以使用制表符或逗号作为分隔符

例如：
```
序号,地址,其他数据,...,得分
1,0x123...abc,其他数据,...,5.8
2,0x456...def,其他数据,...,3.2
```

### 2. 生成Merkle树

执行以下命令生成Merkle树：

```shell
node scripts/generate_merkle_tree.js [batch_size]
```

参数说明：
- `batch_size`：每批处理的记录数，默认为100（可选）

执行后，脚本会：
1. 读取`csv/`目录下的所有CSV文件
2. 处理数据并分批构建Merkle树
3. 生成以下文件到`merkle-data/`目录：
   - `merkle_data.json`：包含Merkle根和示例证明
   - `address_map.json`：地址到批次和索引的映射
   - `batches/`目录下的批次数据文件

生成完成后，将`merkle_data.json`中的`root`值复制到`.env`文件的`MERKLE_ROOT`变量。

### 3. 获取地址的Merkle证明

#### 单个地址证明查询

执行以下命令获取特定地址的Merkle证明：

```shell
node scripts/generate_merkle_proof.js address 0x123...abc
```

命令将输出该地址的完整证明数据，包括：
- 索引（index）
- 地址（address）
- 空投金额（amount，单位为wei）
- 人类可读金额（amountInEther，单位为ether）
- Merkle证明（proof，用于智能合约验证）
- 批次索引（batchIndex）

#### 导出所有地址证明

可以导出所有地址的证明到一个JSON文件：

```shell
node scripts/generate_merkle_proof.js export [output_path]
```

参数说明：
- `output_path`：输出文件路径，如不指定则默认为`merkle-data/all_proofs.json`

## 工作原理

1. 系统采用两层Merkle树结构：
   - 每个批次有自己的Merkle树和根
   - 所有批次根组成顶层Merkle树

2. 空投金额计算公式：
   - 金额 = 1.3^(score-1)，其中score为用户得分
   - 结果会转换为wei单位（18位小数）

3. `MerkleDistributor.sol`合约支持两种领取方式：
   - 标准索引领取（对应顶层Merkle树验证）
   - 批次内领取（对应批次Merkle树验证）
   - 多笔领取（针对同一地址有多个空投记录的情况）

## 支持的网络

项目当前配置支持以下网络：
- Arbitrum主网（arbitrum）
- Arbitrum测试网（arbitrumTestnet）
- Sepolia测试网（sepolia）

可通过在部署命令中指定`--network`参数来选择目标网络。

## 安全注意事项

在生产环境使用前，请确保：
1. 代码已经经过安全审计
2. Merkle树的生成和验证逻辑已经过充分测试
3. 所有管理员密钥都安全存储
4. 批量操作前先进行小规模测试
5. **绝不将包含真实私钥的`.env`文件提交到代码仓库**
