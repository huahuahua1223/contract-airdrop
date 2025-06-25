# 空投合约项目

这个项目包含一个用于空投代币的智能合约系统，包括ERC20代币合约和使用Merkle树进行空投验证的分发合约。

## 项目结构

- `contracts/`：智能合约代码
  - `AirdropToken.sol`：ERC20代币合约
  - `MerkleDistributor.sol`：使用Merkle树的代币分发合约
- `scripts/`：部署和功能脚本
  - `deploy_token.js`：部署代币合约的脚本
  - `deploy.js`：部署空投分发合约的脚本
- `test/`：测试文件
  - `AirdropTest.js`：合约功能测试

## 如何使用

### 安装依赖

```shell
pnpm install
```

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

### 部署空投分发合约

在部署前请确保已设置.env文件，包含以下变量：
- AIRDROP_TOKEN：已部署代币合约的地址
- MERKLE_ROOT：Merkle树的根哈希值

```shell
npx hardhat run scripts/deploy.js --network <network_name>
```

## 高级功能

### 生成Merkle树

项目使用merkletreejs和keccak256来构建Merkle树。可以参考测试文件了解如何为空投生成Merkle树和证明。

### 安全注意事项

在生产环境使用前，请确保：
1. 代码已经经过安全审计
2. Merkle树的生成和验证逻辑已经过充分测试
3. 所有管理员密钥都安全存储
