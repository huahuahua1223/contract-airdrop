const { ethers } = require("hardhat");
require("dotenv").config();

async function main() {
  const tokenAddr = process.env.AIRDROP_TOKEN;
  const merkleRoot = process.env.MERKLE_ROOT;
  const [deployer] = await ethers.getSigners();

  console.log("部署者地址:", deployer.address);

  // 部署 MerkleDistributor 合约
  const Dist = await ethers.getContractFactory("MerkleDistributor");
  const dist = await Dist.deploy(tokenAddr, merkleRoot);
  await dist.waitForDeployment();
  console.log("Distributor 已部署:", await dist.getAddress());

  // 获取 ERC20 合约实例
  const IERC20 = await ethers.getContractFactory("@openzeppelin/contracts/token/ERC20/IERC20.sol:IERC20");
  const token = await IERC20.attach(tokenAddr);

  // 计算需转账总量（按 proof JSON 中的总量）
  const total = await token.balanceOf(deployer.address);
  console.log("部署者 token 余额:", total.toString());

  // 你可替换为你预先计算的总分配量, 如：
  // const amount = ethers.parseUnits("1000000", 18);
  // 也可转全部余额, 但建议逐步转
  const amount = total;

  // 批准 Distributor 合约转账权限
  const distributorAddress = await dist.getAddress();
  const allowanceTx = await token.approve(distributorAddress, amount);
  await allowanceTx.wait();
  console.log(`Approved ${amount} tokens to distributor`);

  // 将 token 转入 Distributor
  const fundTx = await token.transfer(distributorAddress, amount);
  await fundTx.wait();
  console.log(`Transferred ${amount} tokens to distributor`);

  console.log("🎉 Distributor 已完成资助，可供用户 claim");
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
