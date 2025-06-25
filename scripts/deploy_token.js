const { ethers } = require("hardhat");

async function main() {
  const initialSupply = ethers.parseUnits("1000000", 18); // 1,000,000 ADT
  const Token = await ethers.getContractFactory("AirdropToken");
  const token = await Token.deploy(initialSupply);
  
  // 等待交易被挖掘
  await token.waitForDeployment();
  
  console.log("AirdropToken deployed at:", await token.getAddress());
}

main().catch((e) => { console.error(e); process.exit(1); });
