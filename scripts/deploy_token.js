const { ethers } = require("hardhat");

async function main() {
  const tokenName = "空投代币";
  const tokenSymbol = "ADT";
  const initialSupply = ethers.parseUnits("1000000", 18); // 100万 ADT
  const tokenCap = ethers.parseUnits("10000000", 18);    // 1000万上限
  
  const Token = await ethers.getContractFactory("AirdropToken");
  const token = await Token.deploy(tokenName, tokenSymbol, initialSupply, tokenCap);
  
  // 等待交易被挖掘
  await token.waitForDeployment();
  
  console.log("AirdropToken deployed at:", await token.getAddress());
  console.log("Token Name:", tokenName);
  console.log("Token Symbol:", tokenSymbol);
  console.log("Initial Supply:", ethers.formatUnits(initialSupply, 18));
  console.log("Token Cap:", ethers.formatUnits(tokenCap, 18));
}

main().catch((e) => { console.error(e); process.exit(1); });
