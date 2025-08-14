const { ethers } = require("hardhat");

async function main() {
  const tokenName = "UnichatV4.com";
  const tokenSymbol = "UnichatV4.com";
  const initialSupply = ethers.parseUnits("1000000", 18); // 100万 UnichatV4.com
  const tokenCap = ethers.parseUnits("21000000", 18);    // 2100万上限
  
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
