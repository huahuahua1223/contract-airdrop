const { ethers } = require("hardhat");
const { expect } = require("chai");
const { MerkleTree } = require("merkletreejs");
const keccak256 = require("keccak256");

// 用于创建Merkle树的叶子节点
function hashToken(index, account, amount) {
  return Buffer.from(
    ethers.solidityPackedKeccak256(
      ["uint256", "address", "uint256"],
      [index, account, amount]
    ).slice(2),
    "hex"
  );
}

describe("空投合约测试", function () {
  let token;
  let distributor;
  let owner;
  let addr1, addr2, addr3;
  let merkleTree;
  let merkleRoot;
  
  // 测试数据 - 空投名单
  const airdropList = [];
  
  before(async function() {
    // 获取测试账号
    [owner, addr1, addr2, addr3, ...addrs] = await ethers.getSigners();
    
    // 创建空投列表
    airdropList.push({
      index: 0,
      account: addr1.address,
      amount: ethers.parseUnits("100", 18)
    });
    airdropList.push({
      index: 1,
      account: addr2.address,
      amount: ethers.parseUnits("200", 18)
    });
    airdropList.push({
      index: 2,
      account: addr3.address,
      amount: ethers.parseUnits("300", 18)
    });
    
    // 构建Merkle树
    const leafNodes = airdropList.map(obj => 
      hashToken(obj.index, obj.account, obj.amount)
    );
    merkleTree = new MerkleTree(leafNodes, keccak256, { sortPairs: true });
    merkleRoot = merkleTree.getHexRoot();
    
    console.log("Merkle Root:", merkleRoot);
  });
  
  beforeEach(async function() {
    // 部署代币合约
    const initialSupply = ethers.parseUnits("1000000", 18);
    const Token = await ethers.getContractFactory("AirdropToken");
    token = await Token.deploy(initialSupply);
    await token.waitForDeployment();
    
    // 部署分发合约
    const Distributor = await ethers.getContractFactory("MerkleDistributor");
    distributor = await Distributor.deploy(await token.getAddress(), merkleRoot);
    await distributor.waitForDeployment();
    
    // 向分发合约转入足够的代币
    const totalAirdropAmount = ethers.parseUnits("1000", 18);
    await token.transfer(await distributor.getAddress(), totalAirdropAmount);
  });
  
  describe("AirdropToken", function() {
    it("初始供应量应该正确", async function() {
      const totalSupply = await token.totalSupply();
      expect(totalSupply).to.equal(ethers.parseUnits("1000000", 18));
    });
    
    it("部署者应持有所有初始代币", async function() {
      const ownerBalance = await token.balanceOf(owner.address);
      expect(ownerBalance).to.equal(ethers.parseUnits("999000", 18)); // 1000000 - 1000(转给distributor的)
    });
  });
  
  describe("MerkleDistributor", function() {
    it("应正确设置代币地址和 merkle root", async function() {
      expect(await distributor.token()).to.equal(await token.getAddress());
      expect(await distributor.merkleRoot()).to.equal(merkleRoot);
    });
    
    it("用户应该能成功领取空投", async function() {
      const airdropForAddr1 = airdropList[0];
      
      // 获取merkle证明
      const proof = merkleTree.getHexProof(
        hashToken(airdropForAddr1.index, airdropForAddr1.account, airdropForAddr1.amount)
      );
      
      // 验证领取前余额为0
      expect(await token.balanceOf(airdropForAddr1.account)).to.equal(0);
      
      // 验证未领取状态
      expect(await distributor.isClaimed(airdropForAddr1.index)).to.be.false;
      
      // 领取空投
      await distributor.connect(addr1).claim(
        airdropForAddr1.index,
        airdropForAddr1.account, 
        airdropForAddr1.amount, 
        proof
      );
      
      // 验证领取后的余额
      expect(await token.balanceOf(airdropForAddr1.account))
        .to.equal(airdropForAddr1.amount);
        
      // 验证已领取标记
      expect(await distributor.isClaimed(airdropForAddr1.index)).to.be.true;
    });
    
    it("不能重复领取空投", async function() {
      const airdropForAddr1 = airdropList[0];
      const proof = merkleTree.getHexProof(
        hashToken(airdropForAddr1.index, airdropForAddr1.account, airdropForAddr1.amount)
      );
      
      // 第一次领取
      await distributor.connect(addr1).claim(
        airdropForAddr1.index,
        airdropForAddr1.account, 
        airdropForAddr1.amount, 
        proof
      );
      
      // 尝试重复领取应该失败
      await expect(
        distributor.connect(addr1).claim(
          airdropForAddr1.index,
          airdropForAddr1.account, 
          airdropForAddr1.amount, 
          proof
        )
      ).to.be.revertedWith("MerkleDistributor: Drop already claimed");
    });
    
    it("使用无效证明不能领取", async function() {
      const airdropForAddr1 = airdropList[0];
      const wrongProof = merkleTree.getHexProof(
        hashToken(100, airdropForAddr1.account, airdropForAddr1.amount) // 使用错误的索引
      );
      
      // 尝试使用错误证明领取
      await expect(
        distributor.connect(addr1).claim(
          airdropForAddr1.index,
          airdropForAddr1.account, 
          airdropForAddr1.amount, 
          wrongProof
        )
      ).to.be.revertedWith("MerkleDistributor: Invalid proof");
    });
  });
}); 