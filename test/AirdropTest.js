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
  const tokenName = "空投代币";
  const tokenSymbol = "ADT";
  const initialSupply = ethers.parseUnits("1000000", 18);
  const tokenCap = ethers.parseUnits("10000000", 18); // 1000万上限
  
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
    const Token = await ethers.getContractFactory("AirdropToken");
    token = await Token.deploy(
      tokenName,
      tokenSymbol,
      initialSupply,
      tokenCap
    );
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
    it("应该正确设置代币名称和符号", async function() {
      expect(await token.name()).to.equal(tokenName);
      expect(await token.symbol()).to.equal(tokenSymbol);
    });
    
    it("初始供应量应该正确", async function() {
      const totalSupply = await token.totalSupply();
      expect(totalSupply).to.equal(initialSupply);
    });
    
    it("部署者应持有所有初始代币", async function() {
      const ownerBalance = await token.balanceOf(owner.address);
      const expectedBalance = initialSupply - ethers.parseUnits("1000", 18);
      expect(ownerBalance).to.equal(expectedBalance); // 1000000 - 1000(转给distributor的)
    });
    
    it("应该正确设置代币上限", async function() {
      expect(await token.cap()).to.equal(tokenCap);
    });
    
    it("部署者应该拥有MINTER_ROLE角色", async function() {
      const MINTER_ROLE = await token.MINTER_ROLE();
      expect(await token.hasRole(MINTER_ROLE, owner.address)).to.be.true;
    });
    
    it("部署者应该拥有PAUSER_ROLE角色", async function() {
      const PAUSER_ROLE = await token.PAUSER_ROLE();
      expect(await token.hasRole(PAUSER_ROLE, owner.address)).to.be.true;
    });
    
    it("应该能铸造新代币", async function() {
      const mintAmount = ethers.parseUnits("10000", 18);
      await token.mint(addr1.address, mintAmount);
      
      expect(await token.balanceOf(addr1.address)).to.equal(mintAmount);
      const expectedSupply = BigInt(initialSupply) + BigInt(mintAmount);
      expect(await token.totalSupply()).to.equal(expectedSupply);
    });
    
    it("非MINTER_ROLE不能铸造代币", async function() {
      const mintAmount = ethers.parseUnits("10000", 18);
      
      await expect(
        token.connect(addr1).mint(addr1.address, mintAmount)
      ).to.be.reverted; // 由于没有权限会被AccessControl拦截
    });
    
    it("可以暂停和恢复代币转账", async function() {
      // 暂停代币
      await token.pause();
      
      // 尝试转账应该失败
      const transferAmount = ethers.parseUnits("100", 18);
      await expect(
        token.transfer(addr1.address, transferAmount)
      ).to.be.revertedWith("ERC20Pausable: token transfer while paused");
      
      // 恢复代币
      await token.unpause();
      
      // 现在应该可以转账
      await token.transfer(addr1.address, transferAmount);
      expect(await token.balanceOf(addr1.address)).to.equal(transferAmount);
    });
    
    it("铸造不能超过代币上限", async function() {
      // 尝试铸造超过上限的代币
      const overCapAmount = BigInt(tokenCap) - BigInt(initialSupply) + 1n; // 超过上限1个单位
      
      await expect(
        token.mint(addr1.address, overCapAmount)
      ).to.be.revertedWith("ERC20Capped: cap exceeded");
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
    
    it("代币暂停后不能领取空投", async function() {
      const airdropForAddr1 = airdropList[0];
      const proof = merkleTree.getHexProof(
        hashToken(airdropForAddr1.index, airdropForAddr1.account, airdropForAddr1.amount)
      );
      
      // 暂停代币
      await token.pause();
      
      // 尝试领取空投应该失败
      await expect(
        distributor.connect(addr1).claim(
          airdropForAddr1.index,
          airdropForAddr1.account, 
          airdropForAddr1.amount, 
          proof
        )
      ).to.be.revertedWith("ERC20Pausable: token transfer while paused");
      
      // 恢复代币
      await token.unpause();
      
      // 现在应该可以领取
      await distributor.connect(addr1).claim(
        airdropForAddr1.index,
        airdropForAddr1.account, 
        airdropForAddr1.amount, 
        proof
      );
    });
  });
}); 