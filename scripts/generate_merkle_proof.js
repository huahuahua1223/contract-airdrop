const fs = require('fs');
const path = require('path');
const { MerkleTree } = require('merkletreejs');
const keccak256 = require('keccak256');
const { ethers } = require('hardhat');

/**
 * 将地址和金额转换为叶节点哈希
 * @param {number} index 空投索引
 * @param {string} account 账户地址
 * @param {string} amount 金额（以wei为单位）
 * @returns {Buffer} 叶节点哈希
 */
function hashToken(index, account, amount) {
  return Buffer.from(
    ethers.solidityPackedKeccak256(
      ["uint256", "address", "uint256"],
      [index, account, amount]
    ).slice(2),
    'hex'
  );
}

/**
 * 验证Merkle证明
 * @param {string} merkleRoot Merkle根
 * @param {object} proofData 证明数据
 * @returns {boolean} 验证结果
 */
function verifyProof(merkleRoot, proofData) {
  const { index, address, amount, proof } = proofData;
  
  const leaf = hashToken(index, address, amount);
  const isValid = MerkleTree.verify(proof, leaf, merkleRoot, keccak256, { sortPairs: true });
  
  return isValid;
}

/**
 * 获取地址的Merkle证明
 * @param {string} targetAddress 目标地址
 * @returns {object|null} 证明数据或null（如果地址不在空投列表中）
 */
async function getMerkleProof(targetAddress) {
  const normalizedAddress = targetAddress.toLowerCase();
  console.log(`为地址 ${normalizedAddress} 生成Merkle证明...`);
  
  try {
    // 输出目录
    const merkleDir = path.join(__dirname, '../merkle-data');
    
    // 读取地址映射文件
    const addressMapPath = path.join(merkleDir, 'address_map.json');
    if (!fs.existsSync(addressMapPath)) {
      console.error(`错误: 地址映射文件不存在，请先运行 generate_merkle_tree.js 生成Merkle树`);
      return null;
    }
    
    // 解析地址映射
    const addressMap = JSON.parse(fs.readFileSync(addressMapPath, 'utf8'));
    
    // 检查地址是否存在
    if (!addressMap[normalizedAddress]) {
      console.warn(`地址 ${normalizedAddress} 不在空投列表中`);
      return null;
    }
    
    // 获取批次信息
    const { batchIndex, index } = addressMap[normalizedAddress];
    console.log(`找到地址 ${normalizedAddress} 在批次 ${batchIndex}，索引 ${index}`);
    
    // 读取批次数据
    const batchDataPath = path.join(merkleDir, `batches/batch_${batchIndex}.json`);
    if (!fs.existsSync(batchDataPath)) {
      console.error(`错误: 批次数据文件 ${batchDataPath} 不存在`);
      return null;
    }
    
    const batchData = JSON.parse(fs.readFileSync(batchDataPath, 'utf8'));
    
    // 找到记录
    const record = batchData.records.find(r => r.address.toLowerCase() === normalizedAddress);
    if (!record) {
      console.error(`错误: 在批次 ${batchIndex} 中找不到地址 ${normalizedAddress} 的记录`);
      return null;
    }
    
    // 重建批次的Merkle树
    const leaves = batchData.records.map(r => 
      hashToken(r.index, r.address, r.amount)
    );
    
    const batchTree = new MerkleTree(leaves, keccak256, { sortPairs: true });
    
    // 生成批次内的证明
    const leaf = hashToken(record.index, record.address, record.amount);
    const proof = batchTree.getHexProof(leaf);
    
    // 验证证明是否与批次根匹配
    const batchRoot = batchData.root;
    
    const proofData = {
      index: record.index,
      address: record.address,
      amount: record.amount,
      proof: proof,
      amountInEther: ethers.formatUnits(record.amount, 18)
    };
    
    // 验证批次内的证明
    const isBatchProofValid = verifyProof(batchRoot, proofData);
    console.log(`批次内证明验证结果: ${isBatchProofValid ? '有效' : '无效'}`);
    
    if (!isBatchProofValid) {
      console.error(`错误: 批次内证明无效！`);
      return null;
    }
    
    // 读取merkle_data.json获取最终根和所有批次根
    const merkleDataPath = path.join(merkleDir, 'merkle_data.json');
    const merkleData = JSON.parse(fs.readFileSync(merkleDataPath, 'utf8'));
    const rootHash = merkleData.root;
    
    // 对于智能合约验证，需要将批次内证明和批次索引一起传递
    proofData.batchIndex = batchIndex;
    proofData.batchRoot = batchRoot;
    
    console.log(`证明已成功生成，批次索引: ${batchIndex}`);
    return proofData;
  } catch (error) {
    console.error(`生成证明时出错:`, error);
    return null;
  }
}

/**
 * 批量获取证明
 * @param {Array} addresses 地址数组
 * @returns {Object} 地址到证明的映射
 */
async function getBatchProofs(addresses) {
  const results = {};
  let successCount = 0;
  
  for (const address of addresses) {
    const proof = await getMerkleProof(address);
    if (proof) {
      results[address] = proof;
      successCount++;
    }
  }
  
  console.log(`已为 ${successCount}/${addresses.length} 个地址生成证明`);
  return results;
}

/**
 * 导出所有证明到文件
 * @param {string} outputPath 输出文件路径
 */
async function exportAllProofs(outputPath) {
  console.time('导出完成');
  
  try {
    const merkleDir = path.join(__dirname, '../merkle-data');
    const addressMapPath = path.join(merkleDir, 'address_map.json');
    
    if (!fs.existsSync(addressMapPath)) {
      console.error(`错误: 地址映射文件不存在，请先运行 generate_merkle_tree.js 生成Merkle树`);
      return;
    }
    
    const addressMap = JSON.parse(fs.readFileSync(addressMapPath, 'utf8'));
    const addresses = Object.keys(addressMap);
    
    console.log(`开始为 ${addresses.length} 个地址生成证明...`);
    
    const allProofs = await getBatchProofs(addresses);
    
    // 写入文件
    fs.writeFileSync(
      outputPath || path.join(merkleDir, 'all_proofs.json'),
      JSON.stringify(allProofs, null, 2)
    );
    
    console.log(`所有证明已导出到: ${outputPath || path.join(merkleDir, 'all_proofs.json')}`);
  } catch (error) {
    console.error(`导出证明时出错:`, error);
  }
  
  console.timeEnd('导出完成');
}

// 主函数 - 处理命令行参数
async function main() {
  const args = process.argv.slice(2);
  const command = args[0];

  if (!command) {
    console.log(`
使用说明:
  - 为单个地址生成证明: node generate_merkle_proof.js address 0x...
  - 为所有地址生成证明: node generate_merkle_proof.js export [output_path]
  
示例:
  - 生成单个证明: node generate_merkle_proof.js address 0x123...
  - 导出所有证明: node generate_merkle_proof.js export ./all_proofs.json
    `);
    return;
  }

  try {
    switch (command) {
      case 'address':
        const address = args[1];
        if (!address) {
          console.error('错误: 请提供地址');
          return;
        }
        
        const proof = await getMerkleProof(address);
        if (proof) {
          console.log('生成的证明:', JSON.stringify(proof, null, 2));
        }
        break;
        
      case 'export':
        const outputPath = args[1];
        await exportAllProofs(outputPath);
        break;
      
      default:
        console.error(`未知命令: ${command}`);
    }
  } catch (error) {
    console.error('发生错误:', error);
  }
}

// 如果直接运行此脚本
if (require.main === module) {
  main().catch(console.error);
}

module.exports = {
  getMerkleProof,
  getBatchProofs,
  verifyProof,
  exportAllProofs
}; 