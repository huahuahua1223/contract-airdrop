const fs = require('fs');
const path = require('path');
const { MerkleTree } = require('merkletreejs');
const keccak256 = require('keccak256');
const { ethers } = require('hardhat');
const readline = require('readline');
const { performance } = require('perf_hooks');

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
 * 根据得分计算空投金额
 * 公式：1.3的(total_score-1)次方
 * @param {number} totalScore 用户得分
 * @returns {BigInt} 代币数量（wei为单位）
 */
function calculateAmount(totalScore) {
  // 确保totalScore是数字
  const score = parseFloat(totalScore);
  
  // 应用公式：1.3^(score-1)
  if (score <= 0) return BigInt(0);
  
  const baseAmount = Math.pow(1.3, score - 1);
  // 转换为wei (18位小数)，并确保是整数
  const amountInEther = baseAmount.toFixed(6); // 保留6位小数精度
  return ethers.parseUnits(amountInEther, 18);
}

/**
 * 读取CSV文件夹中所有CSV文件
 * @param {string} csvDir CSV文件所在目录
 * @returns {Array} CSV文件路径列表
 */
function getAllCsvFiles(csvDir) {
  try {
    if (!fs.existsSync(csvDir)) {
      console.error(`错误: 目录 ${csvDir} 不存在`);
      return [];
    }
    
    const files = fs.readdirSync(csvDir);
    return files
      .filter(file => file.toLowerCase().endsWith('.csv'))
      .map(file => path.join(csvDir, file));
  } catch (err) {
    console.error(`读取CSV目录时出错:`, err);
    return [];
  }
}

/**
 * 直接处理CSV文件并构建批次数据
 * @param {Array} csvFiles CSV文件列表
 * @param {number} batchSize 每个批次的记录数
 * @param {string} batchDir 批次数据保存目录
 * @returns {Object} 处理结果
 */
async function processCsvFilesIntoBatches(csvFiles, batchSize, batchDir) {
  const batchRoots = [];
  const addressMap = {};
  
  let totalRecords = 0;
  let currentBatchIndex = 0;
  let currentBatchRecords = [];
  let globalIndex = 0;
  
  // 为每个CSV文件处理数据
  for (const csvFile of csvFiles) {
    console.log(`处理CSV文件: ${csvFile}`);
    
    const fileStream = fs.createReadStream(csvFile);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });
    
    let isFirstLine = true;
    let lineCount = 0;
    
    // 处理CSV文件中的每一行
    for await (const line of rl) {
      // 跳过CSV头行
      if (isFirstLine) {
        isFirstLine = false;
        continue;
      }
      
      // 跳过空行
      if (!line.trim()) continue;
      
      lineCount++;
      
      // 解析行数据 - 根据制表符或逗号分隔
      const columns = line.includes('\t') ? line.split('\t') : line.split(',');
      
      // 确保列数足够
      if (columns.length < 9) {
        console.warn(`跳过格式不正确的行: ${line}`);
        continue;
      }
      
      // 从第二列获取地址，从第九列获取分数
      const address = columns[1].trim();
      const score = columns[8].trim();
      
      // 检查地址是否有效
      if (!ethers.isAddress(address)) {
        console.warn(`跳过无效地址: ${address}, 行: ${lineCount}`);
        continue;
      }
      
      // 检查分数是否为数字
      if (isNaN(parseFloat(score))) {
        console.warn(`跳过非数字分数: ${score}, 地址: ${address}`);
        continue;
      }
      
      // 计算代币数量
      const amount = calculateAmount(parseFloat(score));
      
      // 将记录添加到当前批次
      currentBatchRecords.push({
        index: globalIndex,
        address: address,
        amount: amount.toString()
      });
      
      // 记录地址映射信息
      addressMap[address] = {
        batchIndex: currentBatchIndex,
        localIndex: currentBatchRecords.length - 1,
        index: globalIndex
      };
      
      globalIndex++;
      
      // 检查是否需要处理当前批次
      if (currentBatchRecords.length >= batchSize) {
        await processBatch(currentBatchRecords, currentBatchIndex, batchDir, batchRoots);
        currentBatchIndex++;
        currentBatchRecords = [];
        
        // 强制垃圾回收
        if (global.gc) {
          global.gc();
        }
      }
      
      // 每1万条记录报告一次进度
      if (lineCount % 10000 === 0) {
        console.log(`已处理 ${lineCount} 条记录...`);
      }
    }
    
    totalRecords += lineCount;
    console.log(`CSV文件 ${csvFile} 处理完成，共 ${lineCount} 条记录`);
  }
  
  // 处理剩余的记录（最后一个批次可能不满）
  if (currentBatchRecords.length > 0) {
    await processBatch(currentBatchRecords, currentBatchIndex, batchDir, batchRoots);
    currentBatchIndex++;
  }
  
  console.log(`所有CSV文件处理完成，共 ${totalRecords} 条记录，分为 ${currentBatchIndex} 个批次`);
  
  return {
    totalRecords,
    batchCount: currentBatchIndex,
    batchRoots,
    addressMap
  };
}

/**
 * 处理单个批次并构建Merkle树
 * @param {Array} records 批次记录
 * @param {number} batchIndex 批次索引
 * @param {string} batchDir 批次数据保存目录
 * @param {Array} batchRoots 用于收集批次根哈希的数组
 */
async function processBatch(records, batchIndex, batchDir, batchRoots) {
  const startTime = performance.now();
  console.log(`处理第 ${batchIndex + 1} 批数据，共 ${records.length} 条记录`);
  
  const leaves = [];
  
  // 创建叶节点
  for (const record of records) {
    const leaf = hashToken(record.index, record.address, record.amount);
    leaves.push(leaf);
  }
  
  // 构建批次Merkle树
  const merkleTree = new MerkleTree(leaves, keccak256, { sortPairs: true });
  const rootHash = merkleTree.getHexRoot();
  
  const elapsedTime = ((performance.now() - startTime) / 1000).toFixed(2);
  console.log(`批次 ${batchIndex + 1} 的Merkle根: ${rootHash} (耗时: ${elapsedTime}秒)`);
  
  // 保存批次根
  batchRoots.push(rootHash);
  
  // 保存批次数据
  const batchData = {
    batchIndex,
    root: rootHash,
    recordCount: records.length,
    records: records
  };
  
  fs.writeFileSync(
    path.join(batchDir, `batch_${batchIndex}.json`),
    JSON.stringify(batchData, null, 2)
  );
}

/**
 * 批量构建Merkle树
 * @param {number} batchSize 每批处理的记录数
 */
async function buildMerkleTree(batchSize = 100) {
  console.time('构建完成');
  const outputDir = path.join(__dirname, '../merkle-data');
  const batchDir = path.join(outputDir, 'batches');
  
  // 确保输出目录存在
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
  
  if (!fs.existsSync(batchDir)) {
    fs.mkdirSync(batchDir, { recursive: true });
  }

  // 用户CSV模式：读取/csv目录下的所有CSV文件
  console.log('读取/csv目录下的所有CSV文件...');
  const csvDir = path.join(__dirname, '../csv');
  const csvFiles = getAllCsvFiles(csvDir);
  
  if (csvFiles.length === 0) {
    console.error('错误: 未找到CSV文件。请确保在/csv目录下有CSV文件');
    return null;
  }
  
  console.log(`找到 ${csvFiles.length} 个CSV文件: ${csvFiles.join(', ')}`);
  
  // 直接处理CSV文件到批次，避免一次性加载所有数据到内存
  console.log(`开始处理CSV文件，每批次最多 ${batchSize} 条记录`);
  const result = await processCsvFilesIntoBatches(csvFiles, batchSize, batchDir);
  
  const batchRoots = result.batchRoots;
  const addressMap = result.addressMap;
  const totalRecords = result.totalRecords;
  const batchCount = result.batchCount;

  console.log(`处理完成，共 ${totalRecords} 条记录，分为 ${batchCount} 个批次`);

  // 构建顶层Merkle树
  console.log(`构建顶层Merkle树，组合 ${batchCount} 个批次根...`);
  const topTree = new MerkleTree(batchRoots, keccak256, { sortPairs: true });
  const merkleRoot = topTree.getHexRoot();
  
  console.log(`最终Merkle根: ${merkleRoot}`);

  // 生成示例证明
  console.log(`生成示例证明...`);
  const proofExamples = {};
  // 选择前10个地址生成示例证明
  const addresses = Object.keys(addressMap).slice(0, Math.min(10, Object.keys(addressMap).length));
  
  for (const address of addresses) {
    try {
      const { batchIndex } = addressMap[address];
      const batchDataPath = path.join(batchDir, `batch_${batchIndex}.json`);
      const batchData = JSON.parse(fs.readFileSync(batchDataPath));
      
      // 找到地址对应的记录
      const record = batchData.records.find(r => r.address.toLowerCase() === address.toLowerCase());
      if (!record) continue;
      
      // 重建批次的Merkle树
      const leaves = batchData.records.map(r => 
        hashToken(r.index, r.address, r.amount)
      );
      
      const batchTree = new MerkleTree(leaves, keccak256, { sortPairs: true });
      
      // 获取证明
      const leaf = hashToken(record.index, record.address, record.amount);
      const proof = batchTree.getHexProof(leaf);
      
      proofExamples[address] = {
        index: record.index,
        amount: record.amount,
        proof,
        amountInEther: ethers.formatUnits(record.amount, 18)
      };
    } catch (err) {
      console.error(`为地址 ${address} 生成证明时发生错误:`, err);
    }
  }

  // 保存Merkle根和示例证明
  const outputData = {
    root: merkleRoot,
    totalRecords,
    batchSize,
    batchCount,
    examples: proofExamples
  };

  const outputPath = path.join(outputDir, 'merkle_data.json');
  fs.writeFileSync(outputPath, JSON.stringify(outputData, null, 2));
  console.log(`Merkle数据已保存至: ${outputPath}`);
  
  // 保存地址映射信息，方便后续查询
  const addressMapPath = path.join(outputDir, 'address_map.json');
  fs.writeFileSync(addressMapPath, JSON.stringify(addressMap, null, 2));
  
  console.timeEnd('构建完成');
  
  return { merkleRoot, outputPath, addressMap };
}

/**
 * 验证Merkle证明
 * @param {string} merkleRoot Merkle根
 * @param {object} proofData 证明数据
 */
function verifyProof(merkleRoot, proofData) {
  const { index, address, amount, proof } = proofData;
  
  const leaf = hashToken(index, address, amount);
  const isValid = MerkleTree.verify(proof, leaf, merkleRoot, keccak256, { sortPairs: true });
  
  console.log(`地址 ${address} 的证明验证结果: ${isValid ? '有效' : '无效'}`);
  return isValid;
}

/**
 * 显示内存使用情况
 */
function logMemoryUsage() {
  const used = process.memoryUsage();
  const messages = [];
  
  for (const key in used) {
    messages.push(`${key}: ${Math.round(used[key] / 1024 / 1024 * 100) / 100} MB`);
  }
  
  console.log(`内存使用: ${messages.join(', ')}`);
}

// 主函数 - 处理命令行参数
async function main() {
  const args = process.argv.slice(2);
  const batchSize = parseInt(args[0] || '100');
  
  console.log(`
使用说明:
  - 构建Merkle树: node generate_merkle_tree.js [batch_size]
  
  批次大小默认为100条记录
  
  执行前请确保CSV文件已放置在/csv目录下
  CSV文件格式要求：地址在第2列，分数在第9列
  `);

  try {
    // 启用内存使用量日志
    const memoryInterval = setInterval(logMemoryUsage, 30000); // 每30秒输出一次内存使用情况
    
    await buildMerkleTree(batchSize);
    
    clearInterval(memoryInterval);
  } catch (error) {
    console.error('发生错误:', error);
  }
}

// 如果直接运行此脚本
if (require.main === module) {
  main().catch(console.error);
}

module.exports = {
  buildMerkleTree,
  verifyProof,
  hashToken,
  logMemoryUsage,
  calculateAmount
}; 