// 合并代币持有者CSV文件脚本
// 功能：合并多个代币持有者CSV文件，排除非真人地址，聚合积分（取前4个最高积分）
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify/sync');
const { performance } = require('perf_hooks');

// 配置参数
const CONFIG = {
  // 最大积分聚合数量（取前4个最高积分）
  MAX_SCORE_COUNT: 4,
  // 进度报告间隔
  PROGRESS_INTERVAL: 1000,
  // 批处理大小
  BATCH_SIZE: 5000,
  // 文件处理批次大小（每次处理多少个文件后暂存）
  FILE_BATCH_SIZE: 5,
  // 中间结果保存间隔
  SAVE_INTERVAL: 10
};

// 排除关键词列表 - 用于识别非真人地址
const EXCLUSION_KEYWORDS = [
  'exchange', 'fees', 'multisig', 'safe', 'pool', 'lp', 'bridge', 
  'relayer', 'executor', 'deployer', 'exploiter', 'donate', 
  'fake_phishing', 'dead', 'cex', 'cold wallet', 'hot wallet',
  'gnosis safe', 'protocol', 'treasury', 'dao', 'vault',
  'sushiswap', 'uniswap', 'curve.fi', 'aave', 'dodo', 'stargate',
  'pancakeswap', 'coinbase', 'binance', 'kucoin', 'upbit', 'nexo',
  'wintermute', 'orbiter finance', 'layerzero', 'wormhole', 'across protocol',
  'pendle finance', 'premia', 'ramp network', 'l2beat', 'arbiscan'
];

/**
 * 检查地址标签是否应该被排除
 * @param {string} label 地址标签
 * @returns {boolean} true表示应该排除
 */
function shouldExcludeAddress(label) {
  if (!label || typeof label !== 'string') {
    return false; // 没有标签的地址保留
  }
  
  const lowerLabel = label.toLowerCase().trim();
  
  // 检查是否包含排除关键词
  for (const keyword of EXCLUSION_KEYWORDS) {
    if (lowerLabel.includes(keyword)) {
      return true;
    }
  }
  
  return false;
}

/**
 * 从文件名提取代币符号
 * @param {string} fileName CSV文件名
 * @returns {string} 代币符号
 */
function extractTokenSymbol(fileName) {
  // 文件名格式示例：Unishop.ai_0x999999_owners.csv
  // 提取第一个下划线之前的部分作为代币符号
  const match = fileName.match(/^([^_]+)_/);
  return match ? match[1] : fileName.replace('.csv', '').replace('_owners', '');
}

/**
 * 读取单个CSV文件
 * @param {string} filePath CSV文件路径
 * @returns {Promise<Array>} 解析后的记录数组
 */
async function readCSVFile(filePath) {
  const records = [];
  const fileName = path.basename(filePath);
  const tokenSymbol = extractTokenSymbol(fileName);
  
  console.log(`正在读取文件: ${fileName}`);
  
  try {
    const fileContent = fs.readFileSync(filePath, 'utf8');
    const lines = fileContent.split('\n');
    
    // 找到CSV头行（跳过注释行）
    let headerLineIndex = -1;
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      if (line && !line.startsWith('#') && line.includes('owner_address')) {
        headerLineIndex = i;
        break;
      }
    }
    
    if (headerLineIndex === -1) {
      console.error(`文件 ${fileName} 中未找到有效的CSV头行`);
      return [];
    }
    
    // 解析头行
    const headers = lines[headerLineIndex].split(',').map(h => h.trim());
    console.log(`CSV列结构: ${headers.join(', ')}`);
    
    // 处理数据行
    for (let i = headerLineIndex + 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;
      
      const values = line.split(',').map(v => v.trim());
      if (values.length < headers.length) continue;
      
      // 构建记录对象
      const record = {};
      for (let j = 0; j < headers.length; j++) {
        record[headers[j]] = values[j] || '';
      }
      
      // 检查必要字段是否存在
      if (!record.owner_address || !record.value_score) {
        continue;
      }
      
      // 检查地址格式
      const address = record.owner_address.toLowerCase().trim();
      if (!address.startsWith('0x') || address.length !== 42) {
        continue;
      }
      
      // 检查是否应该排除这个地址
      if (shouldExcludeAddress(record.owner_address_label)) {
        continue;
      }
      
      // 解析积分
      const valueScore = parseFloat(record.value_score) || 0;
      if (valueScore <= 0) {
        continue;
      }
      
      // 解析USD价值
      const usdValue = parseFloat(record.usd_value) || 0;
      
      records.push({
        owner_address: address,
        owner_address_label: record.owner_address_label || '',
        usd_value: usdValue,
        balance_formatted: record.balance_formatted || '',
        value_score: valueScore,
        token_symbol: tokenSymbol,
        source_file: fileName
      });
    }
    
    console.log(`文件 ${fileName} 读取完成，有效记录: ${records.length}`);
    return records;
  } catch (error) {
    console.error(`读取文件 ${fileName} 时出错:`, error.message);
    return [];
  }
}

/**
 * 聚合地址数据 - 每个地址取前4个最高积分
 * @param {Array} allRecords 所有记录
 * @returns {Array} 聚合后的记录
 */
function aggregateAddressData(allRecords) {
  console.log(`开始聚合 ${allRecords.length} 条记录...`);
  const startTime = performance.now();
  
  // 按地址分组
  const addressGroups = new Map();
  
  for (const record of allRecords) {
    const address = record.owner_address;
    
    if (!addressGroups.has(address)) {
      addressGroups.set(address, []);
    }
    
    addressGroups.get(address).push(record);
  }
  
  console.log(`共找到 ${addressGroups.size} 个唯一地址`);
  
  const aggregatedResults = [];
  let processedCount = 0;
  
  // 处理每个地址组
  for (const [address, records] of addressGroups) {
    processedCount++;
    
    // 按value_score降序排列，取前4个
    const sortedRecords = records
      .sort((a, b) => b.value_score - a.value_score)
      .slice(0, CONFIG.MAX_SCORE_COUNT);
    
    // 计算总积分
    const totalScore = sortedRecords.reduce((sum, record) => sum + record.value_score, 0);
    
    // 计算总USD价值
    const totalUsdValue = sortedRecords.reduce((sum, record) => sum + record.usd_value, 0);
    
    // 收集代币符号
    const tokenSymbols = sortedRecords.map(r => r.token_symbol);
    const uniqueTokens = [...new Set(tokenSymbols)];
    
    // 收集源文件
    const sourceFiles = sortedRecords.map(r => r.source_file);
    const uniqueFiles = [...new Set(sourceFiles)];
    
    // 选择最好的标签（有标签的记录）
    const bestLabel = sortedRecords.find(r => r.owner_address_label && r.owner_address_label.trim())?.owner_address_label || '';
    
    // 创建聚合记录
    aggregatedResults.push({
      owner_address: address,
      owner_address_label: bestLabel,
      total_score: Math.round(totalScore * 100) / 100, // 保留2位小数
      total_usd_value: Math.round(totalUsdValue * 100) / 100,
      score_count: sortedRecords.length,
      token_symbols: uniqueTokens.join(';'),
      token_count: uniqueTokens.length,
      source_files: uniqueFiles.join(';'),
      individual_scores: sortedRecords.map(r => r.value_score).join(';'),
      individual_values: sortedRecords.map(r => r.usd_value).join(';')
    });
    
    // 进度报告
    if (processedCount % CONFIG.PROGRESS_INTERVAL === 0) {
      console.log(`已处理 ${processedCount}/${addressGroups.size} 个地址...`);
    }
  }
  
  // 按总积分降序排列
  aggregatedResults.sort((a, b) => b.total_score - a.total_score);
  
  const elapsedTime = ((performance.now() - startTime) / 1000).toFixed(2);
  console.log(`聚合完成，耗时 ${elapsedTime} 秒`);
  console.log(`最终结果: ${aggregatedResults.length} 个唯一地址`);
  
  return aggregatedResults;
}

/**
 * 生成统计报告
 * @param {Array} originalRecords 原始记录
 * @param {Array} aggregatedResults 聚合结果
 * @param {Array} processedFiles 已处理的文件列表（用于准确统计代币种类数）
 * @returns {Object} 统计信息
 */
function generateStats(originalRecords, aggregatedResults, processedFiles = []) {
  const stats = {
    originalRecords: originalRecords.length,
    uniqueAddresses: aggregatedResults.length,
    totalTokens: 0,
    excludedRecords: 0, // 这个数字会在读取过程中计算
    averageScore: 0,
    maxScore: 0,
    minScore: 0,
    scoreDistribution: {
      '1-2': 0,
      '3-4': 0,
      '5-6': 0,
      '7+': 0
    }
  };
  
  // 优先使用处理成功的文件列表来统计代币合约数（每个文件代表一个代币合约）
  if (processedFiles && processedFiles.length > 0) {
    let validTokenContracts = 0;
    processedFiles.forEach(fileName => {
      // 精确匹配排除空文件（避免误排除包含相同字符串的有效文件）
      const emptyFilePatterns = ['AMD_0x012965', 'BUFF_0x404853', 'DIA_0x6efa9b', 'TLIP_0xc38526', 'VEE_0x0caadd'];
      const isEmptyFile = emptyFilePatterns.some(pattern => fileName.includes(pattern));
      
      if (!isEmptyFile) {
        validTokenContracts++;
      }
    });
    stats.totalTokens = validTokenContracts;
    console.log(`基于已处理文件统计代币合约数: ${stats.totalTokens} (每个文件代表一个代币合约)`);
  } else {
    // 备用方法：从原始记录统计（可能不准确，如果有文件处理失败）
    const tokenSymbols = new Set();
    for (let i = 0; i < originalRecords.length; i++) {
      tokenSymbols.add(originalRecords[i].token_symbol);
    }
    stats.totalTokens = tokenSymbols.size;
    console.log(`基于原始记录统计代币种类数: ${stats.totalTokens} (按符号去重)`);
  }
  
  if (aggregatedResults.length > 0) {
    // 分批计算统计信息以避免内存问题
    let totalScore = 0;
    let maxScore = 0;
    let minScore = Infinity;
    
    // 分批处理聚合结果
    const batchSize = 10000;
    for (let i = 0; i < aggregatedResults.length; i += batchSize) {
      const batch = aggregatedResults.slice(i, Math.min(i + batchSize, aggregatedResults.length));
      
      for (const result of batch) {
        const score = result.total_score;
        totalScore += score;
        
        if (score > maxScore) maxScore = score;
        if (score < minScore) minScore = score;
        
        // 分数分布统计
        if (score >= 1 && score < 3) stats.scoreDistribution['1-2']++;
        else if (score >= 3 && score < 5) stats.scoreDistribution['3-4']++;
        else if (score >= 5 && score < 7) stats.scoreDistribution['5-6']++;
        else if (score >= 7) stats.scoreDistribution['7+']++;
      }
    }
    
    stats.averageScore = Math.round((totalScore / aggregatedResults.length) * 100) / 100;
    stats.maxScore = maxScore;
    stats.minScore = minScore === Infinity ? 0 : minScore;
  }
  
  return stats;
}

/**
 * 将聚合结果写入CSV文件
 * @param {Array} results 聚合结果
 * @param {string} outputPath 输出文件路径
 * @param {Object} stats 统计信息
 */
function writeResultsToCSV(results, outputPath, stats) {
  try {
    // 确保输出目录存在
    const dir = path.dirname(outputPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    
    // 分割大小：每60万条记录一个文件
    const SPLIT_SIZE = 600000;
    const totalFiles = Math.ceil(results.length / SPLIT_SIZE);
    
    console.log(`开始写入CSV文件，数据将分成 ${totalFiles} 个文件，每个文件最多 ${SPLIT_SIZE.toLocaleString()} 条记录`);
    
    // 定义CSV标题
    const headers = [
      'owner_address',           // 持有者地址
      'owner_address_label',     // 地址标签
      'total_score',             // 总积分（前4个最高积分之和）
      'total_usd_value',         // 总USD价值
      'score_count',             // 参与计算的积分数量
      'token_symbols',           // 代币符号列表
      'token_count',             // 代币种类数量
      'source_files',            // 源文件列表
      'individual_scores',       // 各个积分详情
      'individual_values'        // 各个价值详情
    ];
    
    // 生成文件头部注释的函数
    const createHeader = (fileIndex, recordsInFile, startIndex, endIndex) => {
      let header = `# 代币持有者聚合数据`;
      if (totalFiles > 1) {
        header += ` - 第${fileIndex}部分`;
      }
      header += `\n`;
      header += `# 生成时间: ${new Date().toISOString()}\n`;
      if (totalFiles > 1) {
        header += `# 总记录数: ${results.length.toLocaleString()}\n`;
        header += `# 本文件记录数: ${recordsInFile.toLocaleString()}\n`;
        header += `# 记录范围: ${(startIndex + 1).toLocaleString()} - ${endIndex.toLocaleString()}\n`;
      } else {
        header += `# 记录数: ${results.length.toLocaleString()}\n`;
      }
      header += `# 唯一地址数: ${stats.uniqueAddresses.toLocaleString()}\n`;
      header += `# 代币合约数: ${stats.totalTokens}\n`;
      header += `# 平均积分: ${stats.averageScore}\n`;
      header += `# 积分范围: ${stats.minScore} - ${stats.maxScore}\n`;
      header += `#\n`;
      header += `# 说明:\n`;
      header += `# - 已排除Exchange/Fees/Multisig/Safe/Pool/LP/Bridge/Relayer/Executor/Deployer/Exploiter/Donate/Fake_Phishing/dEaD等标签的地址\n`;
      header += `# - 每个地址最多聚合前4个最高积分\n`;
      header += `# - token_symbols和source_files用分号(;)分隔多个值\n`;
      header += `#\n\n`;
      return header;
    };
    
    const outputFileList = [];
    
    // 分文件写入
    for (let fileIndex = 1; fileIndex <= totalFiles; fileIndex++) {
      const startIndex = (fileIndex - 1) * SPLIT_SIZE;
      const endIndex = Math.min(startIndex + SPLIT_SIZE, results.length);
      const recordsInFile = endIndex - startIndex;
      
      // 生成文件名
      const baseFileName = path.basename(outputPath, '.csv');
      const fileDir = path.dirname(outputPath);
      const splitFileName = totalFiles > 1 ? 
        `${baseFileName}_part${fileIndex}_of_${totalFiles}.csv` : 
        `${baseFileName}.csv`;
      const splitFilePath = path.join(fileDir, splitFileName);
      
      console.log(`\n写入第 ${fileIndex}/${totalFiles} 个文件: ${splitFileName}`);
      console.log(`记录范围: ${(startIndex + 1).toLocaleString()} - ${endIndex.toLocaleString()} (共 ${recordsInFile.toLocaleString()} 条)`);
      
      // 创建写入流
      const writeStream = fs.createWriteStream(splitFilePath);
      
      // 写入文件头部注释
      const headerContent = createHeader(fileIndex, recordsInFile, startIndex, endIndex);
      writeStream.write(headerContent);
      
      // 写入CSV标题行
      writeStream.write(headers.join(',') + '\n');
      
      // 获取当前文件的数据片段
      const fileData = results.slice(startIndex, endIndex);
      
      // 分批写入数据以避免内存问题
      const batchSize = 5000;
      for (let i = 0; i < fileData.length; i += batchSize) {
        const batch = fileData.slice(i, Math.min(i + batchSize, fileData.length));
        
        for (const result of batch) {
          const row = headers.map(header => {
            let value = result[header] || '';
            // 处理包含逗号或引号的字段
            if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\n'))) {
              value = '"' + value.replace(/"/g, '""') + '"';
            }
            return value;
          });
          writeStream.write(row.join(',') + '\n');
        }
        
        // 显示进度
        if ((i + batchSize) % 50000 === 0 || i + batchSize >= fileData.length) {
          console.log(`  已写入 ${Math.min(i + batchSize, fileData.length).toLocaleString()}/${fileData.length.toLocaleString()} 条记录...`);
        }
        
        // 每批处理后强制垃圾回收
        if (global.gc && i % (batchSize * 4) === 0) {
          global.gc();
        }
      }
      
      writeStream.end();
      outputFileList.push({
        fileName: splitFileName,
        filePath: splitFilePath,
        recordCount: recordsInFile,
        startIndex: startIndex + 1,
        endIndex: endIndex
      });
      
      console.log(`✅ 文件 ${splitFileName} 写入完成`);
    }
    
    // 输出汇总信息
    console.log(`\n📄 CSV文件生成完成！`);
    console.log(`📁 输出目录: ${dir}`);
    console.log(`📊 文件详情:`);
    outputFileList.forEach((file, index) => {
      console.log(`  ${index + 1}. ${file.fileName}`);
      console.log(`     记录数: ${file.recordCount.toLocaleString()}`);
      console.log(`     范围: ${file.startIndex.toLocaleString()} - ${file.endIndex.toLocaleString()}`);
    });
    console.log(`📈 总记录数: ${results.length.toLocaleString()}`);
    
    return outputFileList;
    
  } catch (error) {
    console.error(`写入CSV文件时出错:`, error);
    throw error;
  }
}

/**
 * 写入统计报告
 * @param {Object} stats 统计信息
 * @param {string} outputDir 输出目录
 */
function writeStatsReport(stats, outputDir) {
  try {
    const reportPath = path.join(outputDir, 'merge_statistics.txt');
    
    const report = `
代币持有者聚合统计报告
====================================
生成时间: ${new Date().toLocaleString()}

数据概况:
- 原始记录数: ${stats.originalRecords.toLocaleString()}
- 唯一地址数: ${stats.uniqueAddresses.toLocaleString()}
  - 代币合约数: ${stats.totalTokens}
- 数据压缩率: ${((1 - stats.uniqueAddresses / stats.originalRecords) * 100).toFixed(2)}%

积分统计:
- 平均积分: ${stats.averageScore}
- 最高积分: ${stats.maxScore}
- 最低积分: ${stats.minScore}

积分分布:
- 1-2分: ${stats.scoreDistribution['1-2']} 个地址 (${(stats.scoreDistribution['1-2'] / stats.uniqueAddresses * 100).toFixed(2)}%)
- 3-4分: ${stats.scoreDistribution['3-4']} 个地址 (${(stats.scoreDistribution['3-4'] / stats.uniqueAddresses * 100).toFixed(2)}%)
- 5-6分: ${stats.scoreDistribution['5-6']} 个地址 (${(stats.scoreDistribution['5-6'] / stats.uniqueAddresses * 100).toFixed(2)}%)
- 7分以上: ${stats.scoreDistribution['7+']} 个地址 (${(stats.scoreDistribution['7+'] / stats.uniqueAddresses * 100).toFixed(2)}%)

处理规则:
- 排除了包含以下关键词的地址标签: ${EXCLUSION_KEYWORDS.join(', ')}
- 每个地址最多聚合前${CONFIG.MAX_SCORE_COUNT}个最高积分
- 按总积分降序排列

说明:
- 已过滤掉交易所、协议合约、多签钱包等非真人地址
- 保留了个人钱包和未标记的地址
- 积分聚合避免了重复计算问题
====================================
    `;
    
    fs.writeFileSync(reportPath, report);
    console.log(`统计报告已保存到: ${reportPath}`);
  } catch (error) {
    console.error(`写入统计报告时出错:`, error);
  }
}

/**
 * 获取目录中的所有CSV文件
 * @param {string} dirPath 目录路径
 * @returns {Array} CSV文件路径列表
 */
function getAllCSVFiles(dirPath) {
  try {
    if (!fs.existsSync(dirPath)) {
      console.error(`错误: 目录 ${dirPath} 不存在`);
      return [];
    }
    
    const files = fs.readdirSync(dirPath);
    return files
      .filter(file => file.toLowerCase().endsWith('.csv'))
      .map(file => path.join(dirPath, file));
  } catch (err) {
    console.error(`读取目录时出错:`, err);
    return [];
  }
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

/**
 * 保存中间结果到临时文件
 * @param {Array} allRecords 所有已处理的记录
 * @param {Array} processedFiles 已处理的文件列表
 * @param {string} tempDir 临时目录
 */
function saveIntermediateResults(allRecords, processedFiles, tempDir) {
  try {
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }
    
    const tempResultPath = path.join(tempDir, 'intermediate_records.json');
    const processedFilesPath = path.join(tempDir, 'processed_files.json');
    
    // 保存记录（分批写入避免内存问题）
    console.log(`保存中间结果: ${allRecords.length} 条记录...`);
    fs.writeFileSync(tempResultPath, JSON.stringify(allRecords, null, 2));
    
    // 保存已处理文件列表
    fs.writeFileSync(processedFilesPath, JSON.stringify(processedFiles, null, 2));
    
    console.log(`中间结果已保存，共 ${allRecords.length} 条记录，${processedFiles.length} 个已处理文件`);
  } catch (error) {
    console.error(`保存中间结果时出错:`, error);
  }
}

/**
 * 加载中间结果
 * @param {string} tempDir 临时目录
 * @returns {Object} 加载的结果 {allRecords, processedFiles}
 */
function loadIntermediateResults(tempDir) {
  try {
    const tempResultPath = path.join(tempDir, 'intermediate_records.json');
    const processedFilesPath = path.join(tempDir, 'processed_files.json');
    
    if (fs.existsSync(tempResultPath) && fs.existsSync(processedFilesPath)) {
      console.log('发现中间结果文件，正在加载...');
      const allRecords = JSON.parse(fs.readFileSync(tempResultPath, 'utf8'));
      const processedFiles = JSON.parse(fs.readFileSync(processedFilesPath, 'utf8'));
      
      console.log(`已加载中间结果: ${allRecords.length} 条记录，${processedFiles.length} 个已处理文件`);
      return { allRecords, processedFiles };
    }
  } catch (error) {
    console.error(`加载中间结果时出错:`, error);
  }
  
  return { allRecords: [], processedFiles: [] };
}

/**
 * 清理中间文件
 * @param {string} tempDir 临时目录
 */
function cleanupIntermediateFiles(tempDir) {
  try {
    const tempResultPath = path.join(tempDir, 'intermediate_records.json');
    const processedFilesPath = path.join(tempDir, 'processed_files.json');
    
    if (fs.existsSync(tempResultPath)) {
      fs.unlinkSync(tempResultPath);
    }
    
    if (fs.existsSync(processedFilesPath)) {
      fs.unlinkSync(processedFilesPath);
    }
    
    // 删除临时目录（如果为空）
    try {
      fs.rmdirSync(tempDir);
      console.log('已清理临时文件');
    } catch (e) {
      // 目录不为空或其他错误，忽略
    }
  } catch (err) {
    console.error('清理临时文件时出错:', err);
  }
}

/**
 * 主函数
 */
async function main() {
  console.time('总耗时');
  console.log('===============================================');
  console.log('开始合并代币持有者CSV文件');
  console.log('===============================================\n');
  
  try {
    // 获取命令行参数
    const args = process.argv.slice(2);
    
    let inputDir;
    let outputPath;
    
    if (args.length >= 1) {
      inputDir = args[0];
      outputPath = args[1] || path.join(path.dirname(inputDir), 'merged_token_holders.csv');
    } else {
      // 使用默认路径
      inputDir = path.join(__dirname, '../token/token-owners/liquidity_qualified_arb-alltoken');
      outputPath = path.join(__dirname, '../token/merge-owners/merged_token_holders.csv');
      console.log(`使用默认输入目录: ${inputDir}`);
    }
    
    console.log(`输入目录: ${inputDir}`);
    console.log(`输出文件: ${outputPath}`);
    
    // 检查输入目录是否存在
    if (!fs.existsSync(inputDir)) {
      console.error(`错误: 输入目录不存在: ${inputDir}`);
      process.exit(1);
    }
    
    // 创建临时目录
    const tempDir = path.join(__dirname, '../token/merge-owners/temp/merge_token_holders');
    
    // 获取所有CSV文件
    const csvFiles = getAllCSVFiles(inputDir);
    
    if (csvFiles.length === 0) {
      console.error(`错误: 在目录 ${inputDir} 中未找到CSV文件`);
      process.exit(1);
    }
    
    console.log(`找到 ${csvFiles.length} 个CSV文件`);
    
    // 启用内存监控
    const memoryInterval = setInterval(logMemoryUsage, 30000);
    
    // 尝试加载中间结果
    let { allRecords, processedFiles } = loadIntermediateResults(tempDir);
    
    // 过滤掉已处理的文件
    const remainingFiles = csvFiles.filter(file => 
      !processedFiles.includes(path.basename(file))
    );
    
    console.log(`需要处理 ${remainingFiles.length} 个文件 (已处理 ${processedFiles.length} 个文件)`);
    
    // 分批处理文件
    for (let i = 0; i < remainingFiles.length; i++) {
      const csvFile = remainingFiles[i];
      const fileName = path.basename(csvFile);
      
      console.log(`[${csvFiles.indexOf(csvFile) + 1}/${csvFiles.length}] 处理文件: ${fileName}`);
      
      try {
        const records = await readCSVFile(csvFile);
        
        // 避免使用展开运算符导致调用栈溢出，改用循环添加或concat方法
        if (records.length > 100000) {
          // 对于大文件，分批添加
          const batchSize = 10000;
          for (let j = 0; j < records.length; j += batchSize) {
            const batch = records.slice(j, Math.min(j + batchSize, records.length));
            allRecords = allRecords.concat(batch);
          }
          console.log(`大文件 ${fileName} 已分批添加到记录集合中`);
        } else {
          // 对于小文件，使用concat方法
          allRecords = allRecords.concat(records);
        }
        
        processedFiles.push(fileName);
        
        // 每处理几个文件保存一次中间结果
        if ((i + 1) % CONFIG.FILE_BATCH_SIZE === 0 || i === remainingFiles.length - 1) {
          saveIntermediateResults(allRecords, processedFiles, tempDir);
          
          // 强制垃圾回收
          if (global.gc) {
            global.gc();
          }
        }
        
      } catch (error) {
        console.error(`处理文件 ${fileName} 时出错:`, error.message);
        // 保存当前进度后继续
        saveIntermediateResults(allRecords, processedFiles, tempDir);
      }
    }
    
    console.log(`\n所有文件读取完成，共 ${allRecords.length} 条有效记录`);
    console.log('开始聚合地址数据...');
    
    // 聚合地址数据
    const aggregatedResults = aggregateAddressData(allRecords);
    
    // 生成统计信息
    const stats = generateStats(allRecords, aggregatedResults, processedFiles);
    
    // 写入结果文件
    writeResultsToCSV(aggregatedResults, outputPath, stats);
    
    // 写入统计报告
    const outputDir = path.dirname(outputPath);
    writeStatsReport(stats, outputDir);
    
    // 清理临时文件
    cleanupIntermediateFiles(tempDir);
    
    // 清理内存监控
    clearInterval(memoryInterval);
    
    console.log('\n===============================================');
    console.log('合并完成! 📊 统计信息:');
    console.log(`📁 处理文件数: ${csvFiles.length}`);
    console.log(`📝 原始记录数: ${stats.originalRecords.toLocaleString()}`);
    console.log(`👤 唯一地址数: ${stats.uniqueAddresses.toLocaleString()}`);
    console.log(`🪙 代币合约数: ${stats.totalTokens}`);
    console.log(`⭐ 平均积分: ${stats.averageScore}`);
    console.log(`🏆 最高积分: ${stats.maxScore}`);
    console.log(`📊 数据压缩率: ${((1 - stats.uniqueAddresses / stats.originalRecords) * 100).toFixed(2)}%`);
    console.log('===============================================');
    
  } catch (error) {
    console.error('程序执行出错:', error);
    // 如果出错，不要删除临时文件，便于调试和恢复
    process.exit(1);
  }
  
  console.timeEnd('总耗时');
}

// 错误处理
process.on('unhandledRejection', (reason, promise) => {
  console.error('未处理的Promise异常:', reason);
});

process.on('uncaughtException', (error) => {
  console.error('未捕获的异常:', error);
  process.exit(1);
});

// 主函数 - 处理命令行参数
async function cli() {
  const args = process.argv.slice(2);
  
  if (args.length > 0 && (args[0] === '--help' || args[0] === '-h')) {
    console.log(`
使用说明:
  node merge_token_holders.js [input_dir] [output_file]

参数:
  input_dir    - 包含代币持有者CSV文件的目录 (默认: ../token/token-owners/liquidity_qualified_arb-alltoken)
  output_file  - 输出的合并CSV文件路径 (默认: ../token/merge-owners/merged_token_holders.csv)

功能:
  1. 合并多个代币持有者CSV文件
  2. 排除非真人地址（交易所、协议合约、多签钱包等）
  3. 聚合地址积分（每个地址取前4个最高积分）
  4. 生成统计报告

示例:
  node merge_token_holders.js
  node merge_token_holders.js ./token-data ./output/merged.csv
    `);
    return;
  }

  try {
    await main();
  } catch (error) {
    console.error('程序执行出错:', error);
    process.exit(1);
  }
}

// 如果直接运行此脚本
if (require.main === module) {
  cli().catch(console.error);
}

module.exports = {
  shouldExcludeAddress,
  extractTokenSymbol,
  readCSVFile,
  aggregateAddressData,
  generateStats,
  getAllCSVFiles
};