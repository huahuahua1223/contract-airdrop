// 获取代币持有者详细信息及价值分级脚本
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify/sync');
require('dotenv').config();

// API密钥配置
const API_KEY = process.env.MORALIS_API_KEY;
if (!API_KEY) {
  console.error('错误: 未设置MORALIS_API_KEY环境变量，请在.env文件中添加');
  process.exit(1);
}

// 价值分级标准（美元）
const VALUE_TIERS = [
  { min: 100, max: 499, score: 1 },
  { min: 500, max: 999, score: 2 },
  { min: 1000, max: 4999, score: 3 },
  { min: 5000, max: 9999, score: 4 },
  { min: 10000, max: 49999, score: 5 },
  { min: 50000, max: 999999, score: 6 },
  { min: 1000000, max: Infinity, score: 7 }
];

// 特殊代币配置
const SPECIAL_TOKENS = {
  // Unishop.ai 代币的特殊价格
  '0x999999990237e901c537bbd768e09562be02efa5': {
    pricePerToken: 20,
    minBalance: 5 // 最小持有数量阈值（5个代币 = $100）
  }
};

// 配置参数
const CONFIG = {
  // 每次API请求的页面大小
  PAGE_SIZE: 100,
  // 重试次数
  MAX_RETRIES: 3,
  // 重试延迟（毫秒）
  RETRY_DELAY: 2000,
  // 请求超时（毫秒）
  REQUEST_TIMEOUT: 20000,
  // 请求间延迟（毫秒）
  REQUEST_DELAY: 1000,
  // 中间结果保存间隔（处理多少个代币后保存一次）
  SAVE_INTERVAL: 5,
  // 最小USD价值阈值
  MIN_USD_VALUE: 100
};

// CSV输出列标题
const CSV_HEADER = ['owner_address', 'owner_address_label', 'usd_value', 'balance_formatted', 'value_score'];

/**
 * 休眠函数
 * @param {number} ms 毫秒
 * @returns {Promise} Promise
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * 从文件名获取文件标识符
 * @param {string} filePath 文件路径
 * @returns {string} 文件标识符
 */
function getFileIdentifier(filePath) {
  const fileName = path.basename(filePath, path.extname(filePath));
  return fileName;
}

/**
 * 读取CSV文件中的代币信息
 * @param {string} csvPath CSV文件路径
 * @returns {Promise<Array>} 代币信息列表
 */
async function readTokensFromCSV(csvPath) {
  const tokens = [];
  
  try {
    if (!fs.existsSync(csvPath)) {
      console.error(`错误: 文件 ${csvPath} 不存在`);
      return [];
    }
    
    console.log(`正在读取文件: ${csvPath}`);
    
    const parser = fs
      .createReadStream(csvPath)
      .pipe(parse({
        delimiter: ',',
        columns: true, // 使用标题行作为列名
        relax_column_count: true,
        skip_empty_lines: true,
        trim: true
      }));
    
    for await (const record of parser) {
      if (record.contract_address) {
        const contractAddress = record.contract_address.trim().toLowerCase();
        
        // 验证合约地址格式
        if (contractAddress.startsWith('0x') && contractAddress.length === 42) {
          tokens.push({
            contract_address: contractAddress,
            symbol: record.symbol || '',
            name: record.name || '',
            decimals: record.decimals || '',
            totalHolders: record.totalHolders || 0
          });
        }
      }
    }
    
    console.log(`共找到 ${tokens.length} 个有效的代币合约地址`);
    return tokens;
  } catch (err) {
    console.error(`读取CSV文件时出错:`, err);
    return [];
  }
}

/**
 * 计算实际USD价值（处理特殊代币）
 * @param {string} contractAddress 合约地址
 * @param {number} balanceFormatted 格式化余额
 * @param {number} originalUsdValue API返回的原始USD价值
 * @returns {number} 实际USD价值
 */
function calculateActualUsdValue(contractAddress, balanceFormatted, originalUsdValue) {
  const specialToken = SPECIAL_TOKENS[contractAddress.toLowerCase()];
  if (specialToken) {
    return balanceFormatted * specialToken.pricePerToken;
  }
  return originalUsdValue;
}

/**
 * 检查是否符合最小价值阈值（处理特殊代币）
 * @param {string} contractAddress 合约地址
 * @param {number} balanceFormatted 格式化余额
 * @param {number} originalUsdValue API返回的原始USD价值
 * @returns {boolean} 是否符合阈值
 */
function meetsMinValueThreshold(contractAddress, balanceFormatted, originalUsdValue) {
  const specialToken = SPECIAL_TOKENS[contractAddress.toLowerCase()];
  if (specialToken) {
    return balanceFormatted >= specialToken.minBalance;
  }
  return originalUsdValue >= CONFIG.MIN_USD_VALUE;
}

/**
 * 根据USD价值计算分数
 * @param {number} usdValue USD价值
 * @returns {number} 分数 (1-7)
 */
function calculateValueScore(usdValue) {
  for (const tier of VALUE_TIERS) {
    if (usdValue >= tier.min && usdValue <= tier.max) {
      return tier.score;
    }
  }
  return 0; // 低于最小阈值
}

/**
 * 重试包装器
 * @param {Function} fn 需要重试的异步函数
 * @param {Array} args 函数参数
 * @param {number} maxRetries 最大重试次数
 * @param {number} delay 重试延迟（毫秒）
 * @returns {Promise} 函数结果
 */
async function withRetry(fn, args, maxRetries = CONFIG.MAX_RETRIES, delay = CONFIG.RETRY_DELAY) {
  let retries = 0;
  
  while (true) {
    try {
      return await fn(...args);
    } catch (error) {
      retries++;
      if (retries > maxRetries) {
        throw error;
      }
      
      console.warn(`操作失败，第 ${retries}/${maxRetries} 次重试，延迟 ${delay}ms...`);
      await sleep(delay);
      
      // 增加重试延迟（指数退避）
      delay *= 2;
    }
  }
}

/**
 * 获取代币持有者信息（单页）
 * @param {string} contractAddress 代币合约地址
 * @param {string} cursor 分页游标
 * @returns {Promise<Object>} 持有者信息
 */
async function fetchTokenOwnersPage(contractAddress, cursor = '') {
  try {
    let url = `https://deep-index.moralis.io/api/v2.2/erc20/${contractAddress}/owners?chain=arbitrum&order=DESC&limit=${CONFIG.PAGE_SIZE}`;
    
    if (cursor) {
      url += `&cursor=${cursor}`;
    }
    
    const response = await axios.get(url, {
      headers: {
        'accept': 'application/json',
        'X-API-Key': API_KEY
      },
      timeout: CONFIG.REQUEST_TIMEOUT
    });
    
    return response.data;
  } catch (error) {
    // 检查是否为速率限制错误（429）
    if (error.response && error.response.status === 429) {
      console.warn(`获取代币 ${contractAddress} 的持有者信息时被限流，等待更长时间后重试...`);
      await sleep(CONFIG.RETRY_DELAY * 3);
      throw new Error('Rate limited');
    }
    
    // 检查是否为404错误（代币不存在或无效）
    if (error.response && error.response.status === 404) {
      console.warn(`代币 ${contractAddress} 不存在或无效`);
      return { result: [], cursor: null };
    }
    
    console.error(`获取代币 ${contractAddress} 的持有者信息时出错:`, error.message);
    throw error;
  }
}

/**
 * 获取代币所有持有者信息（处理分页）
 * @param {string} contractAddress 代币合约地址
 * @returns {Promise<Array>} 所有持有者信息
 */
async function fetchAllTokenOwners(contractAddress) {
  const allOwners = [];
  let cursor = '';
  let page = 1;
  let hasMorePages = true;
  
  // 检查是否是特殊代币
  const isSpecialToken = SPECIAL_TOKENS[contractAddress.toLowerCase()];
  if (isSpecialToken) {
    console.log(`开始获取特殊代币 ${contractAddress} 的持有者信息（自定义价格: $${isSpecialToken.pricePerToken}/代币，最小阈值: ${isSpecialToken.minBalance}个代币）...`);
  } else {
    console.log(`开始获取代币 ${contractAddress} 的持有者信息...`);
  }
  
  while (hasMorePages) {
    try {
      console.log(`  正在获取第 ${page} 页...`);
      
      // 使用重试机制获取页面数据
      const pageData = await withRetry(fetchTokenOwnersPage, [contractAddress, cursor]);
      
      if (pageData.result && Array.isArray(pageData.result)) {
        let hasLowValueOwner = false;
        
        // 筛选符合条件的持有者
        const qualifiedOwners = pageData.result.filter(owner => {
          // 必须不是合约地址
          if (owner.is_contract === true) {
            return false;
          }
          
          // 获取余额和原始USD价值
          const balanceFormatted = parseFloat(owner.balance_formatted || 0);
          const originalUsdValue = parseFloat(owner.usd_value || 0);
          
          // 检查是否符合最小价值阈值（处理特殊代币）
          const meetsThreshold = meetsMinValueThreshold(contractAddress, balanceFormatted, originalUsdValue);
          
          // 如果不符合阈值，标记为需要停止分页
          if (!meetsThreshold) {
            hasLowValueOwner = true;
            return false;
          }
          
          return true;
        }).map(owner => {
          const balanceFormatted = parseFloat(owner.balance_formatted || 0);
          const originalUsdValue = parseFloat(owner.usd_value || 0);
          
          // 计算实际USD价值（处理特殊代币）
          const actualUsdValue = calculateActualUsdValue(contractAddress, balanceFormatted, originalUsdValue);
          
          return {
            owner_address: owner.owner_address,
            owner_address_label: owner.owner_address_label || '',
            usd_value: actualUsdValue,
            balance_formatted: owner.balance_formatted || '',
            value_score: calculateValueScore(actualUsdValue)
          };
        });
        
        allOwners.push(...qualifiedOwners);
        console.log(`    第 ${page} 页找到 ${qualifiedOwners.length} 个符合条件的持有者`);
        
        // 如果当前页面有价值小于阈值的持有者，停止分页
        if (hasLowValueOwner) {
          if (isSpecialToken) {
            console.log(`    发现余额小于${isSpecialToken.minBalance}个代币的持有者，停止获取后续页面`);
          } else {
            console.log(`    发现USD价值小于${CONFIG.MIN_USD_VALUE}的持有者，停止获取后续页面`);
          }
          hasMorePages = false;
          break;
        }
      }
      
      // 检查是否有下一页
      if (pageData.cursor && pageData.cursor.trim() !== '') {
        cursor = pageData.cursor;
        page++;
        
        // 添加请求间延迟
        await sleep(CONFIG.REQUEST_DELAY);
      } else {
        hasMorePages = false;
      }
      
    } catch (error) {
      console.error(`获取第 ${page} 页时出错:`, error.message);
      
      // 如果是速率限制，等待更长时间后继续
      if (error.message === 'Rate limited') {
        await sleep(CONFIG.RETRY_DELAY * 2);
        continue;
      }
      
      // 其他错误则跳出循环
      break;
    }
  }
  
  console.log(`代币 ${contractAddress} 共找到 ${allOwners.length} 个符合条件的持有者`);
  return allOwners;
}

/**
 * 处理单个代币
 * @param {Object} tokenInfo 代币信息
 * @returns {Promise<Object>} 处理结果
 */
async function processToken(tokenInfo) {
  try {
    console.log(`\n处理代币: ${tokenInfo.contract_address} (${tokenInfo.symbol})`);
    
    // 获取所有符合条件的持有者
    const owners = await fetchAllTokenOwners(tokenInfo.contract_address);
    
    // 按USD价值降序排列
    owners.sort((a, b) => b.usd_value - a.usd_value);
    
    return {
      tokenInfo,
      owners,
      totalQualifiedOwners: owners.length
    };
  } catch (error) {
    console.error(`处理代币 ${tokenInfo.contract_address} 时出错:`, error.message);
    
    return {
      tokenInfo,
      owners: [],
      totalQualifiedOwners: 0
    };
  }
}

/**
 * 将单个代币的结果写入CSV文件
 * @param {Object} result 处理结果
 * @param {string} outputDir 输出目录
 */
function writeTokenResultToCSV(result, outputDir) {
  try {
    // 确保输出目录存在
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
    
    const { tokenInfo, owners } = result;
    // 清理符号中的非法文件名字符
    const cleanSymbol = tokenInfo.symbol.replace(/[<>:"/\\|?*]/g, '_');
    const fileName = `${cleanSymbol}_${tokenInfo.contract_address.slice(0, 8)}_owners.csv`;
    const outputPath = path.join(outputDir, fileName);
    
    // 添加代币信息作为注释
    let csvContent = `# 代币信息: ${tokenInfo.symbol} (${tokenInfo.name})\n`;
    csvContent += `# 合约地址: ${tokenInfo.contract_address}\n`;
    csvContent += `# 符合条件的持有者总数: ${owners.length}\n`;
    csvContent += `# 生成时间: ${new Date().toISOString()}\n\n`;
    
    // 添加CSV数据
    if (owners.length > 0) {
      const csv = stringify(owners, { header: true, columns: CSV_HEADER });
      csvContent += csv;
    } else {
      // 如果没有数据，至少写入标题行
      csvContent += CSV_HEADER.join(',') + '\n';
    }
    
    // 写入文件
    fs.writeFileSync(outputPath, csvContent);
    
    console.log(`代币 ${tokenInfo.symbol} 的结果已保存到: ${outputPath}`);
  } catch (error) {
    console.error(`写入代币 ${tokenInfo.symbol} 的CSV文件时出错:`, error);
  }
}

/**
 * 保存中间结果到临时文件
 * @param {Array} results 已处理的结果
 * @param {Array} processedTokens 已处理的代币
 * @param {string} fileIdentifier 文件标识符
 */
function saveIntermediateResults(results, processedTokens, fileIdentifier) {
  try {
    // 确保输出目录存在
    const tempDir = path.join(__dirname, '../token/token-owners/temp');
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }
    
    const tempResultPath = path.join(tempDir, `owners_temp_results_${fileIdentifier}.json`);
    const processedTokensPath = path.join(tempDir, `processed_tokens_${fileIdentifier}.json`);
    
    // 保存结果
    fs.writeFileSync(tempResultPath, JSON.stringify(results, null, 2));
    
    // 保存已处理的代币
    fs.writeFileSync(processedTokensPath, JSON.stringify(processedTokens, null, 2));
    
    console.log(`中间结果已保存，共处理了 ${processedTokens.length} 个代币`);
  } catch (error) {
    console.error(`保存中间结果时出错:`, error);
  }
}

/**
 * 加载中间结果
 * @param {string} fileIdentifier 文件标识符
 * @returns {Object} 加载的结果，包含results和processedTokens
 */
function loadIntermediateResults(fileIdentifier) {
  try {
    const tempDir = path.join(__dirname, '../token/token-owners/temp');
    const tempResultPath = path.join(tempDir, `owners_temp_results_${fileIdentifier}.json`);
    const processedTokensPath = path.join(tempDir, `processed_tokens_${fileIdentifier}.json`);
    
    if (fs.existsSync(tempResultPath) && fs.existsSync(processedTokensPath)) {
      const results = JSON.parse(fs.readFileSync(tempResultPath, 'utf8'));
      const processedTokens = JSON.parse(fs.readFileSync(processedTokensPath, 'utf8'));
      
      console.log(`已加载中间结果，共 ${results.length} 条记录，${processedTokens.length} 个已处理代币`);
      return { results, processedTokens };
    }
  } catch (error) {
    console.error(`加载中间结果时出错:`, error);
  }
  
  return { results: [], processedTokens: [] };
}

/**
 * 清理中间文件
 * @param {string} fileIdentifier 文件标识符
 */
function cleanupIntermediateFiles(fileIdentifier) {
  try {
    const tempDir = path.join(__dirname, '../token/token-owners/temp');
    const tempResultPath = path.join(tempDir, `owners_temp_results_${fileIdentifier}.json`);
    const processedTokensPath = path.join(tempDir, `processed_tokens_${fileIdentifier}.json`);
    
    if (fs.existsSync(tempResultPath)) {
      fs.unlinkSync(tempResultPath);
    }
    
    if (fs.existsSync(processedTokensPath)) {
      fs.unlinkSync(processedTokensPath);
    }
    
    console.log('已清理中间文件');
  } catch (err) {
    console.error('清理中间文件时出错:', err);
  }
}

/**
 * 处理CSV文件
 * @param {string} csvPath CSV文件路径
 * @returns {Promise<boolean>} 处理结果，成功返回true，失败返回false
 */
async function processCSVFile(csvPath) {
  const fileIdentifier = getFileIdentifier(csvPath);
  console.log(`\n============================================================`);
  console.log(`开始处理CSV文件: ${csvPath}, 标识符: ${fileIdentifier}`);
  console.log(`============================================================\n`);
  
  try {
    // 构建输出目录
    const outputDir = path.join(__dirname, `../token/token-owners/${fileIdentifier}`);
    
    // 读取代币信息
    const allTokens = await readTokensFromCSV(csvPath);
    
    if (allTokens.length === 0) {
      console.error(`未找到有效的代币合约地址，跳过文件 ${csvPath}`);
      return false;
    }
    
    // 尝试加载中间结果
    let { results, processedTokens } = loadIntermediateResults(fileIdentifier);
    
    // 过滤掉已处理的代币
    const processedAddresses = new Set(processedTokens.map(t => t.contract_address));
    const tokensToProcess = allTokens.filter(token => !processedAddresses.has(token.contract_address));
    
    console.log(`开始处理剩余的 ${tokensToProcess.length} 个代币，总共 ${allTokens.length} 个代币...`);
    
    // 如果所有代币都已处理完毕，则直接返回成功
    if (tokensToProcess.length === 0) {
      console.log(`文件 ${csvPath} 的所有代币已经处理完毕，跳过处理...`);
      return true;
    }
    
    // 逐个处理代币（不使用并发，避免API限制）
    for (let i = 0; i < tokensToProcess.length; i++) {
      const token = tokensToProcess[i];
      
      console.log(`\n处理进度: ${i + 1}/${tokensToProcess.length}`);
      
      try {
        // 处理单个代币
        const result = await processToken(token);
        results.push(result);
        
        // 立即为每个代币写入单独的CSV文件
        writeTokenResultToCSV(result, outputDir);
        
        // 更新已处理代币列表
        processedTokens.push(token);
        
        // 每处理一定数量的代币后保存中间结果
        if (processedTokens.length % CONFIG.SAVE_INTERVAL === 0 || 
            i === tokensToProcess.length - 1) {
          saveIntermediateResults(results, processedTokens, fileIdentifier);
        }
        
        // 简单的延迟以避免API限制
        if (i < tokensToProcess.length - 1) {
          console.log(`等待 ${CONFIG.REQUEST_DELAY}ms 后继续下一个代币...`);
          await sleep(CONFIG.REQUEST_DELAY);
        }
      } catch (error) {
        console.error(`处理代币 ${token.contract_address} 时出错:`, error);
        // 保存当前进度并继续
        saveIntermediateResults(results, processedTokens, fileIdentifier);
        // 延迟后继续
        await sleep(CONFIG.RETRY_DELAY);
      }
    }
    
    console.log(`\n============================================================`);
    console.log(`文件 ${csvPath} 处理完成，共处理了 ${processedTokens.length}/${allTokens.length} 个代币`);
    console.log(`结果保存在目录: ${outputDir}`);
    console.log(`============================================================\n`);
    
    // 清理中间文件
    if (processedTokens.length === allTokens.length) {
      cleanupIntermediateFiles(fileIdentifier);
    }
    
    return true;
  } catch (error) {
    console.error(`处理文件 ${csvPath} 时出错:`, error);
    return false;
  }
}

/**
 * 主函数
 */
async function main() {
  console.time('总耗时');
  
  try {
    // 获取命令行参数
    const args = process.argv.slice(2);
    
    if (args.length === 0) {
      // 无参数时使用默认文件
      console.log('未提供CSV文件路径，使用默认文件: ../token/token-holders/liquidity_qualified_arb-alltoken.csv');
      const defaultPath = path.join(__dirname, '../token/token-holders/liquidity_qualified_arb-alltoken.csv');
      await processCSVFile(defaultPath);
    } else {
      // 处理指定文件
      console.log(`处理文件: ${args[0]}`);
      await processCSVFile(args[0]);
    }
  } catch (error) {
    console.error('程序执行出错:', error);
  }
  
  console.timeEnd('总耗时');
}

// 捕获未处理的 Promise 异常
process.on('unhandledRejection', (reason, promise) => {
  console.error('未处理的Promise异常:', reason);
});

// 捕获未捕获的异常
process.on('uncaughtException', (error) => {
  console.error('未捕获的异常:', error);
  process.exit(1);
});

// 执行主函数
if (require.main === module) {
  main().catch(console.error);
}

module.exports = {
  readTokensFromCSV,
  fetchTokenOwnersPage,
  fetchAllTokenOwners,
  processToken,
  calculateValueScore,
  calculateActualUsdValue,
  meetsMinValueThreshold,
  withRetry,
  processCSVFile
};