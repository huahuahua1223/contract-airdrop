// 获取代币持有者人数脚本
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

// 配置参数
const CONFIG = {
  // 批处理大小
  BATCH_SIZE: 100,
  // 重试次数
  MAX_RETRIES: 3,
  // 重试延迟（毫秒）
  RETRY_DELAY: 2000,
  // 请求超时（毫秒）
  REQUEST_TIMEOUT: 15000,
  // 批次间延迟（毫秒）
  BATCH_DELAY: 1500,
  // 中间结果保存间隔（处理多少条代币后保存一次）
  SAVE_INTERVAL: 20,
  // 最小持有者数量阈值
  MIN_HOLDERS_THRESHOLD: 20000
};

// CSV输出列标题
const CSV_HEADER = ['contract_address', 'symbol', 'decimals', 'name', 'totalHolders'];

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
 * 读取CSV文件中的代币合约地址
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
        delimiter: [',', '\t'],
        from_line: 2, // 跳过标题行
        columns: false, // 不使用列名，直接使用索引
        relax_column_count: true, // 允许列数不一致
        skip_empty_lines: true, // 跳过空行
        trim: true // 自动去除字段前后空格
      }));
    
    for await (const record of parser) {
      if (record.length > 0 && record[0]) {
        const contractAddress = record[0].trim();
        
        // 验证合约地址格式
        if (contractAddress.startsWith('0x') && contractAddress.length === 42) {
          // 根据实际CSV格式调整字段映射: contract_address,symbol,decimals,name
          const tokenInfo = {
            contract_address: contractAddress.toLowerCase(),
            symbol: record[1] ? record[1].trim() : '',
            decimals: record[2] ? record[2].trim() : '',
            name: record[3] ? record[3].trim() : ''
          };
          
          tokens.push(tokenInfo);
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
 * 获取代币持有者信息
 * @param {string} contractAddress 代币合约地址
 * @returns {Promise<Object>} 持有者信息
 */
async function fetchTokenHolders(contractAddress) {
  try {
    const url = `https://deep-index.moralis.io/api/v2.2/erc20/${contractAddress}/holders?chain=arbitrum`;
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
      return { totalHolders: 0 };
    }
    
    console.error(`获取代币 ${contractAddress} 的持有者信息时出错:`, error.message);
    throw error;
  }
}

/**
 * 处理单个代币
 * @param {Object} tokenInfo 代币信息
 * @returns {Promise<Object>} 处理结果
 */
async function processToken(tokenInfo) {
  try {
    // 使用重试机制获取持有者信息
    const holdersData = await withRetry(fetchTokenHolders, [tokenInfo.contract_address]);
    
    // 提取总持有者数量
    const totalHolders = holdersData.totalHolders || 0;
    
    // 构建结果对象
    const result = {
      contract_address: tokenInfo.contract_address,
      symbol: tokenInfo.symbol,
      decimals: tokenInfo.decimals,
      name: tokenInfo.name,
      totalHolders: totalHolders
    };
    
    console.log(`代币 ${tokenInfo.contract_address} (${tokenInfo.symbol}): ${totalHolders} 个持有者`);
    
    return result;
  } catch (error) {
    console.error(`处理代币 ${tokenInfo.contract_address} 时出错:`, error.message);
    
    // 返回空结果
    return {
      contract_address: tokenInfo.contract_address,
      symbol: tokenInfo.symbol,
      decimals: tokenInfo.decimals,
      name: tokenInfo.name,
      totalHolders: 0
    };
  }
}

/**
 * 筛选持有者数量超过阈值的代币
 * @param {Array} results 处理结果
 * @param {number} threshold 最小持有者数量阈值
 * @returns {Array} 筛选后的结果
 */
function filterTokensByHolders(results, threshold = CONFIG.MIN_HOLDERS_THRESHOLD) {
  const filtered = results.filter(token => token.totalHolders >= threshold);
  
  console.log(`\n筛选结果: ${filtered.length}/${results.length} 个代币的持有者数量超过 ${threshold} 人`);
  
  // 按持有者数量降序排列
  filtered.sort((a, b) => b.totalHolders - a.totalHolders);
  
  return filtered;
}

/**
 * 将结果写入CSV文件
 * @param {Array} results 处理结果
 * @param {string} outputPath 输出路径
 * @param {boolean} onlyFiltered 是否只保存筛选后的结果
 */
function writeResultsToCSV(results, outputPath, onlyFiltered = false) {
  try {
    // 确保输出目录存在
    const dir = path.dirname(outputPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    
    let dataToWrite = results;
    
    if (onlyFiltered) {
      // 只保存超过阈值的代币
      dataToWrite = filterTokensByHolders(results);
    }
    
    // 将结果转换为CSV字符串
    const csv = stringify(dataToWrite, { header: true, columns: CSV_HEADER });
    
    // 写入文件
    fs.writeFileSync(outputPath, csv);
    
    console.log(`结果已保存到: ${outputPath} (共 ${dataToWrite.length} 条记录)`);
  } catch (error) {
    console.error(`写入CSV文件时出错:`, error);
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
    const tempDir = path.join(__dirname, '../token/token-holders/temp');
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }
    
    const tempResultPath = path.join(tempDir, `holders_temp_results_${fileIdentifier}.json`);
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
    const tempDir = path.join(__dirname, '../token/token-holders/temp');
    const tempResultPath = path.join(tempDir, `holders_temp_results_${fileIdentifier}.json`);
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
    const tempDir = path.join(__dirname, '../token/token-holders/temp');
    const tempResultPath = path.join(tempDir, `holders_temp_results_${fileIdentifier}.json`);
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
    // 构建输出文件路径
    const outputDir = path.join(__dirname, '../token/token-holders');
    const allResultsPath = path.join(outputDir, `token_holders_all_${fileIdentifier}.csv`);
    const filteredResultsPath = path.join(outputDir, `token_holders_filtered_${fileIdentifier}.csv`);
    
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
      // 确保最终结果已写入
      if (results.length > 0) {
        writeResultsToCSV(results, allResultsPath, false);
        writeResultsToCSV(results, filteredResultsPath, true);
      }
      return true;
    }
    
    // 设置并发限制
    const BATCH_SIZE = CONFIG.BATCH_SIZE;
    
    // 分批处理代币
    for (let i = 0; i < tokensToProcess.length; i += BATCH_SIZE) {
      const batch = tokensToProcess.slice(i, i + BATCH_SIZE);
      const batchPromises = batch.map(processToken);
      
      console.log(`处理批次 ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(tokensToProcess.length / BATCH_SIZE)}`);
      
      try {
        // 等待批次完成
        const batchResults = await Promise.all(batchPromises);
        results.push(...batchResults);
        
        // 更新已处理代币列表
        processedTokens.push(...batch);
        
        // 简单的进度报告
        console.log(`已完成 ${processedTokens.length}/${allTokens.length} 个代币`);
        
        // 每处理一定数量的代币后保存中间结果
        if (processedTokens.length % CONFIG.SAVE_INTERVAL === 0 || 
            i + BATCH_SIZE >= tokensToProcess.length) {
          saveIntermediateResults(results, processedTokens, fileIdentifier);
        }
        
        // 简单的延迟以避免API限制
        if (i + BATCH_SIZE < tokensToProcess.length) {
          await sleep(CONFIG.BATCH_DELAY);
        }
      } catch (error) {
        console.error(`处理批次时出错:`, error);
        // 保存当前进度
        saveIntermediateResults(results, processedTokens, fileIdentifier);
        // 延迟后继续
        await sleep(CONFIG.RETRY_DELAY * 2);
      }
    }
    
    // 写入最终结果
    writeResultsToCSV(results, allResultsPath, false); // 所有结果
    writeResultsToCSV(results, filteredResultsPath, true); // 筛选后的结果
    
    console.log(`\n============================================================`);
    console.log(`文件 ${csvPath} 处理完成，共处理了 ${processedTokens.length}/${allTokens.length} 个代币`);
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
      console.log('未提供CSV文件路径，使用默认文件: ../token/arb-alltoken.csv');
      const defaultPath = path.join(__dirname, '../token/arb-alltoken.csv');
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
  fetchTokenHolders,
  processToken,
  filterTokensByHolders,
  withRetry,
  processCSVFile
};