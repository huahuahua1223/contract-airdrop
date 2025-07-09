// 导入所需模块
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const readline = require('readline');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify/sync');
require('dotenv').config();

// API密钥配置
const API_KEY = process.env.MORALIS_API_KEY;
if (!API_KEY) {
  console.error('错误: 未设置MORALIS_API_KEY环境变量，请在.env文件中添加');
  process.exit(1);
}

// 目标代币列表
const TARGET_TOKENS = ['USDT', 'USDC', 'WETH', 'WBTC'];

// 代币固定价格（美元）
const TOKEN_PRICES = {
  usdt: 1,
  usdc: 1,
  weth: 2500,
  wbtc: 100000
};

// 配置参数
const CONFIG = {
  // 批处理大小
  BATCH_SIZE: 150,
  // 重试次数
  MAX_RETRIES: 3,
  // 重试延迟（毫秒）
  RETRY_DELAY: 2000,
  // 请求超时（毫秒）
  REQUEST_TIMEOUT: 10000,
  // 批次间延迟（毫秒）
  BATCH_DELAY: 1000,
  // 中间结果保存间隔（处理多少条地址后保存一次）
  SAVE_INTERVAL: 20
};

// 全局变量用于存储结果
const CSV_HEADER = ['address', 'usdt', 'usdc', 'weth', 'wbtc', 'totalValue'];

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
  // 获取文件名（不含路径和扩展名）
  const fileName = path.basename(filePath, path.extname(filePath));
  return fileName;
}

/**
 * 读取CSV文件中的地址
 * @param {string} csvPath CSV文件路径
 * @returns {Promise<string[]>} 地址列表
 */
async function readAddressesFromCSV(csvPath) {
  const addresses = new Set(); // 使用集合去重
  
  try {
    if (!fs.existsSync(csvPath)) {
      console.error(`错误: 文件 ${csvPath} 不存在`);
      return [];
    }
    
    console.log(`正在读取文件: ${csvPath}`);
    
    const parser = fs
      .createReadStream(csvPath)
      .pipe(parse({
        delimiter: [',', '\t'], // 支持逗号或制表符分隔
        from_line: 2 // 跳过标题行
      }));
    
    for await (const record of parser) {
      // 地址在第二列（索引1）
      if (record.length > 1 && record[1]) {
        const address = record[1].trim();
        if (address.startsWith('0x') && address.length === 42) {
          addresses.add(address.toLowerCase());
        }
      }
    }
    
    console.log(`共找到 ${addresses.size} 个唯一地址`);
    return Array.from(addresses);
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
 * 获取地址的DeFi持仓数据
 * @param {string} address 钱包地址
 * @returns {Promise<Object>} 持仓数据
 */
async function fetchDefiPositions(address) {
  try {
    const url = `https://deep-index.moralis.io/api/v2.2/wallets/${address}/defi/positions?chain=arbitrum`;
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
      console.warn(`获取地址 ${address} 的DeFi持仓数据时被限流，等待更长时间后重试...`);
      await sleep(CONFIG.RETRY_DELAY * 3); // 被限流时等待更长时间
      throw new Error('Rate limited');
    }
    
    console.error(`获取地址 ${address} 的DeFi持仓数据时出错:`, error.message);
    throw error;
  }
}

/**
 * 解析Uniswap持仓数据，提取目标代币
 * @param {Object} positions DeFi持仓数据
 * @returns {Object} 解析后的代币数据
 */
function parseUniswapData(positions) {
  // 初始化结果对象，存储代币余额和USD价值
  const result = {
    // 代币余额(balance_formatted)
    usdt: 0,
    usdc: 0,
    weth: 0,
    wbtc: 0,
    // 总USD价值（将使用固定价格计算）
    totalValue: 0
  };
  
  try {
    // 遍历所有协议
    for (const position of positions) {
      // 只处理Uniswap v2和v3的数据
      if (position.protocol_name === 'Uniswap v2' || position.protocol_name === 'Uniswap v3') {
        // 检查position和tokens是否存在
        if (position.position && position.position.tokens && Array.isArray(position.position.tokens)) {
          // 遍历代币
          for (const token of position.position.tokens) {
            // 只处理supplied类型的目标代币
            if (token.token_type === 'supplied' && 
                TARGET_TOKENS.includes(token.symbol.toUpperCase())) {
              
              // 获取代币符号（小写）
              const symbol = token.symbol.toLowerCase();
              
              // 存储代币余额(balance_formatted)
              const balance = parseFloat(token.balance_formatted || 0);
              result[symbol] += balance;
            }
          }
        }
      }
    }
  } catch (err) {
    console.error('解析Uniswap数据时出错:', err);
    // 继续处理，使用已经收集到的数据
  }
  
  // 使用固定价格计算总价值
  result.totalValue = 
    result.usdt * TOKEN_PRICES.usdt + 
    result.usdc * TOKEN_PRICES.usdc + 
    result.weth * TOKEN_PRICES.weth + 
    result.wbtc * TOKEN_PRICES.wbtc;
  
  return result;
}

/**
 * 处理单个地址
 * @param {string} address 钱包地址
 * @returns {Promise<Object>} 处理结果
 */
async function processAddress(address) {
//   console.log(`正在处理地址: ${address}`);
  
  try {
    // 使用重试机制获取DeFi持仓数据
    const positions = await withRetry(fetchDefiPositions, [address]);
    
    // 解析Uniswap数据
    const tokenData = parseUniswapData(positions);
    
    // 构建结果对象
    return {
      address,
      usdt: tokenData.usdt,
      usdc: tokenData.usdc,
      weth: tokenData.weth,
      wbtc: tokenData.wbtc,
      totalValue: tokenData.totalValue
    };
  } catch (error) {
    console.error(`处理地址 ${address} 时出错:`, error);
    
    // 返回空结果
    return {
      address,
      usdt: 0,
      usdc: 0,
      weth: 0,
      wbtc: 0,
      totalValue: 0
    };
  }
}

/**
 * 将结果写入CSV文件
 * @param {Array} results 处理结果
 * @param {string} outputPath 输出路径
 */
function writeResultsToCSV(results, outputPath) {
  try {
    // 确保输出目录存在
    const dir = path.dirname(outputPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    
    // 将结果转换为CSV字符串
    const csv = stringify(results, { header: true, columns: CSV_HEADER });
    
    // 写入文件
    fs.writeFileSync(outputPath, csv);
    
    console.log(`结果已保存到: ${outputPath}`);
  } catch (error) {
    console.error(`写入CSV文件时出错:`, error);
  }
}

/**
 * 保存中间结果到临时文件
 * @param {Array} results 已处理的结果
 * @param {Array} processedAddresses 已处理的地址
 * @param {string} fileIdentifier 文件标识符
 */
function saveIntermediateResults(results, processedAddresses, fileIdentifier) {
  try {
    // 确保输出目录存在
    const tempDir = path.join(__dirname, '../defi-positions/temp');
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }
    
    const tempResultPath = path.join(tempDir, `defi_temp_results_${fileIdentifier}.json`);
    const processedAddressesPath = path.join(tempDir, `processed_addresses_${fileIdentifier}.json`);
    
    // 保存结果
    fs.writeFileSync(tempResultPath, JSON.stringify(results, null, 2));
    
    // 保存已处理的地址
    fs.writeFileSync(processedAddressesPath, JSON.stringify(processedAddresses, null, 2));
    
    console.log(`中间结果已保存到 ${tempResultPath}，共处理了 ${processedAddresses.length} 个地址`);
  } catch (error) {
    console.error(`保存中间结果时出错:`, error);
  }
}

/**
 * 加载中间结果
 * @param {string} fileIdentifier 文件标识符
 * @returns {Object} 加载的结果，包含results和processedAddresses
 */
function loadIntermediateResults(fileIdentifier) {
  try {
    const tempDir = path.join(__dirname, '../defi-positions/temp');
    const tempResultPath = path.join(tempDir, `defi_temp_results_${fileIdentifier}.json`);
    const processedAddressesPath = path.join(tempDir, `processed_addresses_${fileIdentifier}.json`);
    
    if (fs.existsSync(tempResultPath) && fs.existsSync(processedAddressesPath)) {
      const results = JSON.parse(fs.readFileSync(tempResultPath, 'utf8'));
      const processedAddresses = JSON.parse(fs.readFileSync(processedAddressesPath, 'utf8'));
      
      console.log(`已加载中间结果，共 ${results.length} 条记录，${processedAddresses.length} 个已处理地址`);
      return { results, processedAddresses };
    }
  } catch (error) {
    console.error(`加载中间结果时出错:`, error);
  }
  
  return { results: [], processedAddresses: [] };
}

/**
 * 清理中间文件
 * @param {string} fileIdentifier 文件标识符
 */
function cleanupIntermediateFiles(fileIdentifier) {
  try {
    const tempDir = path.join(__dirname, '../defi-positions/temp');
    const tempResultPath = path.join(tempDir, `defi_temp_results_${fileIdentifier}.json`);
    const processedAddressesPath = path.join(tempDir, `processed_addresses_${fileIdentifier}.json`);
    
    if (fs.existsSync(tempResultPath)) {
      fs.unlinkSync(tempResultPath);
    }
    
    if (fs.existsSync(processedAddressesPath)) {
      fs.unlinkSync(processedAddressesPath);
    }
    
    console.log('已清理中间文件');
  } catch (err) {
    console.error('清理中间文件时出错:', err);
  }
}

/**
 * 处理单个CSV文件
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
    const outputPath = path.join(__dirname, `../defi-positions/defi_positions_${fileIdentifier}.csv`);
    
    // 读取地址
    const allAddresses = await readAddressesFromCSV(csvPath);
    
    if (allAddresses.length === 0) {
      console.error(`未找到有效地址，跳过文件 ${csvPath}`);
      return false;
    }
    
    // 尝试加载中间结果
    let { results, processedAddresses } = loadIntermediateResults(fileIdentifier);
    
    // 过滤掉已处理的地址
    const addressesToProcess = allAddresses.filter(addr => !processedAddresses.includes(addr));
    
    console.log(`开始处理剩余的 ${addressesToProcess.length} 个地址，总共 ${allAddresses.length} 个地址...`);
    
    // 如果所有地址都已处理完毕，则直接返回成功
    if (addressesToProcess.length === 0) {
      console.log(`文件 ${csvPath} 的所有地址已经处理完毕，跳过处理...`);
      // 确保最终结果已写入
      if (results.length > 0) {
        writeResultsToCSV(results, outputPath);
      }
      return true;
    }
    
    // 设置并发限制
    const BATCH_SIZE = CONFIG.BATCH_SIZE;
    
    // 分批处理地址
    for (let i = 0; i < addressesToProcess.length; i += BATCH_SIZE) {
      const batch = addressesToProcess.slice(i, i + BATCH_SIZE);
      const batchPromises = batch.map(processAddress);
      
      console.log(`处理批次 ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(addressesToProcess.length / BATCH_SIZE)}`);
      
      try {
        // 等待批次完成
        const batchResults = await Promise.all(batchPromises);
        results.push(...batchResults);
        
        // 更新已处理地址列表
        processedAddresses.push(...batch);
        
        // 简单的进度报告
        console.log(`已完成 ${processedAddresses.length}/${allAddresses.length} 个地址`);
        
        // 每处理一定数量的地址后保存中间结果
        if (processedAddresses.length % CONFIG.SAVE_INTERVAL === 0 || 
            i + BATCH_SIZE >= addressesToProcess.length) {
          saveIntermediateResults(results, processedAddresses, fileIdentifier);
        }
        
        // 简单的延迟以避免API限制
        if (i + BATCH_SIZE < addressesToProcess.length) {
          await sleep(CONFIG.BATCH_DELAY);
        }
      } catch (error) {
        console.error(`处理批次时出错:`, error);
        // 保存当前进度
        saveIntermediateResults(results, processedAddresses, fileIdentifier);
        // 延迟后继续
        await sleep(CONFIG.RETRY_DELAY * 2);
      }
    }
    
    // 写入最终结果
    writeResultsToCSV(results, outputPath);
    
    console.log(`\n============================================================`);
    console.log(`文件 ${csvPath} 处理完成，共处理了 ${processedAddresses.length}/${allAddresses.length} 个地址`);
    console.log(`============================================================\n`);
    
    // 清理中间文件
    if (processedAddresses.length === allAddresses.length) {
      cleanupIntermediateFiles(fileIdentifier);
    }
    
    return true;
  } catch (error) {
    console.error(`处理文件 ${csvPath} 时出错:`, error);
    return false;
  }
}

/**
 * 处理多个CSV文件
 * @param {string[]} csvPaths CSV文件路径数组
 */
async function processMultipleCSVFiles(csvPaths) {
  console.log(`\n************************************************************`);
  console.log(`开始处理 ${csvPaths.length} 个CSV文件`);
  console.log(`将按顺序依次处理：${csvPaths.join(', ')}`);
  console.log(`************************************************************\n`);
  
  let successCount = 0;
  let failCount = 0;
  
  // 记录开始时间
  const startTime = new Date();
  
  // 顺序处理每个文件
  for (let i = 0; i < csvPaths.length; i++) {
    const csvPath = csvPaths[i];
    console.log(`\n[${i + 1}/${csvPaths.length}] 处理文件: ${csvPath}`);
    
    try {
      const success = await processCSVFile(csvPath);
      if (success) {
        successCount++;
      } else {
        failCount++;
      }
      
      // 处理完一个文件后稍作休息，避免连续请求过多
      if (i < csvPaths.length - 1) {
        console.log(`文件处理完成，休息5秒后继续下一个文件...`);
        await sleep(5000);
      }
    } catch (error) {
      console.error(`处理文件 ${csvPath} 时发生未捕获的错误:`, error);
      failCount++;
    }
  }
  
  // 计算总耗时
  const endTime = new Date();
  const totalMinutes = Math.round((endTime - startTime) / 60000);
  
  console.log(`\n************************************************************`);
  console.log(`所有文件处理完成!`);
  console.log(`成功: ${successCount} 个文件`);
  console.log(`失败: ${failCount} 个文件`);
  console.log(`总耗时: ${totalMinutes} 分钟`);
  console.log(`************************************************************\n`);
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
      console.log('未提供CSV文件路径，使用默认文件: ../csv/1.csv');
      const defaultPath = path.join(__dirname, '../csv/1.csv');
      await processCSVFile(defaultPath);
    } else if (args.length === 1) {
      // 单文件处理
      console.log(`处理单个文件: ${args[0]}`);
      await processCSVFile(args[0]);
    } else {
      // 多文件处理
      console.log(`处理多个文件: ${args.join(', ')}`);
      await processMultipleCSVFiles(args);
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
  readAddressesFromCSV,
  fetchDefiPositions,
  parseUniswapData,
  processAddress,
  withRetry,
  processCSVFile,
  processMultipleCSVFiles
}; 