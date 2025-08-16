// 流动性池筛选脚本 - 筛选符合底池流动性要求的代币
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify/sync');
require('dotenv').config();

// 底池基准币配置（Arbitrum One）- 统一使用小写
const BASE_TOKENS = {
  usdt: {
    address: '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9',
    symbol: 'USDT',
    isStablecoin: true
  },
  usdc: {
    address: '0xaf88d065e77c8cc2239327c5edb3a432268e5831',
    symbol: 'USDC',
    isStablecoin: true
  },
  'usdc.e': {
    address: '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8',
    symbol: 'USDC.e',
    isStablecoin: true
  },
  weth: {
    address: '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
    symbol: 'WETH',
    isStablecoin: false
  },
  wbtc: {
    address: '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f',
    symbol: 'WBTC',
    isStablecoin: false
  }
};

// 配置参数
const CONFIG = {
  // Dexscreener API 限制：每分钟300次请求，每次最多30个地址
  DEXSCREENER_BATCH_SIZE: 30,
  DEXSCREENER_RATE_LIMIT: 200, // 保守设置每分钟请求数
  RATE_LIMIT_WINDOW: 60 * 1000, // 1分钟窗口
  
  // 重试配置
  MAX_RETRIES: 5, // 增加重试次数
  RETRY_DELAY: 3000, // 增加重试延迟
  REQUEST_TIMEOUT: 20000, // 增加超时时间
  
  // 批次间延迟（毫秒）
  BATCH_DELAY: 2500, // 增加批次间延迟
  
  // 流动性阈值
  MIN_LIQUIDITY_USD: 10000,
  
  // 中间结果保存间隔
  SAVE_INTERVAL: 10
};

// 用于跟踪API调用频率
let apiCallTimes = [];

/**
 * 休眠函数
 * @param {number} ms 毫秒
 * @returns {Promise} Promise
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * 随机延迟函数（增加不可预测性）
 * @param {number} baseMs 基础延迟毫秒
 * @param {number} variationPercent 变化百分比（0-100）
 * @returns {Promise} Promise
 */
const randomSleep = (baseMs, variationPercent = 30) => {
  const variation = baseMs * (variationPercent / 100);
  const randomMs = baseMs + (Math.random() * variation * 2 - variation);
  return sleep(Math.max(randomMs, 100)); // 最少100ms
};

/**
 * 检查API调用频率限制
 */
function checkRateLimit() {
  const now = Date.now();
  // 移除超过时间窗口的记录
  apiCallTimes = apiCallTimes.filter(time => now - time < CONFIG.RATE_LIMIT_WINDOW);
  
  if (apiCallTimes.length >= CONFIG.DEXSCREENER_RATE_LIMIT) {
    const waitTime = CONFIG.RATE_LIMIT_WINDOW - (now - apiCallTimes[0]) + 1000; // 额外等待1秒
    return waitTime;
  }
  
  return 0;
}

/**
 * 记录API调用时间
 */
function recordApiCall() {
  apiCallTimes.push(Date.now());
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
 * 从CoinGecko获取WETH和WBTC价格
 * @returns {Promise<Object>} 价格对象 {weth: number, wbtc: number}
 */
async function getCryptoPrices() {
  try {
    const url = 'https://api.coingecko.com/api/v3/simple/price?ids=ethereum,wrapped-bitcoin&vs_currencies=usd';
    const response = await axios.get(url, {
      timeout: CONFIG.REQUEST_TIMEOUT,
      headers: {
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://coingecko.com/',
        'Origin': 'https://coingecko.com',
        'Accept-Encoding': 'gzip, deflate, br'
      }
    });
    
    const data = response.data;
    const prices = {
      weth: data.ethereum?.usd || 0,
      wbtc: data['wrapped-bitcoin']?.usd || 0
    };
    
    console.log(`获取到价格: WETH=$${prices.weth}, WBTC=$${prices.wbtc}`);
    return prices;
  } catch (error) {
    console.error('获取加密货币价格时出错:', error.message);
    console.warn('⚠️  由于网络连接问题，将使用默认价格进行计算');
    
    // 返回默认价格作为后备
    const defaultPrices = {
      weth: 4445, // 根据最近成功获取的价格设置默认值
      wbtc: 117636 // 根据最近成功获取的价格设置默认值
    };
    
    console.log(`使用默认价格: WETH=$${defaultPrices.weth}, WBTC=$${defaultPrices.wbtc}`);
    return defaultPrices;
  }
}

/**
 * 调用Dexscreener API获取代币交易对信息
 * @param {string[]} addresses 代币地址数组（最多30个）
 * @returns {Promise<Object>} API响应数据
 */
async function fetchDexscreenerData(addresses) {
  try {
    // 检查频率限制
    const waitTime = checkRateLimit();
    if (waitTime > 0) {
      console.log(`API频率限制，等待 ${Math.ceil(waitTime / 1000)} 秒...`);
      await sleep(waitTime);
    }
    
    const addressList = addresses.join(',');
    const url = `https://api.dexscreener.com/tokens/v1/arbitrum/${addressList}`;
    
    recordApiCall();
    
    // 添加随机小延迟模拟人类行为
    await randomSleep(200, 100); // 100-300ms的随机延迟
    
    const response = await axios.get(url, {
      timeout: CONFIG.REQUEST_TIMEOUT,
      headers: {
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://dexscreener.com/',
        'Origin': 'https://dexscreener.com',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
      }
    });
    
    return response.data;
  } catch (error) {
    // 检查是否为速率限制错误
    if (error.response && error.response.status === 429) {
      console.warn('⚠️  Dexscreener API被限流，等待更长时间后重试...');
      await sleep(CONFIG.RETRY_DELAY * 5); // 更长的等待时间
      throw new Error('Rate limited');
    }
    
    // 检查其他HTTP错误
    if (error.response) {
      console.warn(`⚠️  API返回错误状态: ${error.response.status}`);
      if (error.response.status >= 500) {
        // 服务器错误，等待后重试
        await sleep(CONFIG.RETRY_DELAY * 2);
        throw new Error(`Server error: ${error.response.status}`);
      }
    }
    
    console.error(`调用Dexscreener API时出错:`, error.message);
    throw error;
  }
}

/**
 * 分析单个交易对的流动性
 * @param {Object} pair 交易对数据
 * @param {Object} cryptoPrices 加密货币价格 {weth, wbtc}
 * @returns {Object|null} 如果符合条件返回结果，否则返回null
 */
function analyzePairLiquidity(pair, cryptoPrices) {
  // 只处理 Arbitrum 链上的交易对
  if (pair.chainId !== 'arbitrum') {
    return null;
  }
  
  const baseToken = pair.baseToken;
  const quoteToken = pair.quoteToken;
  const liquidity = pair.liquidity;
  
  if (!baseToken || !quoteToken || !liquidity) {
    return null;
  }
  
  // 检查是否有我们关注的底池
  let matchedPool = null;
  let isBaseTokenPool = false;
  
  // 检查baseToken是否为底池
  const baseTokenLower = baseToken.address?.toLowerCase();
  for (const [key, poolInfo] of Object.entries(BASE_TOKENS)) {
    if (baseTokenLower === poolInfo.address) {
      matchedPool = poolInfo;
      isBaseTokenPool = true;
      break;
    }
  }
  
  // 检查quoteToken是否为底池
  if (!matchedPool) {
    const quoteTokenLower = quoteToken.address?.toLowerCase();
    for (const [key, poolInfo] of Object.entries(BASE_TOKENS)) {
      if (quoteTokenLower === poolInfo.address) {
        matchedPool = poolInfo;
        isBaseTokenPool = false;
        break;
      }
    }
  }
  
  if (!matchedPool) {
    return null;
  }
  
  // 计算单边美元价值
  let singleSideUsd = 0;
  
  if (matchedPool.isStablecoin) {
    // 稳定币池：直接使用liquidity值
    singleSideUsd = isBaseTokenPool ? 
      parseFloat(liquidity.base || 0) : 
      parseFloat(liquidity.quote || 0);
  } else {
    // WETH/WBTC池：数量 × 价格
    const tokenAmount = isBaseTokenPool ? 
      parseFloat(liquidity.base || 0) : 
      parseFloat(liquidity.quote || 0);
    
    const price = matchedPool.symbol === 'WETH' ? cryptoPrices.weth : cryptoPrices.wbtc;
    singleSideUsd = tokenAmount * price;
  }
  
  // 检查是否达到阈值
  if (singleSideUsd >= CONFIG.MIN_LIQUIDITY_USD) {
    return {
      triggerBaseSymbol: matchedPool.symbol,
      triggerPool: pair.pairAddress,
      dexId: pair.dexId,
      singleSideUsd: Math.round(singleSideUsd * 100) / 100, // 保留两位小数
      url: pair.url,
      liquidityData: {
        totalUsd: parseFloat(liquidity.usd || 0),
        base: parseFloat(liquidity.base || 0),
        quote: parseFloat(liquidity.quote || 0)
      }
    };
  }
  
  return null;
}

/**
 * 处理单个代币的所有交易对
 * @param {Object} tokenData 代币数据
 * @param {Array} pairs 该代币的所有交易对
 * @param {Object} cryptoPrices 加密货币价格
 * @returns {Object|null} 最佳匹配的交易对信息
 */
function processTokenPairs(tokenData, pairs, cryptoPrices) {
  if (!pairs || pairs.length === 0) {
    return null;
  }
  
  let bestMatch = null;
  let maxLiquidity = 0;
  
  // 分析每个交易对
  for (const pair of pairs) {
    const analysis = analyzePairLiquidity(pair, cryptoPrices);
    
    if (analysis && analysis.singleSideUsd > maxLiquidity) {
      maxLiquidity = analysis.singleSideUsd;
      bestMatch = {
        ...tokenData, // 包含原始代币数据
        trigger_base_symbol: analysis.triggerBaseSymbol,
        trigger_pool: analysis.triggerPool,
        dexId: analysis.dexId,
        single_side_usd: analysis.singleSideUsd,
        url: analysis.url
      };
    }
  }
  
  return bestMatch;
}

/**
 * 读取筛选后的代币CSV文件
 * @param {string} csvPath CSV文件路径
 * @returns {Promise<Array>} 代币列表
 */
async function readFilteredTokens(csvPath) {
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
        from_line: 2, // 跳过标题行
        columns: false,
        relax_column_count: true,
        skip_empty_lines: true,
        trim: true
      }));
    
    for await (const record of parser) {
      if (record.length >= 5 && record[0]) {
        const tokenInfo = {
          contract_address: record[0].trim().toLowerCase(),
          symbol: record[1] ? record[1].trim() : '',
          decimals: record[2] ? record[2].trim() : '',
          name: record[3] ? record[3].trim() : '',
          totalHolders: record[4] ? parseInt(record[4]) : 0
        };
        
        // 验证合约地址格式
        if (tokenInfo.contract_address.startsWith('0x') && tokenInfo.contract_address.length === 42) {
          tokens.push(tokenInfo);
        }
      }
    }
    
    console.log(`共读取到 ${tokens.length} 个代币`);
    return tokens;
  } catch (err) {
    console.error(`读取CSV文件时出错:`, err);
    return [];
  }
}

/**
 * 批量处理代币
 * @param {Array} tokens 代币列表
 * @param {Object} cryptoPrices 加密货币价格
 * @returns {Promise<Array>} 符合条件的代币列表
 */
async function processTokensBatch(tokens, cryptoPrices) {
  const results = [];
  const batchSize = CONFIG.DEXSCREENER_BATCH_SIZE;
  
  console.log(`开始批量处理 ${tokens.length} 个代币，每批处理 ${batchSize} 个...`);
  
  for (let i = 0; i < tokens.length; i += batchSize) {
    const batch = tokens.slice(i, i + batchSize);
    const batchNumber = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(tokens.length / batchSize);
    
    console.log(`处理批次 ${batchNumber}/${totalBatches} (${batch.length} 个代币)`);
    
    try {
      // 提取地址列表
      const addresses = batch.map(token => token.contract_address);
      
      // 调用API
      const apiResponse = await withRetry(fetchDexscreenerData, [addresses]);
      
      // Dexscreener API实际返回的是数组，不是对象
      const pairs = Array.isArray(apiResponse) ? apiResponse : (apiResponse?.pairs || []);
      
      if (pairs && pairs.length > 0) {
        // 按代币地址分组交易对
        const pairsByToken = {};
        for (const pair of pairs) {
          const tokenAddress = pair.baseToken?.address?.toLowerCase();
          if (tokenAddress) {
            if (!pairsByToken[tokenAddress]) {
              pairsByToken[tokenAddress] = [];
            }
            pairsByToken[tokenAddress].push(pair);
          }
          
          const quoteTokenAddress = pair.quoteToken?.address?.toLowerCase();
          if (quoteTokenAddress && quoteTokenAddress !== tokenAddress) {
            if (!pairsByToken[quoteTokenAddress]) {
              pairsByToken[quoteTokenAddress] = [];
            }
            pairsByToken[quoteTokenAddress].push(pair);
          }
        }
        
        // 处理每个代币
        for (const token of batch) {
          const tokenPairs = pairsByToken[token.contract_address] || [];
          const matchResult = processTokenPairs(token, tokenPairs, cryptoPrices);
          
          if (matchResult) {
            results.push(matchResult);
            console.log(`✓ ${token.symbol} (${token.contract_address}): ${matchResult.trigger_base_symbol} 池, $${matchResult.single_side_usd}`);
          }
        }
      }
      
      // 随机延迟以避免API限制和增加不可预测性
      if (i + batchSize < tokens.length) {
        await randomSleep(CONFIG.BATCH_DELAY, 40); // 40%的随机变化
      }
      
    } catch (error) {
      console.error(`处理批次 ${batchNumber} 时出错:`, error.message);
      // 继续处理下一批，使用随机延迟
      await randomSleep(CONFIG.RETRY_DELAY, 50); // 50%的随机变化
    }
  }
  
  console.log(`\n筛选完成: ${results.length}/${tokens.length} 个代币符合流动性要求`);
  return results;
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
    
    // 定义CSV标题
    const headers = [
      'contract_address',
      'symbol', 
      'decimals',
      'name',
      'totalHolders',
      'trigger_base_symbol',
      'trigger_pool',
      'dexId',
      'single_side_usd',
      'url'
    ];
    
    // 将结果转换为CSV字符串
    const csv = stringify(results, { 
      header: true, 
      columns: headers
    });
    
    // 写入文件
    fs.writeFileSync(outputPath, csv);
    
    console.log(`结果已保存到: ${outputPath} (共 ${results.length} 条记录)`);
    
    // 输出统计信息
    const stats = {};
    results.forEach(result => {
      const base = result.trigger_base_symbol;
      stats[base] = (stats[base] || 0) + 1;
    });
    
    console.log('\n底池分布统计:');
    Object.entries(stats).forEach(([base, count]) => {
      console.log(`  ${base}: ${count} 个代币`);
    });
    
  } catch (error) {
    console.error(`写入CSV文件时出错:`, error);
  }
}

/**
 * 从文件名获取文件标识符
 * @param {string} filePath 文件路径
 * @returns {string} 文件标识符
 */
function getFileIdentifier(filePath) {
  const fileName = path.basename(filePath, path.extname(filePath));
  return fileName.replace('token_holders_filtered_', '');
}

/**
 * 主函数
 */
async function main() {
  console.time('总耗时');
  
  try {
    // 获取命令行参数
    const args = process.argv.slice(2);
    
    let inputPath;
    if (args.length === 0) {
      // 使用默认文件
      inputPath = path.join(__dirname, '../token/token-holders/token_holders_filtered_arb-alltoken.csv');
      console.log(`未提供输入文件，使用默认文件: ${inputPath}`);
    } else {
      inputPath = args[0];
      console.log(`使用输入文件: ${inputPath}`);
    }
    
    // 检查输入文件是否存在
    if (!fs.existsSync(inputPath)) {
      console.error(`错误: 输入文件不存在: ${inputPath}`);
      process.exit(1);
    }
    
    console.log('\n=== 开始流动性池筛选 ===\n');
    
    // 1. 获取加密货币价格
    console.log('1. 获取WETH和WBTC价格...');
    const cryptoPrices = await getCryptoPrices();
    
    // 2. 读取筛选后的代币列表
    console.log('\n2. 读取代币列表...');
    const tokens = await readFilteredTokens(inputPath);
    
    if (tokens.length === 0) {
      console.error('未找到有效的代币数据');
      process.exit(1);
    }
    
    // 3. 批量处理代币
    console.log('\n3. 开始流动性分析...');
    const qualifiedTokens = await processTokensBatch(tokens, cryptoPrices);
    
    // 4. 保存结果
    console.log('\n4. 保存结果...');
    const fileIdentifier = getFileIdentifier(inputPath);
    const outputPath = path.join(
      path.dirname(inputPath), 
      `liquidity_qualified_${fileIdentifier}.csv`
    );
    
    writeResultsToCSV(qualifiedTokens, outputPath);
    
    console.log('\n=== 筛选完成 ===');
    
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
  readFilteredTokens,
  fetchDexscreenerData,
  analyzePairLiquidity,
  processTokenPairs,
  processTokensBatch,
  getCryptoPrices
};