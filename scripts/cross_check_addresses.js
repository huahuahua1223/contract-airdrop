// 导入所需模块
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify/sync');
const readline = require('readline');
const { performance } = require('perf_hooks');

/**
 * 交叉对比csv-merged目录中的1-9.csv文件与两个过滤条件：
 * 1. 排除170w个孤岛地址快照.csv中存在的地址
 * 2. 排除100w个连续周低gas地址快照.csv中weeks_with_low_gas_behavior>=2的地址
 * 输出清理后的CSV文件（只保留符合条件的地址）
 */

// 配置参数
const CONFIG = {
  // 批处理大小，避免内存占用过大
  BATCH_SIZE: 10000,
  // 进度报告间隔
  PROGRESS_INTERVAL: 50000,
  // 低gas行为周数阈值
  LOW_GAS_WEEKS_THRESHOLD: 20
};

/**
 * 读取170w个孤岛地址快照.csv文件，构建地址集合
 * @param {string} snapshotPath 快照文件路径
 * @returns {Promise<Set>} 地址集合
 */
async function loadSnapshotAddresses(snapshotPath) {
  console.log(`正在读取快照文件: ${snapshotPath}`);
  const startTime = performance.now();
  
  const addresses = new Set();
  
  try {
    if (!fs.existsSync(snapshotPath)) {
      console.error(`错误: 快照文件 ${snapshotPath} 不存在`);
      return addresses;
    }
    
    const fileStream = fs.createReadStream(snapshotPath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });
    
    let isFirstLine = true;
    let lineCount = 0;
    
    for await (const line of rl) {
      // 跳过CSV头行
      if (isFirstLine) {
        isFirstLine = false;
        continue;
      }
      
      // 跳过空行
      if (!line.trim()) continue;
      
      lineCount++;
      
      // 解析行数据 - 根据逗号分隔
      const columns = line.split(',');
      
      // 确保有地址列
      if (columns.length > 0) {
        const address = columns[0].trim().toLowerCase();
        
        // 检查地址是否有效（以0x开头且长度为42）
        if (address.startsWith('0x') && address.length === 42) {
          addresses.add(address);
        }
      }
      
      // 每处理一定数量行报告进度
      if (lineCount % CONFIG.PROGRESS_INTERVAL === 0) {
        console.log(`已读取 ${lineCount} 行，当前地址数: ${addresses.size}`);
      }
    }
    
    const elapsedTime = ((performance.now() - startTime) / 1000).toFixed(2);
    console.log(`✓ 快照文件读取完成，共 ${lineCount} 行，${addresses.size} 个唯一地址，耗时: ${elapsedTime}秒`);
    
    return addresses;
  } catch (error) {
    console.error(`读取快照文件时出错:`, error);
    return addresses;
  }
}

/**
 * 读取1w6个Arbitrum_ENS活跃真人地址.csv文件，构建地址集合
 * @param {string} ensPath ENS地址文件路径
 * @returns {Promise<Object>} 包含地址集合和详细数据的对象
 */
async function loadArbitrumENSAddresses(ensPath) {
  console.log(`正在读取Arbitrum ENS地址文件: ${ensPath}`);
  const startTime = performance.now();
  
  const addresses = new Set();
  const addressData = new Map(); // 存储地址的详细信息
  
  try {
    if (!fs.existsSync(ensPath)) {
      console.error(`错误: Arbitrum ENS地址文件 ${ensPath} 不存在`);
      return { addresses, addressData };
    }
    
    const fileStream = fs.createReadStream(ensPath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });
    
    let isFirstLine = true;
    let headers = [];
    let lineCount = 0;
    
    for await (const line of rl) {
      // 处理CSV头行
      if (isFirstLine) {
        isFirstLine = false;
        headers = line.split(',').map(h => h.trim());
        continue;
      }
      
      // 跳过空行
      if (!line.trim()) continue;
      
      lineCount++;
      
      // 解析行数据
      const columns = line.split(',').map(c => c.trim());
      
      // 确保有地址列
      if (columns.length > 0) {
        const address = columns[0].trim().toLowerCase();
        
        // 检查地址是否有效（以0x开头且长度为42）
        if (address.startsWith('0x') && address.length === 42) {
          addresses.add(address);
          
          // 存储地址的详细信息
          const addressInfo = {};
          for (let i = 0; i < Math.min(headers.length, columns.length); i++) {
            addressInfo[headers[i]] = columns[i];
          }
          addressData.set(address, addressInfo);
        }
      }
      
      // 每处理一定数量行报告进度
      if (lineCount % CONFIG.PROGRESS_INTERVAL === 0) {
        console.log(`已读取 ${lineCount} 行，当前ENS地址数: ${addresses.size}`);
      }
    }
    
    const elapsedTime = ((performance.now() - startTime) / 1000).toFixed(2);
    console.log(`✓ Arbitrum ENS地址文件读取完成，共 ${lineCount} 行，${addresses.size} 个唯一地址，耗时: ${elapsedTime}秒`);
    
    return { addresses, addressData };
  } catch (error) {
    console.error(`读取Arbitrum ENS地址文件时出错:`, error);
    return { addresses, addressData };
  }
}

/**
 * 读取100w个连续周低gas地址快照.csv文件，构建周数大于等于阈值的地址集合
 * @param {string} lowGasPath 低gas地址快照文件路径
 * @returns {Promise<Set>} 应该被过滤的地址集合
 */
async function loadLowGasAddresses(lowGasPath) {
  console.log(`正在读取低gas地址快照文件: ${lowGasPath}`);
  const startTime = performance.now();
  
  const filterAddresses = new Set();
  
  try {
    if (!fs.existsSync(lowGasPath)) {
      console.error(`错误: 低gas地址快照文件 ${lowGasPath} 不存在`);
      return filterAddresses;
    }
    
    const fileStream = fs.createReadStream(lowGasPath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });
    
    let isFirstLine = true;
    let headers = [];
    let weeksColumnIndex = -1;
    let addressColumnIndex = 0;
    let lineCount = 0;
    
    for await (const line of rl) {
      // 处理CSV头行
      if (isFirstLine) {
        isFirstLine = false;
        headers = line.split(',').map(h => h.trim());
        
        // 查找 weeks_with_low_gas_behavior 列的索引
        weeksColumnIndex = headers.findIndex(h => h === 'weeks_with_low_gas_behavior');
        
        if (weeksColumnIndex === -1) {
          console.error(`错误: 在低gas地址快照文件中找不到 'weeks_with_low_gas_behavior' 列`);
          return filterAddresses;
        }
        
        continue;
      }
      
      // 跳过空行
      if (!line.trim()) continue;
      
      lineCount++;
      
      // 解析行数据
      const columns = line.split(',').map(c => c.trim());
      
      // 检查有效性
      if (columns.length > Math.max(addressColumnIndex, weeksColumnIndex)) {
        const address = columns[addressColumnIndex].toLowerCase();
        const weeksWithLowGas = parseInt(columns[weeksColumnIndex], 10);
        
        // 检查地址格式及周数条件
        if (address.startsWith('0x') && address.length === 42 && 
            !isNaN(weeksWithLowGas) && weeksWithLowGas >= CONFIG.LOW_GAS_WEEKS_THRESHOLD) {
          filterAddresses.add(address);
        }
      }
      
      // 每处理一定数量行报告进度
      if (lineCount % CONFIG.PROGRESS_INTERVAL === 0) {
        console.log(`已读取 ${lineCount} 行，当前符合过滤条件的地址数: ${filterAddresses.size}`);
      }
    }
    
    const elapsedTime = ((performance.now() - startTime) / 1000).toFixed(2);
    console.log(`✓ 低gas地址快照文件读取完成，共 ${lineCount} 行`);
    console.log(`✓ 找到 ${filterAddresses.size} 个 weeks_with_low_gas_behavior >= ${CONFIG.LOW_GAS_WEEKS_THRESHOLD} 的地址`);
    console.log(`✓ 耗时: ${elapsedTime}秒`);
    
    return filterAddresses;
  } catch (error) {
    console.error(`读取低gas地址快照文件时出错:`, error);
    return filterAddresses;
  }
}

/**
 * 处理单个CSV文件，去除匹配的地址
 * @param {string} csvPath CSV文件路径
 * @param {Set} snapshotAddresses 孤岛地址快照集合
 * @param {Set} lowGasAddresses 低gas行为地址集合
 * @returns {Promise<Object>} 处理结果
 */
async function processCsvFile(csvPath, snapshotAddresses, lowGasAddresses) {
  const fileName = path.basename(csvPath);
  console.log(`\n处理文件: ${fileName}`);
  const startTime = performance.now();
  
  const results = {
    fileName,
    totalRecords: 0,
    excludedRecords: 0,
    keptRecords: 0,
    excludedAddresses: [],
    keptData: []
  };
  
  try {
    if (!fs.existsSync(csvPath)) {
      console.error(`错误: 文件 ${csvPath} 不存在`);
      return results;
    }
    
    const fileStream = fs.createReadStream(csvPath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });
    
    let isFirstLine = true;
    let headers = [];
    let lineCount = 0;
    
    for await (const line of rl) {
      // 处理CSV头行
      if (isFirstLine) {
        isFirstLine = false;
        // 解析头行 - 根据逗号或制表符分隔
        headers = line.includes('\t') ? line.split('\t') : line.split(',');
        continue;
      }
      
      // 跳过空行
      if (!line.trim()) continue;
      
      lineCount++;
      results.totalRecords++;
      
      // 解析行数据 - 根据逗号或制表符分隔
      const columns = line.includes('\t') ? line.split('\t') : line.split(',');
      
      // 确保列数足够，并找到address列（第二列，索引为1）
      if (columns.length > 1) {
        const address = columns[1].trim().toLowerCase();
        
        // 检查地址是否在快照中或低gas行为地址集合中
        if (address.startsWith('0x') && address.length === 42 && 
            (snapshotAddresses.has(address) || lowGasAddresses.has(address))) {
          // 如果地址在任一排除集合中，则排除这条记录
          results.excludedRecords++;
          results.excludedAddresses.push(address);
        } else {
          // 如果地址不在任一排除集合中，则保留这条记录
          results.keptRecords++;
          
          // 构建保留记录的数据对象
          const recordData = {};
          for (let i = 0; i < Math.min(headers.length, columns.length); i++) {
            recordData[headers[i]] = columns[i].trim();
          }
          results.keptData.push(recordData);
        }
      }
      
      // 每处理一定数量行报告进度
      if (lineCount % CONFIG.PROGRESS_INTERVAL === 0) {
        console.log(`已处理 ${lineCount} 行，排除 ${results.excludedRecords} 个地址，保留 ${results.keptRecords} 个地址...`);
      }
    }
    
    const elapsedTime = ((performance.now() - startTime) / 1000).toFixed(2);
    console.log(`✓ 文件 ${fileName} 处理完成`);
    console.log(`  总记录数: ${results.totalRecords}`);
    console.log(`  排除地址数: ${results.excludedRecords}`);
    console.log(`  保留地址数: ${results.keptRecords}`);
    console.log(`  排除率: ${(results.excludedRecords / results.totalRecords * 100).toFixed(2)}%`);
    console.log(`  保留率: ${(results.keptRecords / results.totalRecords * 100).toFixed(2)}%`);
    console.log(`  耗时: ${elapsedTime}秒`);
    
    return results;
  } catch (error) {
    console.error(`处理文件 ${csvPath} 时出错:`, error);
    return results;
  }
}

/**
 * 将ENS地址添加到清洗后的结果中（每个ENS地址只添加一次）
 * 同时为已存在的地址和新添加的地址都添加ens_name列
 * @param {Array} allResults 所有文件的处理结果
 * @param {Map} ensAddressData ENS地址的详细数据
 * @param {Set} snapshotAddresses 孤岛地址快照集合
 * @param {Set} lowGasAddresses 低gas行为地址集合
 * @returns {Object} 合并结果统计
 */
function mergeENSAddresses(allResults, ensAddressData, snapshotAddresses, lowGasAddresses) {
  console.log('\n开始将Arbitrum ENS地址添加到清洗结果中，并为相关地址添加ens_name...');
  const startTime = performance.now();
  
  const mergeStats = {
    totalENSAddresses: ensAddressData.size,
    addedToResults: 0,
    alreadyExists: 0,
    filteredOut: 0,
    ensNameAdded: 0,  // 新增：统计添加ens_name的数量
    addedToFiles: {}
  };
  
  // 第一步：为所有文件中已存在的地址添加ens_name列
  console.log('  步骤1: 为已存在的地址添加ens_name...');
  const globalExistingAddresses = new Set();
  
  for (const result of allResults) {
    if (result.keptData.length === 0) continue;
    
    // 确保每个记录都有ens_name字段
    for (const record of result.keptData) {
      // 查找address字段，可能在不同的列中
      const addressValue = record.address || 
                          record[Object.keys(record)[1]] || // 假设第二列是地址
                          Object.values(record).find(val => 
                            typeof val === 'string' && 
                            val.toLowerCase().startsWith('0x') && 
                            val.length === 42);
      
      if (addressValue) {
        const normalizedAddress = addressValue.toLowerCase();
        globalExistingAddresses.add(normalizedAddress);
        
        // 如果这个地址在ENS数据中存在，添加ens_name
        if (ensAddressData.has(normalizedAddress)) {
          const ensData = ensAddressData.get(normalizedAddress);
          const ensName = ensData.ens_name || '';
          
          // 添加或更新ens_name字段
          if (!record.ens_name) {
            record.ens_name = ensName;
          }
        } else {
          // 如果不在ENS数据中，确保有空的ens_name字段
          if (!record.ens_name) {
            record.ens_name = '';
          }
        }
      }
    }
  }
  
  // 创建ENS地址使用状态跟踪
  const ensUsageTracker = new Map(); // address -> {used: boolean, data: object}
  for (const [address, ensData] of ensAddressData) {
    ensUsageTracker.set(address, { used: false, data: ensData });
  }
  
  // 初始化文件统计
  for (const result of allResults) {
    if (result.keptData.length === 0) continue;
    
    const fileName = result.fileName;
    mergeStats.addedToFiles[fileName] = {
      originalCount: result.keptData.length,
      ensAdded: 0,
      finalCount: 0
    };
  }
  
  // 第二步：将新的ENS地址添加到各个文件中（每个地址只添加一次）
  console.log('  步骤2: 添加新的ENS地址到清洗结果...');
  for (const result of allResults) {
    if (result.keptData.length === 0) continue;
    
    const fileName = result.fileName;
    console.log(`    正在为文件 ${fileName} 添加新的ENS地址...`);
    
    // 检查结果数据的列结构
    const sampleRecord = result.keptData[0];
    const hasAddressColumn = 'address' in sampleRecord;
    const headers = Object.keys(sampleRecord);
    
    // 为当前文件添加ENS地址
    for (const [address, ensTrackData] of ensUsageTracker) {
      // 跳过已经使用过的地址
      if (ensTrackData.used) continue;
      
      // 检查是否应该被过滤掉
      if (snapshotAddresses.has(address) || lowGasAddresses.has(address)) {
        ensTrackData.used = true; // 标记为已处理
        mergeStats.filteredOut++;
        continue;
      }
      
      // 检查是否已经存在于结果中
      if (globalExistingAddresses.has(address)) {
        ensTrackData.used = true; // 标记为已处理
        mergeStats.alreadyExists++;
        continue;
      }
      
      // 构建ENS记录数据，匹配现有文件的列结构
      const ensRecord = {};
      const ensData = ensTrackData.data;
      
      if (hasAddressColumn) {
        // 如果结果文件有address列，直接使用ENS数据
        Object.assign(ensRecord, ensData);
        ensRecord.address = ensData.address; // 确保address字段存在
        ensRecord.ens_name = ensData.ens_name || ''; // 确保ens_name字段存在
        
        // 填充其他可能缺失的列
        for (const header of headers) {
          if (!(header in ensRecord)) {
            ensRecord[header] = ''; // 对于ENS数据中没有的列，设为空字符串
          }
        }
      } else {
        // 如果结果文件没有address列，需要适配列结构
        // 假设第一列是序号，第二列是地址
        ensRecord[headers[0]] = ''; // 序号留空，后续可以重新编号
        ensRecord[headers[1]] = ensData.address; // 地址
        
        // 确保ens_name字段存在
        let ensNameSet = false;
        
        // 其他列根据ENS数据填充或留空
        for (let i = 2; i < headers.length; i++) {
          const header = headers[i];
          
          if (header === 'ens_name') {
            ensRecord[header] = ensData.ens_name || '';
            ensNameSet = true;
          } else {
            // 尝试匹配ENS数据中的字段，或者设为空字符串
            ensRecord[header] = ensData[header] || '';
          }
        }
        
        // 如果没有ens_name列，添加一个
        if (!ensNameSet && !headers.includes('ens_name')) {
          ensRecord.ens_name = ensData.ens_name || '';
        }
      }
      
      // 添加到结果中
      result.keptData.push(ensRecord);
      globalExistingAddresses.add(address);
      ensTrackData.used = true; // 标记为已使用
      mergeStats.addedToResults++;
      mergeStats.addedToFiles[fileName].ensAdded++;
    }
    
    mergeStats.addedToFiles[fileName].finalCount = result.keptData.length;
  }
  
  // 统计最终清洗结果中实际有ens_name值的记录数量
  console.log('  步骤3: 统计最终结果中的ens_name数量...');
  let actualEnsNameCount = 0;
  
  for (const result of allResults) {
    if (result.keptData.length === 0) continue;
    
    for (const record of result.keptData) {
      // 检查是否有非空的ens_name值
      if (record.ens_name && record.ens_name.trim() !== '') {
        actualEnsNameCount++;
      }
    }
  }
  
  mergeStats.ensNameAdded = actualEnsNameCount;
  
  const elapsedTime = ((performance.now() - startTime) / 1000).toFixed(2);
  console.log(`✓ ENS地址合并完成，耗时: ${elapsedTime}秒`);
  console.log(`  总ENS地址数: ${mergeStats.totalENSAddresses}`);
  console.log(`  成功添加新地址: ${mergeStats.addedToResults}`);
  console.log(`  已存在地址: ${mergeStats.alreadyExists}`);
  console.log(`  被过滤地址: ${mergeStats.filteredOut}`);
  console.log(`  实际ens_name数量: ${mergeStats.ensNameAdded}`);
  
  return mergeStats;
}

/**
 * 将清理后的数据写入CSV文件
 * @param {Array} keptData 保留的数据
 * @param {string} outputPath 输出文件路径
 * @param {string} fileName 原文件名
 */
function writeCleanedDataToCSV(keptData, outputPath, fileName) {
  try {
    if (keptData.length === 0) {
      console.log(`  文件 ${fileName} 所有记录都被排除，跳过输出`);
      return;
    }
    
    // 确保输出目录存在
    const dir = path.dirname(outputPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    
    // 将结果转换为CSV字符串
    const csv = stringify(keptData, { header: true });
    
    // 写入文件
    fs.writeFileSync(outputPath, csv);
    
    console.log(`  ✓ 清理后的数据已保存到: ${outputPath}`);
  } catch (error) {
    console.error(`写入CSV文件时出错:`, error);
  }
}

/**
 * 生成地址清理报告
 * @param {Array} allResults 所有文件的处理结果
 * @param {string} outputDir 输出目录
 * @param {Object} mergeStats ENS地址合并统计信息（可选）
 */
function generateCleanupReport(allResults, outputDir, mergeStats = null) {
  try {
    const reportPath = path.join(outputDir, 'cleanup_report.txt');
    
    let reportContent = `
地址清理报告
==========================================
生成时间: ${new Date().toLocaleString()}
处理目标: csv-merged目录中的1-9.csv文件
清理标准: 
1. 排除170w个孤岛地址快照.csv中存在的地址
2. 排除100w个连续周低gas地址快照.csv中weeks_with_low_gas_behavior >= ${CONFIG.LOW_GAS_WEEKS_THRESHOLD}的地址
3. 添加1w6个Arbitrum_ENS活跃真人地址.csv中的地址到清洗结果（排除已被过滤的地址）

详细结果:
`;
    
    let totalRecords = 0;
    let totalExcluded = 0;
    let totalKept = 0;
    const allExcludedAddresses = new Set();
    
    // 统计每个文件的结果
    for (const result of allResults) {
      totalRecords += result.totalRecords;
      totalExcluded += result.excludedRecords;
      totalKept += result.keptRecords;
      
      // 收集所有排除的地址（去重）
      for (const address of result.excludedAddresses) {
        allExcludedAddresses.add(address);
      }
      
      reportContent += `
文件: ${result.fileName}
  总记录数: ${result.totalRecords}
  排除地址数: ${result.excludedRecords}
  保留地址数: ${result.keptRecords}
  排除率: ${(result.excludedRecords / result.totalRecords * 100).toFixed(2)}%
  保留率: ${(result.keptRecords / result.totalRecords * 100).toFixed(2)}%
`;
    }
    
    reportContent += `
==========================================
汇总统计:
处理文件数: ${allResults.length}
总记录数: ${totalRecords}
总排除次数: ${totalExcluded}
总保留记录数: ${totalKept}
唯一排除地址数: ${allExcludedAddresses.size}
总体排除率: ${(totalExcluded / totalRecords * 100).toFixed(2)}%
总体保留率: ${(totalKept / totalRecords * 100).toFixed(2)}%`;

    // 添加ENS地址合并统计信息
    if (mergeStats) {
      reportContent += `

Arbitrum ENS地址添加统计:
总ENS地址数: ${mergeStats.totalENSAddresses}
成功添加新地址: ${mergeStats.addedToResults}
已存在地址: ${mergeStats.alreadyExists}
被过滤地址: ${mergeStats.filteredOut}
实际ens_name数量: ${mergeStats.ensNameAdded}

各文件ENS地址添加详情:`;
      
      for (const [fileName, stats] of Object.entries(mergeStats.addedToFiles)) {
        reportContent += `
  ${fileName}: 原始${stats.originalCount} → 添加${stats.ensAdded} → 最终${stats.finalCount}`;
      }
    }

    reportContent += `

说明: 
- 排除次数可能大于唯一地址数，因为同一地址可能在多个文件中出现
- 排除基于address列的完全匹配（不区分大小写）
- ENS地址在添加前会检查是否已被孤岛或低gas过滤条件排除
- 每个ENS地址只会被添加一次，按文件顺序分配到各个清洗后的文件中
- 对于已存在的地址，如果在ENS文件中有对应记录，会添加其ens_name到结果中
- 新添加的ENS地址会包含完整的ens_name信息
- 所有记录都会确保有ens_name列（没有对应ENS信息的为空字符串）
- 实际ens_name数量统计的是最终结果中有非空ens_name值的记录总数
- 清理后的CSV文件保存在 ${outputDir} 目录下，文件名格式为 cleaned_[原文件名].csv
==========================================
`;
    
    fs.writeFileSync(reportPath, reportContent);
    console.log(`\n📊 地址清理报告已生成: ${reportPath}`);
    
    return {
      totalFiles: allResults.length,
      totalRecords,
      totalExcluded,
      totalKept,
      uniqueExcluded: allExcludedAddresses.size,
      excludeRate: (totalExcluded / totalRecords * 100).toFixed(2),
      keepRate: (totalKept / totalRecords * 100).toFixed(2)
    };
  } catch (error) {
    console.error('生成报告时出错:', error);
    return null;
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
 * 主函数 - 执行地址清理
 */
async function cleanAddresses() {
  console.log('===============================================');
  console.log('开始执行地址清理任务');
  console.log('===============================================\n');
  console.time('总耗时');
  
  try {
    // 定义路径
    const csvMergedDir = path.join(__dirname, '../csv-merged');
    const snapshotPath = path.join(csvMergedDir, '170w个孤岛地址快照.csv');
    const lowGasPath = path.join(csvMergedDir, '100w个连续周低gas地址快照.csv');
    const ensPath = path.join(csvMergedDir, '1w6个Arbitrum_ENS活跃真人地址.csv');
    const outputDir = path.join(__dirname, '../cleaned-results');
    
    // 确保输出目录存在
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
    
    // 步骤1: 读取孤岛地址快照文件
    const snapshotAddresses = await loadSnapshotAddresses(snapshotPath);
    
    if (snapshotAddresses.size === 0) {
      console.error('错误: 未能读取到有效的孤岛地址快照，程序退出');
      return;
    }
    
    // 步骤2: 读取低gas地址快照文件
    const lowGasAddresses = await loadLowGasAddresses(lowGasPath);
    
    if (lowGasAddresses.size === 0) {
      console.warn('警告: 未能读取到有效的低gas地址快照数据，将只进行孤岛地址的过滤');
    }
    
    // 步骤2.5: 读取Arbitrum ENS地址文件
    const ensResult = await loadArbitrumENSAddresses(ensPath);
    const { addresses: ensAddresses, addressData: ensAddressData } = ensResult;
    
    if (ensAddresses.size === 0) {
      console.warn('警告: 未能读取到有效的Arbitrum ENS地址数据，将跳过ENS地址添加步骤');
    }
    
    // 步骤3: 获取要处理的CSV文件列表（1.csv到9.csv）
    const csvFiles = [];
    for (let i = 1; i <= 9; i++) {
      const csvFile = path.join(csvMergedDir, `${i}.csv`);
      if (fs.existsSync(csvFile)) {
        csvFiles.push(csvFile);
      } else {
        console.warn(`警告: 文件 ${i}.csv 不存在，跳过`);
      }
    }
    
    if (csvFiles.length === 0) {
      console.error('错误: 未找到任何要处理的CSV文件（1.csv到9.csv）');
      return;
    }
    
    console.log(`\n找到 ${csvFiles.length} 个CSV文件需要处理`);
    console.log(`孤岛地址快照数: ${snapshotAddresses.size}`);
    console.log(`低gas地址过滤数: ${lowGasAddresses.size}`);
    console.log(`Arbitrum ENS地址数: ${ensAddresses.size}`);
    console.log(`处理策略: 排除孤岛地址快照中存在的地址，以及连续周低gas行为大于等于${CONFIG.LOW_GAS_WEEKS_THRESHOLD}的地址，最后添加Arbitrum ENS地址`);
    
    // 启用内存使用量监控
    const memoryInterval = setInterval(logMemoryUsage, 60000); // 每60秒输出一次内存使用情况
    
    // 步骤3: 处理每个CSV文件
    const allResults = [];
    for (let i = 0; i < csvFiles.length; i++) {
      const csvFile = csvFiles[i];
      const fileName = path.basename(csvFile, '.csv');
      
      console.log(`\n[${i + 1}/${csvFiles.length}] 开始处理文件: ${fileName}.csv`);
      
      // 处理文件
      const result = await processCsvFile(csvFile, snapshotAddresses, lowGasAddresses);
      allResults.push(result);
      
      // 保存清理后的数据到单独的文件
      if (result.keptData.length > 0) {
        const outputPath = path.join(outputDir, `cleaned_${fileName}.csv`);
        writeCleanedDataToCSV(result.keptData, outputPath, fileName);
      }
      
      // 强制垃圾回收
      if (global.gc) {
        global.gc();
      }
    }
    
    clearInterval(memoryInterval);
    
    // 步骤3.5: 将Arbitrum ENS地址添加到清洗结果中
    let mergeStats = null;
    if (ensAddresses.size > 0) {
      console.log('\n===============================================');
      console.log('添加Arbitrum ENS地址到清洗结果...');
      console.log('===============================================');
      
      mergeStats = mergeENSAddresses(allResults, ensAddressData, snapshotAddresses, lowGasAddresses);
      
      // 重新保存包含ENS地址的清理后数据
      for (const result of allResults) {
        if (result.keptData.length > 0) {
          const fileName = path.basename(result.fileName, '.csv');
          const outputPath = path.join(outputDir, `cleaned_${fileName}.csv`);
          writeCleanedDataToCSV(result.keptData, outputPath, fileName);
        }
      }
    }
    
    // 步骤4: 生成汇总报告
    console.log('\n===============================================');
    console.log('生成地址清理汇总报告...');
    console.log('===============================================');
    
    const summary = generateCleanupReport(allResults, outputDir, mergeStats);
    
    if (summary) {
      console.log('\n🎉 地址清理任务完成!');
      console.log(`📁 结果文件保存在: ${outputDir}`);
      console.log(`📄 处理文件数: ${summary.totalFiles}`);
      console.log(`📊 总记录数: ${summary.totalRecords}`);
      console.log(`🗑️ 总排除次数: ${summary.totalExcluded}`);
      console.log(`✅ 总保留记录数: ${summary.totalKept}`);
      console.log(`🔍 唯一排除地址数: ${summary.uniqueExcluded}`);
      console.log(`📈 总体排除率: ${summary.excludeRate}%`);
      console.log(`📈 总体保留率: ${summary.keepRate}%`);
    }
    
  } catch (error) {
    console.error('地址清理过程中发生错误:', error);
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

// 主函数 - 处理命令行参数
async function main() {
  const args = process.argv.slice(2);
  
  if (args.length > 0 && (args[0] === '--help' || args[0] === '-h')) {
    console.log(`
使用说明:
  node cross_check_addresses.js

功能:
  从csv-merged目录中的1-9.csv文件中执行三步处理：
  1. 排除170w个孤岛地址快照.csv中存在的地址
  2. 排除100w个连续周低gas地址快照.csv中weeks_with_low_gas_behavior >= 2的地址
  3. 添加1w6个Arbitrum_ENS活跃真人地址.csv中的地址到清洗结果（排除已被过滤的地址）
  输出清理后的CSV文件（包含原始清洗后的地址和新增的ENS地址）

输出:
  - cleaned-results目录下的cleaned_[文件名].csv文件（包含清理后的记录）
  - cleanup_report.txt文件（清理汇总报告）

示例:
  node scripts/cross_check_addresses.js
    `);
    return;
  }

  try {
    await cleanAddresses();
  } catch (error) {
    console.error('程序执行出错:', error);
    process.exit(1);
  }
}

// 如果直接运行此脚本
if (require.main === module) {
  main().catch(console.error);
}

module.exports = {
  cleanAddresses,
  loadSnapshotAddresses,
  loadLowGasAddresses,
  loadArbitrumENSAddresses,
  processCsvFile,
  mergeENSAddresses,
  generateCleanupReport
};