// 导入所需模块
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify/sync');
const readline = require('readline');
const { performance } = require('perf_hooks');

/**
 * 交叉对比csv-merged目录中的1-9.csv文件与170w个孤岛地址快照.csv文件
 * 从1.csv到9.csv中去除那些在170w个孤岛地址快照.csv中存在的地址
 * 输出清理后的CSV文件（只保留不在快照中的地址）
 */

// 配置参数
const CONFIG = {
  // 批处理大小，避免内存占用过大
  BATCH_SIZE: 10000,
  // 进度报告间隔
  PROGRESS_INTERVAL: 50000
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
 * 处理单个CSV文件，去除匹配的地址
 * @param {string} csvPath CSV文件路径
 * @param {Set} snapshotAddresses 快照地址集合
 * @returns {Promise<Object>} 处理结果
 */
async function processCsvFile(csvPath, snapshotAddresses) {
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
        
        // 检查地址是否在快照中
        if (address.startsWith('0x') && address.length === 42 && snapshotAddresses.has(address)) {
          // 如果地址在快照中，则排除这条记录
          results.excludedRecords++;
          results.excludedAddresses.push(address);
        } else {
          // 如果地址不在快照中，则保留这条记录
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
 */
function generateCleanupReport(allResults, outputDir) {
  try {
    const reportPath = path.join(outputDir, 'cleanup_report.txt');
    
    let reportContent = `
地址清理报告
==========================================
生成时间: ${new Date().toLocaleString()}
处理目标: csv-merged目录中的1-9.csv文件
清理标准: 排除170w个孤岛地址快照.csv中存在的地址

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
总体保留率: ${(totalKept / totalRecords * 100).toFixed(2)}%

说明: 
- 排除次数可能大于唯一地址数，因为同一地址可能在多个文件中出现
- 排除基于address列的完全匹配（不区分大小写）
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
    const outputDir = path.join(__dirname, '../cleaned-results');
    
    // 确保输出目录存在
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
    
    // 步骤1: 读取快照文件地址
    const snapshotAddresses = await loadSnapshotAddresses(snapshotPath);
    
    if (snapshotAddresses.size === 0) {
      console.error('错误: 未能读取到有效的快照地址，程序退出');
      return;
    }
    
    // 步骤2: 获取要处理的CSV文件列表（1.csv到9.csv）
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
    console.log(`快照地址数: ${snapshotAddresses.size}`);
    console.log(`处理策略: 排除快照中存在的地址，保留其他地址`);
    
    // 启用内存使用量监控
    const memoryInterval = setInterval(logMemoryUsage, 60000); // 每60秒输出一次内存使用情况
    
    // 步骤3: 处理每个CSV文件
    const allResults = [];
    for (let i = 0; i < csvFiles.length; i++) {
      const csvFile = csvFiles[i];
      const fileName = path.basename(csvFile, '.csv');
      
      console.log(`\n[${i + 1}/${csvFiles.length}] 开始处理文件: ${fileName}.csv`);
      
      // 处理文件
      const result = await processCsvFile(csvFile, snapshotAddresses);
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
    
    // 步骤4: 生成汇总报告
    console.log('\n===============================================');
    console.log('生成地址清理汇总报告...');
    console.log('===============================================');
    
    const summary = generateCleanupReport(allResults, outputDir);
    
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
  从csv-merged目录中的1-9.csv文件中去除170w个孤岛地址快照.csv中存在的地址
  输出清理后的CSV文件（只保留不在快照中的地址）

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
  processCsvFile,
  generateCleanupReport
};