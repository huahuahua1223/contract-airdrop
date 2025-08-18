// 将merge-owners数据合并到cleaned-results中的脚本
// 功能：处理地址匹配、积分相加、去重标记、文件分割
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify/sync');
const { performance } = require('perf_hooks');

// 配置参数
const CONFIG = {
  // 每个输出文件的最大记录数
  MAX_RECORDS_PER_FILE: 500000,
  // 进度报告间隔
  PROGRESS_INTERVAL: 500000,
  // 批处理大小
  BATCH_SIZE: 5000,
  // 内存管理间隔
  MEMORY_CHECK_INTERVAL: 50000
};

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
 * 读取CSV文件并解析为对象数组
 * @param {string} filePath 文件路径
 * @param {Function} skipComments 是否跳过注释行
 * @returns {Promise<Array>} 解析后的记录数组
 */
async function readCSVFile(filePath, skipComments = false) {
  return new Promise((resolve, reject) => {
    const records = [];
    const fileName = path.basename(filePath);
    console.log(`正在读取文件: ${fileName}`);
    
    const parser = fs
      .createReadStream(filePath)
      .pipe(parse({
        columns: true,
        skip_empty_lines: true,
        skip_lines_with_error: true,
        trim: true,
        comment: skipComments ? '#' : false
      }));
    
    parser.on('readable', function() {
      let record;
      while ((record = parser.read()) !== null) {
        records.push(record);
      }
    });
    
    parser.on('error', function(err) {
      console.error(`读取文件 ${fileName} 时出错:`, err.message);
      reject(err);
    });
    
    parser.on('end', function() {
      console.log(`文件 ${fileName} 读取完成，共 ${records.length} 条记录`);
      resolve(records);
    });
  });
}

/**
 * 读取所有cleaned-results文件
 * @param {string} cleanedDir cleaned-results目录路径
 * @returns {Promise<Array>} 所有记录的数组
 */
async function readAllCleanedFiles(cleanedDir) {
  console.log('开始读取cleaned-results文件...');
  const cleanedFiles = fs.readdirSync(cleanedDir)
    .filter(file => file.startsWith('cleaned_') && file.endsWith('.csv'))
    .sort();
  
  console.log(`发现 ${cleanedFiles.length} 个cleaned文件`);
  
  const allRecords = [];
  for (let i = 0; i < cleanedFiles.length; i++) {
    const file = cleanedFiles[i];
    const filePath = path.join(cleanedDir, file);
    
    console.log(`[${i + 1}/${cleanedFiles.length}] 处理文件: ${file}`);
    
    try {
      const records = await readCSVFile(filePath);
      
      // 处理BOM和特殊字符
      records.forEach(record => {
        if (record.address) {
          record.address = record.address.replace(/^\\uFEFF/, '').toLowerCase().trim();
          // 确保total_score是数字
          record.total_score = parseInt(record.total_score) || 0;
        }
      });
      
      // 分批添加记录以避免内存问题
      const batchSize = 10000;
      for (let j = 0; j < records.length; j += batchSize) {
        const batch = records.slice(j, Math.min(j + batchSize, records.length));
        allRecords.push(...batch);
      }
      
      console.log(`文件 ${file} 已添加 ${records.length} 条记录，总计: ${allRecords.length}`);
      
      // 内存检查
      if ((i + 1) % 3 === 0) {
        logMemoryUsage();
        if (global.gc) {
          global.gc();
        }
      }
      
    } catch (error) {
      console.error(`处理文件 ${file} 时出错:`, error.message);
      throw error;
    }
  }
  
  console.log(`cleaned-results数据读取完成，总计: ${allRecords.length} 条记录`);
  return allRecords;
}

/**
 * 读取所有merge-owners文件
 * @param {string} mergeDir merge-owners目录路径
 * @returns {Promise<Array>} 所有记录的数组
 */
async function readAllMergeFiles(mergeDir) {
  console.log('开始读取merge-owners文件...');
  const mergeFiles = fs.readdirSync(mergeDir)
    .filter(file => file.includes('merged_token_holders_part') && file.endsWith('.csv'))
    .sort();
  
  console.log(`发现 ${mergeFiles.length} 个merge文件`);
  
  const allRecords = [];
  for (let i = 0; i < mergeFiles.length; i++) {
    const file = mergeFiles[i];
    const filePath = path.join(mergeDir, file);
    
    console.log(`[${i + 1}/${mergeFiles.length}] 处理文件: ${file}`);
    
    try {
      const records = await readCSVFile(filePath, true); // 跳过注释行
      
      // 过滤和处理记录
      const validRecords = records.filter(record => {
        return record.owner_address && 
               record.owner_address.startsWith('0x') && 
               record.owner_address.length === 42 &&
               record.total_score;
      }).map(record => ({
        owner_address: record.owner_address.toLowerCase().trim(),
        total_score: parseInt(record.total_score) || 0,
        // 保留其他有用信息用于调试
        token_count: record.token_count || 0,
        score_count: record.score_count || 0
      }));
      
      // 分批添加记录
      const batchSize = 10000;
      for (let j = 0; j < validRecords.length; j += batchSize) {
        const batch = validRecords.slice(j, Math.min(j + batchSize, validRecords.length));
        allRecords.push(...batch);
      }
      
      console.log(`文件 ${file} 已添加 ${validRecords.length} 条有效记录，总计: ${allRecords.length}`);
      
      // 内存检查
      if ((i + 1) % 1 === 0) {
        logMemoryUsage();
        if (global.gc) {
          global.gc();
        }
      }
      
    } catch (error) {
      console.error(`处理文件 ${file} 时出错:`, error.message);
      throw error;
    }
  }
  
  console.log(`merge-owners数据读取完成，总计: ${allRecords.length} 条记录`);
  return allRecords;
}

/**
 * 合并数据并处理去重
 * @param {Array} cleanedRecords cleaned-results的记录
 * @param {Array} mergeRecords merge-owners的记录
 * @returns {Object} 合并结果和统计信息
 */
function mergeData(cleanedRecords, mergeRecords) {
  console.log('开始合并数据...');
  console.time('数据合并耗时');
  
  // 创建cleaned-results的地址映射
  console.log('创建cleaned-results地址映射...');
  const cleanedMap = new Map();
  let processedCleaned = 0;
  
  cleanedRecords.forEach(record => {
    if (record.address) {
      cleanedMap.set(record.address, {
        ...record,
        originalIndex: processedCleaned
      });
      processedCleaned++;
      
      if (processedCleaned % CONFIG.PROGRESS_INTERVAL === 0) {
        console.log(`已处理 ${processedCleaned}/${cleanedRecords.length} 个cleaned记录...`);
      }
    }
  });
  
  console.log(`cleaned-results映射创建完成，共 ${cleanedMap.size} 个唯一地址`);
  
  // 处理merge-owners数据
  console.log('处理merge-owners数据...');
  const mergeMap = new Map();
  const duplicateAddresses = [];
  const newAddresses = [];
  let processedMerge = 0;
  
  mergeRecords.forEach(record => {
    const address = record.owner_address;
    
    if (cleanedMap.has(address)) {
      // 地址已存在，需要累加积分
      const existingRecord = cleanedMap.get(address);
      const newTotalScore = (existingRecord.total_score || 0) + (record.total_score || 0);
      
      // 更新记录，设置上限为24
      existingRecord.total_score = Math.min(newTotalScore, 24);
      existingRecord.merged = true; // 标记为已合并
      existingRecord.merge_info = {
        original_score: existingRecord.total_score - record.total_score,
        added_score: record.total_score,
        token_count: record.token_count,
        score_count: record.score_count
      };
      
      cleanedMap.set(address, existingRecord);
      duplicateAddresses.push(address);
    } else {
      // 新地址，添加到新地址列表
      newAddresses.push({
        address: address,
        total_score: Math.min(record.total_score, 24), // 设置上限为24
        // 从第一个cleaned记录复制结构，设置默认值
        is_sender: 0,
        is_receiver: 0,
        is_dex_user_v2: 0,
        is_dex_user_v3: 0,
        is_bridge_user: 0,
        is_lending_user: 0,
        label: '',
        status_reason: '',
        balance_wei: 0,
        user_label: '',
        ens_name: '',
        merged: false, // 标记为新增地址
        merge_info: {
          original_score: 0,
          added_score: Math.min(record.total_score, 24), // 设置上限为24
          token_count: record.token_count,
          score_count: record.score_count
        }
      });
    }
    
    processedMerge++;
    if (processedMerge % CONFIG.PROGRESS_INTERVAL === 0) {
      console.log(`已处理 ${processedMerge}/${mergeRecords.length} 个merge记录...`);
    }
  });
  
  console.timeEnd('数据合并耗时');
  
  // 创建最终结果数组
  console.log('创建最终结果...');
  const finalResults = [];
  
  // 添加所有cleaned记录（包括已更新的）
  for (const [address, record] of cleanedMap) {
    finalResults.push(record);
  }
  
  // 添加新地址（分批处理避免栈溢出）
  const batchSize = 10000;
  for (let i = 0; i < newAddresses.length; i += batchSize) {
    const batch = newAddresses.slice(i, Math.min(i + batchSize, newAddresses.length));
    finalResults.push(...batch);
  }
  
  console.log(`合并完成！总计: ${finalResults.length} 条记录`);
  
  const stats = {
    originalCleanedCount: cleanedRecords.length,
    originalMergeCount: mergeRecords.length,
    duplicateCount: duplicateAddresses.length,
    newAddressCount: newAddresses.length,
    finalCount: finalResults.length,
    scoreUpdatedCount: duplicateAddresses.length
  };
  
  return { results: finalResults, stats };
}

/**
 * 将结果写入多个CSV文件
 * @param {Array} results 合并后的结果
 * @param {string} outputDir 输出目录
 * @param {Object} stats 统计信息
 */
function writeResultsToFiles(results, outputDir, stats) {
  console.log('开始写入结果文件...');
  console.time('文件写入耗时');
  
  // 确保输出目录存在
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
  
  const totalFiles = Math.ceil(results.length / CONFIG.MAX_RECORDS_PER_FILE);
  const files = [];
  
  // 定义CSV标题
  const headers = [
    'address',
    'is_sender',
    'is_receiver', 
    'is_dex_user_v2',
    'is_dex_user_v3',
    'is_bridge_user',
    'is_lending_user',
    'total_score',
    'label',
    'status_reason',
    'balance_wei',
    'user_label',
    'ens_name'
  ];
  
  for (let fileIndex = 0; fileIndex < totalFiles; fileIndex++) {
    const startIndex = fileIndex * CONFIG.MAX_RECORDS_PER_FILE;
    const endIndex = Math.min(startIndex + CONFIG.MAX_RECORDS_PER_FILE, results.length);
    const fileRecords = results.slice(startIndex, endIndex);
    
    const fileName = totalFiles > 1 
      ? `merged_cleaned_part${fileIndex + 1}_of_${totalFiles}.csv`
      : 'merged_cleaned.csv';
    const filePath = path.join(outputDir, fileName);
    
    console.log(`写入第 ${fileIndex + 1}/${totalFiles} 个文件: ${fileName}`);
    console.log(`记录范围: ${startIndex + 1} - ${endIndex} (共 ${fileRecords.length} 条)`);
    
    // 创建CSV内容
    const csvContent = [];
    
    // 直接添加标题行（第一行）
    csvContent.push(headers.join(','));
    
    // 添加数据行
    let writtenCount = 0;
    fileRecords.forEach(record => {
      const row = headers.map(header => {
        let value = record[header] || '';
        // 处理特殊字符和引号
        if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\\n'))) {
          value = `"${value.replace(/"/g, '""')}"`;
        }
        return value;
      });
      
      csvContent.push(row.join(','));
      writtenCount++;
      
      if (writtenCount % 50000 === 0) {
        console.log(`  已写入 ${writtenCount}/${fileRecords.length} 条记录...`);
      }
    });
    
    // 写入文件
    fs.writeFileSync(filePath, csvContent.join('\n'), 'utf8');
    
    files.push({
      name: fileName,
      path: filePath,
      records: fileRecords.length,
      range: `${startIndex + 1} - ${endIndex}`
    });
    
    console.log(`✅ 文件 ${fileName} 写入完成`);
  }
  
  console.timeEnd('文件写入耗时');
  
  // 显示文件汇总
  console.log('📄 CSV文件生成完成！');
  console.log(`📁 输出目录: ${path.resolve(outputDir)}`);
  console.log('📊 文件详情:');
  files.forEach((file, index) => {
    console.log(`  ${index + 1}. ${file.name}`);
    console.log(`     记录数: ${file.records.toLocaleString()}`);
    console.log(`     范围: ${file.range}`);
  });
  console.log(`📈 总记录数: ${results.length.toLocaleString()}`);
  
  return files;
}

/**
 * 生成合并统计报告
 * @param {Object} stats 统计信息
 * @param {string} outputDir 输出目录
 */
function generateMergeReport(stats, outputDir) {
  const reportContent = [
    '# 数据合并统计报告',
    `# 生成时间: ${new Date().toISOString()}`,
    '',
    '## 数据源统计',
    `- 原始cleaned-results记录数: ${stats.originalCleanedCount.toLocaleString()}`,
    `- 原始merge-owners记录数: ${stats.originalMergeCount.toLocaleString()}`,
    '',
    '## 合并结果统计', 
    `- 积分更新地址数: ${stats.scoreUpdatedCount.toLocaleString()}`,
    `- 新增地址数: ${stats.newAddressCount.toLocaleString()}`,
    `- 最终总记录数: ${stats.finalCount.toLocaleString()}`,
    '',
    '## 处理说明',
    '- 对于重复地址：原有积分 + 新增积分',
    '- 对于新地址：直接追加到末尾',
    '- 输出文件按每50万条记录分割',
    '',
    '## 数据完整性',
    `- 预期总记录数: ${stats.originalCleanedCount + stats.newAddressCount}`,
    `- 实际总记录数: ${stats.finalCount}`,
    `- 数据完整性: ${stats.finalCount === (stats.originalCleanedCount + stats.newAddressCount) ? '✅ 正确' : '❌ 异常'}`,
  ];
  
  const reportPath = path.join(outputDir, 'merge_report.txt');
  fs.writeFileSync(reportPath, reportContent.join('\n'), 'utf8');
  
  console.log(`合并统计报告已保存到: ${reportPath}`);
}

/**
 * 主函数
 */
async function main() {
  console.time('总耗时');
  console.log('===============================================');
  console.log('开始合并merge-owners数据到cleaned-results');
  console.log('===============================================');
  
  try {
    // 获取命令行参数
    const args = process.argv.slice(2);
    
    const cleanedDir = args[0] || path.join(__dirname, '../cleaned-results');
    const mergeDir = args[1] || path.join(__dirname, '../token/merge-owners');
    const outputDir = args[2] || path.join(__dirname, '../final-results');
    
    console.log(`cleaned-results目录: ${cleanedDir}`);
    console.log(`merge-owners目录: ${mergeDir}`);
    console.log(`输出目录: ${outputDir}`);
    
    // 检查目录是否存在
    if (!fs.existsSync(cleanedDir)) {
      throw new Error(`cleaned-results目录不存在: ${cleanedDir}`);
    }
    if (!fs.existsSync(mergeDir)) {
      throw new Error(`merge-owners目录不存在: ${mergeDir}`);
    }
    
    logMemoryUsage();
    
    // 读取所有数据
    const cleanedRecords = await readAllCleanedFiles(cleanedDir);
    const mergeRecords = await readAllMergeFiles(mergeDir);
    
    console.log('数据读取完成！');
    logMemoryUsage();
    
    // 合并数据
    const { results, stats } = mergeData(cleanedRecords, mergeRecords);
    
    console.log('数据合并完成！');
    logMemoryUsage();
    
    // 写入结果文件
    const files = writeResultsToFiles(results, outputDir, stats);
    
    // 生成统计报告
    generateMergeReport(stats, outputDir);
    
    console.log('===============================================');
    console.log('合并完成! 📊 统计信息:');
    console.log(`📁 原始cleaned记录数: ${stats.originalCleanedCount.toLocaleString()}`);
    console.log(`📝 原始merge记录数: ${stats.originalMergeCount.toLocaleString()}`);
    console.log(`🔄 积分更新地址数: ${stats.scoreUpdatedCount.toLocaleString()}`);
    console.log(`➕ 新增地址数: ${stats.newAddressCount.toLocaleString()}`);
    console.log(`📊 最终总记录数: ${stats.finalCount.toLocaleString()}`);
    console.log(`📄 输出文件数: ${files.length}`);
    console.log('===============================================');
    
  } catch (error) {
    console.error('❌ 处理过程中出现错误:', error.message);
    console.error(error.stack);
    process.exit(1);
  }
  
  console.timeEnd('总耗时');
}

// 错误处理
process.on('unhandledRejection', (reason, promise) => {
  console.error('未处理的Promise拒绝:', reason);
  process.exit(1);
});

process.on('uncaughtException', (error) => {
  console.error('未捕获的异常:', error);
  process.exit(1);
});

// 如果作为主模块运行
if (require.main === module) {
  main();
}

module.exports = {
  readCSVFile,
  readAllCleanedFiles,
  readAllMergeFiles,
  mergeData,
  writeResultsToFiles,
  generateMergeReport
};