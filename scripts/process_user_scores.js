// 导入所需模块
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify/sync');

// 配置路径
const DEFI_POSITIONS_DIR = path.join(__dirname, '../defi-positions');
const FILTERED_DIR = path.join(DEFI_POSITIONS_DIR, 'filtered');
const OUTPUT_FILE = path.join(__dirname, '../csv/user_scores.csv');

// 积分计算规则
const SCORE_RULES = [
  { threshold: 1000000, score: 12, label: '巨鲸' },
  { threshold: 100000, score: 6, label: '大户' },
  { threshold: 1000, score: 3, label: '中户' },
  { threshold: 100, score: 2, label: '小户' },
  { threshold: 20, score: 1, label: '极小户' }
];

/**
 * 计算持仓积分
 * @param {number} totalValue 持仓总价值（美元）
 * @returns {Object} 包含积分和标签的对象
 */
function calculateScore(totalValue) {
  for (const rule of SCORE_RULES) {
    if (totalValue >= rule.threshold) {
      return {
        score: rule.score,
        label: rule.label
      };
    }
  }
  return { score: 0, label: '未达标' };
}

/**
 * 处理单个CSV文件
 * @param {string} filePath CSV文件路径
 * @returns {Promise<Object>} 处理结果和筛选后的记录
 */
async function processDefiPositionsFile(filePath) {
  console.log(`处理文件: ${filePath}`);
  
  const results = {
    total: 0,
    filtered: 0,
    whales: 0
  };
  
  const records = [];
  
  try {
    const parser = fs
      .createReadStream(filePath)
      .pipe(parse({
        columns: true,
        skip_empty_lines: true
      }));
    
    for await (const record of parser) {
      results.total++;
      
      // 确保totalValue是数字
      const totalValue = parseFloat(record.totalValue || 0);
      
      // 筛选出totalValue大于20的记录
      if (totalValue > 20) {
        results.filtered++;
        
        // 计算积分和标签
        const { score, label } = calculateScore(totalValue);
        
        // 如果是巨鲸，记录数量
        if (label === '巨鲸') {
          results.whales++;
        }
        
        // 添加新的字段
        records.push({
          ...record,
          score,
          user_label: label
        });
      }
    }
    
    return { results, records };
  } catch (error) {
    console.error(`处理文件 ${filePath} 时出错:`, error);
    return {
      results: {
        total: results.total,
        filtered: 0,
        whales: 0
      },
      records: []
    };
  }
}

/**
 * 执行完整的数据处理流程
 */
async function processUserData() {
  console.log('==========================================');
  console.log('开始处理用户DeFi持仓数据并生成积分表');
  console.log('==========================================\n');
  
  try {
    // 读取defi-positions目录中的所有CSV文件
    const files = fs.readdirSync(DEFI_POSITIONS_DIR);
    
    // 筛选出CSV文件
    const csvFiles = files.filter(file => 
      file.endsWith('.csv') && 
      file.includes('defi_positions_') &&
      fs.statSync(path.join(DEFI_POSITIONS_DIR, file)).isFile()
    );
    
    if (csvFiles.length === 0) {
      console.error(`错误: 在目录 ${DEFI_POSITIONS_DIR} 中没有找到CSV文件`);
      return;
    }
    
    console.log(`找到 ${csvFiles.length} 个CSV文件需要处理`);
    
    // 用于存储所有用户数据的映射
    const userDataMap = new Map();
    
    // 总结果统计
    const totalResults = {
      files: csvFiles.length,
      totalRecords: 0,
      filteredRecords: 0,
      whales: 0,
      errors: 0
    };
    
    // 处理每个CSV文件
    for (const file of csvFiles) {
      const filePath = path.join(DEFI_POSITIONS_DIR, file);
      const { results, records } = await processDefiPositionsFile(filePath);
      
      // 累计结果
      totalResults.totalRecords += results.total;
      totalResults.filteredRecords += results.filtered;
      totalResults.whales += results.whales;
      
      console.log(`文件 ${file} 共 ${results.total} 条记录，筛选出 ${results.filtered} 条记录，包含 ${results.whales} 个巨鲸地址`);
      
      // 将符合条件的记录添加到用户数据映射中
      for (const record of records) {
        const address = record.address;
        const totalValue = parseFloat(record.totalValue || 0);
        
        // 如果地址已存在，取较高的价值
        if (userDataMap.has(address)) {
          const existingData = userDataMap.get(address);
          if (totalValue > existingData.totalValue) {
            userDataMap.set(address, record);
          }
        } else {
          // 新地址，直接添加
          userDataMap.set(address, record);
        }
      }
      
      // 可选：保存筛选后的数据（如果需要）
      if (records.length > 0 && false) { // 设为false禁用单文件输出，减少磁盘IO
        // 确保输出目录存在
        if (!fs.existsSync(FILTERED_DIR)) {
          fs.mkdirSync(FILTERED_DIR, { recursive: true });
        }
        
        const fileName = path.basename(filePath);
        const outputPath = path.join(FILTERED_DIR, `filtered_${fileName}`);
        
        const csv = stringify(records, { 
          header: true,
          columns: [...Object.keys(records[0])]
        });
        fs.writeFileSync(outputPath, csv);
      }
    }
    
    // 将Map转换为数组，并按totalValue降序排列
    const sortedUserData = Array.from(userDataMap.values())
      .sort((a, b) => parseFloat(b.totalValue) - parseFloat(a.totalValue));
    
    // 确保输出目录存在
    const outputDir = path.dirname(OUTPUT_FILE);
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
    
    // 写入最终的用户积分表
    const csv = stringify(sortedUserData, { 
      header: true,
      columns: ['address', 'totalValue', 'score', 'user_label', 'usdt', 'usdc', 'weth', 'wbtc']
    });
    fs.writeFileSync(OUTPUT_FILE, csv);
    
    // 统计数据
    const whaleCount = sortedUserData.filter(user => user.user_label === '巨鲸').length;
    const largeHolderCount = sortedUserData.filter(user => user.user_label === '大户').length;
    const mediumHolderCount = sortedUserData.filter(user => user.user_label === '中户').length;
    const smallHolderCount = sortedUserData.filter(user => user.user_label === '小户').length;
    const tinyHolderCount = sortedUserData.filter(user => user.user_label === '极小户').length;
    
    // 输出总结果
    console.log('\n==========================================');
    console.log('处理完成! 总结:');
    console.log(`处理文件数: ${totalResults.files}`);
    console.log(`总记录数: ${totalResults.totalRecords}`);
    console.log(`符合条件记录数(>20美元): ${totalResults.filteredRecords}`);
    console.log(`去重后的用户数: ${sortedUserData.length}`);
    console.log('\n用户分布统计:');
    console.log(`巨鲸地址(>100万美元): ${whaleCount} 个，占比 ${(whaleCount / sortedUserData.length * 100).toFixed(2)}%`);
    console.log(`大户(>10万美元): ${largeHolderCount} 个，占比 ${(largeHolderCount / sortedUserData.length * 100).toFixed(2)}%`);
    console.log(`中户(>1000美元): ${mediumHolderCount} 个，占比 ${(mediumHolderCount / sortedUserData.length * 100).toFixed(2)}%`);
    console.log(`小户(>100美元): ${smallHolderCount} 个，占比 ${(smallHolderCount / sortedUserData.length * 100).toFixed(2)}%`);
    console.log(`极小户(>20美元): ${tinyHolderCount} 个，占比 ${(tinyHolderCount / sortedUserData.length * 100).toFixed(2)}%`);
    console.log(`\n用户积分表已保存至: ${OUTPUT_FILE}`);
    console.log('==========================================');
    
  } catch (error) {
    console.error('处理数据时出错:', error);
  }
}

// 执行主函数
if (require.main === module) {
  processUserData().catch(console.error);
}

module.exports = {
  processUserData,
  calculateScore
}; 