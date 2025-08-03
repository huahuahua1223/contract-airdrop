const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const { stringify } = require('csv-stringify/sync');

/**
 * 合并user_scores.csv中的积分和标签到所有CSV文件
 * 将csv文件中的total_score与user_scores.csv中的score相加
 */
async function mergeUserScores() {
  console.log('开始合并用户积分数据...');
  
  // 创建输出目录
  const outputDir = path.join(__dirname, '..', 'csv-merged');
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
  
  try {
    // 读取user_scores.csv数据
    const userScoresPath = path.join(__dirname, '..', 'defi-positions', 'user_scores.csv');
    const userScoresData = fs.readFileSync(userScoresPath, 'utf8');
    const userScores = parse(userScoresData, { 
      columns: true,
      skip_empty_lines: true
    });
    
    console.log(`已读取用户积分数据，共 ${userScores.length} 条记录`);
    
    // 创建地址到积分和标签的映射
    const addressMap = new Map();
    for (const row of userScores) {
      // 假设user_scores.csv中有address、score和user_label列
      const address = row.address?.toLowerCase();
      if (address) {
        addressMap.set(address, {
          score: parseFloat(row.score) || 0, // 确保是数字
          user_label: row.user_label
        });
      }
    }
    
    console.log(`共创建了 ${addressMap.size} 个地址映射`);
    
    // 读取csv文件夹中的所有CSV文件
    const csvDir = path.join(__dirname, '..', 'csv');
    const csvFiles = fs.readdirSync(csvDir)
      .filter(file => file.toLowerCase().endsWith('.csv'))
      .map(file => path.join(csvDir, file));
    
    console.log(`找到 ${csvFiles.length} 个CSV文件需要处理`);
    
    // 处理每个CSV文件
    for (const csvFile of csvFiles) {
      const fileName = path.basename(csvFile);
      console.log(`处理文件: ${fileName}`);
      
      try {
        // 读取CSV文件内容
        const csvContent = fs.readFileSync(csvFile, 'utf8');
        
        // 解析CSV内容
        const records = parse(csvContent, { 
          skip_empty_lines: true,
          // 自动检测分隔符（逗号或制表符）
          delimiter: csvContent.includes('\t') ? '\t' : ','
        });
        
        // 获取表头
        const headers = records[0];
        
        // 确定地址列索引（假设是第2列，索引为1）
        const addressColumnIndex = 1;
        
        // 查找total_score列索引
        let totalScoreIndex = headers.indexOf('total_score');
        // 如果没有total_score列，查找score列
        if (totalScoreIndex === -1) {
          totalScoreIndex = headers.indexOf('score');
        }
        // 如果还是没找到，添加total_score列
        if (totalScoreIndex === -1) {
          headers.push('total_score');
          totalScoreIndex = headers.length - 1;
        }
        
        // 添加user_label列
        let labelColumnIndex = headers.indexOf('user_label');
        if (labelColumnIndex === -1) {
          headers.push('user_label');
          labelColumnIndex = headers.length - 1;
        }
        
        // 处理每一行数据
        for (let i = 1; i < records.length; i++) {
          const row = records[i];
          // 确保行有足够的列
          while (row.length < headers.length) {
            row.push('');
          }
          
          // 获取地址并转为小写
          const address = row[addressColumnIndex]?.toLowerCase();
          if (address && addressMap.has(address)) {
            const userData = addressMap.get(address);
            
            // 合并积分：将原有积分与user_scores.csv中的积分相加
            const existingScore = parseFloat(row[totalScoreIndex]) || 0;
            const userScore = userData.score || 0;
            const totalScore = existingScore + userScore;
            
            // 更新total_score列
            row[totalScoreIndex] = totalScore.toString();
            
            // 更新标签（如果原来没有标签）
            if (!row[labelColumnIndex] && userData.user_label) {
              row[labelColumnIndex] = userData.user_label;
            }
            
            // console.log(`地址 ${address} 积分合并: ${existingScore} + ${userScore} = ${totalScore}`);
          }
        }
        
        // 将处理后的数据写入新文件
        const outputPath = path.join(outputDir, fileName);
        const output = stringify(records);
        fs.writeFileSync(outputPath, output);
        
        console.log(`✓ 已处理并保存: ${fileName}`);
      } catch (err) {
        console.error(`处理文件 ${fileName} 时出错:`, err);
      }
    }
    
    // 生成合并统计报告
    const summaryPath = path.join(outputDir, '_merge_summary.txt');
    const summary = `
合并用户积分报告
===================
日期: ${new Date().toLocaleString()}
处理文件数: ${csvFiles.length}
用户积分记录数: ${userScores.length}
有效地址映射: ${addressMap.size}
输出目录: ${outputDir}
说明: 已将csv目录中文件的total_score列与user_scores.csv中的score列相加
    `;
    fs.writeFileSync(summaryPath, summary);
    
    console.log('✅ 数据合并完成!');
    console.log(`合并后的CSV文件已保存到: ${outputDir}`);
    
  } catch (err) {
    console.error('合并用户积分数据时发生错误:', err);
    process.exit(1);
  }
}

// 执行合并函数
mergeUserScores().catch(console.error); 