# -*- coding: utf-8 -*-
"""
超级字典构建优化版（步骤2）
============================
功能：合并自动匹配和人工审核结果，构建Master Company Dictionary

改进点：
1. 更好的错误处理和验证
2. 冲突检测和报告
3. 详细的日志记录
4. 统计信息输出
"""

import pandas as pd
import os
import pickle
import logging
from datetime import datetime

# ==========================================
# 配置日志
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'dict_building_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 配置：输入文件列表
# ==========================================
# 可以包含多年份的文件，按需添加
FILES_TO_PROCESS = [
    # --- 1993-1997年文件 ---
    'Step1_Manual_Review.xlsx',      # 人工审核后的文件（删除错误匹配）
    'Step1_Auto_Results.xlsx',       # 自动匹配结果
    
    # --- 如果有其他年份，添加在此 ---
    # '1998_2000_Manual_Review.xlsx',
    # '1998_2000_Auto_Results.xlsx',

]

# 输出文件配置
OUTPUT_DICT_FILE = 'Master_Company_Dictionary.pkl'       # 用于代码加载（Pickle格式）
OUTPUT_EXCEL_FILE = 'Master_Company_Dictionary_VIEW.xlsx' # 用于人工查看（Excel格式）

# ==========================================
# 2. 主处理函数
# ==========================================

def build_master_dictionary(files_list):
    """
    构建超级字典
    返回: master_dict, statistics
    """
    logger.info("=" * 60)
    logger.info("开始构建超级字典（Master Company Dictionary）")
    logger.info("=" * 60)
    
    master_dict = {}  # 结构: { 'Assignee_Original': 'Original_Acquiror_Name' }
    source_stats = []
    conflicts = []  # 记录冲突
    
    for file_path in files_list:
        if not os.path.exists(file_path):
            logger.warning(f"⚠️  跳过：找不到文件 {file_path}")
            continue
        
        logger.info(f"\n正在处理: {file_path}")
        
        try:
            df = pd.read_excel(file_path)
            
            # 检查必要的列
            required_cols = ['Assignee_Original', 'Original_Acquiror_Name']
            if not all(col in df.columns for col in required_cols):
                logger.error(f"   ❌ 错误：缺少必要列 {required_cols}，跳过此文件")
                continue
            
            # 过滤无效行
            df_valid = df.dropna(subset=required_cols)
            df_valid = df_valid[
                (df_valid['Assignee_Original'].astype(str).str.strip() != "") &
                (df_valid['Original_Acquiror_Name'].astype(str).str.strip() != "")
            ]
            
            logger.info(f"   有效行数: {len(df_valid)}")
            
            # 统计信息
            count_new = 0
            count_duplicate = 0
            count_conflict = 0
            
            for idx, row in df_valid.iterrows():
                assignee_raw = str(row['Assignee_Original']).strip()
                acquiror_std = str(row['Original_Acquiror_Name']).strip()
                
                if assignee_raw not in master_dict:
                    # 新映射
                    master_dict[assignee_raw] = acquiror_std
                    count_new += 1
                else:
                    # 已存在的映射
                    existing = master_dict[assignee_raw]
                    if existing == acquiror_std:
                        # 重复但一致
                        count_duplicate += 1
                    else:
                        # 冲突！
                        count_conflict += 1
                        conflicts.append({
                            'Assignee': assignee_raw,
                            'Existing_Acquiror': existing,
                            'New_Acquiror': acquiror_std,
                            'Source_File': file_path
                        })
                        # 策略：保留第一次出现的映射，记录冲突
                        logger.warning(f"   ⚠️  冲突: '{assignee_raw}' 已映射为 '{existing}'，新值 '{acquiror_std}' 被忽略")
            
            logger.info(f"   ✅ 处理完成: 新增 {count_new}，重复 {count_duplicate}，冲突 {count_conflict}")
            
            source_stats.append({
                'File': os.path.basename(file_path),
                'Valid_Rows': len(df_valid),
                'New_Mappings': count_new,
                'Duplicates': count_duplicate,
                'Conflicts': count_conflict
            })
            
        except Exception as e:
            logger.error(f"   ❌ 读取失败: {e}")
    
    return master_dict, source_stats, conflicts


def save_dictionary(master_dict, source_stats, conflicts):
    """保存字典和统计信息"""
    logger.info("\n" + "=" * 60)
    logger.info("保存结果")
    logger.info("=" * 60)
    
    if not master_dict:
        logger.error("❌ 错误：字典为空！没有提取到任何映射关系。")
        return False
    
    # 1. 保存为 Pickle（用于后续代码加载）
    with open(OUTPUT_DICT_FILE, 'wb') as f:
        pickle.dump(master_dict, f)
    logger.info(f"✅ Pickle文件已保存: {OUTPUT_DICT_FILE}")
    
    # 2. 保存为 Excel（用于人工查看）
    df_out = pd.DataFrame(
        list(master_dict.items()), 
        columns=['Assignee_Original_Name', 'Mapped_Acquiror_Name']
    )
    df_out = df_out.sort_values('Mapped_Acquiror_Name').reset_index(drop=True)
    df_out.to_excel(OUTPUT_EXCEL_FILE, index=False)
    logger.info(f"✅ Excel文件已保存: {OUTPUT_EXCEL_FILE}")
    
    # 3. 保存统计信息
    if source_stats:
        df_stats = pd.DataFrame(source_stats)
        stats_file = 'Dictionary_Build_Statistics.xlsx'
        df_stats.to_excel(stats_file, index=False)
        logger.info(f"✅ 统计信息已保存: {stats_file}")
    
    # 4. 如果有冲突，保存冲突报告
    if conflicts:
        df_conflicts = pd.DataFrame(conflicts)
        conflict_file = 'Dictionary_Conflicts.xlsx'
        df_conflicts.to_excel(conflict_file, index=False)
        logger.warning(f"⚠️  冲突报告已保存: {conflict_file} ({len(conflicts)} 条冲突)")
    
    return True


def print_summary(master_dict, source_stats, conflicts):
    """打印摘要信息"""
    logger.info("\n" + "=" * 60)
    logger.info("构建完成摘要")
    logger.info("=" * 60)
    
    logger.info(f"\n📊 总体统计:")
    logger.info(f"   - 总映射关系数: {len(master_dict):,}")
    logger.info(f"   - 处理文件数: {len(source_stats)}")
    logger.info(f"   - 检测到的冲突: {len(conflicts)}")
    
    if source_stats:
        logger.info(f"\n📁 各文件贡献:")
        for stat in source_stats:
            logger.info(f"   {stat['File']}")
            logger.info(f"      新增: {stat['New_Mappings']}, 重复: {stat['Duplicates']}, 冲突: {stat['Conflicts']}")
    
    # 统计映射到同一公司的变体数
    from collections import Counter
    acquiror_counts = Counter(master_dict.values())
    top_companies = acquiror_counts.most_common(10)
    
    logger.info(f"\n🏢 变体最多的公司（Top 10）:")
    for company, count in top_companies:
        logger.info(f"   {company}: {count} 个变体")
    
    if conflicts:
        logger.info(f"\n⚠️  警告: 发现 {len(conflicts)} 个冲突，请检查 Dictionary_Conflicts.xlsx")
        logger.info("   冲突处理策略: 保留首次出现的映射")


# ==========================================
# 3. 主执行流程
# ==========================================

def main():
    start_time = datetime.now()
    
    # 构建字典
    master_dict, source_stats, conflicts = build_master_dictionary(FILES_TO_PROCESS)
    
    # 保存结果
    success = save_dictionary(master_dict, source_stats, conflicts)
    
    if success:
        # 打印摘要
        print_summary(master_dict, source_stats, conflicts)
        
        # 计算耗时
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"\n⏱  总耗时: {duration:.2f} 秒")
        logger.info("\n" + "=" * 60)
        logger.info("🎉 超级字典构建成功！")
        logger.info("=" * 60)
        logger.info(f"\n下一步:")
        logger.info(f"   1. 检查 {OUTPUT_EXCEL_FILE} 确认映射关系")
        logger.info(f"   2. 如有冲突，审查 Dictionary_Conflicts.xlsx")
        logger.info(f"   3. 运行步骤3（最终聚合）使用此字典")
        
        return True
    else:
        logger.error("\n❌ 字典构建失败")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
