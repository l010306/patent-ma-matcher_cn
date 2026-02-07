# -*- coding: utf-8 -*-
"""
Compustat匹配 - 步骤4B：应用审核结果（优化版）
================================================
功能：读取人工审核后的验证文件，将Compustat ID合并到final_outcome

改进点：
1. 详细的数据验证
2. 保留ID的前导零（使用dtype=str）
3. 详细日志
"""

import pandas as pd
import logging
from datetime import datetime

# ==========================================
# 配置日志
# ==========================================
# 确保logs文件夹存在
import os
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/compustat_merge_4B_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 路径配置
# ==========================================
PATH_MAIN = "/Users/lidachuan/Desktop/Patent Data/final_outcome_1993_1997_COMPLETE.xlsx"
PATH_COMPUSTAT = "/Users/lidachuan/Desktop/Patent Data/compustat_19802025.csv"
PATH_VERIFIED = "/Users/lidachuan/Desktop/Patent Data/company_match_verification.xlsx"
PATH_OUTPUT = "/Users/lidachuan/Desktop/Patent Data/final_outcome.xlsx"

# ==========================================
# 2. 主处理函数
# ==========================================

def main():
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info("Compustat匹配 - 步骤4B：应用审核结果（优化版）")
    logger.info("=" * 60)
    
    # ========== 步骤1: 读取数据 ==========
    logger.info("\n步骤 1/4: 读取数据...")
    
    # 读取主表
    logger.info("   加载主表...")
    try:
        df_main = pd.read_excel(PATH_MAIN)
        logger.info(f"   ✅ 主表加载完成: {len(df_main):,} 行")
    except Exception as e:
        logger.error(f"   ❌ 读取失败: {e}")
        return False
    
    # 读取人工验证表（已审核）
    logger.info("   加载人工验证表...")
    try:
        df_verify = pd.read_excel(
            PATH_VERIFIED, 
            usecols=['Acquiror_Original', 'Matched_Compustat_Original']
        )
        # 去重
        df_verify = df_verify.drop_duplicates(subset=['Acquiror_Original'])
        logger.info(f"   ✅ 验证表加载完成: {len(df_verify):,} 个有效匹配对")
    except Exception as e:
        logger.error(f"   ❌ 读取失败: {e}")
        logger.error("   请确保已完成步骤4A并人工审核了验证文件")
        return False
    
    # 读取 Compustat 数据（保留前导零）
    logger.info("   加载 Compustat 数据...")
    try:
        cols_to_load = ['conm', 'gvkey', 'cusip', 'cik']
        df_comp = pd.read_csv(
            PATH_COMPUSTAT, 
            usecols=cols_to_load, 
            dtype=str,  # 保留前导零
            low_memory=False
        )
        logger.info(f"   ✅ Compustat 数据加载完成: {len(df_comp):,} 行")
    except ValueError:
        # 如果列名不匹配，尝试全量读取
        logger.warning("   列名可能不匹配，尝试全量读取...")
        df_comp = pd.read_csv(PATH_COMPUSTAT, dtype=str, low_memory=False)
        logger.info(f"   ✅ Compustat 数据加载完成（全量）: {len(df_comp):,} 行")
    except Exception as e:
        logger.error(f"   ❌ 读取失败: {e}")
        return False
    
    # ========== 步骤2: 处理 Compustat 数据 ==========
    logger.info("\n步骤 2/4: 构建 Compustat 字典...")
    
    # 去除 conm 为空的行
    df_comp_clean = df_comp[df_comp['conm'].notna()].copy()
    
    # 按 conm 去重（保留第一条记录）
    df_comp_unique = df_comp_clean.drop_duplicates(subset=['conm'])
    
    logger.info(f"   ✅ Compustat 唯一公司: {len(df_comp_unique):,}")
    
    # ========== 步骤3: 合并数据 ==========
    logger.info("\n步骤 3/4: 合并数据...")
    
    # 3.1 将验证表与Compustat ID合并
    logger.info("   阶段 3.1: 获取 Compustat ID...")
    df_verify_with_ids = pd.merge(
        df_verify,
        df_comp_unique[['conm', 'gvkey', 'cusip', 'cik']],
        left_on='Matched_Compustat_Original',
        right_on='conm',
        how='left'
    )
    
    # 统计匹配成功率
    id_matched = df_verify_with_ids['gvkey'].notna().sum()
    logger.info(f"   ✅ ID匹配成功: {id_matched} / {len(df_verify)} ({id_matched/len(df_verify)*100:.1f}%)")
    
    # 3.2 与主表合并（填充现有的gvkey/cusip/cik列）
    logger.info("   阶段 3.2: 填充主表的ID列...")
    
    # 创建映射字典
    acquiror_to_ids = {}
    for _, row in df_verify_with_ids.iterrows():
        acquiror_name = row['Acquiror_Original']
        acquiror_to_ids[acquiror_name] = {
            'gvkey': row.get('gvkey', None),
            'cusip': row.get('cusip', None),
            'cik': row.get('cik', None),
            'compustat_name': row.get('Matched_Compustat_Original', None)
        }
    
    # 填充现有列（保留原有值，仅填充空值）
    df_final = df_main.copy()
    
    # 确保列存在
    for col in ['gvkey', 'cusip', 'cik', 'compustat_name']:
        if col not in df_final.columns:
            df_final[col] = None
    
    # 逐行填充
    for idx, row in df_final.iterrows():
        acquiror_name = row['acquiror_name']
        if acquiror_name in acquiror_to_ids:
            ids = acquiror_to_ids[acquiror_name]
            # 只填充空值
            if pd.isna(df_final.at[idx, 'gvkey']):
                df_final.at[idx, 'gvkey'] = ids['gvkey']
            if pd.isna(df_final.at[idx, 'cusip']):
                df_final.at[idx, 'cusip'] = ids['cusip']
            if pd.isna(df_final.at[idx, 'cik']):
                df_final.at[idx, 'cik'] = ids['cik']
            if pd.isna(df_final.at[idx, 'compustat_name']):
                df_final.at[idx, 'compustat_name'] = ids['compustat_name']
    
    logger.info(f"   ✅ 填充完成，最终行数: {len(df_final):,}")
    
    # ========== 步骤4: 保存结果 ==========
    logger.info("\n步骤 4/4: 保存结果...")
    
    try:
        df_final.to_excel(PATH_OUTPUT, index=False)
        logger.info(f"   ✅ 文件已保存: {PATH_OUTPUT}")
    except Exception as e:
        logger.error(f"   ❌ 保存失败: {e}")
        return False
    
    # ========== 完成摘要 ==========
    duration = (datetime.now() - start_time).total_seconds()
    
    # 统计
    total_rows = len(df_final)
    matched_count = df_final['compustat_name'].notna().sum()
    match_rate = matched_count / total_rows * 100
    
    has_gvkey = df_final['gvkey'].notna().sum()
    has_cusip = df_final['cusip'].notna().sum()
    has_cik = df_final['cik'].notna().sum()
    
    logger.info("\n" + "=" * 60)
    logger.info("步骤4B 完成！")
    logger.info("=" * 60)
    logger.info(f"⏱  总耗时: {duration:.2f} 秒")
    logger.info(f"\n📊 结果统计:")
    logger.info(f"   - 总行数: {total_rows:,}")
    logger.info(f"   - 匹配 Compustat: {matched_count:,} ({match_rate:.1f}%)")
    logger.info(f"   - 有 gvkey: {has_gvkey:,}")
    logger.info(f"   - 有 cusip: {has_cusip:,}")
    logger.info(f"   - 有 cik: {has_cik:,}")
    logger.info(f"\n📁 输出文件:")
    logger.info(f"   {PATH_OUTPUT}")
    logger.info(f"\n✅ 完整数据处理流程全部完成！🎉")
    logger.info("=" * 60)
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
