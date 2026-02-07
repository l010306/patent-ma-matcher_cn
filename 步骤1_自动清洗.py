# -*- coding: utf-8 -*-
"""
专利数据匹配优化版
==================
主要改进:
1. 使用 rapidfuzz 替代 fuzzywuzzy (性能提升 10-100倍)
2. 改进的名称清洗函数
3. 并行处理支持
4. 质量控制检查
5. 更好的日志和进度跟踪
6. 改进的发明人统计逻辑
"""

import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process
from tqdm import tqdm
import re
import logging
from datetime import datetime
from multiprocessing import Pool, cpu_count
import warnings
warnings.filterwarnings('ignore')

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
        logging.FileHandler(f'logs/matching_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 改进的清洗函数
# ==========================================

def clean_company_name(name):
    """
    优化的标准化清洗函数
    改进点:
    - 更精确的后缀处理
    - 保留常见缩写
    - 处理特殊字符的上下文
    """
    if pd.isna(name) or not isinstance(name, str):
        return ""
    
    name = str(name).upper().strip()
    
    # 1. 处理常见符号
    name = name.replace('&', ' AND ')
    name = name.replace('-', ' ')
    name = name.replace("'", '')
    
    # 2. 扩展常见缩写（提高匹配率）
    abbreviations = {
        r'\bINTL\b': 'INTERNATIONAL',
        r'\bNATL\b': 'NATIONAL',
        r'\bCORP\b': 'CORPORATION',
        r'\bINC\b': 'INCORPORATED',
        r'\bMFG\b': 'MANUFACTURING',
        r'\bTECH\b': 'TECHNOLOGY',
        r'\bSYS\b': 'SYSTEMS',
    }
    for abbr, full in abbreviations.items():
        name = re.sub(abbr, full, name)
    
    # 3. 分阶段去除后缀（按优先级）
    # 首先去除完整形式，避免部分匹配问题
    suffixes_priority = [
        # 完整形式优先
        r'\bINCORPORATED\b', r'\bCORPORATION\b', r'\bCOMPANY\b',
        r'\bLIMITED\b', r'\bGROUP\b',
        # 带点缩写
        r'\bCORP\.?\b', r'\bINC\.?\b', r'\bLTD\.?\b', 
        r'\bCO\.?\b', r'\bL\.L\.C\.?\b', r'\bPLC\.?\b',
        # 其他形式
        r'\bLLC\b', r'\bS\.A\.\b', r'\bNV\b', r'\bGMBH\b',
        r'\bSA\b', r'\bAG\b', r'\bKK\b'
    ]
    
    for suffix in suffixes_priority:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
    # 4. 去除标点（保留字母数字空格）
    name = re.sub(r'[^A-Z0-9\s]', ' ', name)
    
    # 5. 合并多余空格
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name


# ==========================================
# 2. 发明人统计函数（取最大值）
# ==========================================

def calculate_inventor_count_vectorized(df, inventor_cols):
    """
    按照韩语要求：取 inventors 列和名字列计数的较大值
    使用向量化操作提升性能
    """
    # 1. 从 inventors 列获取数值
    num_from_column = pd.to_numeric(df['inventors'], errors='coerce').fillna(0)
    
    # 2. 从名字列计数（向量化操作）
    num_from_names = df[inventor_cols].notna().sum(axis=1)
    
    # 3. 取两者中的较大值（符合韩语要求：둘 중 큰 값을 기준으로）
    return np.maximum(num_from_column, num_from_names)


# ==========================================
# 3. 批量模糊匹配函数（支持并行）
# ==========================================

def fuzzy_match_batch(args):
    """批量处理模糊匹配（用于并行处理）"""
    unmatched_chunk, acquiror_list, threshold, tier_name = args
    
    results = []
    for _, row in unmatched_chunk.iterrows():
        assignee = row['assignee']
        clean = row['clean_name']
        
        # 使用 rapidfuzz 的 extractOne，设置 score_cutoff 提前过滤
        match_result = process.extractOne(
            clean, 
            acquiror_list, 
            scorer=fuzz.token_set_ratio,
            score_cutoff=threshold
        )
        
        if match_result:
            match_name, score, _ = match_result
            results.append({
                'Assignee_Original': assignee,
                'Assignee_Clean': clean,
                'Matched_Acquiror_Clean': match_name,
                'Match_Type': f'Fuzzy (≥{threshold})',
                'Similarity': score,
                'Tier': tier_name
            })
    
    return results


# ==========================================
# 4. 质量控制检查
# ==========================================

def validate_matches(df_matches):
    """
    质量控制检查
    返回问题列表和统计信息
    """
    issues = []
    stats = {}
    
    if df_matches.empty:
        return issues, stats
    
    # 1. 检测一对多匹配（一个专利公司匹配多个并购公司）
    duplicates = df_matches.groupby('Assignee_Original')['Matched_Acquiror_Clean'].nunique()
    one_to_many = duplicates[duplicates > 1]
    if len(one_to_many) > 0:
        issues.append(f"⚠️  警告: {len(one_to_many)} 个专利公司匹配到多个并购公司（需人工检查）")
        stats['one_to_many'] = len(one_to_many)
    
    # 2. 检测分数分布
    score_dist = df_matches['Similarity'].describe()
    stats['score_distribution'] = score_dist.to_dict()
    
    low_score = df_matches[df_matches['Similarity'] < 95]
    if len(low_score) > 0:
        issues.append(f"ℹ️  信息: {len(low_score)} 个匹配分数 < 95，建议重点审查")
        stats['low_score_count'] = len(low_score)
    
    # 3. 检测过短名称（可能误匹配）
    short_names = df_matches[df_matches['Assignee_Clean'].str.len() < 3]
    if len(short_names) > 0:
        issues.append(f"⚠️  警告: {len(short_names)} 个公司名过短（如 '3M'），需人工验证")
        stats['short_names'] = len(short_names)
    
    # 4. 匹配类型统计
    match_type_dist = df_matches['Match_Type'].value_counts()
    stats['match_types'] = match_type_dist.to_dict()
    
    return issues, stats


# ==========================================
# 5. 主处理流程
# ==========================================

def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("专利数据匹配流程开始（优化版）")
    logger.info("=" * 60)
    
    # ========== 数据加载 ==========
    logger.info("正在加载数据...")
    
    # 请根据实际情况修改路径
    df_main = pd.read_excel('/Users/lidachuan/Desktop/Patent Data/final_outcome.xlsx')
    df_main.drop_duplicates(subset=['acquiror_name'], keep='first', inplace=True)
    logger.info(f"✅ 主数据库加载完成: {len(df_main)} 家公司")
    
    df_patent = pd.read_csv('/Users/lidachuan/Desktop/Patent Data/1993-1997/patent_database.csv')
    df_patent.dropna(subset=['assignee'], inplace=True)
    logger.info(f"✅ 专利数据库加载完成: {len(df_patent)} 条记录")
    
    # ========== 名称清洗 ==========
    logger.info("\n正在清洗公司名称...")
    df_main['clean_name'] = df_main['acquiror_name'].apply(clean_company_name)
    df_patent['clean_name'] = df_patent['assignee'].apply(clean_company_name)
    
    # 去除清洗后为空的行
    df_patent = df_patent[df_patent['clean_name'] != ""].copy()
    logger.info(f"✅ 清洗完成，保留 {len(df_patent)} 条有效专利记录")
    
    # ========== 发明人统计 ==========
    logger.info("\n正在计算发明人数量...")
    inventor_name_cols = [f'inventor_name{i}' for i in range(1, 11)]
    
    # 确保列存在
    for col in inventor_name_cols:
        if col not in df_patent.columns:
            df_patent[col] = np.nan
    
    # 使用向量化函数计算发明人数（取最大值，符合韩语要求）
    df_patent['final_inventor_count'] = calculate_inventor_count_vectorized(
        df_patent, 
        inventor_name_cols
    )
    logger.info(f"✅ 发明人统计完成，平均每专利 {df_patent['final_inventor_count'].mean():.2f} 人")
    
    # ========== 生成汇总表 ==========
    logger.info("\n正在生成公司汇总表...")
    df_summary = df_patent.groupby(['assignee', 'clean_name']).agg({
        'application_year': 'count',
        'final_inventor_count': 'sum'
    }).reset_index()
    
    df_summary.rename(columns={
        'application_year': 'patent_count', 
        'final_inventor_count': 'inventor_sum'
    }, inplace=True)
    
    df_summary = df_summary.sort_values(by='patent_count', ascending=False).reset_index(drop=True)
    logger.info(f"✅ 汇总完成，共 {len(df_summary)} 家专利持有公司")
    
    # 保存中间文件
    if not os.path.exists('temp'):
        os.makedirs('temp')
    df_summary.to_pickle("temp/temp_summary_optimized.pkl")
    
    # ========== 数据分层 ==========
    logger.info("\n正在进行数据分层...")
    total_count = len(df_summary)
    top_5_idx = int(total_count * 0.05)
    
    df_tier1 = df_summary.iloc[:top_5_idx].copy()
    df_remaining = df_summary.iloc[top_5_idx:].copy()
    df_tier2 = df_remaining[df_remaining['patent_count'] > 5].copy()
    df_tier3 = df_remaining[df_remaining['patent_count'] <= 5].copy()
    
    logger.info(f"✅ 分层完成:")
    logger.info(f"   - Tier 1 (Top 5%): {len(df_tier1)} 家公司")
    logger.info(f"   - Tier 2 (>5专利): {len(df_tier2)} 家公司")
    logger.info(f"   - Tier 3 (其余): {len(df_tier3)} 家公司")
    
    # ========== 准备匹配 ==========
    acquiror_clean_set = set(df_main['clean_name'].dropna().unique())
    acquiror_clean_list = list(acquiror_clean_set)
    logger.info(f"\n并购数据库包含 {len(acquiror_clean_list)} 个唯一公司名")
    
    matches_for_review = []
    matches_auto = []
    
    # ========== 执行匹配 ==========
    def perform_matching(df_target, tier_name, fuzzy_threshold=None, use_parallel=False):
        """通用匹配函数"""
        strict_res = []
        fuzzy_res = []
        
        logger.info(f"\n--- 处理 {tier_name} ({len(df_target)} 条记录) ---")
        
        # 1. 严格匹配
        logger.info("  步骤 1/2: 执行严格匹配...")
        unmatched_list = []
        
        for idx, row in df_target.iterrows():
            assignee = row['assignee']
            clean = row['clean_name']
            
            if clean in acquiror_clean_set:
                strict_res.append({
                    'Assignee_Original': assignee,
                    'Assignee_Clean': clean,
                    'Matched_Acquiror_Clean': clean,
                    'Match_Type': 'Strict',
                    'Similarity': 100,
                    'Tier': tier_name
                })
            else:
                unmatched_list.append(row)
        
        logger.info(f"  ✅ 严格匹配: {len(strict_res)} 条命中")
        
        # 2. 模糊匹配
        if fuzzy_threshold is not None and len(unmatched_list) > 0:
            logger.info(f"  步骤 2/2: 执行模糊匹配 (阈值={fuzzy_threshold})...")
            
            df_unmatched = pd.DataFrame(unmatched_list)
            
            if use_parallel and len(df_unmatched) > 100:
                # 并行处理（数据量大时使用）
                n_cores = min(cpu_count() - 1, 4)  # 最多使用4核
                chunks = np.array_split(df_unmatched, n_cores)
                
                logger.info(f"  使用 {n_cores} 核心并行处理...")
                with Pool(n_cores) as pool:
                    args_list = [(chunk, acquiror_clean_list, fuzzy_threshold, tier_name) 
                                for chunk in chunks]
                    results = pool.map(fuzzy_match_batch, args_list)
                
                # 合并结果
                for batch_result in results:
                    fuzzy_res.extend(batch_result)
            else:
                # 串行处理（数据量小时更快）
                for _, row in tqdm(df_unmatched.iterrows(), total=len(df_unmatched), desc="  模糊匹配"):
                    assignee = row['assignee']
                    clean = row['clean_name']
                    
                    match_result = process.extractOne(
                        clean, 
                        acquiror_clean_list, 
                        scorer=fuzz.token_set_ratio,
                        score_cutoff=fuzzy_threshold
                    )
                    
                    if match_result:
                        match_name, score, _ = match_result
                        fuzzy_res.append({
                            'Assignee_Original': assignee,
                            'Assignee_Clean': clean,
                            'Matched_Acquiror_Clean': match_name,
                            'Match_Type': f'Fuzzy (≥{fuzzy_threshold})',
                            'Similarity': score,
                            'Tier': tier_name
                        })
            
            logger.info(f"  ✅ 模糊匹配: {len(fuzzy_res)} 条命中")
        
        return strict_res, fuzzy_res
    
    # Tier 1: 严格 + 模糊(90) → 全部人工审核
    t1_strict, t1_fuzzy = perform_matching(df_tier1, "Tier 1", fuzzy_threshold=90, use_parallel=True)
    matches_for_review.extend(t1_strict)
    matches_for_review.extend(t1_fuzzy)
    
    # Tier 2: 严格(自动) + 模糊100(人工)
    t2_strict, t2_fuzzy = perform_matching(df_tier2, "Tier 2", fuzzy_threshold=100, use_parallel=True)
    matches_auto.extend(t2_strict)
    matches_for_review.extend(t2_fuzzy)
    
    # Tier 3: 仅严格(自动)
    t3_strict, t3_fuzzy = perform_matching(df_tier3, "Tier 3", fuzzy_threshold=None)
    matches_auto.extend(t3_strict)
    
    # ========== 质量检查 ==========
    logger.info("\n" + "=" * 60)
    logger.info("执行质量控制检查...")
    logger.info("=" * 60)
    
    df_all_matches = pd.DataFrame(matches_for_review + matches_auto)
    issues, stats = validate_matches(df_all_matches)
    
    for issue in issues:
        logger.info(issue)
    
    # ========== 导出结果 ==========
    logger.info("\n正在导出结果文件...")
    
    def get_orig_acquiror(clean_val):
        try:
            return df_main[df_main['clean_name'] == clean_val]['acquiror_name'].iloc[0]
        except:
            return ""
    
    # 1. 人工审核文件
    if matches_for_review:
        df_review = pd.DataFrame(matches_for_review)
        df_review['Original_Acquiror_Name'] = df_review['Matched_Acquiror_Clean'].apply(get_orig_acquiror)
        df_review.sort_values(by=['Match_Type', 'Similarity'], ascending=[True, True], inplace=True)
        
        output_review = "Step1_Manual_Review.xlsx"
        df_review.to_excel(output_review, index=False)
        logger.info(f"✅ 人工审核文件: {output_review} ({len(df_review)} 条)")
    
    # 2. 自动接受文件
    if matches_auto:
        df_auto = pd.DataFrame(matches_auto)
        df_auto['Original_Acquiror_Name'] = df_auto['Matched_Acquiror_Clean'].apply(get_orig_acquiror)
        
        output_auto = "Step1_Auto_Results.xlsx"
        df_auto.to_excel(output_auto, index=False)
        logger.info(f"✅ 自动接受文件: {output_auto} ({len(df_auto)} 条)")
    
    # ========== 统计摘要 ==========
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("\n" + "=" * 60)
    logger.info("处理完成！统计摘要:")
    logger.info("=" * 60)
    logger.info(f"总耗时: {duration:.2f} 秒")
    logger.info(f"处理速度: {len(df_patent) / duration:.0f} 条/秒")
    logger.info(f"\n匹配结果:")
    logger.info(f"  - 总匹配数: {len(df_all_matches)}")
    logger.info(f"  - 需人工审核: {len(matches_for_review)}")
    logger.info(f"  - 自动接受: {len(matches_auto)}")
    logger.info(f"  - 匹配率: {len(df_all_matches) / len(df_summary) * 100:.2f}%")
    
    if 'match_types' in stats:
        logger.info(f"\n匹配类型分布:")
        for match_type, count in stats['match_types'].items():
            logger.info(f"  - {match_type}: {count}")
    
    logger.info("\n日志文件已保存，请检查详细信息。")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
