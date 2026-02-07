# -*- coding: utf-8 -*-
"""
Compustat匹配 - 步骤4A：生成验证文件（优化版）
================================================
功能：将final_outcome与Compustat匹配，生成人工验证文件

改进点：
1. 使用rapidfuzz替代thefuzz（更快）
2. 详细的日志和进度显示
3. 改进的清洗函数（与步骤1一致）
"""

import pandas as pd
import re
from rapidfuzz import process, fuzz
from tqdm import tqdm
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
        logging.FileHandler(f'logs/compustat_match_4A_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 路径与配置
# ==========================================
PATH_MA = "/Users/lidachuan/Desktop/Patent Data/final_outcome_1993_1997_COMPLETE.xlsx"
PATH_COMPUSTAT = "/Users/lidachuan/Desktop/Patent Data/compustat_19802025.csv"
OUTPUT_VERIFICATION = "/Users/lidachuan/Desktop/Patent Data/company_match_verification.xlsx"

FUZZY_THRESHOLD = 90  # 模糊匹配阈值

# ==========================================
# 2. 清洗函数（与步骤1保持一致）
# ==========================================

def clean_company_name(name):
    """优化的标准化清洗函数"""
    if pd.isna(name) or not isinstance(name, str):
        return ""
    
    name = str(name).upper().strip()
    
    # 1. 处理常见符号
    name = name.replace('&', ' AND ')
    name = name.replace('-', ' ')
    name = name.replace("'", '')
    
    # 2. 扩展常见缩写
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
    
    # 3. 去除后缀
    suffixes_priority = [
        r'\bINCORPORATED\b', r'\bCORPORATION\b', r'\bCOMPANY\b',
        r'\bLIMITED\b', r'\bGROUP\b',
        r'\bCORP\.?\b', r'\bINC\.?\b', r'\bLTD\.?\b', 
        r'\bCO\.?\b', r'\bL\.L\.C\.?\b', r'\bPLC\.?\b',
        r'\bLLC\b', r'\bS\.A\\.b', r'\bNV\b', r'\bGMBH\b',
        r'\bSA\b', r'\bAG\b', r'\bKK\b'
    ]
    
    for suffix in suffixes_priority:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
    # 4. 去除标点
    name = re.sub(r'[^A-Z0-9\s]', ' ', name)
    
    # 5. 合并空格
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name


# ==========================================
# 3. 主处理函数
# ==========================================

def main():
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info("Compustat匹配 - 步骤4A：生成验证文件（优化版）")
    logger.info("=" * 60)
    
    # ========== 步骤1: 加载数据 ==========
    logger.info("\n步骤 1/4: 加载数据...")
    
    # 读取 M&A 数据
    logger.info("   加载 M&A 数据...")
    try:
        df_ma = pd.read_excel(PATH_MA)
        logger.info(f"   ✅ M&A 数据加载成功: {len(df_ma):,} 行")
    except Exception as e:
        logger.error(f"   ❌ 读取失败: {e}")
        return False
    
    # 过滤：只处理有 patent_name 的行
    df_ma_target = df_ma[df_ma['patent_name'].notna()].copy()
    logger.info(f"   过滤 patent_name 非空: {len(df_ma_target):,} 行待匹配")
    
    # 读取 Compustat 数据（只读取 conm 列以节省内存）
    logger.info("   加载 Compustat 数据（仅公司名列）...")
    try:
        # 策略：大文件只读取需要的列（conm）
        df_comp = pd.read_csv(PATH_COMPUSTAT, usecols=['conm'], low_memory=False)
        logger.info(f"   ✅ Compustat 数据加载成功: {len(df_comp):,} 行")
    except ValueError:
        # 如果列名不匹配，尝试全量读取（但可能很慢）
        logger.warning("   列名 'conm' 未找到，尝试全量读取...")
        df_comp = pd.read_csv(PATH_COMPUSTAT, low_memory=False)
        logger.info(f"   ✅ Compustat 数据加载成功（全量）: {len(df_comp):,} 行")
    except Exception as e:
        logger.error(f"   ❌ 读取失败: {e}")
        return False
    
    # ========== 步骤2: 清洗数据 ==========
    logger.info("\n步骤 2/4: 清洗公司名称...")
    
    # 清洗 M&A 的 acquiror_name
    df_ma_target['clean_acquiror'] = df_ma_target['acquiror_name'].apply(clean_company_name)
    
    # 清洗 Compustat 的 conm
    df_comp['clean_conm'] = df_comp['conm'].apply(clean_company_name)
    
    # 创建 Compustat 查找集合
    compustat_unique = df_comp[df_comp['clean_conm'] != ""][['conm', 'clean_conm']].drop_duplicates(subset=['clean_conm'])
    compustat_clean_set = set(compustat_unique['clean_conm'])
    compustat_clean_list = list(compustat_unique['clean_conm'])
    
    logger.info(f"   ✅ Compustat 唯一公司名: {len(compustat_clean_list):,}")
    
    # ========== 步骤3: 执行匹配 ==========
    logger.info("\n步骤 3/4: 执行匹配...")
    
    strict_res = []
    fuzzy_res = []
    unmatched_rows = []
    
    # 3.1 严格匹配
    logger.info("   阶段 3.1: 精确匹配...")
    for idx, row in df_ma_target.iterrows():
        acquiror_orig = row['acquiror_name']
        acquiror_clean = row['clean_acquiror']
        
        if not acquiror_clean:
            continue
        
        if acquiror_clean in compustat_clean_set:
            strict_res.append({
                'Acquiror_Original': acquiror_orig,
                'Acquiror_Clean': acquiror_clean,
                'Matched_Compustat_Clean': acquiror_clean,
                'Match_Type': 'Strict',
                'Score': 100
            })
        else:
            unmatched_rows.append(row)
    
    logger.info(f"   ✅ 精确匹配: {len(strict_res)} 条")
    logger.info(f"   待模糊匹配: {len(unmatched_rows)} 条")
    
    # 3.2 模糊匹配
    if len(unmatched_rows) > 0:
        logger.info(f"   阶段 3.2: 模糊匹配 (阈值 {FUZZY_THRESHOLD})...")
        
        for row in tqdm(unmatched_rows, desc="   匹配进度"):
            acquiror_orig = row['acquiror_name']
            acquiror_clean = row['clean_acquiror']
            
            match_result = process.extractOne(
                acquiror_clean, 
                compustat_clean_list, 
                scorer=fuzz.token_set_ratio,
                score_cutoff=FUZZY_THRESHOLD
            )
            
            if match_result:
                match_name, score, _ = match_result
                fuzzy_res.append({
                    'Acquiror_Original': acquiror_orig,
                    'Acquiror_Clean': acquiror_clean,
                    'Matched_Compustat_Clean': match_name,
                    'Match_Type': 'Fuzzy',
                    'Score': score
                })
        
        logger.info(f"   ✅ 模糊匹配: {len(fuzzy_res)} 条")
    
    # ========== 步骤4: 生成验证文件 ==========
    logger.info("\n步骤 4/4: 生成人工验证文件...")
    
    # 合并结果
    df_strict = pd.DataFrame(strict_res)
    df_fuzzy = pd.DataFrame(fuzzy_res)
    df_all_matches = pd.concat([df_strict, df_fuzzy], ignore_index=True)
    
    if df_all_matches.empty:
        logger.warning("   ⚠️  没有匹配到任何结果")
        return False
    
    # 找回 Compustat 原始名称
    clean_to_original_map = dict(zip(compustat_unique['clean_conm'], compustat_unique['conm']))
    df_all_matches['Matched_Compustat_Original'] = df_all_matches['Matched_Compustat_Clean'].map(clean_to_original_map)
    
    # 选择输出列
    output_columns = [
        'Acquiror_Original',
        'Matched_Compustat_Original',
        'Match_Type',
        'Score',
        'Acquiror_Clean',
        'Matched_Compustat_Clean'
    ]
    
    df_verify = df_all_matches[output_columns].copy()
    
    # 排序：Fuzzy 在前，分数低的优先审查
    df_verify.sort_values(by=['Match_Type', 'Score'], ascending=[True, True], inplace=True)
    
    # 导出
    df_verify.to_excel(OUTPUT_VERIFICATION, index=False)
    
    # ========== 完成摘要 ==========
    duration = (datetime.now() - start_time).total_seconds()
    
    logger.info("\n" + "=" * 60)
    logger.info("步骤4A 完成！")
    logger.info("=" * 60)
    logger.info(f"⏱  总耗时: {duration:.2f} 秒")
    logger.info(f"📊 匹配结果:")
    logger.info(f"   - 精确匹配: {len(strict_res)}")
    logger.info(f"   - 模糊匹配: {len(fuzzy_res)}")
    logger.info(f"   - 总计: {len(df_verify):,} 对")
    logger.info(f"\n📁 输出文件:")
    logger.info(f"   {OUTPUT_VERIFICATION}")
    logger.info(f"\n⚠️  下一步（重要）:")
    logger.info(f"   1. 打开 {OUTPUT_VERIFICATION}")
    logger.info(f"   2. 人工审核，删除错误的匹配行")
    logger.info(f"   3. 保存文件（保持文件名不变）")
    logger.info(f"   4. 运行步骤4B（Compustat匹配_步骤4B_优化版.py）")
    logger.info("=" * 60)
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
