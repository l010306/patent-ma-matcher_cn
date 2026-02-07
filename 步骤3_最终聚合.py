# -*- coding: utf-8 -*-
"""
最终数据聚合优化版（步骤3）
============================
功能：使用超级字典将专利数据聚合到final_outcome文件

改进点：
1. 详细的日志记录
2. 进度显示
3. 数据验证
4. 向量化发明人统计（韩语要求）
5. 自动处理缺失列
"""

import pandas as pd
import numpy as np
import pickle
import logging
from datetime import datetime
from tqdm import tqdm

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
        logging.FileHandler(f'logs/aggregation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 路径配置
# ==========================================
# 超级字典路径
DICT_PATH = '/Users/lidachuan/Desktop/Patent Data/Master_Company_Dictionary.pkl'

# 主数据库模板路径
FINAL_OUTCOME_PATH = '/Users/lidachuan/Desktop/Patent Data/final_outcome.xlsx'

# 专利数据库路径（可以是单个CSV或目录）
PATENT_DB_PATH = '/Users/lidachuan/Desktop/Patent Data/1993-1997/patent_database.csv'

# 输出文件路径
OUTPUT_PATH = '/Users/lidachuan/Desktop/Patent Data/final_outcome_1993_1997_COMPLETE.xlsx'

# ==========================================
# 2. 发明人统计函数（符合韩语要求）
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
    
    # 3. 取两者中的较大值（符合韩语要求）
    return np.maximum(num_from_column, num_from_names)

# ==========================================
# 3. 主处理函数
# ==========================================

def load_master_dictionary():
    """加载超级字典"""
    logger.info("步骤 1/6: 加载超级字典...")
    try:
        with open(DICT_PATH, 'rb') as f:
            master_dict = pickle.load(f)
        logger.info(f"   ✅ 字典加载成功，包含 {len(master_dict):,} 个映射关系")
        return master_dict
    except FileNotFoundError:
        logger.error(f"   ❌ 错误：找不到字典文件 {DICT_PATH}")
        logger.error("   请先运行步骤2（超级字典构建）")
        raise


def load_main_database():
    """加载主数据库模板"""
    logger.info("\n步骤 2/6: 加载主数据库模板...")
    try:
        df_main = pd.read_excel(FINAL_OUTCOME_PATH)
        df_main.drop_duplicates(subset=['acquiror_name'], keep='first', inplace=True)
        logger.info(f"   ✅ 模板加载成功，共 {len(df_main):,} 家公司")
        return df_main
    except FileNotFoundError:
        logger.error(f"   ❌ 错误：找不到模板文件 {FINAL_OUTCOME_PATH}")
        raise


def load_patent_database():
    """加载专利数据库"""
    logger.info("\n步骤 3/6: 加载专利数据库...")
    try:
        df_patent = pd.read_csv(PATENT_DB_PATH, low_memory=False)
        original_count = len(df_patent)
        
        # 去除assignee为空的行
        df_patent.dropna(subset=['assignee'], inplace=True)
        logger.info(f"   ✅ 专利数据加载完成: {len(df_patent):,} 条有效记录（原始 {original_count:,}）")
        return df_patent
    except FileNotFoundError:
        logger.error(f"   ❌ 错误：找不到专利数据文件 {PATENT_DB_PATH}")
        raise


def process_patent_data(df_patent, master_dict):
    """处理专利数据：应用字典映射和统计发明人"""
    logger.info("\n步骤 4/6: 处理专利数据...")
    
    # 应用映射
    logger.info("   应用字典映射...")
    df_patent['assignee_stripped'] = df_patent['assignee'].astype(str).str.strip()
    df_patent['Matched_Acquiror'] = df_patent['assignee_stripped'].map(master_dict)
    
    # 统计匹配率
    matched_count = df_patent['Matched_Acquiror'].notna().sum()
    match_rate = matched_count / len(df_patent) * 100
    logger.info(f"   ✅ 映射完成: {matched_count:,} / {len(df_patent):,} ({match_rate:.2f}%)")
    
    # 只保留匹配成功的
    df_matched = df_patent.dropna(subset=['Matched_Acquiror']).copy()
    
    # 清洗年份
    logger.info("   清洗年份数据...")
    df_matched['application_year'] = pd.to_numeric(df_matched['application_year'], errors='coerce')
    df_matched = df_matched.dropna(subset=['application_year'])
    df_matched['application_year'] = df_matched['application_year'].astype(int)
    
    # 统计发明人数量（按韩语要求）
    logger.info("   计算发明人数量...")
    inventor_name_cols = [f'inventor_name{i}' for i in range(1, 11)]
    
    # 确保列存在
    for col in inventor_name_cols:
        if col not in df_matched.columns:
            df_matched[col] = np.nan
    
    df_matched['final_inventor_count'] = calculate_inventor_count_vectorized(
        df_matched, 
        inventor_name_cols
    )
    
    logger.info(f"   ✅ 处理完成，平均每专利 {df_matched['final_inventor_count'].mean():.2f} 位发明人")
    
    return df_matched


def aggregate_data(df_matched):
    """聚合数据：按公司和年份统计"""
    logger.info("\n步骤 5/6: 聚合数据...")
    
    # 按公司和年份分组
    logger.info("   按公司和年份分组统计...")
    df_grouped = df_matched.groupby(['Matched_Acquiror', 'application_year']).agg({
        'assignee': 'count',  # 专利数量
        'final_inventor_count': 'sum'  # 发明人总数
    }).reset_index()
    
    # 透视表：专利数量
    logger.info("   生成专利数量透视表...")
    pivot_patent = df_grouped.pivot(
        index='Matched_Acquiror', 
        columns='application_year', 
        values='assignee'
    )
    pivot_patent.columns = [f'patent_{int(col)}' for col in pivot_patent.columns]
    
    # 透视表：发明人数量
    logger.info("   生成发明人数量透视表...")
    pivot_inventor = df_grouped.pivot(
        index='Matched_Acquiror', 
        columns='application_year', 
        values='final_inventor_count'
    )
    pivot_inventor.columns = [f'patent_inventor_{int(col)}' for col in pivot_inventor.columns]
    
    # 合并透视表
    df_stats = pd.concat([pivot_patent, pivot_inventor], axis=1).reset_index()
    df_stats.rename(columns={'Matched_Acquiror': 'acquiror_name'}, inplace=True)
    
    logger.info(f"   ✅ 聚合完成，涵盖 {len(df_stats)} 家公司")
    logger.info(f"   年份范围: {pivot_patent.columns.tolist()[:3]}...{pivot_patent.columns.tolist()[-3:]}")
    
    # 收集公司别名
    logger.info("   收集公司别名...")
    df_names = df_matched.groupby('Matched_Acquiror')['assignee'].apply(
        lambda x: list(set(x))
    ).reset_index()
    
    # 展开别名列表
    max_len = df_names['assignee'].apply(len).max() if not df_names.empty else 0
    name_cols = ['patent_name'] + [f'patent_name_{i}' for i in range(1, max_len)]
    names_expanded = pd.DataFrame(df_names['assignee'].tolist(), index=df_names.index)
    names_expanded = names_expanded.iloc[:, :len(name_cols)]
    names_expanded.columns = name_cols[:names_expanded.shape[1]]
    
    df_names = pd.concat([df_names[['Matched_Acquiror']], names_expanded], axis=1)
    df_names.rename(columns={'Matched_Acquiror': 'acquiror_name'}, inplace=True)
    
    return df_stats, df_names


def merge_to_final_outcome(df_main, df_stats, df_names):
    """合并到最终文件"""
    logger.info("\n步骤 6/6: 合并到最终文件...")
    
    # 清理可能存在的旧列
    logger.info("   清理旧的统计列...")
    cols_to_remove = [c for c in df_main.columns 
                     if c.startswith('patent_') or c.startswith('patent_inventor_')]
    if cols_to_remove:
        df_main = df_main.drop(columns=cols_to_remove, errors='ignore')
        logger.info(f"   移除了 {len(cols_to_remove)} 个旧列")
    
    # 合并统计数据
    logger.info("   合并统计数据...")
    df_final = pd.merge(df_main, df_stats, on='acquiror_name', how='left')
    
    # 合并别名数据
    logger.info("   合并别名数据...")
    df_final = pd.merge(df_final, df_names, on='acquiror_name', how='left')
    
    # 填充 NaN 为 0（仅数值列）
    stat_cols = [c for c in df_final.columns 
                if (c.startswith('patent_') or c.startswith('patent_inventor_')) 
                and 'name' not in c]
    df_final[stat_cols] = df_final[stat_cols].fillna(0).astype(int)
    
    logger.info(f"   ✅ 合并完成，最终文件共 {len(df_final)} 行")
    
    # 统计有数据的公司
    companies_with_patents = (df_final[stat_cols].sum(axis=1) > 0).sum()
    logger.info(f"   其中 {companies_with_patents} 家公司有专利数据")
    
    return df_final


def save_output(df_final):
    """保存输出文件"""
    logger.info("\n保存结果...")
    df_final.to_excel(OUTPUT_PATH, index=False)
    logger.info(f"✅ 结果已保存至: {OUTPUT_PATH}")


# ==========================================
# 4. 主执行流程
# ==========================================

def main():
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info("开始最终数据聚合流程（优化版）")
    logger.info("=" * 60)
    
    try:
        # 步骤1: 加载字典
        master_dict = load_master_dictionary()
        
        # 步骤2: 加载主数据库
        df_main = load_main_database()
        
        # 步骤3: 加载专利数据
        df_patent = load_patent_database()
        
        # 步骤4: 处理专利数据
        df_matched = process_patent_data(df_patent, master_dict)
        
        # 步骤5: 聚合数据
        df_stats, df_names = aggregate_data(df_matched)
        
        # 步骤6: 合并到最终文件
        df_final = merge_to_final_outcome(df_main, df_stats, df_names)
        
        # 保存结果
        save_output(df_final)
        
        # 完成摘要
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("\n" + "=" * 60)
        logger.info("处理完成！")
        logger.info("=" * 60)
        logger.info(f"⏱  总耗时: {duration:.2f} 秒")
        logger.info(f"📊 处理速度: {len(df_patent) / duration:.0f} 条/秒")
        logger.info(f"\n✅ 下一步: 运行步骤4（Compustat匹配）")
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ 处理失败: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
