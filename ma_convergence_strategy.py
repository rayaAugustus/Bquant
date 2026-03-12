import dai
import pandas as pd
import numpy as np

# ========== 均线粘合选股策略 ==========
# 寻找5MA, 10MA, 20MA三个均线粘合，并且位于30MA上方的股票

def calculate_ma(df, window):
    """计算简单移动平均线"""
    return df['close'].rolling(window=window).mean()

def find_ma_convergence_stocks(trade_date, convergence_threshold=0.02):
    """
    寻找均线粘合的股票
    
    参数:
        trade_date: 交易日期，格式 'YYYY-MM-DD'
        convergence_threshold: 均线粘合阈值，默认2%
    
    返回:
        符合条件的股票DataFrame
    """
    # 获取历史数据（用于计算30日均线）
    df = dai.query("""
        SELECT 
            instrument, name, date, close, volume, amount, turn
        FROM cn_stock_bar1d
        ORDER BY instrument, date
        """,
        filters={"date": ["2020-01-01", trade_date]}
    ).df()
    
    if df.empty:
        print("未获取到数据")
        return pd.DataFrame()
    
    # 按日期排序（升序）
    df = df.sort_values(['instrument', 'date'])
    
    # 计算各条均线
    df['ma5'] = df.groupby('instrument')['close'].transform(lambda x: x.rolling(5).mean())
    df['ma10'] = df.groupby('instrument')['close'].transform(lambda x: x.rolling(10).mean())
    df['ma20'] = df.groupby('instrument')['close'].transform(lambda x: x.rolling(20).mean())
    df['ma30'] = df.groupby('instrument')['close'].transform(lambda x: x.rolling(30).mean())
    
    # 获取指定日期的数据
    target_df = df[df['date'] == trade_date].copy()
    
    if target_df.empty:
        print(f"未找到 {trade_date} 的数据")
        return pd.DataFrame()
    
    # 计算均线之间的差异（相对于20MA的百分比）
    target_df['diff5_20'] = abs(target_df['ma5'] - target_df['ma20']) / target_df['ma20']
    target_df['diff10_20'] = abs(target_df['ma10'] - target_df['ma20']) / target_df['ma20']
    target_df['diff5_10'] = abs(target_df['ma5'] - target_df['ma10']) / target_df['ma10']
    
    # 计算粘合度评分（三个差异的平均值）
    target_df['convergence_score'] = (target_df['diff5_20'] + target_df['diff10_20'] + target_df['diff5_10']) / 3
    
    # 筛选条件：
    # 1. 5MA、10MA、20MA三者之间差异小于阈值（粘合）
    # 2. 收盘价位于30MA上方
    # 3. 排除均线为NaN的股票
    condition = (
        (target_df['diff5_20'] < convergence_threshold) &
        (target_df['diff10_20'] < convergence_threshold) &
        (target_df['diff5_10'] < convergence_threshold) &
        (target_df['close'] > target_df['ma30']) &
        (target_df['ma5'].notna()) &
        (target_df['ma10'].notna()) &
        (target_df['ma20'].notna()) &
        (target_df['ma30'].notna())
    )
    
    result = target_df[condition].copy()
    
    # 按粘合度评分排序（越小越粘合）
    result = result.sort_values('convergence_score')
    
    # 选择输出列
    output_cols = [
        'instrument', 'name', 'date', 'close', 'turn',
        'ma5', 'ma10', 'ma20', 'ma30',
        'diff5_20', 'diff10_20', 'diff5_10', 'convergence_score'
    ]
    
    return result[output_cols]


# ========== 使用示例 ==========
if __name__ == "__main__":
    # 查询某一天的均线粘合股票
    trade_date = "2024-03-01"  # 可以修改为任意交易日
    
    print(f"正在查询 {trade_date} 的均线粘合股票...")
    print("=" * 80)
    
    stocks = find_ma_convergence_stocks(trade_date, convergence_threshold=0.02)
    
    if not stocks.empty:
        print(f"找到 {len(stocks)} 只符合条件的股票：")
        print("-" * 80)
        print(stocks.to_string(index=False))
    else:
        print("未找到符合条件的股票")
