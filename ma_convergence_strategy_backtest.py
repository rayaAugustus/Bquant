from bigmodule import M
import dai
import pandas as pd

# ========== 均线粘合选股策略回测（优化版） ==========
# 寻找5MA, 10MA, 20MA三个均线粘合，并且位于30MA上方的股票

# 获取股票列表（限制数量以减少内存占用）
def get_instruments_sample(max_stocks=500):
    """获取A股股票代码列表（采样）"""
    df = dai.query("""
        SELECT DISTINCT instrument 
        FROM cn_stock_bar1d 
        WHERE date >= '2024-01-01'
        LIMIT 500
    """).df()
    instruments = df['instrument'].tolist()
    print(f"获取到 {len(instruments)} 只股票（限制{max_stocks}只）")
    return instruments[:max_stocks]

# 交易引擎：初始化函数，只执行一次
def initialize(context):
    from bigtrader.finance.commission import PerOrder
    # 设置交易手续费
    context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    # 存储选股结果
    context.selected_stocks = []
    context.last_rebalance_date = None
    context.rebalance_interval = 5  # 每5个交易日调仓一次

# 交易引擎：数据处理函数，每个周期执行一次
def handle_data(context, data):
    import pandas as pd
    
    dt = data.current_dt.strftime('%Y-%m-%d')
    current_date = data.current_dt.date()
    
    # 获取当前持仓
    positions = context.get_account_positions()
    current_hold_instruments = set(positions.keys())
    
    # 检查是否需要调仓（每5个交易日）
    if context.last_rebalance_date is not None:
        days_since_last = (current_date - context.last_rebalance_date).days
        if days_since_last < context.rebalance_interval:
            # 非调仓日，只检查卖出条件
            _check_exit_signals(context, data, current_hold_instruments, dt)
            return
    
    # 选股：寻找均线粘合的股票
    selected_stocks = select_ma_convergence_stocks(data, context.instruments, dt)
    
    if not selected_stocks:
        print(dt, '未找到符合条件的股票')
        return
    
    context.selected_stocks = selected_stocks
    context.last_rebalance_date = current_date
    
    print(dt, f'选股完成，共选出 {len(selected_stocks)} 只股票')
    
    # 卖出不在选股列表中的股票
    for ins in list(current_hold_instruments):
        if ins not in selected_stocks:
            context.order_target_percent(ins, 0)
            print(dt, f'卖出 {ins}')
    
    # 计算每只股票的仓位（等权分配）
    if len(selected_stocks) > 0:
        target_weight = 1.0 / len(selected_stocks)
        
        # 买入选中的股票
        for ins in selected_stocks:
            context.order_target_percent(ins, target_weight)
            print(dt, f'买入 {ins}，仓位 {target_weight:.2%}')


def _check_exit_signals(context, data, current_hold_instruments, dt):
    """检查卖出信号：收盘价跌破30日均线则卖出"""
    for ins in list(current_hold_instruments):
        try:
            # 获取历史数据计算30日均线
            close_history = data.history(ins, 'close', 30, '1d')
            if len(close_history) < 30:
                continue
            
            ma30 = close_history.mean()
            current_close = close_history[-1]
            
            # 卖出条件：收盘价跌破30日均线
            if current_close < ma30:
                context.order_target_percent(ins, 0)
                print(dt, f'卖出 {ins}，跌破30日均线')
        except Exception:
            continue


def select_ma_convergence_stocks(data, instruments, dt, top_n=10, convergence_threshold=0.02):
    """
    选股函数：寻找均线粘合且位于30MA上方的股票
    
    参数:
        data: bigtrader数据对象
        instruments: 股票列表
        dt: 当前日期字符串
        top_n: 最多选择多少只股票（默认10只）
        convergence_threshold: 均线粘合阈值（默认2%）
    
    返回:
        选中的股票代码列表
    """
    selected = []
    convergence_scores = []
    
    for ins in instruments:
        try:
            # 获取历史收盘价（35天用于计算均线）
            close_history = data.history(ins, 'close', 35, '1d')
            if len(close_history) < 30:
                continue
            
            # 计算均线
            ma5 = close_history[-5:].mean()
            ma10 = close_history[-10:].mean()
            ma20 = close_history[-20:].mean()
            ma30 = close_history[-30:].mean()
            current_close = close_history[-1]
            
            # 检查均线是否为有效值
            if pd.isna(ma5) or pd.isna(ma10) or pd.isna(ma20) or pd.isna(ma30):
                continue
            
            # 计算均线之间的差异
            diff5_20 = abs(ma5 - ma20) / ma20
            diff10_20 = abs(ma10 - ma20) / ma20
            diff5_10 = abs(ma5 - ma10) / ma10
            
            # 选股条件：
            # 1. 5MA、10MA、20MA三者之间差异小于阈值（粘合）
            # 2. 收盘价位于30MA上方
            if (diff5_20 < convergence_threshold and 
                diff10_20 < convergence_threshold and 
                diff5_10 < convergence_threshold and 
                current_close > ma30):
                
                # 计算粘合度评分（越小越粘合）
                convergence_score = (diff5_20 + diff10_20 + diff5_10) / 3
                selected.append(ins)
                convergence_scores.append(convergence_score)
                
        except Exception:
            continue
    
    # 按粘合度排序，选择最粘合的top_n只股票
    if selected:
        df = pd.DataFrame({
            'instrument': selected,
            'score': convergence_scores
        })
        df = df.sort_values('score').head(top_n)
        return df['instrument'].tolist()
    
    return []


# ========== 回测配置 ==========
# 获取股票列表（限制500只以减少内存占用）
instruments = get_instruments_sample(max_stocks=500)

data = {
    "start_date": '2020-01-01',  # 缩短回测区间
    'end_date': '2024-12-31',
    'market': 'cn_stock',
    'instruments': instruments
}

m3 = M.bigtrader.v30(
    data=data,
    initialize=initialize,
    handle_data=handle_data,
    capital_base=1000000,
    frequency="daily",
    product_type="股票",
    rebalance_period_type="交易日",
    rebalance_period_days="1",
    rebalance_period_roll_forward=True,
    backtest_engine_mode="标准模式",
    before_start_days=60,
    volume_limit=1,
    order_price_field_buy="open",
    order_price_field_sell="open",
    benchmark="沪深300指数",
    plot_charts=True
)
