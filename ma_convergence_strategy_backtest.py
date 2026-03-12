from bigmodule import M
import dai
import pandas as pd

# ========== 均线粘合选股策略回测（移动止盈版） ==========
# 寻找5MA, 10MA, 20MA三个均线粘合，并且位于30MA上方的股票
# 策略特点：买入后持有，不做周期调仓，只做移动止盈止损

# 获取股票列表（限制数量以减少内存占用）
def get_instruments_sample(max_stocks=500):
    """获取创业板股票代码列表（list_sector=2）"""
    df = dai.query("""
        SELECT DISTINCT a.instrument 
        FROM cn_stock_bar1d a
        INNER JOIN cn_stock_basic_info b ON a.instrument = b.instrument
        WHERE a.date >= '2024-01-01' AND b.list_sector = 2
        LIMIT 500
    """).df()
    instruments = df['instrument'].tolist()
    print(f"获取到 {len(instruments)} 只创业板股票（限制{max_stocks}只）")
    return instruments[:max_stocks]

# 交易引擎：初始化函数，只执行一次
def initialize(context):
    from bigtrader.finance.commission import PerOrder
    # 设置交易手续费
    context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    # 存储每只股票的买入成本和移动止盈线
    context.position_info = {}  # {instrument: {'cost_price': x, 'stop_loss_price': y, 'profit_protect': False}}
    # 最大持仓数量
    context.max_positions = 10
    # 盈利保护触发比例（10%）
    context.profit_trigger = 0.10

# 交易引擎：数据处理函数，每个周期执行一次
def handle_data(context, data):
    import pandas as pd

    dt = data.current_dt.strftime('%Y-%m-%d')

    # 获取当前持仓
    positions = context.get_account_positions()
    current_hold_instruments = set(positions.keys())

    # 第一步：检查止盈止损（对持仓股票）
    _check_exit_signals(context, data, current_hold_instruments, dt)

    # 第二步：检查是否有空仓位，有则选股买入
    available_slots = context.max_positions - len(current_hold_instruments)
    if available_slots > 0:
        _buy_new_stocks(context, data, available_slots, dt)


def _check_exit_signals(context, data, current_hold_instruments, dt):
    """
    检查卖出信号：
    1. 移动止盈：盈利超10%后，止损线上移至成本价或10日均线
    2. 跌破40日均线止损
    """
    for ins in list(current_hold_instruments):
        try:
            # 获取持仓信息
            pos_info = context.position_info.get(ins, {})
            cost_price = pos_info.get('cost_price', 0)
            stop_loss_price = pos_info.get('stop_loss_price', 0)
            profit_protect = pos_info.get('profit_protect', False)

            # 获取历史数据
            close_history = data.history(ins, 'close', 40, '1d')
            if len(close_history) < 40:
                continue

            current_close = close_history[-1]
            ma10 = close_history[-10:].mean()
            ma40 = close_history.mean()

            # 计算当前盈亏比例
            if cost_price > 0:
                profit_ratio = (current_close - cost_price) / cost_price
            else:
                profit_ratio = 0

            # 移动止盈逻辑
            if not profit_protect and profit_ratio >= context.profit_trigger:
                # 盈利超过10%，启动盈利保护
                # 将止损线上移至 max(成本价, 10日均线)
                new_stop_loss = max(cost_price, ma10)
                context.position_info[ins]['stop_loss_price'] = new_stop_loss
                context.position_info[ins]['profit_protect'] = True
                print(dt, f'{ins} 盈利{profit_ratio:.2%}，启动移动止盈，止损线移至{new_stop_loss:.2f}')

            # 检查是否触发止损
            if profit_protect and current_close < stop_loss_price:
                context.order_target_percent(ins, 0)
                del context.position_info[ins]
                print(dt, f'卖出 {ins}，触发移动止盈止损（成本{cost_price:.2f}，止损线{stop_loss_price:.2f}，现价{current_close:.2f}）')
                continue

            # 跌破40日均线止损（基础止损）
            if current_close < ma40:
                context.order_target_percent(ins, 0)
                if ins in context.position_info:
                    del context.position_info[ins]
                print(dt, f'卖出 {ins}，跌破40日均线')

        except Exception:
            continue


def _buy_new_stocks(context, data, available_slots, dt):
    """买入新股票"""
    # 选股：寻找均线粘合的股票
    selected_stocks = select_ma_convergence_stocks(data, context.instruments, dt, top_n=available_slots)

    if not selected_stocks:
        return

    print(dt, f'发现 {len(selected_stocks)} 只新标的，准备买入')

    # 计算每只股票的仓位（等权分配）
    target_weight = 1.0 / context.max_positions

    # 买入选中的股票
    for ins in selected_stocks:
        try:
            # 获取当前价格作为成本价参考
            close_history = data.history(ins, 'close', 5, '1d')
            if len(close_history) == 0:
                continue
            cost_price = close_history[-1]

            # 买入
            context.order_target_percent(ins, target_weight)

            # 记录持仓信息
            context.position_info[ins] = {
                'cost_price': cost_price,
                'stop_loss_price': 0,  # 初始不设移动止盈线
                'profit_protect': False
            }

            print(dt, f'买入 {ins}，仓位 {target_weight:.2%}，成本价 {cost_price:.2f}')

        except Exception:
            continue


def select_ma_convergence_stocks(data, instruments, dt, top_n=10, convergence_threshold=0.02):
    """
    选股函数：寻找均线粘合且位于30MA上方的股票
    排除已持仓的股票
    """
    selected = []
    convergence_scores = []

    # 获取当前持仓，避免重复买入
    positions = data._context.get_account_positions()
    hold_instruments = set(positions.keys())

    for ins in instruments:
        # 跳过已持仓的股票
        if ins in hold_instruments:
            continue

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
    "start_date": '2023-01-01',
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
