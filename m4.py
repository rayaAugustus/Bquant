from bigmodule import M

# ========== 1. 因子与信号计算 ==========
# 这里把 expr 用三引号包裹，避免字符串未闭合问题，并将表达式写成逐行独立计算，
# 避免在同一表达式里引用刚定义的别名（有些解析器不支持）。
m1 = M.input_features_dai.v30(
    mode="表达式",
    expr="""
m_avg(close, 5) AS ma5
m_avg(close, 10) AS ma10
m_avg(close, 20) AS ma20
m_avg(close, 30) AS ma30
abs(m_avg(close, 5) - m_avg(close, 20)) / m_avg(close, 20) AS diff5
abs(m_avg(close, 10) - m_avg(close, 20)) / m_avg(close, 20) AS diff10
m_lag(m_avg(close, 30), 1) AS ma30_yesterday
(diff5 + diff10) AS score
(diff5 < 0.02) AND (diff10 < 0.02) AND (close > m_avg(close, 30)) AND (m_avg(close, 30) >= m_lag(m_avg(close, 30), 1)) AS entry_signal
(close < m_avg(close, 30)) AS exit_signal
""",
    expr_filters="list_days > 60 AND st_status = 0",
    expr_tables="cn_stock_prefactors",
    extra_fields="date,instrument,close",
    order_by="date, instrument",
    expr_drop_na=True,
    extract_data=False,
    m_name="m1"
)

# ========== 2. 抽取回测区间数据 ==========
m2 = M.extract_data_dai.v20(
    sql=m1.data,
    start_date="2018-01-01",
    start_date_bound_to_trading_date=True,
    end_date="2024-12-31",
    end_date_bound_to_trading_date=True,
    before_start_days=60,
    keep_before=False,
    debug=False,
    m_name="m2"
)

# ========== 3. 仓位分配 ==========
m3 = M.score_to_position.v4(
    data=m2.data,
    score_field="score ASC",
    position_expr="1 AS position",
    hold_count=20,
    total_position=1,
    m_name="m3"
)

# ========== 4. 回测逻辑 ==========
def m4_initialize_bigquant_run(context):
    from bigtrader.finance.commission import PerOrder
    context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    context.data = context.options["data"]
    context.instruments = list(context.data["instrument"].unique())

def m4_handle_data_bigquant_run(context, data):
    import pandas as pd
    current_date_str = data.current_dt.strftime("%Y-%m-%d")
    today_df = context.data[context.data["date"] == current_date_str]
    if len(today_df) == 0:
        return

    positions = context.get_account_positions()
    current_hold_instruments = set(positions.keys())

    # 卖出：收盘跌破30日均线则卖出
    for ins in list(current_hold_instruments):
        row = today_df[today_df["instrument"] == ins]
        if len(row) == 0:
            continue
        try:
            exit_signal = bool(row["exit_signal"].values[0])
        except Exception:
            continue
        if exit_signal:
            context.order_target_percent(ins, 0)

    # 仅在调仓日执行买入逻辑
    if not context.rebalance_period.is_signal_date(data.current_dt.date()):
        return

    todays_positions_df = today_df[today_df["position"] > 0]
    if len(todays_positions_df) == 0:
        return

    n = len(todays_positions_df)
    if n == 0:
        return
    target_weight = 1.0 / n

    for _, row in todays_positions_df.iterrows():
        ins = row["instrument"]
        try:
            entry_signal = bool(row["entry_signal"])
        except Exception:
            continue
        if not entry_signal:
            continue
        context.order_target_percent(ins, target_weight)

m4 = M.bigtrader.v43(
    data=m3.data,
    start_date="2018-01-01",
    end_date="2024-12-31",
    initialize=m4_initialize_bigquant_run,
    handle_data=m4_handle_data_bigquant_run,
    capital_base=1000000,
    frequency="daily",
    product_type="股票",
    rebalance_period_type="交易日",
    rebalance_period_days="1",
    order_price_field_buy="open",
    order_price_field_sell="open",
    benchmark="沪深300指数",
    start_portfolio_value=1000000,
    before_start_days=0,
    volume_limit=1,
    plot_charts=True,
    debug=False,
    backtest_only=False,
    m_name="m4"
)