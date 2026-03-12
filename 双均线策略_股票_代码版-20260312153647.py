from bigmodule import M
import dai



# 交易引擎：初始化函数，只执行一次
def initialize(context):
    from bigtrader.finance.commission import PerOrder
    # 系统已经设置了默认的交易手续费和滑点，要修改手续费可使用如下函数
    context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))  
    context.ins = context.instruments 

# 交易引擎：数据处理函数，每个周期执行一次
def handle_data(context, data):
    import pandas as pd
    from datetime import datetime, timedelta
    dt = data.current_dt.strftime('%Y-%m-%d')

    ma5 = data.history(context.ins[0], 'close', 5,'1d').mean() 
    ma80 = data.history(context.ins[0], 'close', 80,'1d').mean() 

    positions = context.get_account_positions()

    if (ma5 > ma80) and (context.ins[0]  not in positions):
        stock = context.ins[0]
        context.order_target_percent(stock, 0.5) 
        print(dt, '金叉买入 持半仓')

    
    elif (ma5 < ma80)  and (context.ins[0]  in positions):
        if positions[context.ins[0]].avail_qty > 0: 
            stock = context.ins[0]
            context.order_target_percent(stock, 0)
            print(dt, '死叉卖出 持空仓')
    


data = {
"start_date":'2015-01-01', 
'end_date':'2024-10-08', 
'market':'cn_stock',
'instruments':['600519.SH']

}

m3 = M.bigtrader.v30(
    data=data, 
    initialize=initialize,
    handle_data=handle_data,
    capital_base=500000,
    frequency="""daily""",
    product_type="""股票""",
    rebalance_period_type="""交易日""",
    rebalance_period_days="""1""",
    rebalance_period_roll_forward=True,
    backtest_engine_mode="""标准模式""",
    before_start_days=0,
    volume_limit=1,
    order_price_field_buy="""open""",
    order_price_field_sell="""open""",
    benchmark="""沪深300指数""",
    plot_charts=True
)