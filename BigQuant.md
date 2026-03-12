基本信息(cn_stock_basic_info)
数据描述
该表旨在提供中国股市中上市公司的基本信息。包括了公司的基本识别信息（地址、曾用名、简介等）、上市详情、发行数据以及其他关键财务和法律信息。

表结构
字段	字段类型	字段描述
instrument	string	证券代码
name	string	证券简称
full_name	string	公司名称
en_name	string	英文名称
used_name	string	曾用名
instrument_b	string	B 股代码
name_b	string	B 股简称
instrument_hk	string	H 股代码
name_hk	string	H 股简称
exchange	string	交易市场
list_sector	int8	上市板块代码：1-主板；2-创业板；3-科创板；4-北交所
security_type	string	证券类别
industry	string	所属东财行业
office_addr	string	办公地址
regster_addr	string	注册地址
register_captial	double	注册资本
nums_employees	double	雇员人数
nums_managers	double	管理人数
law_firm	string	律师事务所
account_firm	string	会计事务所
profile	string	公司简介
estab_date	timestamp[ns]	成立日期
list_date	timestamp[ns]	上市日期
delist_date	timestamp[ns]	退市日期
online_list	timestamp[ns]	网上发行日期
ipo_price	double	发行价格
ipo_nums	double	发行量（股）
ipo_pe	double	发行市盈率
ipo_parvalue	double	每股面值（元）
net_amount_funds	double	募资净额（元）
offline_dtor	double	网下中签率
dtor	double	定价中签率
f_open	double	首日开盘价
f_high	double	首日最高价
f_close	double	首日收盘价
f_turnover	double	首日换手率
corp_nature	string	企业性质
corp_scale	string	企业规模


用例1：查询特定公司的基本面信息
import dai
df = dai.query("""
    SELECT
        full_name, ipo_pe, ipo_price, industry, profile
    FROM cn_stock_basic_info
    WHERE instrument = '000001.SZ'""",
).df()



股票后复权日行情(cn_stock_bar1d)
数据描述
该表记录了股票市场中各证券的日行情数据，特别采用后复权价格来展示。主要字段包含累计后复权因子、开高低收、昨收盘价、成交笔数、换手率、涨跌停价等。后复权处理是为了消除因股票分红、配股、转增股本等原因引起的价格变动，从而更真实地反映股票的价值变化

表结构
字段	字段类型	字段描述
instrument	string	证券代码
name	string	证券简称
adjust_factor	double	累计后复权因子
pre_close	double	昨收盘价（后复权）
open	double	开盘价（后复权）
close	double	收盘价（后复权）
high	double	最高价（后复权）
low	double	最低价（后复权）
volume	int64	成交量
deal_number	int32	成交笔数
amount	double	成交金额
change_ratio	double	涨跌幅（后复权）
turn	double	换手率
upper_limit	double	涨停价
lower_limit	double	跌停价
date	timestamp[ns]	日期

用例2：查询某只股票在某个时间段内的收盘价、成交量和成交金额
import dai
df = dai.query("""
    SELECT date, close, volume, amount
    FROM cn_stock_bar1d
    WHERE instrument = '000002.SZ'
    ORDER BY date""",
    filters={"date": ["2023-01-01", "2023-03-31"]}
).df()