"""端到端验证：用少量股票验证核心链路"""
import sys
sys.path.insert(0, '.')

from db import init_db, get_conn, read_df
from indicators import get_all_indicators
from analyzer import get_realtime_price

print("=== 1. 初始化数据库 ===")
init_db()
print("OK")

print("\n=== 2. 同步股票列表（只取前20只测试） ===")
import baostock as bs
bs.login()
rs = bs.query_stock_basic()
data = []
while (rs.error_code == '0') and rs.next():
    data.append(rs.get_row_data())
bs.logout()

import pandas as pd
df_all = pd.DataFrame(data, columns=rs.fields)
df_active = df_all[(df_all['type'] == '1') & (df_all['status'] == '1')]

test_codes = ['sh.600036', 'sh.600519', 'sz.000858']
df_test = df_active[df_active['code'].isin(test_codes)]

conn = get_conn()
conn.execute("DELETE FROM stock_list")
for _, row in df_test.iterrows():
    bs_code = row['code']
    code = bs_code.split('.')[1]
    market = bs_code.split('.')[0]
    conn.execute("INSERT OR REPLACE INTO stock_list (code, name, ipo_date, market) VALUES (?,?,?,?)",
                 (code, row['code_name'], row['ipoDate'], market))
conn.commit()
conn.close()
print(f"插入 {len(df_test)} 只测试股票")

print("\n=== 3. 同步日K线（3只股票，2年数据） ===")
import time
bs.login()
conn = get_conn()
for _, row in df_test.iterrows():
    bs_code = row['code']
    code = bs_code.split('.')[1]
    t0 = time.time()
    rs = bs.query_history_k_data_plus(bs_code,
        "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ",
        start_date='2020-04-01', end_date='2026-04-05',
        frequency='d', adjustflag='2')
    rows = []
    while (rs.error_code == '0') and rs.next():
        rows.append(rs.get_row_data())
    df_k = pd.DataFrame(rows, columns=rs.fields)
    for _, kr in df_k.iterrows():
        conn.execute("""INSERT OR REPLACE INTO daily_kline
            (code,date,open,high,low,close,volume,amount,turn,pct_chg,pe_ttm,pb_mrq)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (code, kr['date'],
             float(kr['open']) if kr['open'] else None,
             float(kr['high']) if kr['high'] else None,
             float(kr['low']) if kr['low'] else None,
             float(kr['close']) if kr['close'] else None,
             float(kr['volume']) if kr['volume'] else None,
             float(kr['amount']) if kr['amount'] else None,
             float(kr['turn']) if kr['turn'] else None,
             float(kr['pctChg']) if kr['pctChg'] else None,
             float(kr['peTTM']) if kr['peTTM'] else None,
             float(kr['pbMRQ']) if kr['pbMRQ'] else None))
    conn.commit()
    elapsed = time.time() - t0
    print(f"  {bs_code} ({row['code_name']}): {len(df_k)} 条, {elapsed:.1f}s")
bs.logout()
conn.close()

print("\n=== 4. 写入 stock_info（手动填市值） ===")
conn = get_conn()
infos = [
    ('600036', '招商银行', 9853, 8062, '银行', 6.56, 0.91, 0.045),
    ('600519', '贵州茅台', 18283, 18283, '白酒', 25.0, 8.5, 0.035),
    ('000858', '五粮液', 4020, 4020, '白酒', 18.0, 3.2, 0.03),
]
for code, name, mc, cc_, ind, pe, pb, dy in infos:
    conn.execute("""INSERT OR REPLACE INTO stock_info
        (code, name, market_cap, circ_cap, industry, pe, pb, dividend_yield, updated_at)
        VALUES (?,?,?,?,?,?,?,?,date('now'))""",
        (code, name, mc, cc_, ind, pe, pb, dy))
conn.commit()
conn.close()
print("OK")

print("\n=== 5. 测试技术指标计算 ===")
result = get_all_indicators('600036')
daily = result['daily']
weekly = result['weekly']
monthly = result['monthly']
print(f"  日线: {len(daily)} 条, MA250最新={daily['ma250'].iloc[-1]:.2f}")
print(f"  周线: {len(weekly)} 条, MA250最新={weekly['ma250'].iloc[-1]:.2f}")
print(f"  月线: {len(monthly)} 条")
print(f"  日线BOLL最新: mid={daily['boll_mid'].iloc[-1]:.2f}, upper={daily['boll_upper'].iloc[-1]:.2f}, lower={daily['boll_lower'].iloc[-1]:.2f}")
print(f"  日线MACD最新: dif={daily['dif'].iloc[-1]:.4f}, dea={daily['dea'].iloc[-1]:.4f}")

print("\n=== 6. 测试选股引擎 ===")
from screener import check_ma250_status, get_candidate_pool
pool = get_candidate_pool()
print(f"  候选池: {len(pool)} 只")
print(pool)

status = check_ma250_status('600036')
print(f"\n  招商银行均线状态:")
print(f"    250日线: {status['ma250_daily']:.2f}, 在上方: {status['above_ma250_daily']}")
print(f"    250周线: {status['ma250_weekly']:.2f}, 在上方: {status['above_ma250_weekly']}")
print(f"    偏离度: {status['distance_to_ma250_daily_pct']:.2f}%")

print("\n=== 7. 测试实时行情 ===")
try:
    price = get_realtime_price('600036')
    print(f"  招商银行实时: {price}")
except Exception as e:
    print(f"  实时行情获取失败（网络问题可忽略）: {e}")

print("\n=== 8. 测试个股分析报告 ===")
from analyzer import analyze
report = analyze('600036')
print(report[:1000])
print("...(报告截断)")

print("\n" + "="*50)
print("  端到端验证完成！")
print("="*50)
