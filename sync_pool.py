"""同步指定池子的股票数据：K线 + 分红 + 市值"""
import baostock as bs
import akshare as ak
import pandas as pd
import time
import sqlite3
from db import init_db, get_conn

POOL = [
    # (code, name, market, bs_code)  港股单独处理
    ("600036", "招商银行", "sh", "sh.600036"),
    ("600030", "中信证券", "sh", "sh.600030"),
    ("601318", "中国平安", "sh", "sh.601318"),
    ("600795", "国电电力", "sh", "sh.600795"),
    ("601985", "中国核电", "sh", "sh.601985"),
    ("003816", "中国广核", "sz", "sz.003816"),
    ("000538", "云南白药", "sz", "sz.000538"),
    ("601919", "中远海控", "sh", "sh.601919"),
    ("600690", "海尔智家", "sh", "sh.600690"),
]

HK_POOL = [
    ("06066", "中信建投证券", "hk"),
]

def sync_a_shares():
    init_db()
    conn = get_conn()

    # 1. 写入 stock_list
    print("=== 写入股票列表 ===")
    for code, name, market, _ in POOL:
        conn.execute(
            "INSERT OR REPLACE INTO stock_list (code, name, ipo_date, market) VALUES (?,?,?,?)",
            (code, name, "", market))
    for code, name, market in HK_POOL:
        conn.execute(
            "INSERT OR REPLACE INTO stock_list (code, name, ipo_date, market) VALUES (?,?,?,?)",
            (code, name, "", market))
    conn.commit()
    print(f"  写入 {len(POOL) + len(HK_POOL)} 只股票")

    # 2. 同步A股K线（baostock，6年数据）
    print("\n=== 同步A股日K线（Baostock，6年） ===")
    bs.login()
    for code, name, market, bs_code in POOL:
        t0 = time.time()
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ",
            start_date="2020-04-01", end_date="2026-04-05",
            frequency="d", adjustflag="2")
        rows = []
        while (rs.error_code == '0') and rs.next():
            rows.append(rs.get_row_data())

        for r in rows:
            conn.execute("""INSERT OR REPLACE INTO daily_kline
                (code,date,open,high,low,close,volume,amount,turn,pct_chg,pe_ttm,pb_mrq)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (code, r[0],
                 float(r[2]) if r[2] else None,
                 float(r[3]) if r[3] else None,
                 float(r[4]) if r[4] else None,
                 float(r[5]) if r[5] else None,
                 float(r[6]) if r[6] else None,
                 float(r[7]) if r[7] else None,
                 float(r[8]) if r[8] else None,
                 float(r[9]) if r[9] else None,
                 float(r[10]) if r[10] else None,
                 float(r[11]) if r[11] else None))
        conn.commit()
        elapsed = time.time() - t0
        print(f"  {code} {name}: {len(rows)} 条, {elapsed:.1f}s")
        time.sleep(0.3)
    bs.logout()

    # 3. 同步分红数据（akshare）
    print("\n=== 同步分红数据（AKShare） ===")
    for code, name, market, _ in POOL:
        try:
            df = ak.stock_history_dividend_detail(symbol=code, indicator="分红")
            count = 0
            for _, row in df.iterrows():
                div = row.get("派息")
                if div and float(div) > 0:
                    ann_date = str(row.get("公告日期", ""))[:10]
                    ex_date = str(row.get("除权除息日", ""))[:10] if pd.notna(row.get("除权除息日")) else None
                    conn.execute("""INSERT OR IGNORE INTO dividend
                        (code, report_date, announce_date, dividend_per_share, ex_date)
                        VALUES (?,?,?,?,?)""",
                        (code, ann_date, ann_date, float(div), ex_date))
                    count += 1
            conn.commit()
            print(f"  {code} {name}: {count} 条分红记录")
        except Exception as e:
            print(f"  {code} {name}: 分红获取失败 - {e}")
        time.sleep(0.5)

    # 4. 同步市值/行业信息（akshare，可能不稳定）
    print("\n=== 同步市值信息（AKShare） ===")
    for code, name, market, bs_code in POOL:
        try:
            df_info = ak.stock_individual_info_em(symbol=code)
            info_dict = dict(zip(df_info['item'], df_info['value']))
            mc = float(info_dict.get('总市值', 0)) / 1e8
            cc_ = float(info_dict.get('流通市值', 0)) / 1e8
            ind = info_dict.get('行业', '')

            # PE/PB 从最新K线取
            row_k = conn.execute(
                "SELECT pe_ttm, pb_mrq, close FROM daily_kline WHERE code=? ORDER BY date DESC LIMIT 1",
                (code,)).fetchone()
            pe = row_k[0] if row_k else None
            pb = row_k[1] if row_k else None
            close_price = row_k[2] if row_k else None

            # 股息率：最近一年分红/股价
            div_row = conn.execute("""
                SELECT SUM(dividend_per_share) FROM dividend
                WHERE code=? AND ex_date >= date('now', '-365 days')
            """, (code,)).fetchone()
            total_div = div_row[0] if div_row and div_row[0] else 0
            dy = total_div / close_price if close_price and close_price > 0 else 0

            conn.execute("""INSERT OR REPLACE INTO stock_info
                (code, name, market_cap, circ_cap, industry, pe, pb, dividend_yield, updated_at)
                VALUES (?,?,?,?,?,?,?,?,date('now'))""",
                (code, name, mc, cc_, ind, pe, pb, dy))
            conn.commit()
            print(f"  {code} {name}: 市值{mc:.0f}亿, 行业={ind}, PE={pe}, 股息率={dy*100:.2f}%")
        except Exception as e:
            # fallback: 用K线数据估算
            row_k = conn.execute(
                "SELECT pe_ttm, pb_mrq, close FROM daily_kline WHERE code=? ORDER BY date DESC LIMIT 1",
                (code,)).fetchone()
            pe = row_k[0] if row_k else None
            pb = row_k[1] if row_k else None
            print(f"  {code} {name}: 东方财富接口失败，用K线估值 PE={pe}, PB={pb}")
            conn.execute("""INSERT OR REPLACE INTO stock_info
                (code, name, market_cap, circ_cap, industry, pe, pb, dividend_yield, updated_at)
                VALUES (?,?,?,?,?,?,?,?,date('now'))""",
                (code, name, 0, 0, '', pe, pb, 0))
            conn.commit()
        time.sleep(1)

    # 5. 港股数据（AKShare 新浪源）
    print("\n=== 同步港股数据 ===")
    for code, name, market in HK_POOL:
        try:
            t0 = time.time()
            df = ak.stock_hk_daily(symbol=code, adjust="qfq")
            # 只取最近6年
            df = df[df['date'] >= '2020-04-01']
            for _, row in df.iterrows():
                conn.execute("""INSERT OR REPLACE INTO daily_kline
                    (code,date,open,high,low,close,volume,amount,turn,pct_chg,pe_ttm,pb_mrq)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (code, str(row['date'])[:10],
                     float(row['open']), float(row['high']),
                     float(row['low']), float(row['close']),
                     float(row['volume']), float(row.get('amount', 0)),
                     None, None, None, None))
            conn.commit()
            elapsed = time.time() - t0
            print(f"  {code} {name}: {len(df)} 条, {elapsed:.1f}s")

            # 港股市值/估值
            try:
                df_fi = ak.stock_hk_financial_indicator_em(symbol=code)
                if len(df_fi) > 0:
                    fi = df_fi.iloc[0]
                    mc = float(fi.get('总市值(港元)', 0)) / 1e8
                    pe = float(fi.get('市盈率', 0)) if fi.get('市盈率') else None
                    pb = float(fi.get('市净率', 0)) if fi.get('市净率') else None
                    dy_str = fi.get('股息率TTM(%)', 0)
                    dy = float(dy_str) / 100 if dy_str else 0
                    conn.execute("""INSERT OR REPLACE INTO stock_info
                        (code, name, market_cap, circ_cap, industry, pe, pb, dividend_yield, updated_at)
                        VALUES (?,?,?,?,?,?,?,?,date('now'))""",
                        (code, name, mc, 0, '券商', pe, pb, dy))
                    conn.commit()
                    print(f"    市值{mc:.0f}亿港元, PE={pe}, 股息率={dy*100:.2f}%")
            except Exception as e2:
                print(f"    港股财务指标获取失败: {e2}")
                conn.execute("""INSERT OR REPLACE INTO stock_info
                    (code, name, market_cap, circ_cap, industry, pe, pb, dividend_yield, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,date('now'))""",
                    (code, name, 0, 0, '券商', None, None, 0))
                conn.commit()
        except Exception as e:
            print(f"  {code} {name}: 港股K线获取失败 - {e}")
        time.sleep(0.5)

    conn.close()
    print("\n=== 同步完成！===")


if __name__ == "__main__":
    sync_a_shares()
