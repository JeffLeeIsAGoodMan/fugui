"""全量同步：只灌入市值 >= 500亿的A股
策略：先用 baostock 全市场最新K线快速筛出大市值股票，再只拉这些的历史K线
"""
import baostock as bs
import akshare as ak
import pandas as pd
import time
import sys
import os
from db import init_db, get_conn
from config import MIN_MARKET_CAP, HISTORY_YEARS


def flush_print(msg):
    print(msg, flush=True)


def get_qq_spot_batch(codes):
    """通过腾讯财经批量获取股票实时数据
    返回: dict[code, {'name': str, 'market_cap': float}]
    """
    import requests

    # 构建腾讯代码格式
    qq_codes = []
    for code in codes:
        if code.startswith('6'):
            qq_codes.append(f"sh{code}")
        else:
            qq_codes.append(f"sz{code}")

    url = f"http://qt.gtimg.cn/q={','.join(qq_codes)}"

    try:
        r = requests.get(url, timeout=30)
        r.encoding = 'gbk'
        text = r.text

        result = {}
        for line in text.split(';'):
            if 'v_' not in line or '~' not in line:
                continue

            # 解析: v_sh600036="1~招商银行..."
            parts = line.split('=')
            if len(parts) < 2:
                continue

            code_key = parts[0].strip().replace('v_', '')  # sh600036
            code = code_key[2:]  # 600036
            market = 'sh' if code_key.startswith('sh') else 'sz'

            data = parts[1].strip().strip('"')
            fields = data.split('~')

            if len(fields) > 45:
                name = fields[1]
                # 总市值在字段45 (单位: 亿元)
                market_cap = float(fields[44]) if fields[44] else 0

                result[code] = {
                    'code': code,
                    'name': name,
                    'market': market,
                    'bs_code': f"{market}.{code}",
                    'market_cap': market_cap
                }

        return result
    except Exception as e:
        flush_print(f"  腾讯接口失败: {e}")
        return {}


def get_qq_all_spot():
    """获取全市场市值数据
    返回: DataFrame[代码, 名称, 总市值(亿元)]
    """
    flush_print("  从腾讯财经获取全市场数据...")

    # 先获取所有股票列表
    bs.login()
    rs = bs.query_stock_basic()
    all_codes = []
    while (rs.error_code == '0') and rs.next():
        row = rs.get_row_data()
        # row format: [code, code_name, ipoDate, ?, type, status] (6 columns)
        if len(row) >= 6:
            code, name, ipo_date, _, stock_type, status = row[0], row[1], row[2], row[3], row[4], row[5]
            if stock_type == "1" and status == "1":  # 上市A股
                all_codes.append(code.split('.')[1])  # sh.600036 -> 600036
    bs.logout()

    flush_print(f"  共 {len(all_codes)} 只上市A股，分批获取市值...")

    # 腾讯接口每次最多支持约800只，分批获取
    all_data = []
    batch_size = 800
    for i in range(0, len(all_codes), batch_size):
        batch = all_codes[i:i+batch_size]
        data = get_qq_spot_batch(batch)
        all_data.extend(data.values())

        if (i // batch_size + 1) % 5 == 0:
            flush_print(f"  已获取 {len(all_data)} 只...")

    return pd.DataFrame(all_data)


def step1_filter_large_caps():
    """快速筛选市值>=500亿的股票
    方法：腾讯财经全市场实时行情（包含市值），秒级完成
    """
    flush_print("=== 第1步：通过腾讯财经获取全市场实时行情 ===")

    # 使用腾讯接口
    df_spot = get_qq_all_spot()

    if df_spot.empty:
        flush_print("  腾讯失败，尝试AKShare...")
        try:
            df_spot = ak.stock_zh_a_spot_em()
            df_spot['market_cap'] = pd.to_numeric(df_spot['总市值'], errors='coerce') / 1e8
            df_spot['code'] = df_spot['代码'].astype(str).str.strip()
            df_spot['name'] = df_spot['名称']
            df_spot['market'] = df_spot['code'].apply(lambda x: 'sh' if x.startswith('6') else 'sz')
            df_spot['bs_code'] = df_spot['market'] + '.' + df_spot['code']
        except Exception as e:
            flush_print(f"  AKShare也失败: {e}")
            return []

    flush_print(f"  获取到 {len(df_spot)} 只股票")

    # 筛选大市值
    df_large = df_spot[df_spot['market_cap'] >= MIN_MARKET_CAP].copy()

    large_caps = df_large[['code', 'name', 'market', 'bs_code', 'market_cap']].to_dict('records')
    large_caps.sort(key=lambda x: x['market_cap'], reverse=True)

    flush_print(f"  筛选完成: {len(large_caps)} 只市值>={MIN_MARKET_CAP}亿")

    if large_caps:
        flush_print(f"  前10大:")
        for s in large_caps[:10]:
            flush_print(f"    {s['code']} {s['name']:<8} {s['market_cap']:.0f}亿")
        flush_print(f"  最小: {large_caps[-1]['code']} {large_caps[-1]['name']} {large_caps[-1]['market_cap']:.0f}亿")

    return large_caps


def step2_sync_kline(large_caps):
    """同步历史K线"""
    n = len(large_caps)
    flush_print(f"\n=== 第3步：同步日K线（{n}只 × 6年）===")

    bs.login()
    conn = get_conn()

    start_date = f"{2026 - HISTORY_YEARS}-04-01"
    end_date = "2026-04-05"
    success = 0
    total_rows = 0

    for i, stock in enumerate(large_caps):
        try:
            rs = bs.query_history_k_data_plus(
                stock['bs_code'],
                "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ",
                start_date=start_date, end_date=end_date,
                frequency="d", adjustflag="2")
            rows = []
            while (rs.error_code == '0') and rs.next():
                rows.append(rs.get_row_data())

            if rows:
                conn.executemany("""INSERT OR REPLACE INTO daily_kline
                    (code,date,open,high,low,close,volume,amount,turn,pct_chg,pe_ttm,pb_mrq)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    [(stock['code'], r[0],
                      float(r[2]) if r[2] else None,
                      float(r[3]) if r[3] else None,
                      float(r[4]) if r[4] else None,
                      float(r[5]) if r[5] else None,
                      float(r[6]) if r[6] else None,
                      float(r[7]) if r[7] else None,
                      float(r[8]) if r[8] else None,
                      float(r[9]) if r[9] else None,
                      float(r[10]) if r[10] else None,
                      float(r[11]) if r[11] else None)
                     for r in rows])
                success += 1
                total_rows += len(rows)
        except Exception:
            pass

        if (i + 1) % 20 == 0:
            conn.commit()
            flush_print(f"  K线进度: {i+1}/{n} ({(i+1)/n*100:.0f}%), "
                        f"已入库 {total_rows:,} 条")

        time.sleep(0.05)

    conn.commit()
    conn.close()
    bs.logout()
    flush_print(f"  K线完成: {success}/{n} 只, 共 {total_rows:,} 条")


def step3_sync_info(large_caps):
    """写入股票列表 + 市值信息"""
    flush_print(f"\n=== 第4步：写入股票列表和市值信息 ===")
    conn = get_conn()

    # 批量获取总股本
    flush_print("  从Baostock获取总股本...")
    bs.login()
    for stock in large_caps:
        try:
            total_share = None
            for year, quarter in [(2025, 4), (2025, 3), (2024, 4)]:
                rs_p = bs.query_profit_data(code=stock['bs_code'], year=year, quarter=quarter)
                rows_p = []
                while (rs_p.error_code == '0') and rs_p.next():
                    rows_p.append(rs_p.get_row_data())
                if rows_p:
                    ts_idx = rs_p.fields.index('totalShare')
                    ts_str = rows_p[0][ts_idx]
                    if ts_str:
                        total_share = float(ts_str)
                        break
            stock['total_share'] = total_share if total_share else 0
        except Exception:
            stock['total_share'] = 0
        time.sleep(0.02)  # 避免限频
    bs.logout()

    for stock in large_caps:
        conn.execute(
            "INSERT OR REPLACE INTO stock_list (code, name, ipo_date, market) VALUES (?,?,?,?)",
            (stock['code'], stock['name'], '', stock['market']))

        # PE/PB from 最新K线
        row = conn.execute(
            "SELECT pe_ttm, pb_mrq FROM daily_kline WHERE code=? ORDER BY date DESC LIMIT 1",
            (stock['code'],)).fetchone()
        pe = row[0] if row else None
        pb = row[1] if row else None

        conn.execute("""INSERT OR REPLACE INTO stock_info
            (code, name, market_cap, circ_cap, industry, pe, pb, dividend_yield, total_share, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,date('now'))""",
            (stock['code'], stock['name'], stock['market_cap'], 0,
             '', pe, pb, 0, stock['total_share']))

    conn.commit()
    conn.close()
    flush_print(f"  写入 {len(large_caps)} 只")


def step4_sync_dividends(large_caps):
    """同步分红"""
    n = len(large_caps)
    flush_print(f"\n=== 第5步：同步分红数据 ===")
    conn = get_conn()
    success = 0

    for i, stock in enumerate(large_caps):
        try:
            df = ak.stock_history_dividend_detail(symbol=stock['code'], indicator="分红")
            count = 0
            for _, row in df.iterrows():
                div = row.get("派息")
                if div and float(div) > 0:
                    ann_date = str(row.get("公告日期", ""))[:10]
                    ex_date = str(row.get("除权除息日", ""))[:10] if pd.notna(row.get("除权除息日")) else None
                    conn.execute("""INSERT OR IGNORE INTO dividend
                        (code, report_date, announce_date, dividend_per_share, ex_date)
                        VALUES (?,?,?,?,?)""",
                        (stock['code'], ann_date, ann_date, float(div), ex_date))
                    count += 1
            if count > 0:
                success += 1
        except Exception:
            pass

        if (i + 1) % 50 == 0:
            conn.commit()
            flush_print(f"  分红进度: {i+1}/{n}")
        time.sleep(0.3)

    # 更新股息率
    flush_print("  计算股息率...")
    for stock in large_caps:
        row_k = conn.execute(
            "SELECT close FROM daily_kline WHERE code=? ORDER BY date DESC LIMIT 1",
            (stock['code'],)).fetchone()
        close = row_k[0] if row_k else 0
        div_row = conn.execute("""
            SELECT SUM(dividend_per_share) FROM dividend
            WHERE code=? AND ex_date >= date('now', '-365 days')
        """, (stock['code'],)).fetchone()
        total_div_per10 = div_row[0] if div_row and div_row[0] else 0
        dy = (total_div_per10 / 10.0) / close if close > 0 else 0
        conn.execute("UPDATE stock_info SET dividend_yield=? WHERE code=?", (dy, stock['code']))

    conn.commit()
    conn.close()
    flush_print(f"  分红完成: {success} 只有记录")


def main():
    t_start = time.time()
    init_db()

    conn = get_conn()
    conn.execute("DELETE FROM stock_list")
    conn.execute("DELETE FROM daily_kline")
    conn.execute("DELETE FROM dividend")
    conn.execute("DELETE FROM stock_info")
    conn.commit()
    conn.close()

    large_caps = step1_filter_large_caps()
    if not large_caps:
        flush_print("没有找到符合条件的股票！")
        return

    step3_sync_info(large_caps)
    step2_sync_kline(large_caps)
    step4_sync_dividends(large_caps)

    elapsed = time.time() - t_start
    db_size = os.path.getsize(os.path.join(os.path.dirname(__file__), 'fugui.db')) / 1024 / 1024

    flush_print(f"\n{'='*50}")
    flush_print(f"  全量同步完成！")
    flush_print(f"  股票数: {len(large_caps)} 只（市值>={MIN_MARKET_CAP}亿）")
    flush_print(f"  总耗时: {elapsed/60:.1f} 分钟")
    flush_print(f"  数据库: {db_size:.1f} MB")
    flush_print(f"{'='*50}")


if __name__ == "__main__":
    main()
