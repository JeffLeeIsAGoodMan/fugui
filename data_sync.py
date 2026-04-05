"""数据同步模块"""
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

import baostock as bs
import akshare as ak
import pandas as pd
from db import (
    get_conn, init_db, save_df, read_df, execute,
    get_sync_value, set_sync_value
)
from config import HISTORY_YEARS


def to_bs_code(code: str, market: str) -> str:
    """本地 code + market 转 baostock 格式"""
    return f"{market}.{code}"


def from_bs_code(bs_code: str) -> Tuple[str, str]:
    """baostock 格式转本地 (code, market)"""
    parts = bs_code.split(".")
    return parts[1], parts[0]


def sync_stock_list():
    """同步A股全量股票列表"""
    print("同步股票列表...")
    lg = bs.login()
    if lg.error_code != "0":
        print(f"登录失败: {lg.error_msg}")
        return

    try:
        rs = bs.query_stock_basic()
        if rs.error_code != "0":
            print(f"查询失败: {rs.error_msg}")
            return

        data_list = []
        while (rs.error_code == "0") & rs.next():
            row = rs.get_row_data()
            # row: [code, code_name, ipoDate, type, status]
            code, name, ipo_date, stock_type, status = row
            if stock_type == "1" and status == "1":  # 股票且上市中
                local_code, market = from_bs_code(code)
                data_list.append({
                    "code": local_code,
                    "name": name,
                    "ipo_date": ipo_date if ipo_date else None,
                    "market": market
                })

        df = pd.DataFrame(data_list)
        # 使用 DELETE + INSERT 而不是 replace，保留主键约束
        conn = get_conn()
        conn.execute("DELETE FROM stock_list")
        conn.commit()
        conn.close()
        # 逐条 INSERT OR REPLACE
        for _, row in df.iterrows():
            execute(
                "INSERT OR REPLACE INTO stock_list (code, name, ipo_date, market) VALUES (?, ?, ?, ?)",
                (row['code'], row['name'], row['ipo_date'], row['market'])
            )
        print(f"股票列表同步完成，共 {len(df)} 只")
    finally:
        bs.logout()


def _get_last_date_for_code(code: str) -> Optional[str]:
    """获取某只股票在daily_kline表中的最后一条记录日期"""
    result = read_df(
        "SELECT date FROM daily_kline WHERE code = ? ORDER BY date DESC LIMIT 1",
        (code,)
    )
    if result.empty:
        return None
    return result.iloc[0]['date']


def _load_failed_codes() -> List[str]:
    """从sync_log加载上次失败的代码列表"""
    failed_codes_str = get_sync_value('failed_codes_kline')
    if not failed_codes_str:
        return []
    return [c.strip() for c in failed_codes_str.split(',') if c.strip()]


def _save_failed_codes(codes: List[str]):
    """保存失败代码列表到sync_log"""
    if codes:
        set_sync_value('failed_codes_kline', ','.join(codes))
    else:
        set_sync_value('failed_codes_kline', '')


def sync_daily_kline(full: bool = False):
    """同步日K线数据"""
    print(f"同步日K线（{'全量' if full else '增量'}）...")

    # 获取股票列表
    stocks = read_df("SELECT code, market FROM stock_list")
    if stocks.empty:
        print("股票列表为空，请先同步股票列表")
        return

    lg = bs.login()
    if lg.error_code != "0":
        print(f"登录失败: {lg.error_msg}")
        return

    def fetch_stock_data(code: str, market: str, fetch_start: str, fetch_end: str, all_data: list) -> Tuple[bool, str]:
        """获取单只股票数据，返回(是否成功, 错误信息)"""
        bs_code = to_bs_code(code, market)
        rs = bs.query_history_k_data_plus(
            bs_code,
            fields,
            start_date=fetch_start,
            end_date=fetch_end,
            frequency="d",
            adjustflag="2"  # 前复权
        )

        if rs.error_code == "0":
            while rs.next():
                row_data = rs.get_row_data()
                # row_data: [date, code, open, high, low, close, volume, amount, turn, pctChg, peTTM, pbMRQ]
                all_data.append({
                    "code": code,
                    "date": row_data[0],
                    "open": float(row_data[2]) if row_data[2] else None,
                    "high": float(row_data[3]) if row_data[3] else None,
                    "low": float(row_data[4]) if row_data[4] else None,
                    "close": float(row_data[5]) if row_data[5] else None,
                    "volume": float(row_data[6]) if row_data[6] else None,
                    "amount": float(row_data[7]) if row_data[7] else None,
                    "turn": float(row_data[8]) if row_data[8] else None,
                    "pct_chg": float(row_data[9]) if row_data[9] else None,
                    "pe_ttm": float(row_data[10]) if row_data[10] else None,
                    "pb_mrq": float(row_data[11]) if row_data[11] else None,
                })
            return True, ""
        else:
            return False, rs.error_msg

    def commit_data(all_data: list, last_commit: int) -> int:
        """提交数据到数据库"""
        if last_commit < len(all_data):
            conn = get_conn()
            conn.executemany(
                """INSERT OR REPLACE INTO daily_kline
                (code, date, open, high, low, close, volume, amount, turn, pct_chg, pe_ttm, pb_mrq)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (
                        d["code"], d["date"], d["open"], d["high"],
                        d["low"], d["close"], d["volume"], d["amount"],
                        d["turn"], d["pct_chg"], d["pe_ttm"], d["pb_mrq"]
                    )
                    for d in all_data[last_commit:]
                ]
            )
            conn.commit()
            conn.close()
        return len(all_data)

    try:
        fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ"
        all_data = []
        total = len(stocks)
        last_commit = 0
        failed_codes = []  # 记录失败的股票代码
        success_count = 0

        # 确定日期范围
        if full:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=HISTORY_YEARS * 365)).strftime("%Y-%m-%d")
            execute("DELETE FROM daily_kline")
            prev_failed_codes = []
        else:
            last_date = get_sync_value("last_kline_date")
            if last_date:
                start_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = (datetime.now() - timedelta(days=HISTORY_YEARS * 365)).strftime("%Y-%m-%d")
            end_date = datetime.now().strftime("%Y-%m-%d")
            # 加载上次失败的代码列表
            prev_failed_codes = _load_failed_codes()

        need_normal_sync = True
        if start_date > end_date:
            print("数据已是最新，无需同步")
            if not prev_failed_codes:
                return
            # 仍有上次失败的股票需要补拉
            print(f"发现上次失败的 {len(prev_failed_codes)} 只股票，尝试补拉...")
            need_normal_sync = False
        else:
            print(f"日期范围: {start_date} 至 {end_date}")

        # 第一步：正常增量同步（如果需要）
        if need_normal_sync:
            for idx, row in stocks.iterrows():
                code = row["code"]
                market = row["market"]

                success, err_msg = fetch_stock_data(code, market, start_date, end_date, all_data)

                if success:
                    success_count += 1
                else:
                    failed_codes.append((code, err_msg))

                # 每100只提交一次
                if (idx + 1) % 100 == 0 or idx == total - 1:
                    last_commit = commit_data(all_data, last_commit)
                    print(f"进度: {idx + 1}/{total}")

                time.sleep(0.05)  # 避免限频

        # 第二步：增量模式下，处理上次失败的代码补拉
        still_failed_codes = []
        if not full and prev_failed_codes:
            print(f"\n开始补拉上次失败的 {len(prev_failed_codes)} 只股票...")
            for code in prev_failed_codes:
                # 获取该股票的市场
                stock_info = stocks[stocks['code'] == code]
                if stock_info.empty:
                    print(f"  跳过 {code}: 不在股票列表中")
                    still_failed_codes.append(code)
                    continue

                market = stock_info.iloc[0]['market']

                # 获取该股票的最后日期
                last_stock_date = _get_last_date_for_code(code)
                if last_stock_date:
                    # 从最后日期的次日开始补拉
                    stock_start = (datetime.strptime(last_stock_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    # 如果没有记录，使用全局start_date
                    stock_start = start_date if need_normal_sync else (datetime.now() - timedelta(days=HISTORY_YEARS * 365)).strftime("%Y-%m-%d")

                if stock_start > end_date:
                    # 该股票数据已经是最新，认为补拉成功
                    print(f"  {code}: 数据已是最新")
                    continue

                print(f"  {code}: 补拉 {stock_start} 至 {end_date}")
                success, err_msg = fetch_stock_data(code, market, stock_start, end_date, all_data)

                if success:
                    print(f"  {code}: 补拉成功")
                else:
                    print(f"  {code}: 补拉失败 - {err_msg}")
                    still_failed_codes.append(code)

                time.sleep(0.05)

            # 提交补拉的数据
            last_commit = commit_data(all_data, last_commit)

        # 第三步：判断推进水位并保存失败列表
        failure_rate = len(failed_codes) / total if total > 0 else 0

        # 合并本次新失败的和仍然失败的（去重）
        new_failed_codes = [c for c, _ in failed_codes]
        all_failed_codes = list(set(new_failed_codes + still_failed_codes))
        _save_failed_codes(all_failed_codes)

        if len(failed_codes) == 0 or failure_rate < 0.05:
            set_sync_value("last_kline_date", end_date)
            print(f"\n日K线同步完成，共 {len(all_data)} 条记录，成功 {success_count}/{total}")
            if all_failed_codes:
                print(f"警告：{len(all_failed_codes)} 只股票同步失败（失败率 {failure_rate*100:.1f}% < 5%，已推进水位），将在下次补拉")
                for code in all_failed_codes[:10]:
                    print(f"  - {code}")
                if len(all_failed_codes) > 10:
                    print(f"  ... 还有 {len(all_failed_codes) - 10} 只")
        else:
            print(f"\n错误：{len(failed_codes)} 只股票同步失败（失败率 {failure_rate*100:.1f}% >= 5%），不推进水位")
            print("失败股票列表（前20个）：")
            for code, err in failed_codes[:20]:
                print(f"  - {code}: {err}")
            raise Exception(f"日K同步失败率过高 ({failure_rate*100:.1f}%)，请检查网络或数据源")

    finally:
        bs.logout()


def sync_stock_info():
    """同步个股信息（市值等）"""
    print("同步个股信息...")

    stocks = read_df("SELECT code FROM stock_list")
    if stocks.empty:
        print("股票列表为空")
        return

    success_count = 0
    skip_count = 0

    for idx, row in stocks.iterrows():
        code = row["code"]

        try:
            # 获取个股信息
            df = ak.stock_individual_info_em(symbol=code)
            if df.empty:
                skip_count += 1
                continue

            # 提取字段
            info = dict(zip(df["item"].values, df["value"].values))

            name = info.get("股票名称", "")
            total_cap = info.get("总市值", None)  # 元
            circ_cap = info.get("流通市值", None)  # 元
            industry = info.get("行业", "")

            # 转换为亿元
            market_cap = float(total_cap) / 1e8 if total_cap else None
            circ_cap = float(circ_cap) / 1e8 if circ_cap else None

            # 从daily_kline获取最新PE/PB
            pe_pb_df = read_df(
                "SELECT pe_ttm, pb_mrq, close FROM daily_kline WHERE code=? ORDER BY date DESC LIMIT 1",
                (code,)
            )
            pe = pe_pb_df["pe_ttm"].iloc[0] if not pe_pb_df.empty else None
            pb = pe_pb_df["pb_mrq"].iloc[0] if not pe_pb_df.empty else None
            close_price = pe_pb_df["close"].iloc[0] if not pe_pb_df.empty else None

            # 从dividend计算股息率（最近一年分红 / 当前股价）
            dividend_yield = None
            if close_price and close_price > 0:
                div_df = read_df(
                    """SELECT dividend_per_share FROM dividend
                    WHERE code=? AND ex_date IS NOT NULL
                    AND ex_date >= date('now', '-365 days')""",  # 最近365天
                    (code,)
                )
                if not div_df.empty:
                    total_div = div_df["dividend_per_share"].sum()
                    dividend_yield = total_div / close_price  # 存小数（如0.05代表5%）

            execute(
                """INSERT OR REPLACE INTO stock_info
                (code, name, market_cap, circ_cap, industry, pe, pb, dividend_yield, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (code, name, market_cap, circ_cap, industry, pe, pb, dividend_yield, datetime.now().strftime("%Y-%m-%d"))
            )

            success_count += 1
            if (idx + 1) % 100 == 0:
                print(f"进度: {idx + 1}/{len(stocks)}，成功 {success_count}，失败 {skip_count}")

            time.sleep(0.1)  # 避免限频

        except Exception as e:
            skip_count += 1
            continue

    print(f"个股信息同步完成，成功 {success_count}，跳过 {skip_count}")


def sync_dividend():
    """同步分红数据"""
    print("同步分红数据...")

    stocks = read_df("SELECT code FROM stock_list")
    if stocks.empty:
        print("股票列表为空")
        return

    success_count = 0
    skip_count = 0

    for idx, row in stocks.iterrows():
        code = row["code"]

        try:
            df = ak.stock_history_dividend_detail(symbol=code, indicator="分红")
            if df.empty:
                skip_count += 1
                continue

            # 字段映射：公告日期, 派息, 除权除息日
            for _, div_row in df.iterrows():
                announce_date = div_row.get("公告日期", None)
                dividend = div_row.get("派息", None)
                ex_date = div_row.get("除权除息日", None)
                report_date = div_row.get("报告期", announce_date)  # 如果没有报告期，用公告日期

                # 处理NaT
                if pd.isna(ex_date) or ex_date == "NaT":
                    ex_date = None
                if pd.isna(announce_date) or announce_date == "NaT":
                    announce_date = None
                if pd.isna(report_date) or report_date == "NaT":
                    report_date = None

                # 跳过无效的股息数据
                if dividend is None or (isinstance(dividend, float) and pd.isna(dividend)):
                    continue

                try:
                    execute(
                        """INSERT OR IGNORE INTO dividend
                        (code, report_date, announce_date, dividend_per_share, ex_date)
                        VALUES (?, ?, ?, ?, ?)""",
                        (code, str(report_date) if report_date else None,
                         str(announce_date) if announce_date else None,
                         float(dividend) if dividend else 0.0,
                         str(ex_date) if ex_date else None)
                    )
                except Exception:
                    continue

            success_count += 1
            if (idx + 1) % 100 == 0:
                print(f"进度: {idx + 1}/{len(stocks)}，成功 {success_count}，失败 {skip_count}")

            time.sleep(0.1)

        except Exception as e:
            skip_count += 1
            continue

    print(f"分红数据同步完成，成功 {success_count}，跳过 {skip_count}")


def sync_all(full: bool = False):
    """一键同步所有数据"""
    start_time = time.time()

    init_db()
    print("数据库已初始化")

    step_start = time.time()
    sync_stock_list()
    print(f"股票列表同步耗时: {time.time() - step_start:.1f}秒")

    step_start = time.time()
    sync_daily_kline(full=full)
    print(f"日K线同步耗时: {time.time() - step_start:.1f}秒")

    step_start = time.time()
    sync_dividend()
    print(f"分红数据同步耗时: {time.time() - step_start:.1f}秒")

    step_start = time.time()
    sync_stock_info()
    print(f"个股信息同步耗时: {time.time() - step_start:.1f}秒")

    print(f"总耗时: {time.time() - start_time:.1f}秒")


if __name__ == "__main__":
    sync_all(full=True)
