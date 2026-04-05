"""个股分析报告模块"""
import requests
import pandas as pd
from datetime import datetime
from typing import Optional, Union

import config
from db import read_df
import indicators


def get_realtime_price(code: str) -> dict:
    """通过新浪HTTP接口获取实时行情
    返回 {'price': float, 'change_pct': float, 'volume': float, 'amount': float}
    """
    # 获取市场类型
    market_row = read_df(
        "SELECT market FROM stock_list WHERE code=?", (code,)
    )
    if market_row.empty:
        return {'price': 0.0, 'change_pct': 0.0, 'volume': 0.0, 'amount': 0.0}

    market = market_row.iloc[0]['market']

    # 构建新浪代码
    if market == 'sh':
        sina_code = f"sh{code}"
    elif market == 'sz':
        sina_code = f"sz{code}"
    else:
        sina_code = f"{market}{code}"

    url = f"https://hq.sinajs.cn/list={sina_code}"
    headers = {"Referer": "https://finance.sina.com.cn"}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'gbk'
        data = response.text

        # 解析返回数据
        # 格式: var hq_str_sh600036="招商银行,33.50,33.20,33.80,34.00,33.40,33.80,33.90,1234567,45678901,..."
        start = data.find('"')
        end = data.find('"', start + 1)
        if start == -1 or end == -1:
            return {'price': 0.0, 'change_pct': 0.0, 'volume': 0.0, 'amount': 0.0}

        content = data[start + 1:end]
        parts = content.split(',')

        if len(parts) < 30:
            return {'price': 0.0, 'change_pct': 0.0, 'volume': 0.0, 'amount': 0.0}

        name = parts[0]
        current_price = float(parts[3])  # 当前价
        pre_close = float(parts[2])      # 昨收
        volume = float(parts[8])         # 成交量（手）
        amount = float(parts[9])         # 成交额（元）

        if pre_close > 0:
            change_pct = (current_price - pre_close) / pre_close * 100
        else:
            change_pct = 0.0

        return {
            'name': name,
            'price': current_price,
            'change_pct': change_pct,
            'volume': volume,
            'amount': amount
        }
    except Exception:
        return {'price': 0.0, 'change_pct': 0.0, 'volume': 0.0, 'amount': 0.0}


def analyze(code: str) -> str:
    """生成个股分析报告，返回格式化的文本字符串"""
    lines = []

    # 获取股票基本信息
    stock_info = read_df(
        "SELECT * FROM stock_info WHERE code=?", (code,)
    )
    if stock_info.empty:
        return f"错误：未找到股票 {code} 的信息"

    info = stock_info.iloc[0]
    name = info['name']
    market_cap = info['market_cap']
    circ_cap = info['circ_cap']
    industry = info['industry'] if pd.notna(info['industry']) else '未知'
    pe = info['pe'] if pd.notna(info['pe']) else None
    pb = info['pb'] if pd.notna(info['pb']) else None
    dividend_yield = info['dividend_yield'] if pd.notna(info['dividend_yield']) else None

    # 获取实时行情
    realtime = get_realtime_price(code)
    current_price = realtime['price']
    change_pct = realtime['change_pct']

    # 标题
    lines.append("═" * 50)
    lines.append(f"  {name}（{code}）策略分析报告")
    lines.append("═" * 50)
    lines.append("")

    # 一、基本信息
    lines.append("一、基本信息")
    lines.append(f"  最新价:     {current_price:.2f} 元 ({change_pct:+.2f}%)")
    lines.append(f"  总市值:     {market_cap:.2f} 亿元")
    if circ_cap and pd.notna(circ_cap):
        lines.append(f"  流通市值:   {circ_cap:.2f} 亿元")
    lines.append(f"  所属行业:   {industry}")
    pe_str = f"{pe:.2f}" if pe else "--"
    pb_str = f"{pb:.2f}" if pb else "--"
    lines.append(f"  PE(TTM):    {pe_str}")
    lines.append(f"  PB(MRQ):    {pb_str}")
    lines.append("")

    # 二、分红与股息
    lines.append("二、分红与股息")
    if dividend_yield and dividend_yield > 0:
        lines.append(f"  最近股息率: {dividend_yield * 100:.2f}%")
    else:
        lines.append(f"  最近股息率: --")

    # 获取近5年分红记录
    dividends = read_df(
        "SELECT * FROM dividend WHERE code=? ORDER BY ex_date DESC LIMIT 5",
        (code,)
    )
    if not dividends.empty:
        lines.append("  近5年分红记录:")
        for _, row in dividends.iterrows():
            year = row['ex_date'][:4] if row['ex_date'] else '--'
            div = row['dividend_per_share']
            ex_date = row['ex_date'] if row['ex_date'] else '--'
            lines.append(f"    {year}年: 每股派息 {div:.2f} 元 (除权日: {ex_date})")

        # 分红稳定性评价
        if len(dividends) >= 3:
            lines.append("  分红评价:   连续多年分红，具备一定稳定性")
        else:
            lines.append("  分红评价:   分红记录较少，需谨慎评估")
    else:
        lines.append("  分红记录:   暂无分红数据")
    lines.append("")

    # 三、均线状态
    lines.append("三、均线状态（核心）")
    lines.append("-" * 40)

    # 获取日/周/月技术指标
    df_daily = indicators.get_daily_indicators(code)
    df_weekly = indicators.get_weekly_indicators(code)
    df_monthly = indicators.get_monthly_indicators(code)

    # 大周期（250周均线）
    lines.append("  【大周期 - 250周均线】")
    if df_weekly is not None and not df_weekly.empty and 'ma250' in df_weekly.columns:
        latest_weekly = df_weekly.iloc[-1]
        weekly_close = latest_weekly['close']
        ma250_weekly = latest_weekly['ma250']

        if pd.notna(ma250_weekly):
            lines.append(f"    250周均线: {ma250_weekly:.2f} 元")
            lines.append(f"    当前周收:  {weekly_close:.2f} 元")
            if weekly_close > ma250_weekly:
                lines.append(f"    位置关系:  股价在250周线上方 ✓")
                weekly_ok = True
            else:
                lines.append(f"    位置关系:  股价在250周线下方 ✗")
                weekly_ok = False
        else:
            lines.append(f"    250周均线: 数据不足")
            weekly_ok = False
    else:
        lines.append(f"    250周均线: 数据不足")
        weekly_ok = False

    lines.append("")

    # 小周期（250日均线）
    lines.append("  【小周期 - 250日均线】")
    if df_daily is not None and not df_daily.empty and 'ma250' in df_daily.columns:
        latest_daily = df_daily.iloc[-1]
        daily_close = latest_daily['close']
        ma250_daily = latest_daily['ma250']

        if pd.notna(ma250_daily):
            distance_pct = (daily_close - ma250_daily) / ma250_daily * 100
            lines.append(f"    250日均线: {ma250_daily:.2f} 元")
            lines.append(f"    最新收盘:  {daily_close:.2f} 元")
            lines.append(f"    偏离度:    {distance_pct:+.2f}%")
            if daily_close > ma250_daily:
                lines.append(f"    位置关系:  股价在250日线上方")
                daily_ok = True
            else:
                lines.append(f"    位置关系:  股价在250日线下方")
                daily_ok = False
        else:
            lines.append(f"    250日均线: 数据不足")
            daily_ok = False
            distance_pct = None
    else:
        lines.append(f"    250日均线: 数据不足")
        daily_ok = False
        distance_pct = None

    lines.append("")

    # 四、技术指标
    lines.append("四、技术指标")
    lines.append("-" * 40)

    # BOLL 状态
    lines.append("  【布林带位置】")
    for period, df in [('日线', df_daily), ('周线', df_weekly), ('月线', df_monthly)]:
        if df is not None and not df.empty:
            latest = df.iloc[-1]
            close = latest['close']
            upper = latest.get('boll_upper')
            mid = latest.get('boll_mid')
            lower = latest.get('boll_lower')

            if pd.notna(upper) and pd.notna(lower):
                if close > upper:
                    position = "上轨上方（强势）"
                elif close > mid:
                    position = "上轨与中轨之间"
                elif close > lower:
                    position = "中轨与下轨之间"
                else:
                    position = "下轨下方（弱势）"
                lines.append(f"    {period}: {position}")
            else:
                lines.append(f"    {period}: 数据不足")
    lines.append("")

    # MACD 状态
    lines.append("  【MACD指标】")
    for period, df in [('日线', df_daily), ('周线', df_weekly)]:
        if df is not None and not df.empty:
            latest = df.iloc[-1]
            dif = latest.get('dif')
            dea = latest.get('dea')
            macd_hist = latest.get('macd_hist')

            if pd.notna(dif) and pd.notna(dea):
                lines.append(f"    {period}:")
                lines.append(f"      DIF: {dif:.3f}, DEA: {dea:.3f}, MACD柱: {macd_hist:.3f}")

                # 检查最近5日金叉/死叉
                if len(df) >= 5:
                    recent = df.tail(10)
                    cross_signal = "无"
                    for i in range(len(recent) - 1, 0, -1):
                        prev = recent.iloc[i - 1]
                        curr = recent.iloc[i]
                        if prev['dif'] < prev['dea'] and curr['dif'] >= curr['dea']:
                            days_ago = len(recent) - 1 - i
                            cross_signal = f"{days_ago}日前金叉"
                            break
                        elif prev['dif'] > prev['dea'] and curr['dif'] <= curr['dea']:
                            days_ago = len(recent) - 1 - i
                            cross_signal = f"{days_ago}日前死叉"
                            break
                    lines.append(f"      近期信号: {cross_signal}")
            else:
                lines.append(f"    {period}: 数据不足")
    lines.append("")

    # 五、策略匹配结论
    lines.append("五、策略匹配结论")
    lines.append("-" * 40)

    if not weekly_ok:
        conclusion = "  ❌ 不符合策略，不关注"
        detail = "     原因：大周期不健康（股价位于250周线下方）"
    elif weekly_ok and not daily_ok:
        conclusion = "  ⏳ 待突破，等放量站上250日线后再介入"
        detail = f"     当前股价在250日线下方，距离均线约 {abs(distance_pct):.2f}%"
    elif weekly_ok and daily_ok:
        # 检查是否刚突破
        is_breakout = False
        if df_daily is not None and not df_daily.empty and len(df_daily) >= 10:
            for i in range(min(5, len(df_daily) - 1), 0, -1):
                prev = df_daily.iloc[-i - 1]
                curr = df_daily.iloc[-i]
                if prev['close'] <= prev['ma250'] and curr['close'] > curr['ma250']:
                    is_breakout = True
                    break

        # 检查是否回踩
        is_touch = distance_pct is not None and abs(distance_pct) <= config.MA250_TOUCH_THRESHOLD * 100

        if is_breakout:
            conclusion = "  🚀 刚突破250日线，注意确认放量"
            detail = "     近期出现站上250日线的突破信号"
        elif is_touch:
            conclusion = "  🎯 龙回头买点！股价回踩到250日线附近"
            detail = f"     股价距离250日线仅 {distance_pct:+.2f}%，符合回踩买入条件"
        else:
            conclusion = "  ✓ 符合策略，可关注"
            detail = "     大周期和小周期均处于健康状态"
    else:
        conclusion = "  ⚠ 数据不足，无法判断"
        detail = ""

    lines.append(conclusion)
    if detail:
        lines.append(detail)
    lines.append("")

    # 六、风险提示
    lines.append("═" * 50)
    lines.append("  风险提示：本报告仅基于技术指标的量化分析，")
    lines.append("           不构成投资建议。股市有风险，投资需谨慎。")
    lines.append("═" * 50)

    return "\n".join(lines)


if __name__ == "__main__":
    # 测试
    import sys
    if len(sys.argv) > 1:
        code = sys.argv[1]
    else:
        code = "600036"  # 默认测试招商银行
    print(analyze(code))
