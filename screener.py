"""选股引擎模块"""
import pandas as pd
from typing import Optional
from db import read_df
from config import MIN_MARKET_CAP, MA250_TOUCH_THRESHOLD, VOLUME_RATIO_THRESHOLD
from indicators import get_daily_indicators, get_weekly_indicators, get_monthly_indicators, get_all_indicators


def get_candidate_pool() -> pd.DataFrame:
    """获取候选池：市值 ≥ MIN_MARKET_CAP 的所有股票"""
    sql = """
        SELECT code, name, market_cap, industry, pe, pb, dividend_yield
        FROM stock_info
        WHERE market_cap >= ?
        ORDER BY market_cap DESC
    """
    return read_df(sql, (MIN_MARKET_CAP,))


def check_ma250_status(code: str) -> dict:
    """检查单只股票的均线状态"""
    # 使用get_all_indicators只读一次数据库
    indicators = get_all_indicators(code)
    df_daily = indicators['daily']
    df_weekly = indicators['weekly']
    df_monthly = indicators['monthly']

    if df_daily.empty or df_weekly.empty:
        return {}

    latest_daily = df_daily.iloc[-1]
    latest_weekly = df_weekly.iloc[-1]

    daily_close = float(latest_daily['close'])
    ma250_daily = float(latest_daily.get('ma250', pd.NA))
    weekly_close = float(latest_weekly['close'])
    ma250_weekly = float(latest_weekly.get('ma250', pd.NA))

    above_ma250_daily = False
    if pd.notna(ma250_daily) and daily_close > ma250_daily:
        above_ma250_daily = True

    above_ma250_weekly = False
    if pd.notna(ma250_weekly) and weekly_close > ma250_weekly:
        above_ma250_weekly = True

    days_since_cross_above_250d = None
    days_since_cross_below_250d = None
    days_since_cross_below_250w = None  # 周线下穿距今天数（以周线数据点计）
    breakout_then_pullback = False  # 突破后远离再回踩的标志

    if len(df_daily) >= 2 and 'ma250' in df_daily.columns:
        df_daily['above_ma250'] = df_daily['close'] > df_daily['ma250']
        df_daily['cross_above'] = (~df_daily['above_ma250'].shift(1).fillna(False)) & df_daily['above_ma250']
        df_daily['cross_below'] = (df_daily['above_ma250'].shift(1).fillna(False)) & (~df_daily['above_ma250'])

        cross_above_dates = df_daily[df_daily['cross_above']].index
        if len(cross_above_dates) > 0:
            days_since_cross_above_250d = len(df_daily) - 1 - cross_above_dates[-1]

            # 检查最近一次上穿后是否曾经远离均线（上涨超过3%）再回踩
            last_cross_idx = cross_above_dates[-1]
            if last_cross_idx < len(df_daily) - 1:
                # 上穿后的数据
                post_cross_data = df_daily.iloc[last_cross_idx:]
                if len(post_cross_data) >= 2:
                    # 计算相对均线的最大偏离度
                    post_cross_data = post_cross_data.copy()
                    post_cross_data['distance_pct'] = (post_cross_data['close'] - post_cross_data['ma250']) / post_cross_data['ma250'] * 100
                    max_distance = post_cross_data['distance_pct'].max()
                    # 曾经远离均线3%以上，且当前回踩到2%以内
                    if max_distance > 3.0:
                        breakout_then_pullback = True

        cross_below_dates = df_daily[df_daily['cross_below']].index
        if len(cross_below_dates) > 0:
            days_since_cross_below_250d = len(df_daily) - 1 - cross_below_dates[-1]

    # 计算周线下穿250周线的天数
    if len(df_weekly) >= 2 and 'ma250' in df_weekly.columns:
        df_weekly['above_ma250'] = df_weekly['close'] > df_weekly['ma250']
        df_weekly['cross_below'] = (df_weekly['above_ma250'].shift(1).fillna(False)) & (~df_weekly['above_ma250'])

        cross_below_weeks = df_weekly[df_weekly['cross_below']].index
        if len(cross_below_weeks) > 0:
            # 距离最近一个cross_below的周数
            weeks_since = len(df_weekly) - 1 - cross_below_weeks[-1]
            # 转换为交易日天数（每周约5个交易日）
            days_since_cross_below_250w = weeks_since * 5

    distance_to_ma250_daily_pct = 0.0
    if pd.notna(ma250_daily) and ma250_daily != 0:
        distance_to_ma250_daily_pct = (daily_close - ma250_daily) / ma250_daily * 100

    recent_volume_ratio = float(latest_daily.get('volume_ratio', 0)) if pd.notna(latest_daily.get('volume_ratio')) else 0.0

    def get_boll_position(row):
        close = row.get('close')
        upper = row.get('boll_upper')
        mid = row.get('boll_mid')
        lower = row.get('boll_lower')

        if pd.isna(close) or pd.isna(upper) or pd.isna(mid) or pd.isna(lower):
            return 'unknown'

        if close > upper:
            return 'above_upper'
        elif close > mid:
            return 'upper_mid'
        elif close > lower:
            return 'mid_lower'
        else:
            return 'below_lower'

    boll_daily_position = get_boll_position(latest_daily)
    boll_weekly_position = get_boll_position(latest_weekly)

    boll_monthly_position = 'unknown'
    if not df_monthly.empty:
        boll_monthly_position = get_boll_position(df_monthly.iloc[-1])

    macd_daily_hist = float(latest_daily.get('macd_hist', 0)) if pd.notna(latest_daily.get('macd_hist')) else 0.0

    def get_macd_cross(df, col_dif='dif', col_dea='dea'):
        if col_dif not in df.columns or col_dea not in df.columns:
            return 'none'

        df = df.copy()
        df['dif_gt_dea'] = df[col_dif] > df[col_dea]
        df['golden_cross'] = (~df['dif_gt_dea'].shift(1).fillna(False)) & df['dif_gt_dea']
        df['death_cross'] = (df['dif_gt_dea'].shift(1).fillna(False)) & (~df['dif_gt_dea'])

        recent = df.tail(5)
        if recent['golden_cross'].any():
            return 'golden'
        elif recent['death_cross'].any():
            return 'death'
        return 'none'

    macd_daily_cross = get_macd_cross(df_daily)
    macd_weekly_cross = get_macd_cross(df_weekly)

    return {
        'code': code,
        'daily_close': daily_close,
        'ma250_daily': ma250_daily if pd.notna(ma250_daily) else None,
        'above_ma250_daily': above_ma250_daily,
        'weekly_close': weekly_close,
        'ma250_weekly': ma250_weekly if pd.notna(ma250_weekly) else None,
        'above_ma250_weekly': above_ma250_weekly,
        'days_since_cross_above_250d': days_since_cross_above_250d,
        'days_since_cross_below_250d': days_since_cross_below_250d,
        'days_since_cross_below_250w': days_since_cross_below_250w,
        'distance_to_ma250_daily_pct': distance_to_ma250_daily_pct,
        'recent_volume_ratio': recent_volume_ratio,
        'boll_daily_position': boll_daily_position,
        'boll_weekly_position': boll_weekly_position,
        'boll_monthly_position': boll_monthly_position,
        'macd_daily_hist': macd_daily_hist,
        'macd_daily_cross': macd_daily_cross,
        'macd_weekly_cross': macd_weekly_cross,
        'breakout_then_pullback': breakout_then_pullback,
    }


def scan_watchlist(cached_status: dict = None) -> pd.DataFrame:
    """场景1：待突破池（观察名单）"""
    print("正在扫描待突破池...")
    pool = get_candidate_pool()
    results = []

    for idx, row in pool.iterrows():
        code = row['code']
        if cached_status and code in cached_status:
            status = cached_status[code]
        else:
            status = check_ma250_status(code)

        if not status:
            continue

        if status['above_ma250_weekly'] and not status['above_ma250_daily']:
            results.append({
                'code': code,
                'name': row['name'],
                'market_cap': row['market_cap'],
                'daily_close': status['daily_close'],
                'ma250_daily': status['ma250_daily'],
                'ma250_weekly': status['ma250_weekly'],
                'distance_to_ma250_daily_pct': status['distance_to_ma250_daily_pct'],
                'industry': row['industry'],
                'dividend_yield': row['dividend_yield'],
            })

        if (idx + 1) % 50 == 0:
            print(f"  已处理 {idx + 1}/{len(pool)} 只股票")

    df = pd.DataFrame(results)
    if not df.empty:
        # 按绝对值升序，离250日线越近越靠前
        df = df.sort_values('distance_to_ma250_daily_pct', key=abs, ascending=True)
    print(f"待突破池扫描完成，找到 {len(df)} 只股票")
    return df


def scan_breakout(days: int = 5, cached_status: dict = None) -> pd.DataFrame:
    """场景2：刚突破信号

    在候选池（市值≥300亿 + 250周线上方）中，检查最近N日是否有上穿250日线的信号 + 量比达标
    """
    print(f"正在扫描刚突破信号（最近{days}日内突破）...")
    pool = get_candidate_pool()
    results = []

    for idx, row in pool.iterrows():
        code = row['code']
        if cached_status and code in cached_status:
            status = cached_status[code]
        else:
            status = check_ma250_status(code)

        if not status:
            continue

        # 基本条件：大周期OK（250周线上方）
        if not status['above_ma250_weekly']:
            continue

        # 关键条件：最近N日内上穿250日均线 + 当前在250日线上方 + 量比达标
        cross_days = status['days_since_cross_above_250d']
        volume_ratio = status['recent_volume_ratio']

        if (cross_days is not None and cross_days <= days and
            status['above_ma250_daily'] and
            volume_ratio >= VOLUME_RATIO_THRESHOLD):
            results.append({
                'code': code,
                'name': row['name'],
                'market_cap': row['market_cap'],
                'daily_close': status['daily_close'],
                'ma250_daily': status['ma250_daily'],
                'ma250_weekly': status['ma250_weekly'],
                'distance_to_ma250_daily_pct': status['distance_to_ma250_daily_pct'],
                'days_since_cross_above_250d': cross_days,
                'recent_volume_ratio': volume_ratio,
                'industry': row['industry'],
                'dividend_yield': row['dividend_yield'],
            })

        if (idx + 1) % 50 == 0:
            print(f"  已处理 {idx + 1}/{len(pool)} 只股票")

    df = pd.DataFrame(results)
    print(f"刚突破信号扫描完成，找到 {len(df)} 只股票")
    return df


def scan_dragon_return(cached_status: dict = None) -> pd.DataFrame:
    """场景3：龙回头信号（最佳买点）

    条件：
    - 市值≥300亿
    - 大周期OK（250周线上方）
    - 曾经突破过250日线（3个月内有上穿记录）
    - 突破后曾经远离均线（上涨>3%）再回踩
    - 当前股价回踩到250日线附近
    """
    print("正在扫描龙回头信号...")
    pool = get_candidate_pool()
    results = []

    trading_days_3m = 60

    for idx, row in pool.iterrows():
        code = row['code']
        if cached_status and code in cached_status:
            status = cached_status[code]
        else:
            status = check_ma250_status(code)

        if not status:
            continue

        if not status['above_ma250_weekly']:
            continue

        # 当前股价回踩到250日线附近（距离<=2%）
        distance = abs(status['distance_to_ma250_daily_pct'])
        if distance > MA250_TOUCH_THRESHOLD * 100:
            continue

        # 3个月内有上穿记录
        cross_days = status['days_since_cross_above_250d']
        if cross_days is None or cross_days > trading_days_3m:
            continue

        # 突破后曾经远离均线再回踩（避免把刚突破贴线震荡的股票误判为龙回头）
        if not status.get('breakout_then_pullback', False):
            continue

        results.append({
            'code': code,
            'name': row['name'],
            'market_cap': row['market_cap'],
            'daily_close': status['daily_close'],
            'ma250_daily': status['ma250_daily'],
            'distance_to_ma250_daily_pct': status['distance_to_ma250_daily_pct'],
            'boll_daily_position': status['boll_daily_position'],
            'macd_daily_cross': status['macd_daily_cross'],
            'industry': row['industry'],
            'dividend_yield': row['dividend_yield'],
        })

        if (idx + 1) % 50 == 0:
            print(f"  已处理 {idx + 1}/{len(pool)} 只股票")

    df = pd.DataFrame(results)
    print(f"龙回头信号扫描完成，找到 {len(df)} 只股票")
    return df


def scan_hold_warning(cached_status: dict = None) -> pd.DataFrame:
    """场景4：持有监控预警"""
    print("正在扫描持有监控预警...")
    pool = get_candidate_pool()
    results = []

    for idx, row in pool.iterrows():
        code = row['code']
        if cached_status and code in cached_status:
            status = cached_status[code]
        else:
            status = check_ma250_status(code)

        if not status:
            continue

        warning_type = None

        cross_below_daily = status['days_since_cross_below_250d']
        if cross_below_daily is not None and cross_below_daily <= 3:
            warning_type = '日线跌破'

        # 检查周线跌破：验证是否是最近2周（10个交易日）内下穿250周线
        cross_below_weekly = status.get('days_since_cross_below_250w')
        if cross_below_weekly is not None and cross_below_weekly <= 10:  # 2周约10个交易日
            warning_type = '周线跌破'

        if warning_type:
            results.append({
                'code': code,
                'name': row['name'],
                'warning_type': warning_type,
                'daily_close': status['daily_close'],
                'ma250_daily': status['ma250_daily'],
                'ma250_weekly': status['ma250_weekly'],
            })

        if (idx + 1) % 50 == 0:
            print(f"  已处理 {idx + 1}/{len(pool)} 只股票")

    df = pd.DataFrame(results)
    print(f"持有监控预警扫描完成，找到 {len(df)} 只股票")
    return df


def scan_all() -> dict:
    """运行所有4种筛选，返回 {场景key: DataFrame}

    场景key统一使用英文: watchlist, breakout, dragon, warning
    """
    print("开始全市场扫描...")

    # 先计算候选池所有股票的状态，缓存供多个场景复用（优化性能）
    pool = get_candidate_pool()
    print(f"候选池共 {len(pool)} 只股票")

    cached_status = {}
    for idx, row in pool.iterrows():
        code = row['code']
        status = check_ma250_status(code)
        if status:
            cached_status[code] = status
        if (idx + 1) % 50 == 0:
            print(f"  已计算 {idx + 1}/{len(pool)} 只股票状态")

    print(f"状态计算完成，共 {len(cached_status)} 只有效数据")

    return {
        'watchlist': scan_watchlist(cached_status),
        'breakout': scan_breakout(cached_status=cached_status),
        'dragon': scan_dragon_return(cached_status),
        'warning': scan_hold_warning(cached_status),
    }
