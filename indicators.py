"""技术指标计算模块"""
import pandas as pd
import numpy as np
from db import read_df
from config import MA_LONG_PERIOD, BOLL_PERIOD, BOLL_STD, MACD_FAST, MACD_SLOW, MACD_SIGNAL


def calc_ma(series: pd.Series, period: int) -> pd.Series:
    """简单移动平均线"""
    return series.rolling(window=period, min_periods=period).mean()


def calc_boll(close: pd.Series, period: int = 20, std_n: int = 2) -> tuple:
    """布林带，返回 (mid, upper, lower)"""
    mid = calc_ma(close, period)
    std = close.rolling(window=period, min_periods=period).std()
    upper = mid + std_n * std
    lower = mid - std_n * std
    return mid, upper, lower


def calc_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple:
    """MACD，返回 (dif, dea, macd_hist)
    dif = EMA(fast) - EMA(slow)
    dea = EMA(dif, signal)
    macd_hist = 2 * (dif - dea)
    """
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd_hist = 2 * (dif - dea)
    return dif, dea, macd_hist


def calc_volume_ratio(volume: pd.Series, period: int = 5) -> pd.Series:
    """量比 = 当日成交量 / 过去 N 日平均成交量"""
    ma_volume = volume.rolling(window=period, min_periods=period).mean()
    return volume / ma_volume


def resample_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """日线 DataFrame 转周线，聚合规则：
    date -> 每周最后一个交易日
    open -> 周内第一条
    close -> 周内最后一条
    high -> max
    low -> min
    volume -> sum
    amount -> sum
    """
    if df.empty:
        return df.copy()

    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    df = df.sort_index()

    weekly = df.resample('W-FRI').agg({
        'code': 'last',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'amount': 'sum',
        'turn': 'last',
        'pct_chg': 'last',
        'pe_ttm': 'last',
        'pb_mrq': 'last',
    })

    weekly = weekly.dropna(subset=['close'])
    weekly = weekly.reset_index()
    weekly['date'] = weekly['date'].dt.strftime('%Y-%m-%d')

    return weekly


def resample_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """日线 DataFrame 转月线，聚合规则同上，按自然月"""
    if df.empty:
        return df.copy()

    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    df = df.sort_index()

    monthly = df.resample('ME').agg({
        'code': 'last',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'amount': 'sum',
        'turn': 'last',
        'pct_chg': 'last',
        'pe_ttm': 'last',
        'pb_mrq': 'last',
    })

    monthly = monthly.dropna(subset=['close'])
    monthly = monthly.reset_index()
    monthly['date'] = monthly['date'].dt.strftime('%Y-%m-%d')

    return monthly


def get_daily_indicators(code: str) -> pd.DataFrame:
    """获取单只股票的日线 + 所有技术指标
    从 daily_kline 表读取该股票全部日K线
    计算并添加列：
    - ma250: 250日均线
    - boll_mid, boll_upper, boll_lower: 日线布林带
    - dif, dea, macd_hist: 日线MACD
    - volume_ratio: 量比
    返回完整 DataFrame（带指标列）
    """
    df = read_df(
        "SELECT * FROM daily_kline WHERE code = ? ORDER BY date",
        (code,)
    )

    if df.empty:
        return df

    df = df.sort_values('date').reset_index(drop=True)

    close = df['close']
    volume = df['volume']

    df['ma250'] = calc_ma(close, MA_LONG_PERIOD)

    boll_mid, boll_upper, boll_lower = calc_boll(close, BOLL_PERIOD, BOLL_STD)
    df['boll_mid'] = boll_mid
    df['boll_upper'] = boll_upper
    df['boll_lower'] = boll_lower

    dif, dea, macd_hist = calc_macd(close, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
    df['dif'] = dif
    df['dea'] = dea
    df['macd_hist'] = macd_hist

    df['volume_ratio'] = calc_volume_ratio(volume, 5)

    return df


def get_weekly_indicators(code: str) -> pd.DataFrame:
    """获取单只股票的周线 + 技术指标
    从 daily_kline 表读取日K线，合成周K线（按自然周聚合）：
    - date: 取周五（或该周最后一个交易日）
    - open: 周一开盘价
    - close: 周五收盘价
    - high: 周内最高
    - low: 周内最低
    - volume: 周成交量合计
    然后计算：
    - ma250: 250周均线
    - boll_mid, boll_upper, boll_lower
    - dif, dea, macd_hist
    返回周线 DataFrame
    """
    df = read_df(
        "SELECT * FROM daily_kline WHERE code = ? ORDER BY date",
        (code,)
    )

    if df.empty:
        return df

    weekly = resample_to_weekly(df)

    if weekly.empty:
        return weekly

    close = weekly['close']

    weekly['ma250'] = calc_ma(close, MA_LONG_PERIOD)

    boll_mid, boll_upper, boll_lower = calc_boll(close, BOLL_PERIOD, BOLL_STD)
    weekly['boll_mid'] = boll_mid
    weekly['boll_upper'] = boll_upper
    weekly['boll_lower'] = boll_lower

    dif, dea, macd_hist = calc_macd(close, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
    weekly['dif'] = dif
    weekly['dea'] = dea
    weekly['macd_hist'] = macd_hist

    return weekly


def get_monthly_indicators(code: str) -> pd.DataFrame:
    """获取单只股票的月线 + 技术指标
    从日K线合成月K线（按自然月聚合），计算 BOLL 和 MACD
    返回月线 DataFrame
    """
    df = read_df(
        "SELECT * FROM daily_kline WHERE code = ? ORDER BY date",
        (code,)
    )

    if df.empty:
        return df

    monthly = resample_to_monthly(df)

    if monthly.empty:
        return monthly

    close = monthly['close']

    monthly['ma250'] = calc_ma(close, MA_LONG_PERIOD)

    boll_mid, boll_upper, boll_lower = calc_boll(close, BOLL_PERIOD, BOLL_STD)
    monthly['boll_mid'] = boll_mid
    monthly['boll_upper'] = boll_upper
    monthly['boll_lower'] = boll_lower

    dif, dea, macd_hist = calc_macd(close, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
    monthly['dif'] = dif
    monthly['dea'] = dea
    monthly['macd_hist'] = macd_hist

    return monthly


def get_all_indicators(code: str) -> dict:
    """一次性获取单只股票的所有周期指标

    只读一次数据库，内部重采样计算日线/周线/月线指标
    返回: {
        'daily': DataFrame,
        'weekly': DataFrame,
        'monthly': DataFrame
    }
    """
    # 只读一次日K线数据
    df = read_df(
        "SELECT * FROM daily_kline WHERE code = ? ORDER BY date",
        (code,)
    )

    if df.empty:
        return {'daily': df, 'weekly': df, 'monthly': df}

    df = df.sort_values('date').reset_index(drop=True)

    # 计算日线指标
    close_daily = df['close']
    volume_daily = df['volume']

    df['ma250'] = calc_ma(close_daily, MA_LONG_PERIOD)

    boll_mid, boll_upper, boll_lower = calc_boll(close_daily, BOLL_PERIOD, BOLL_STD)
    df['boll_mid'] = boll_mid
    df['boll_upper'] = boll_upper
    df['boll_lower'] = boll_lower

    dif, dea, macd_hist = calc_macd(close_daily, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
    df['dif'] = dif
    df['dea'] = dea
    df['macd_hist'] = macd_hist

    df['volume_ratio'] = calc_volume_ratio(volume_daily, 5)

    # 重采样为周线并计算指标
    weekly = resample_to_weekly(df[['code', 'date', 'open', 'high', 'low', 'close',
                                     'volume', 'amount', 'turn', 'pct_chg', 'pe_ttm', 'pb_mrq']])

    if not weekly.empty:
        close_weekly = weekly['close']
        weekly['ma250'] = calc_ma(close_weekly, MA_LONG_PERIOD)

        w_boll_mid, w_boll_upper, w_boll_lower = calc_boll(close_weekly, BOLL_PERIOD, BOLL_STD)
        weekly['boll_mid'] = w_boll_mid
        weekly['boll_upper'] = w_boll_upper
        weekly['boll_lower'] = w_boll_lower

        w_dif, w_dea, w_macd_hist = calc_macd(close_weekly, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        weekly['dif'] = w_dif
        weekly['dea'] = w_dea
        weekly['macd_hist'] = w_macd_hist

    # 重采样为月线并计算指标
    monthly = resample_to_monthly(df[['code', 'date', 'open', 'high', 'low', 'close',
                                       'volume', 'amount', 'turn', 'pct_chg', 'pe_ttm', 'pb_mrq']])

    if not monthly.empty:
        close_monthly = monthly['close']
        monthly['ma250'] = calc_ma(close_monthly, MA_LONG_PERIOD)

        m_boll_mid, m_boll_upper, m_boll_lower = calc_boll(close_monthly, BOLL_PERIOD, BOLL_STD)
        monthly['boll_mid'] = m_boll_mid
        monthly['boll_upper'] = m_boll_upper
        monthly['boll_lower'] = m_boll_lower

        m_dif, m_dea, m_macd_hist = calc_macd(close_monthly, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        monthly['dif'] = m_dif
        monthly['dea'] = m_dea
        monthly['macd_hist'] = m_macd_hist

    return {
        'daily': df,
        'weekly': weekly,
        'monthly': monthly
    }
