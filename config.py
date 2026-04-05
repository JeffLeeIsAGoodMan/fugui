"""全局配置"""
import os

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_DIR, "fugui.db")

# 策略参数
MIN_MARKET_CAP = 500  # 最低市值（亿元）
MA_LONG_PERIOD = 250  # 长周期均线天数 / 周数
BOLL_PERIOD = 20
BOLL_STD = 2
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# 龙回头判断：股价距离 MA250 的偏离度阈值
MA250_TOUCH_THRESHOLD = 0.02  # 2% 以内视为"回踩到均线附近"

# 放量突破：量比阈值
VOLUME_RATIO_THRESHOLD = 1.5

# K线数据至少需要的天数（250周 ≈ 1250个交易日，取6年够用）
HISTORY_YEARS = 6
