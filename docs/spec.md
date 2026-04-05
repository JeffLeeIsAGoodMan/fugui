# 富贵选股系统 — 需求规格文档

## 1. 项目概述

基于"吴富贵低波动红利"策略的 A 股 / 港股智能选股系统。每日自动筛选符合条件的股票，并支持个股深度分析。

### 技术栈
- Python 3.9+
- SQLite（本地数据库）
- Baostock（A股K线 + 估值指标）
- AKShare（市值/股息/财务/港股/同花顺数据）
- 新浪 HTTP 接口（实时行情）

### 项目结构

```
fugui/
├── config.py        # 全局配置（已存在）
├── db.py            # 数据库层（已存在）
├── data_sync.py     # 数据同步模块（待创建）
├── indicators.py    # 技术指标计算（待创建）
├── screener.py      # 选股引擎（待创建）
├── analyzer.py      # 个股分析报告（待创建）
├── main.py          # CLI 主入口（待创建）
├── fugui.db         # SQLite 数据库（运行时生成）
└── docs/
    └── spec.md      # 本文档
```

---

## 2. 已有文件

### config.py

```python
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_DIR, "fugui.db")

MIN_MARKET_CAP = 300          # 最低市值（亿元）
MA_LONG_PERIOD = 250          # 均线周期
BOLL_PERIOD = 20
BOLL_STD = 2
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
MA250_TOUCH_THRESHOLD = 0.02  # 2% 以内视为"回踩到均线附近"
VOLUME_RATIO_THRESHOLD = 1.5  # 放量突破量比阈值
HISTORY_YEARS = 6             # 历史数据年数
```

### db.py

提供以下函数：
- `get_conn()` → sqlite3.Connection
- `init_db()` → 创建表（stock_list, daily_kline, dividend, stock_info, sync_log）
- `save_df(df, table, if_exists="append")` → DataFrame 写入表
- `read_df(sql, params)` → 读取返回 DataFrame
- `execute(sql, params)` → 执行 SQL
- `get_sync_value(key)` / `set_sync_value(key, value)` → 同步进度记录

#### 表结构

**stock_list**：全量股票列表
```
code TEXT PK, name TEXT, ipo_date TEXT, market TEXT (sh/sz)
```

**daily_kline**：日K线
```
code TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL,
volume REAL, amount REAL, turn REAL, pct_chg REAL,
pe_ttm REAL, pb_mrq REAL
PK(code, date)
```

**dividend**：分红记录
```
code TEXT, report_date TEXT, announce_date TEXT,
dividend_per_share REAL, ex_date TEXT
PK(code, report_date, announce_date)
```

**stock_info**：个股最新信息（市值等）
```
code TEXT PK, name TEXT, market_cap REAL(亿), circ_cap REAL(亿),
industry TEXT, pe REAL, pb REAL, dividend_yield REAL,
total_share REAL, updated_at TEXT
```

**sync_log**：同步进度记录
```
key TEXT PK, value TEXT
```

---

## 3. data_sync.py — 数据同步模块

### 3.1 职责

从外部数据源拉取数据存入 SQLite，支持首次全量和每日增量。

### 3.2 核心函数

```python
def sync_stock_list():
    """同步A股全量股票列表 → stock_list 表
    数据源：baostock.query_stock_basic()
    筛选 type='1'(股票) status='1'(上市中)
    code 格式统一为纯数字 6 位（去掉 sh./sz. 前缀），market 字段存 sh/sz
    """

def sync_daily_kline(full: bool = False):
    """同步日K线数据 → daily_kline 表
    数据源：baostock.query_history_k_data_plus
    字段：date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ
    adjustflag='2'（前复权）

    full=True：全量拉取最近 HISTORY_YEARS 年数据
    full=False：增量拉取，从 sync_log['last_kline_date'] 次日开始到今天

    注意事项：
    - baostock 的 code 格式是 sh.600036，需要和本地 code(600036) + market(sh) 互转
    - 使用 sync_log 记录最后同步日期
    - 全量模式下先 DELETE FROM daily_kline 再写入
    - 批量处理：每 100 只股票打印一次进度
    - 每只股票查询后 sleep(0.05) 避免限频
    """

def sync_stock_info():
    """同步个股信息（市值等） → stock_info 表
    数据源：akshare.stock_individual_info_em(symbol=code)
    提取：总市值(转换为亿)、流通市值(转换为亿)、行业

    注意：东方财富接口可能不稳定，失败的跳过不阻断
    只同步 stock_list 中的股票
    stock_info 用 INSERT OR REPLACE 更新

    补充 PE/PB：从 daily_kline 最新一条取 pe_ttm, pb_mrq
    补充 dividend_yield：从 dividend 表计算（最近一年每股分红总和 / 当前股价）
    """

def sync_dividend():
    """同步分红数据 → dividend 表
    数据源：akshare.stock_history_dividend_detail(symbol=code, indicator="分红")
    字段映射：
      公告日期 → announce_date
      派息 → dividend_per_share
      除权除息日 → ex_date（NaT 转为 None）

    只同步 stock_list 中的股票
    用 INSERT OR IGNORE 避免重复
    失败的跳过
    """

def sync_all(full: bool = False):
    """一键同步所有数据
    顺序：init_db → sync_stock_list → sync_daily_kline(full) → sync_dividend → sync_stock_info
    打印每步耗时
    """
```

### 3.3 Baostock 注意事项
- 调用前必须 `bs.login()`，结束后 `bs.logout()`
- code 格式：`sh.600036` / `sz.000858`
- 本地存储 code 统一为 6 位纯数字：`600036`
- 转换函数：`to_bs_code(code, market)` → `"sh.600036"`，`from_bs_code(bs_code)` → `("600036", "sh")`

### 3.4 性能优化
- 全量模式预计 5000 只 × 6 年，耗时较长（小时级别），打印进度
- 增量模式只拉当日数据，几分钟完成
- 使用 `executemany` 批量插入而非 `to_sql`（性能更好）
- 每处理完 100 只股票做一次 commit

---

## 4. indicators.py — 技术指标计算模块

### 4.1 职责

读取 daily_kline，计算各种技术指标，返回 DataFrame。纯计算模块，不写数据库。

### 4.2 核心函数

```python
def calc_ma(series: pd.Series, period: int) -> pd.Series:
    """简单移动平均线"""

def calc_boll(close: pd.Series, period: int = 20, std_n: int = 2) -> tuple[pd.Series, pd.Series, pd.Series]:
    """布林带，返回 (mid, upper, lower)"""

def calc_macd(close: pd.Series, fast=12, slow=26, signal=9) -> tuple[pd.Series, pd.Series, pd.Series]:
    """MACD，返回 (dif, dea, macd_hist)
    dif = EMA(fast) - EMA(slow)
    dea = EMA(dif, signal)
    macd_hist = 2 * (dif - dea)
    """

def calc_volume_ratio(volume: pd.Series, period: int = 5) -> pd.Series:
    """量比 = 当日成交量 / 过去 N 日平均成交量"""

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

def get_monthly_indicators(code: str) -> pd.DataFrame:
    """获取单只股票的月线 + 技术指标
    从日K线合成月K线（按自然月聚合），计算 BOLL 和 MACD
    返回月线 DataFrame
    """

def resample_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """日线 DataFrame 转周线，聚合规则：
    date → 每周最后一个交易日
    open → 周内第一条
    close → 周内最后一条
    high → max
    low → min
    volume → sum
    amount → sum
    """

def resample_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """日线 DataFrame 转月线，聚合规则同上，按自然月"""
```

### 4.3 计算注意事项
- 所有指标计算前检查数据量是否足够（如 MA250 需要至少 250 条记录）
- 不足时对应列填 NaN，不报错
- 周线合成时 date 列需先转 datetime 类型
- MACD 中 EMA 使用 pandas 的 ewm(span=N, adjust=False).mean()

---

## 5. screener.py — 选股引擎

### 5.1 职责

根据策略条件筛选全市场股票，输出符合条件的股票列表。

### 5.2 核心函数

```python
def get_candidate_pool() -> pd.DataFrame:
    """获取候选池：市值 ≥ MIN_MARKET_CAP 的所有股票
    从 stock_info 表读取，返回 code, name, market_cap, industry, pe, pb, dividend_yield
    """

def check_ma250_status(code: str) -> dict:
    """检查单只股票的均线状态
    返回 dict:
    {
        'code': str,
        'daily_close': float,       # 最新收盘价
        'ma250_daily': float,       # 250日均线值
        'above_ma250_daily': bool,  # 是否在250日线上方
        'weekly_close': float,      # 最新周收盘价
        'ma250_weekly': float,      # 250周均线值
        'above_ma250_weekly': bool, # 是否在250周线上方

        # 以下用于信号判断
        'days_since_cross_above_250d': int | None,  # 最近一次上穿250日线距今天数
        'days_since_cross_below_250d': int | None,  # 最近一次下穿250日线距今天数
        'distance_to_ma250_daily_pct': float,        # 当前价距250日线偏离度(%)
        'recent_volume_ratio': float,                 # 最近一日量比

        # BOLL 状态
        'boll_daily_position': str,  # 'above_upper' / 'upper_mid' / 'mid_lower' / 'below_lower'
        'boll_weekly_position': str,
        'boll_monthly_position': str,

        # MACD 状态
        'macd_daily_hist': float,    # 最新 MACD 柱
        'macd_daily_cross': str,     # 'golden' / 'death' / 'none'（最近5日是否有金叉/死叉）
        'macd_weekly_cross': str,
    }
    """

def scan_watchlist() -> pd.DataFrame:
    """场景1：待突破池（观察名单）
    条件：
    - 市值 ≥ 300亿
    - 大周期OK：股价在250周均线上方（above_ma250_weekly=True）
    - 小周期受压：股价在250日均线下方（above_ma250_daily=False）
    返回：code, name, market_cap, daily_close, ma250_daily, ma250_weekly,
          distance_to_ma250_daily_pct, industry, dividend_yield
    按 distance_to_ma250_daily_pct 升序排列（离250日线越近越靠前）
    """

def scan_breakout(days: int = 5) -> pd.DataFrame:
    """场景2：刚突破信号
    条件：
    - 在待突破池的基础上
    - 最近 N 日内上穿250日均线（days_since_cross_above_250d ≤ days）
    - 放量确认：recent_volume_ratio ≥ VOLUME_RATIO_THRESHOLD
    返回额外字段：days_since_cross_above_250d, recent_volume_ratio
    """

def scan_dragon_return() -> pd.DataFrame:
    """场景3：龙回头信号（最佳买点）
    条件：
    - 市值 ≥ 300亿
    - 大周期OK（250周线上方）
    - 曾经突破过250日线（3个月内有上穿记录）
    - 当前股价回踩到250日线附近：|distance_to_ma250_daily_pct| ≤ MA250_TOUCH_THRESHOLD
    - 加分项（展示但不硬筛）：BOLL日线在中轨或下轨附近，MACD日线即将金叉

    返回：code, name, market_cap, daily_close, ma250_daily,
          distance_to_ma250_daily_pct, boll_daily_position, macd_daily_cross,
          industry, dividend_yield
    """

def scan_hold_warning() -> pd.DataFrame:
    """场景4：持有监控预警
    条件（满足任一即预警）：
    - 日线级别：最近3日内下穿250日线
    - 周线级别：最近2周内下穿250周线（更严重）
    返回：code, name, warning_type('日线跌破'/'周线跌破'), daily_close,
          ma250_daily, ma250_weekly
    """

def scan_all() -> dict:
    """运行所有4种筛选，返回 {场景名: DataFrame}"""
```

### 5.3 性能说明
- `get_candidate_pool()` 先从 stock_info 筛出市值达标的股票（大幅减少后续计算量）
- 对候选池中的每只股票逐一调用 `check_ma250_status()`
- 全部计算在内存中完成，不写数据库
- 打印进度：每处理 50 只打印一次

---

## 6. analyzer.py — 个股分析报告

### 6.1 职责

输入一个股票代码，输出完整的策略匹配分析报告（文本格式），类似策略原档中对招商银行的分析。

### 6.2 核心函数

```python
def analyze(code: str) -> str:
    """生成个股分析报告，返回格式化的文本字符串

    报告结构：
    ══════════════════════════════════════
     {name}（{code}）策略分析报告
    ══════════════════════════════════════

    一、基本信息
      - 最新价 / 涨跌幅
      - 总市值 / 流通市值
      - 所属行业
      - PE(TTM) / PB(MRQ)
      来源：stock_info 表 + 新浪实时行情

    二、分红与股息
      - 最近股息率
      - 近5年分红记录（年份、每股派息、除权日）
      - 分红稳定性评价
      来源：dividend 表

    三、均线状态（核心）
      大周期（250周均线）：
        - 250周均线值
        - 当前股价 vs 250周线 → 在上方/下方
        - 判断：大周期健康/不健康

      小周期（250日均线）：
        - 250日均线值
        - 当前股价 vs 250日线 → 在上方/下方
        - 距离250日线偏离度
        - 判断：小周期状态

    四、技术指标
      BOLL（日/周/月）：
        - 当前价在布林带的位置
      MACD（日/周）：
        - DIF/DEA/MACD柱值
        - 最近金叉/死叉情况

    五、策略匹配结论
      根据以上数据，给出明确判断：
      - 如果大周期不健康 → "不符合策略，不关注"
      - 如果大周期OK + 小周期受压 → "待突破，等放量站上250日线(XX元)后再介入"
      - 如果大周期OK + 小周期OK → "符合策略，可关注"
      - 如果刚突破 → "刚突破250日线，注意确认放量"
      - 如果龙回头 → "龙回头买点！股价回踩到250日线附近"

    六、风险提示
      固定文本："本报告仅基于技术指标的量化分析，不构成投资建议。"
    """

def get_realtime_price(code: str) -> dict:
    """通过新浪HTTP接口获取实时行情
    返回 {'price': float, 'change_pct': float, 'volume': float, 'amount': float}

    URL: https://hq.sinajs.cn/list={market}{code}
    需要 headers: Referer: https://finance.sina.com.cn
    A股: sh600036 / sz000858
    港股: hk00700

    通过 stock_list 表的 market 字段确定前缀
    """
```

---

## 7. main.py — CLI 主入口

### 7.1 使用方式

```bash
# 首次全量同步（耗时较长）
python main.py sync --full

# 每日增量同步
python main.py sync

# 全市场扫描（运行所有4种筛选）
python main.py scan

# 只看某个场景
python main.py scan --watchlist
python main.py scan --breakout
python main.py scan --dragon
python main.py scan --warning

# 个股分析
python main.py analyze 600036
python main.py analyze 600519
```

### 7.2 实现

```python
import argparse
from db import init_db
from data_sync import sync_all, sync_daily_kline, sync_stock_list, sync_dividend, sync_stock_info
from screener import scan_all, scan_watchlist, scan_breakout, scan_dragon_return, scan_hold_warning
from analyzer import analyze

def main():
    parser = argparse.ArgumentParser(description="富贵选股系统")
    subparsers = parser.add_subparsers(dest="command")

    # sync
    sync_parser = subparsers.add_parser("sync", help="同步数据")
    sync_parser.add_argument("--full", action="store_true", help="全量同步")

    # scan
    scan_parser = subparsers.add_parser("scan", help="全市场扫描")
    scan_parser.add_argument("--watchlist", action="store_true")
    scan_parser.add_argument("--breakout", action="store_true")
    scan_parser.add_argument("--dragon", action="store_true")
    scan_parser.add_argument("--warning", action="store_true")

    # analyze
    analyze_parser = subparsers.add_parser("analyze", help="个股分析")
    analyze_parser.add_argument("code", help="股票代码，如 600036")

    args = parser.parse_args()
    init_db()

    if args.command == "sync":
        sync_all(full=args.full)
    elif args.command == "scan":
        # 如果没有指定具体场景，则运行全部
        # 如果指定了则只运行对应的
        # 结果用 tabulate 格式化输出
        pass
    elif args.command == "analyze":
        report = analyze(args.code)
        print(report)
    else:
        parser.print_help()
```

### 7.3 输出格式
- scan 结果用表格输出（pandas to_string 或 tabulate）
- analyze 结果用格式化文本输出
- 所有输出使用中文

---

## 8. 已验证可用的数据接口清单

| 数据 | 接口 | 状态 |
|------|------|------|
| A股股票列表 | `baostock.query_stock_basic()` | ✅ 5194只 |
| A股日K线+估值 | `baostock.query_history_k_data_plus` (含 peTTM, pbMRQ) | ✅ |
| A股分红 | `akshare.stock_history_dividend_detail(symbol, indicator="分红")` | ✅ |
| A股个股信息/市值 | `akshare.stock_individual_info_em(symbol)` | ⚠️ 不稳定，可能被拒 |
| A股实时行情 | 新浪 `hq.sinajs.cn` HTTP | ✅ |
| 同花顺财务摘要 | `akshare.stock_financial_abstract_ths(symbol)` | ✅ |
| 港股实时行情 | `akshare.stock_hk_spot()` | ✅ |
| 港股日K线 | `akshare.stock_hk_daily(symbol, adjust)` | ✅ |

**stock_individual_info_em 不稳定的应对**：
- sync_stock_info 中对每只股票 try/except，失败则跳过
- 市值也可以用 baostock 的 totalShare × 最新收盘价 粗略计算作为 fallback
- baostock query_profit_data 中有 totalShare 字段

---

## 9. 依赖

```
akshare
baostock
pandas
numpy
```

虚拟环境已创建在 `.venv/`，以上包已安装。

---

## 10. 重要约定

1. 所有 code 在本地存储和函数参数中统一用 **6位纯数字字符串**：`"600036"`、`"000858"`
2. 日期格式统一 `"YYYY-MM-DD"`
3. 金额单位：市值用**亿元**，其他金额用**元**
4. 所有模块 import 只依赖 config.py 和 db.py，模块之间不互相 import（screener 和 analyzer 可以 import indicators）
5. 打印信息用中文
6. 不要加类型注解中的 `|` 语法（Python 3.9 不支持），用 `Optional` 或 `Union` 代替
