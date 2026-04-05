# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

富贵选股系统 (Fugui Stock Screener) - A Chinese A-share stock screening system based on a "low-volatility dividend" strategy using 250-day and 250-week moving averages as core signals.

## Common Commands

```bash
# Activate virtual environment
source .venv/bin/activate

# Full data sync (first time or after long gaps) - takes hours
python main.py sync --full

# Daily incremental sync - takes minutes
python main.py sync

# Run all 4 screening scenarios
python main.py scan

# Run specific screening scenarios
python main.py scan --watchlist   # 待突破池 (stocks above 250W MA, below 250D MA)
python main.py scan --breakout    # 刚突破信号 (recent 250D MA breakout with volume)
python main.py scan --dragon      # 龙回头信号 (pullback to 250D MA after breakout)
python main.py scan --warning     # 持有预警 (recently fell below 250D/250W MA)

# Individual stock analysis
python main.py analyze 600036

# End-to-end test with 3 stocks
python test_e2e.py
```

## Architecture

### Data Flow
1. **Data Sources:**
   - Baostock: A-share stock list, daily K-line data (OHLCV), PE/PB ratios
   - AKShare: Market cap, dividends, individual stock info
   - Sina Finance HTTP API: Real-time prices

2. **Database Layer (db.py):**
   - SQLite with WAL mode enabled
   - Tables: stock_list, daily_kline, dividend, stock_info, sync_log
   - All DB access through `get_conn()`, `read_df()`, `execute()`

3. **Technical Indicators (indicators.py):**
   - Pure calculation module - no DB writes
   - Reads from daily_kline, computes MA250, BOLL, MACD, volume ratio
   - Resamples daily to weekly/monthly using pandas
   - `get_all_indicators(code)` reads DB once and returns all timeframes

4. **Screening Engine (screener.py):**
   - `get_candidate_pool()` filters by market cap (≥500亿 configurable in config.py)
   - `check_ma250_status(code)` computes all technical signals for a stock
   - 4 screening scenarios match different strategy conditions
   - Status caching in `scan_all()` prevents redundant calculations

5. **Analyzer (analyzer.py):**
   - Generates formatted text reports for individual stocks
   - Fetches real-time prices from Sina API
   - Provides strategy match conclusion based on MA250 position

### Key Configuration (config.py)
- `MIN_MARKET_CAP = 500` - Minimum market cap in 亿元
- `MA250_TOUCH_THRESHOLD = 0.02` - 2% threshold for "near MA250" (龙回头)
- `VOLUME_RATIO_THRESHOLD = 1.5` - Volume ratio for breakout confirmation
- `HISTORY_YEARS = 6` - How much historical data to fetch

### Code Conventions
- Stock codes are always 6-digit strings (e.g., "600036", "000858")
- Dates use "YYYY-MM-DD" format
- Market cap stored in 亿元 (100 million CNY)
- Baostock codes use "sh.600036" format - use `to_bs_code()`/`from_bs_code()` for conversion
- All user-facing output in Chinese

### Testing
- `test_e2e.py` - End-to-end validation with 3 test stocks (600036, 600519, 000858)
- Other test_*.py files are for specific component validation

## Important Implementation Notes

1. **Baostock requires login/logout:** Always wrap API calls in `bs.login()` / `bs.logout()`

2. **AKShare is unstable:** `stock_individual_info_em()` may fail - always use try/except

3. **Sina real-time API:** Requires Referer header: `{"Referer": "https://finance.sina.com.cn"}`

4. **Data sync failure handling:** Failed stock codes are saved to sync_log and retried on next incremental sync

5. **Performance:** Full sync is slow (hour-level). Uses batch commits every 100 stocks and sleep(0.05) between API calls to avoid rate limits.

## File Structure

```
├── config.py        # Global configuration
├── db.py            # SQLite database layer
├── data_sync.py     # Data synchronization from external APIs
├── indicators.py    # Technical indicator calculations
├── screener.py      # Stock screening engine
├── analyzer.py      # Individual stock analysis
├── main.py          # CLI entry point
├── fugui.db         # SQLite database (runtime generated)
└── docs/spec.md     # Detailed specification (in Chinese)
```

## Strategy Logic Summary

The system implements 4 screening scenarios based on 250-day and 250-week moving averages:

1. **待突破池 (Watchlist):** Price above 250W MA but below 250D MA - waiting for breakout
2. **刚突破 (Breakout):** Recently crossed above 250D MA with volume confirmation (量比≥1.5)
3. **龙回头 (Dragon Return):** Price pulled back to within 2% of 250D MA after previous breakout (best entry point)
4. **持有预警 (Hold Warning):** Price recently fell below 250D or 250W MA - exit signal

Core principle: "Long-term trend (250W) determines direction, short-term position (250D) determines entry/exit."
