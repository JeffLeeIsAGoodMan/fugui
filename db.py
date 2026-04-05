"""数据库层：SQLite 初始化与读写"""
import sqlite3
from typing import Optional
import pandas as pd
from config import DB_PATH


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS stock_list (
            code       TEXT PRIMARY KEY,
            name       TEXT,
            ipo_date   TEXT,
            market     TEXT   -- sh / sz
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_kline (
            code   TEXT,
            date   TEXT,
            open   REAL,
            high   REAL,
            low    REAL,
            close  REAL,
            volume REAL,
            amount REAL,
            turn   REAL,
            pct_chg REAL,
            pe_ttm  REAL,
            pb_mrq  REAL,
            PRIMARY KEY (code, date)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS dividend (
            code       TEXT,
            report_date TEXT,
            announce_date TEXT,
            dividend_per_share REAL,
            ex_date    TEXT,
            PRIMARY KEY (code, report_date, announce_date)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS stock_info (
            code        TEXT PRIMARY KEY,
            name        TEXT,
            market_cap  REAL,  -- 总市值（亿元），最近一次更新
            circ_cap    REAL,  -- 流通市值（亿元）
            industry    TEXT,
            pe          REAL,
            pb          REAL,
            dividend_yield REAL,  -- 最近股息率
            total_share REAL,
            updated_at  TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    conn.commit()
    conn.close()


def save_df(df: pd.DataFrame, table: str, if_exists: str = "append"):
    conn = get_conn()
    df.to_sql(table, conn, if_exists=if_exists, index=False)
    conn.close()


def read_df(sql: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df


def execute(sql: str, params=None):
    conn = get_conn()
    conn.execute(sql, params or [])
    conn.commit()
    conn.close()


def get_sync_value(key: str) -> Optional[str]:
    conn = get_conn()
    row = conn.execute("SELECT value FROM sync_log WHERE key=?", (key,)).fetchone()
    conn.close()
    return row[0] if row else None


def set_sync_value(key: str, value: str):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO sync_log (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print(f"数据库已初始化: {DB_PATH}")
