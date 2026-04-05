"""CLI 主入口"""
import argparse
from db import init_db
from data_sync import sync_all
from screener import (
    scan_all,
    scan_watchlist,
    scan_breakout,
    scan_dragon_return,
    scan_hold_warning,
)
from analyzer import analyze


def main():
    parser = argparse.ArgumentParser(description="富贵选股系统")
    subparsers = parser.add_subparsers(dest="command")

    # sync
    sync_parser = subparsers.add_parser("sync", help="同步数据")
    sync_parser.add_argument("--full", action="store_true", help="全量同步")

    # scan
    scan_parser = subparsers.add_parser("scan", help="全市场扫描")
    scan_parser.add_argument("--watchlist", action="store_true", help="待突破池")
    scan_parser.add_argument("--breakout", action="store_true", help="刚突破信号")
    scan_parser.add_argument("--dragon", action="store_true", help="龙回头信号")
    scan_parser.add_argument("--warning", action="store_true", help="持有预警")

    # analyze
    analyze_parser = subparsers.add_parser("analyze", help="个股分析")
    analyze_parser.add_argument("code", help="股票代码，如 600036")

    args = parser.parse_args()
    init_db()

    if args.command == "sync":
        sync_all(full=args.full)
    elif args.command == "scan":
        run_scan(args)
    elif args.command == "analyze":
        report = analyze(args.code)
        print(report)
    else:
        parser.print_help()


def run_scan(args):
    """执行扫描"""
    # 判断是否有指定具体场景
    has_specific = args.watchlist or args.breakout or args.dragon or args.warning

    if not has_specific:
        # 运行所有扫描
        print("运行全市场扫描...")
        results = scan_all()

        print("\n" + "=" * 60)
        print("【场景1】待突破池（观察名单）")
        print("=" * 60)
        df_watch = results.get("watchlist")
        if df_watch is not None and len(df_watch) > 0:
            print(df_watch.to_string(index=False))
            print(f"\n共 {len(df_watch)} 只股票")
        else:
            print("暂无符合条件的股票")

        print("\n" + "=" * 60)
        print("【场景2】刚突破信号")
        print("=" * 60)
        df_breakout = results.get("breakout")
        if df_breakout is not None and len(df_breakout) > 0:
            print(df_breakout.to_string(index=False))
            print(f"\n共 {len(df_breakout)} 只股票")
        else:
            print("暂无符合条件的股票")

        print("\n" + "=" * 60)
        print("【场景3】龙回头信号（最佳买点）")
        print("=" * 60)
        df_dragon = results.get("dragon")
        if df_dragon is not None and len(df_dragon) > 0:
            print(df_dragon.to_string(index=False))
            print(f"\n共 {len(df_dragon)} 只股票")
        else:
            print("暂无符合条件的股票")

        print("\n" + "=" * 60)
        print("【场景4】持有监控预警")
        print("=" * 60)
        df_warning = results.get("warning")
        if df_warning is not None and len(df_warning) > 0:
            print(df_warning.to_string(index=False))
            print(f"\n共 {len(df_warning)} 只股票")
        else:
            print("暂无符合条件的股票")

    else:
        # 只运行指定的扫描
        if args.watchlist:
            print("扫描待突破池...")
            df = scan_watchlist()
            if len(df) > 0:
                print(df.to_string(index=False))
                print(f"\n共 {len(df)} 只股票")
            else:
                print("暂无符合条件的股票")

        if args.breakout:
            print("扫描刚突破信号...")
            df = scan_breakout()
            if len(df) > 0:
                print(df.to_string(index=False))
                print(f"\n共 {len(df)} 只股票")
            else:
                print("暂无符合条件的股票")

        if args.dragon:
            print("扫描龙回头信号...")
            df = scan_dragon_return()
            if len(df) > 0:
                print(df.to_string(index=False))
                print(f"\n共 {len(df)} 只股票")
            else:
                print("暂无符合条件的股票")

        if args.warning:
            print("扫描持有预警...")
            df = scan_hold_warning()
            if len(df) > 0:
                print(df.to_string(index=False))
                print(f"\n共 {len(df)} 只股票")
            else:
                print("暂无符合条件的股票")


if __name__ == "__main__":
    main()
