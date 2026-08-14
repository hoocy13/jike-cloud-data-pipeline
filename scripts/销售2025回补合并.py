#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""校验并合并 2025 年销售归档、未归档暂存数据。

默认仅构建、校验 merge_stage；显式传入 --commit 才会替换正式表中
2025-01-01（含）至 2026-01-01（不含）的数据。
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import pymysql

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from config import DB_CONFIG


SCHEMA = "ods"
START = "2025-01-01 00:00:00"
END = "2026-01-01 00:00:00"
LOCK_NAME = "jky_sales_2025_backfill_merge"

DATASETS = (
    {
        "label": "销售单查询",
        "formal": "销售单查询",
        "archive": "销售单查询_2025归档_stage",
        "active": "销售单查询_2025未归档_stage",
        "merge": "销售单查询_2025_merge_stage",
        "time_col": "下单时间",
        "order_col": "订单编号",
    },
    {
        "label": "销售单明细账",
        "formal": "销售单明细账",
        "archive": "销售单明细账_2025归档_stage",
        "active": "销售单明细账_2025未归档_stage",
        "merge": "销售单明细账_2025_merge_stage",
        "time_col": "下单时间",
        "order_col": "订单编号",
    },
)


def qi(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def qualified(table: str) -> str:
    return f"{qi(SCHEMA)}.{qi(table)}"


def connect(retries: int = 5):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return pymysql.connect(**DB_CONFIG, autocommit=False)
        except Exception as exc:  # pragma: no cover - operational retry
            last_error = exc
            if attempt == retries:
                break
            print(f"[WARN] MySQL 连接失败，10 秒后重试（{attempt}/{retries}）", flush=True)
            time.sleep(10)
    raise last_error


def table_exists(cur, table: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*)
          FROM information_schema.tables
         WHERE table_schema=%s AND table_name=%s
        """,
        (SCHEMA, table),
    )
    return bool(cur.fetchone()[0])


def columns(cur, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema=%s AND table_name=%s
         ORDER BY ordinal_position
        """,
        (SCHEMA, table),
    )
    return [row[0] for row in cur.fetchall()]


def scalar(cur, sql: str, params=()):
    cur.execute(sql, params)
    return cur.fetchone()[0]


def source_stats(cur, cfg: dict, source: str) -> dict:
    table = qualified(cfg[source])
    time_col = qi(cfg["time_col"])
    order_col = qi(cfg["order_col"])
    cur.execute(
        f"""
        SELECT COUNT(*) AS rows_count,
               COUNT(DISTINCT {order_col}) AS orders_count,
               MIN({time_col}) AS min_time,
               MAX({time_col}) AS max_time,
               SUM({order_col} IS NULL OR {order_col}='') AS empty_orders,
               SUM({time_col} IS NULL) AS empty_times
          FROM {table}
         WHERE {time_col} >= %s AND {time_col} < %s
        """,
        (START, END),
    )
    row = cur.fetchone()
    return {
        "rows": row[0] or 0,
        "orders": row[1] or 0,
        "min": row[2],
        "max": row[3],
        "empty_orders": row[4] or 0,
        "empty_times": row[5] or 0,
    }


def build_merge(cur, cfg: dict) -> None:
    for key in ("formal", "archive", "active"):
        if not table_exists(cur, cfg[key]):
            raise RuntimeError(f"{cfg['label']}缺少数据表：{SCHEMA}.{cfg[key]}")

    formal_cols = columns(cur, cfg["formal"])
    for key in ("archive", "active"):
        if columns(cur, cfg[key]) != formal_cols:
            raise RuntimeError(f"{cfg['label']}的 {key} 暂存表结构与正式表不一致")

    merge_table = qualified(cfg["merge"])
    cur.execute(f"CREATE TABLE IF NOT EXISTS {merge_table} LIKE {qualified(cfg['formal'])}")
    if columns(cur, cfg["merge"]) != formal_cols:
        raise RuntimeError(f"{cfg['label']}的 merge_stage 结构与正式表不一致")
    cur.execute(f"DELETE FROM {merge_table}")

    cols = ", ".join(qi(col) for col in formal_cols)
    time_col = qi(cfg["time_col"])
    order_col = qi(cfg["order_col"])
    active = qualified(cfg["active"])
    archive = qualified(cfg["archive"])

    # 未归档数据优先。若迁移边界两边出现同一订单，按整单取未归档版本，
    # 避免销售明细账因跨来源重复而被双计。
    cur.execute(
        f"""
        INSERT INTO {merge_table} ({cols})
        SELECT {cols}
          FROM {active}
         WHERE {time_col} >= %s AND {time_col} < %s
        """,
        (START, END),
    )
    active_rows = cur.rowcount
    cur.execute(
        f"""
        INSERT INTO {merge_table} ({cols})
        SELECT {", ".join("a." + qi(col) for col in formal_cols)}
          FROM {archive} a
         WHERE a.{time_col} >= %s AND a.{time_col} < %s
           AND NOT EXISTS (
               SELECT 1
                 FROM {active} u
                WHERE u.{order_col} = a.{order_col}
                  AND u.{time_col} >= %s AND u.{time_col} < %s
           )
        """,
        (START, END, START, END),
    )
    archive_rows = cur.rowcount
    print(
        f"[MERGE STAGE] {cfg['label']}：未归档 {active_rows} 行 + 归档 {archive_rows} 行",
        flush=True,
    )


def validate_merge(cur, cfg: dict) -> dict:
    stats = source_stats(cur, cfg, "merge")
    if not stats["rows"]:
        raise RuntimeError(f"{cfg['label']} merge_stage 没有 2025 数据")
    if stats["empty_orders"] or stats["empty_times"]:
        raise RuntimeError(
            f"{cfg['label']}关键字段为空：订单编号 {stats['empty_orders']}，下单时间 {stats['empty_times']}"
        )

    cur.execute(
        f"""
        SELECT DATE_FORMAT({qi(cfg['time_col'])}, '%%Y-%%m') AS month_key,
               COUNT(*) AS rows_count,
               COUNT(DISTINCT {qi(cfg['order_col'])}) AS orders_count
          FROM {qualified(cfg['merge'])}
         WHERE {qi(cfg['time_col'])} >= %s AND {qi(cfg['time_col'])} < %s
         GROUP BY month_key
         ORDER BY month_key
        """,
        (START, END),
    )
    months = cur.fetchall()
    expected = {f"2025-{month:02d}" for month in range(1, 13)}
    actual = {row[0] for row in months}
    missing = sorted(expected - actual)
    if missing:
        raise RuntimeError(f"{cfg['label']}缺少月份：{', '.join(missing)}")

    print(
        f"[VALID] {cfg['label']}：{stats['rows']} 行，{stats['orders']} 个订单，"
        f"{stats['min']} ~ {stats['max']}",
        flush=True,
    )
    for month_key, rows_count, orders_count in months:
        print(f"  {month_key}: {rows_count} 行 / {orders_count} 单", flush=True)
    return stats


def commit_formal(cur, cfg: dict) -> None:
    formal = qualified(cfg["formal"])
    merge = qualified(cfg["merge"])
    time_col = qi(cfg["time_col"])
    cols = columns(cur, cfg["formal"])
    col_list = ", ".join(qi(col) for col in cols)
    before = scalar(
        cur,
        f"SELECT COUNT(*) FROM {formal} WHERE {time_col} >= %s AND {time_col} < %s",
        (START, END),
    )
    if before:
        backup = f"{cfg['formal']}_2025替换前备份_{datetime.now():%Y%m%d_%H%M%S}"
        cur.execute(f"CREATE TABLE {qualified(backup)} LIKE {formal}")
        cur.execute(
            f"""
            INSERT INTO {qualified(backup)} ({col_list})
            SELECT {col_list} FROM {formal}
             WHERE {time_col} >= %s AND {time_col} < %s
            """,
            (START, END),
        )
        print(f"[BACKUP] 已备份 {before} 行至 {SCHEMA}.{backup}", flush=True)

    cur.execute(
        f"DELETE FROM {formal} WHERE {time_col} >= %s AND {time_col} < %s",
        (START, END),
    )
    inserted = 0
    for month in range(1, 13):
        month_start = f"2025-{month:02d}-01 00:00:00"
        if month == 12:
            month_end = END
        else:
            month_end = f"2025-{month + 1:02d}-01 00:00:00"
        cur.execute(
            f"""
            INSERT INTO {formal} ({col_list})
            SELECT {col_list} FROM {merge}
             WHERE {time_col} >= %s AND {time_col} < %s
            """,
            (month_start, month_end),
        )
        inserted += cur.rowcount
        print(
            f"[FORMAL] {cfg['label']} {month:02d} 月写入 {cur.rowcount} 行",
            flush=True,
        )
    print(f"[FORMAL] {cfg['label']}合计写入 {inserted} 行", flush=True)


def parse_args():
    parser = argparse.ArgumentParser(description="校验并合并 2025 年销售回补暂存数据")
    parser.add_argument("--commit", action="store_true", help="校验通过后替换正式表 2025 数据")
    parser.add_argument(
        "--use-existing-stage",
        action="store_true",
        help="跳过重建，直接复核并使用现有 merge_stage（须与 --commit 同时使用）",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.use_existing_stage and not args.commit:
        raise ValueError("--use-existing-stage 必须与 --commit 同时使用")
    conn = connect()
    lock_acquired = False
    try:
        with conn.cursor() as cur:
            lock_acquired = scalar(cur, "SELECT GET_LOCK(%s, 30)", (LOCK_NAME,)) == 1
            if not lock_acquired:
                raise RuntimeError("未能取得销售回补合并锁")
            for cfg in DATASETS:
                if args.use_existing_stage:
                    if not table_exists(cur, cfg["merge"]):
                        raise RuntimeError(f"{cfg['label']}缺少现有 merge_stage")
                else:
                    print(f"[SOURCE] {cfg['label']} 归档：{source_stats(cur, cfg, 'archive')}", flush=True)
                    print(f"[SOURCE] {cfg['label']} 未归档：{source_stats(cur, cfg, 'active')}", flush=True)
                    build_merge(cur, cfg)
                validate_merge(cur, cfg)
            if not args.use_existing_stage:
                conn.commit()

            if not args.commit:
                print("[DONE] 暂存合并与校验完成；未修改正式表。", flush=True)
                return

            conn.begin()
            try:
                for cfg in DATASETS:
                    commit_formal(cur, cfg)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            for cfg in DATASETS:
                formal_stats = source_stats(
                    cur,
                    {**cfg, "merge": cfg["formal"]},
                    "merge",
                )
                merge_stats = source_stats(cur, cfg, "merge")
                if formal_stats["rows"] != merge_stats["rows"]:
                    raise RuntimeError(
                        f"{cfg['label']}正式表复核不一致："
                        f"{formal_stats['rows']} != {merge_stats['rows']}"
                    )
                print(f"[CHECK] {cfg['label']}正式表 2025：{formal_stats['rows']} 行", flush=True)
            print("[DONE] 两张正式表的 2025 数据已完成回补。", flush=True)
    finally:
        if lock_acquired:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT RELEASE_LOCK(%s)", (LOCK_NAME,))
            except Exception:
                pass
        conn.close()


if __name__ == "__main__":
    main()
