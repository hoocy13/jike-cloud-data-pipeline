#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""等待后台 ADS 发布完成，避免用长连接 SSH 跟踪全量构建。"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

import pymysql


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from config import DB_CONFIG


def connect_ads() -> Any:
    config = dict(DB_CONFIG)
    config["database"] = "ads"
    config.update(connect_timeout=20, read_timeout=60, write_timeout=60)
    return pymysql.connect(**config)


def query_one(sql: str, params: tuple[Any, ...]) -> tuple[Any, ...] | None:
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            connection = connect_ads()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql, params)
                    return cursor.fetchone()
            finally:
                connection.close()
        except pymysql.MySQLError as exc:
            last_error = exc
            print(f"[WARN] ADS 状态查询失败，10秒后重试 ({attempt}/5): {exc}", flush=True)
            time.sleep(10)
    raise RuntimeError("连续 5 次无法查询 ADS 发布状态") from last_error


def wait_sales(deadline: float, interval: int, started_within_minutes: int) -> tuple[int, str]:
    discover_sql = """
        SELECT id, data_version, status, source_end_date, error_code
        FROM ads_publish_batch
        WHERE dataset='sales_daily'
          AND created_at >= UTC_TIMESTAMP() - INTERVAL %s MINUTE
        ORDER BY id DESC LIMIT 1
    """
    tracked_sql = """
        SELECT id, data_version, status, source_end_date, error_code
        FROM ads_publish_batch WHERE id=%s AND dataset='sales_daily'
    """
    newer_sql = """
        SELECT id, data_version, status, source_end_date, error_code
        FROM ads_publish_batch
        WHERE dataset='sales_daily' AND id > %s
        ORDER BY id DESC LIMIT 1
    """
    tracked_id: int | None = None
    failed_since: float | None = None
    while time.monotonic() < deadline:
        if tracked_id is None:
            row = query_one(discover_sql, (started_within_minutes,))
        else:
            row = query_one(tracked_sql, (tracked_id,))
        if row:
            batch_id, version, status, source_end, error_code = row
            tracked_id = int(batch_id)
            print(
                f"[ADS] sales batch={batch_id}, status={status}, "
                f"source_end={source_end}, version={version}",
                flush=True,
            )
            if status == "ready":
                return int(batch_id), str(version)
            if status == "failed":
                failed_since = failed_since or time.monotonic()
                newer = query_one(newer_sql, (tracked_id,))
                if newer:
                    tracked_id = int(newer[0])
                    failed_since = None
                    print(f"[ADS] 检测到发布器自动重试，切换跟踪 sales batch={tracked_id}", flush=True)
                    continue
                if time.monotonic() - failed_since >= 180:
                    raise RuntimeError(
                        f"ADS sales 发布失败且3分钟内没有自动重试: "
                        f"batch={batch_id}, error={error_code}"
                    )
        else:
            if tracked_id is None:
                print("[ADS] 等待新的 sales_daily 批次出现", flush=True)
            else:
                raise RuntimeError(f"正在跟踪的 ADS sales 批次不存在: batch={tracked_id}")
        time.sleep(interval)
    raise TimeoutError("等待 ADS sales_daily 发布超时")


def wait_inventory(deadline: float, interval: int, sales_batch_id: int) -> tuple[int, str]:
    sql = """
        SELECT id, data_version, status, source_end_date, error_code
        FROM ads_publish_batch
        WHERE dataset='inventory_overview' AND id > %s
        ORDER BY id DESC LIMIT 1
    """
    while time.monotonic() < deadline:
        row = query_one(sql, (sales_batch_id,))
        if row:
            batch_id, version, status, source_end, error_code = row
            print(
                f"[ADS] inventory batch={batch_id}, status={status}, "
                f"source_end={source_end}, version={version}",
                flush=True,
            )
            if status == "ready":
                return int(batch_id), str(version)
            if status == "failed":
                raise RuntimeError(f"ADS inventory 发布失败: batch={batch_id}, error={error_code}")
        else:
            print("[ADS] sales 已完成，等待 inventory_overview 批次出现", flush=True)
        time.sleep(interval)
    raise TimeoutError("等待 ADS inventory_overview 发布超时")


def main() -> None:
    parser = argparse.ArgumentParser(description="等待后台 ADS 销售与库存批次发布完成")
    parser.add_argument("--timeout", type=int, default=5400)
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--settle-seconds", type=int, default=20)
    parser.add_argument("--started-within-minutes", type=int, default=120)
    args = parser.parse_args()
    if min(args.timeout, args.interval, args.started_within_minutes) <= 0 or args.settle_seconds < 0:
        raise ValueError("timeout、interval、started-within-minutes 必须大于0，settle-seconds不能小于0")

    print(f"[ADS] 等待后台发布启动，先静置 {args.settle_seconds} 秒", flush=True)
    time.sleep(args.settle_seconds)
    deadline = time.monotonic() + args.timeout
    sales_id, sales_version = wait_sales(deadline, args.interval, args.started_within_minutes)
    inventory_id, inventory_version = wait_inventory(deadline, args.interval, sales_id)
    print(
        f"[DONE] ADS 发布完成: sales={sales_id}/{sales_version}; "
        f"inventory={inventory_id}/{inventory_version}",
        flush=True,
    )


if __name__ == "__main__":
    main()
