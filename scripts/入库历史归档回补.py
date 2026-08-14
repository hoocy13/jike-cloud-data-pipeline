#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""使用归档页导出 cURL 回补 2025 年采购入库主单及明细。

归档、未归档分别写入独立暂存表，不直接修改正式表。
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import shlex
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

import pymysql


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from config import DB_CONFIG


BASE_URL = "https://env3.jkyservice.com"
LIST_URL = f"{BASE_URL}/jkyun/erp-busiorder/goodsdoc/listGoodsDoc"
TABLES = {
    "archive": ("入库查询_2025归档_stage", "入库查询明细_2025归档_stage"),
    "active": ("入库查询_2025未归档_stage", "入库查询明细_2025未归档_stage"),
}
EXPORT_ONLY_FIELDS = {"ids", "commonVerify", "exportType", "version"}


def load_base_module():
    path = SCRIPT_DIR / "入库查询_web.py"
    spec = importlib.util.spec_from_file_location("inbound_query_base", path)
    if not spec or not spec.loader:
        raise RuntimeError(f"无法加载 {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def normalize_curl_text(text: str) -> str:
    return text.replace("^\r\n", " ").replace("^\n", " ").replace("^", "")


def parse_archive_export_curl(path: Path) -> dict[str, Any]:
    tokens = shlex.split(
        normalize_curl_text(path.read_text(encoding="utf-8-sig")),
        posix=True,
    )
    url = cookie = raw_data = ""
    headers: dict[str, str] = {}
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token in ("-H", "--header"):
            index += 1
            key, value = tokens[index].split(":", 1)
            headers[key.strip().lower()] = value.strip()
        elif token in ("-b", "--cookie"):
            index += 1
            cookie = tokens[index]
        elif token in ("--data-raw", "--data", "--data-binary", "-d"):
            index += 1
            raw_data = tokens[index]
        elif not token.startswith("-") and not url:
            url = token
        index += 1

    if "startExcelExport" not in url or not raw_data:
        raise ValueError("请提供归档入库页面的 startExcelExport cURL")
    params = dict(parse_qsl(raw_data, keep_blank_values=True))
    try:
        condition = json.loads(params["conditionJson"])
    except (KeyError, json.JSONDecodeError) as exc:
        raise ValueError("cURL 缺少有效 conditionJson") from exc
    if str(condition.get("archived")) != "1":
        raise ValueError("提供的 cURL 不是已归档数据请求（archived != 1）")
    if headers.get("module_code") != "stockIn_List":
        raise ValueError("提供的 cURL 不是入库查询页面请求")
    if not headers.get("authorization") or not cookie:
        raise ValueError("cURL 缺少 authorization 或 cookie")
    for field in EXPORT_ONLY_FIELDS:
        condition.pop(field, None)
    return {
        "url": LIST_URL,
        "headers": headers,
        "cookie": cookie,
        "params": condition,
    }


def parse_datetime(value: str) -> datetime:
    if len(value) == 10:
        value += " 00:00:00"
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def month_windows(start: datetime, end: datetime):
    current = start
    while current < end:
        if current.month == 12:
            next_month = current.replace(
                year=current.year + 1,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
            )
        else:
            next_month = current.replace(
                month=current.month + 1,
                day=1,
                hour=0,
                minute=0,
                second=0,
            )
        yield current, min(next_month, end)
        current = min(next_month, end)


def connect(retries: int = 5):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return pymysql.connect(**DB_CONFIG)
        except pymysql.MySQLError as exc:
            last_error = exc
            if attempt == retries:
                break
            print(f"[WARN] MySQL 连接失败，10 秒后重试（{attempt}/{retries}）", flush=True)
            time.sleep(10)
    raise last_error


def ensure_stage_tables(cur, header_table: str, detail_table: str) -> None:
    cur.execute(f"CREATE TABLE IF NOT EXISTS `{header_table}` LIKE `入库查询`")
    cur.execute(f"CREATE TABLE IF NOT EXISTS `{detail_table}` LIKE `入库查询明细`")


def write_stage_window(
    module,
    header_table: str,
    detail_table: str,
    headers: list[dict[str, Any]],
    details: list[dict[str, Any]],
    start: datetime,
    end: datetime,
) -> None:
    suffix = uuid.uuid4().hex[:10]
    header_tmp = f"{header_table}_tmp_{suffix}"
    detail_tmp = f"{detail_table}_tmp_{suffix}"
    conn = connect()
    try:
        with conn.cursor() as cur:
            ensure_stage_tables(cur, header_table, detail_table)
            cur.execute(f"CREATE TABLE `{header_tmp}` LIKE `{header_table}`")
            cur.execute(f"CREATE TABLE `{detail_tmp}` LIKE `{detail_table}`")
            module.insert_rows(
                cur,
                header_tmp,
                headers,
                list(module.HEADER_FIELDS.values()) + ["updatetime"],
            )
            module.insert_rows(
                cur,
                detail_tmp,
                details,
                list(module.DETAIL_FIELDS.values()) + ["updatetime"],
            )
            conn.commit()

            conn.begin()
            cur.execute(
                f"""
                DELETE d FROM `{detail_table}` d
                JOIN `{header_table}` h ON h.`docId`=d.`docId`
                WHERE h.`入库时间` >= %s AND h.`入库时间` < %s
                """,
                (start, end),
            )
            cur.execute(
                f"DELETE FROM `{header_table}` WHERE `入库时间` >= %s AND `入库时间` < %s",
                (start, end),
            )
            cur.execute(f"INSERT INTO `{header_table}` SELECT * FROM `{header_tmp}`")
            cur.execute(f"INSERT INTO `{detail_table}` SELECT * FROM `{detail_tmp}`")
            conn.commit()
            print(
                f"[DB] {header_table} {start:%Y-%m}: "
                f"主单 {len(headers)}，明细 {len(details)}",
                flush=True,
            )
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{header_tmp}`")
                cur.execute(f"DROP TABLE IF EXISTS `{detail_tmp}`")
            conn.commit()
        finally:
            conn.close()


def fetch_window(
    module,
    source: str,
    base_info: dict[str, Any],
    start: datetime,
    end: datetime,
    page_size: int,
    workers: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    info = {
        "url": LIST_URL,
        "headers": dict(base_info["headers"]),
        "cookie": base_info["cookie"],
        "params": dict(base_info["params"]),
    }
    archived = "1" if source == "archive" else "0"
    info["params"].update(archived=archived)
    raw_headers = module.fetch_headers(info, start, end, page_size)
    doc_ids = [str(row["docId"]) for row in raw_headers]
    print(
        f"[FETCH] {source} {start:%Y-%m}: 主单 {len(doc_ids)}，开始获取明细",
        flush=True,
    )
    raw_details = (
        module.fetch_details(info, doc_ids, page_size, workers)
        if doc_ids
        else []
    )
    updated = datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    return (
        module.normalize(raw_headers, module.HEADER_FIELDS, module.HEADER_DECIMALS, updated),
        module.normalize(raw_details, module.DETAIL_FIELDS, module.DETAIL_DECIMALS, updated),
    )


def parse_args():
    parser = argparse.ArgumentParser(description="回补 2025 年采购入库归档及未归档数据")
    parser.add_argument("--curl", required=True, help="归档入库页面 startExcelExport cURL 文件")
    parser.add_argument("--source", choices=("archive", "active", "both"), default="both")
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default="2026-01-01")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.page_size <= 0 or args.workers <= 0:
        raise ValueError("page-size 和 workers 必须大于 0")
    start = parse_datetime(args.start)
    end = parse_datetime(args.end)
    if start >= end:
        raise ValueError("end 必须晚于 start")

    module = load_base_module()
    info = parse_archive_export_curl(Path(args.curl))
    sources = ("archive", "active") if args.source == "both" else (args.source,)
    totals = {source: [0, 0] for source in sources}
    for source in sources:
        header_table, detail_table = TABLES[source]
        for window_start, window_end in month_windows(start, end):
            headers, details = fetch_window(
                module,
                source,
                info,
                window_start,
                window_end,
                args.page_size,
                args.workers,
            )
            if not args.no_db:
                write_stage_window(
                    module,
                    header_table,
                    detail_table,
                    headers,
                    details,
                    window_start,
                    window_end,
                )
            totals[source][0] += len(headers)
            totals[source][1] += len(details)
    for source, (header_count, detail_count) in totals.items():
        print(
            f"[DONE] {source}: 主单 {header_count}，明细 {detail_count}",
            flush=True,
        )


if __name__ == "__main__":
    main()
