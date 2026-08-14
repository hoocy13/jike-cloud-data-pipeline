#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
吉客云销售历史归档数据回补。

使用“归档数据”页面触发导出当前页时产生的 getExportMode cURL 作为
登录态和导出字段来源，直接分页调用归档查询接口。默认写入独立暂存表，
不会改动正式 ODS 表。
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shlex
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode

import pandas as pd
import requests


BASE_URL = "https://env3.jkyservice.com"
WEB_APP_KEY = "jackyun_web_browser_2024"
WEB_SIGN_SECRET = os.getenv("JKY_WEB_SIGN_SECRET", "")
CHINA_TZ = timezone(timedelta(hours=8))
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent


DATASETS = {
    "order": {
        "label": "销售单查询",
        "module_file": "销售单查询_web.py",
        "module_code": "order_queryv2",
        "referer": f"{BASE_URL}/oms/order/order_queryv2.html",
        "endpoint": "/jkyun/oms-auto/trade/hisList",
        "gmt_begin": "gmtCreatedBegin",
        "gmt_end": "gmtCreatedEnd",
        "default_table": "销售单查询_2025归档_stage",
        "expected_excel_type": "8003",
    },
    "detail": {
        "label": "销售单明细账",
        "module_file": "销售单明细账_web.py",
        "module_code": "order_detail_list",
        "referer": f"{BASE_URL}/oms/order/order_detail_list.html",
        "endpoint": "/jkyun/oms-auto/trade/detailHisList",
        "gmt_begin": "gmtCreateBegin",
        "gmt_end": "gmtCreateEnd",
        "default_table": "销售单明细账_2025归档_stage",
        "expected_excel_type": "8015",
    },
}


def load_module(file_name: str):
    path = SCRIPT_DIR / file_name
    spec = importlib.util.spec_from_file_location(f"archive_base_{path.stem}", path)
    if not spec or not spec.loader:
        raise RuntimeError(f"无法加载模块：{path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def normalize_curl_text(text: str) -> str:
    return text.replace("^\r\n", " ").replace("^\n", " ").replace("^", "")


def parse_init_curl(path: str) -> dict[str, Any]:
    raw = Path(path).read_text(encoding="utf-8-sig")
    tokens = shlex.split(normalize_curl_text(raw), posix=True)
    if not tokens or tokens[0].lower() != "curl":
        raise ValueError(f"{path} 不是有效 cURL")
    url = ""
    headers: dict[str, str] = {}
    cookie = ""
    body = ""
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token in ("-H", "--header"):
            index += 1
            name, value = tokens[index].split(":", 1)
            headers[name.strip().lower()] = value.strip()
        elif token in ("-b", "--cookie"):
            index += 1
            cookie = tokens[index]
        elif token in ("--data-raw", "--data", "--data-binary", "-d"):
            index += 1
            body = tokens[index]
        elif token.startswith("--data-raw="):
            body = token.split("=", 1)[1]
        elif not token.startswith("-") and not url:
            url = token
        index += 1
    if "getExportMode" not in url:
        raise ValueError("归档 cURL 必须是 getExportMode 请求")
    params = dict(parse_qsl(body, keep_blank_values=True))
    if not headers.get("authorization") or not cookie:
        raise ValueError("归档 cURL 缺少 authorization 或 cookie")
    return {"url": url, "headers": headers, "cookie": cookie, "params": params}


def validate_curl(dataset: str, curl_info: dict[str, Any]) -> None:
    cfg = DATASETS[dataset]
    params = curl_info["params"]
    referer = curl_info["headers"].get("referer", "")
    module_code = curl_info["headers"].get("module_code", "")
    if cfg["referer"] not in referer or module_code != cfg["module_code"]:
        raise ValueError(f"提供的 cURL 不是{cfg['label']}归档页面请求")
    if params.get("excelType") != cfg["expected_excel_type"]:
        raise ValueError(
            f"{cfg['label']}归档 excelType 应为 {cfg['expected_excel_type']}，"
            f"实际为 {params.get('excelType')}"
        )


def signed_params(params: dict[str, Any], authorization: str) -> dict[str, str]:
    out = {key: "" if value is None else str(value) for key, value in params.items()}
    out.update(
        timestamp=str(int(time.time() * 1000)),
        access_token=authorization,
        appkey=WEB_APP_KEY,
    )
    out.pop("sign", None)
    payload = "".join(
        key + value for key, value in sorted(out.items()) if value != ""
    )
    out["sign"] = hashlib.md5(
        (WEB_SIGN_SECRET + payload + WEB_SIGN_SECRET).encode("utf-8")
    ).hexdigest().upper()
    return out


def request_headers(dataset: str, curl_info: dict[str, Any]) -> dict[str, str]:
    cfg = DATASETS[dataset]
    source = curl_info["headers"]
    result = {
        "accept": "*/*",
        "accept-language": source.get("accept-language", "zh-CN,zh;q=0.9"),
        "authorization": source["authorization"],
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "module_code": cfg["module_code"],
        "origin": BASE_URL,
        "referer": cfg["referer"],
        "user-agent": source.get("user-agent", "Mozilla/5.0"),
        "x-requested-with": "XMLHttpRequest",
        "cookie": curl_info["cookie"],
    }
    for key in ("ati", "bx-v"):
        if source.get(key):
            result[key] = source[key]
    return result


def parse_datetime(value: str) -> datetime:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        value += " 00:00:00"
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def to_epoch_ms(value: datetime) -> int:
    return int(value.replace(tzinfo=CHINA_TZ).timestamp() * 1000)


def month_windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    windows = []
    current = start
    while current < end:
        if current.month == 12:
            next_month = current.replace(
                year=current.year + 1, month=1, day=1, hour=0, minute=0, second=0
            )
        else:
            next_month = current.replace(
                month=current.month + 1, day=1, hour=0, minute=0, second=0
            )
        nxt = min(next_month, end)
        windows.append((current, nxt))
        current = nxt
    return windows


def get_nested(row: dict[str, Any], path: str) -> Any:
    value: Any = row
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            value = None
            break
        value = value[part]
    if value is None:
        value = row.get(path.split(".")[-1])
    return value


def base_query(dataset: str, module, window_start: datetime, window_end: datetime) -> dict[str, Any]:
    normal_curl = PROJECT_DIR / "curl" / f"{DATASETS[dataset]['label']}_curl.txt"
    curl_info = module.parse_curl_file(str(normal_curl))
    params = module.export_params_from_curl(curl_info)
    params = module.set_export_time_window(params, window_start, window_end)
    condition = json.loads(params["conditionJson"])
    if dataset == "order":
        query = condition.get("jsonStr") or {}
        query.pop("tradeIds", None)
    else:
        query = condition.get("filterOrderDetailDto") or condition.get("jsonStr") or {}
        query.pop("subTradeIds", None)
        query["hasQueryHistory"] = 1
    return query


def archive_fields(dataset: str, module, curl_info: dict[str, Any]) -> list[str]:
    try:
        header_json = json.loads(curl_info["params"].get("headersJson", "{}"))
        fields = header_json.get("enName") or []
    except json.JSONDecodeError:
        fields = []
    output_fields = module.EXPORT_FIELDS
    if fields != output_fields:
        print(
            f"[WARN] {DATASETS[dataset]['label']} cURL 字段与标准字段不同，"
            "按脚本标准字段查询",
            flush=True,
        )
    # 销售单主表归档列表使用带对象前缀的 grid 字段；明细账使用扁平字段。
    if dataset == "order":
        return module.EXPORT_CONDITION_FIELDS
    return output_fields


def post_archive_page(
    session: requests.Session,
    dataset: str,
    curl_info: dict[str, Any],
    query: dict[str, Any],
    fields: list[str],
    page_index: int,
    page_size: int,
    retries: int,
) -> list[dict[str, Any]]:
    cfg = DATASETS[dataset]
    headers = request_headers(dataset, curl_info)
    raw_params = {
        "jsonStr": json.dumps(query, ensure_ascii=False, separators=(",", ":")),
        "cols": json.dumps(fields, ensure_ascii=False, separators=(",", ":")),
        "pageIndex": page_index,
        "pageSize": page_size,
    }
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            body = signed_params(raw_params, headers["authorization"])
            response = session.post(
                BASE_URL + cfg["endpoint"],
                headers=headers,
                data=urlencode(body),
                timeout=(30, 180),
            )
            payload = response.json()
            if response.status_code != 200 or payload.get("code") != 200:
                raise RuntimeError(
                    f"HTTP {response.status_code}, code={payload.get('code')}, "
                    f"msg={payload.get('msg')}"
                )
            rows = (payload.get("result") or {}).get("data")
            if rows is None:
                rows = payload.get("data") or []
            if not isinstance(rows, list):
                raise RuntimeError(f"归档接口返回 data 类型异常：{type(rows).__name__}")
            return rows
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                delay = attempt * 3
                print(
                    f"[WARN] page={page_index} 第 {attempt}/{retries} 次失败：{exc}；"
                    f"{delay}s 后重试",
                    flush=True,
                )
                time.sleep(delay)
    raise RuntimeError(f"page={page_index} 重试失败：{last_error}")


def fetch_window(
    dataset: str,
    module,
    curl_info: dict[str, Any],
    window_start: datetime,
    window_end: datetime,
    archive_start: datetime,
    archive_end: datetime,
    page_size: int,
    retries: int,
) -> list[dict[str, Any]]:
    cfg = DATASETS[dataset]
    query = base_query(dataset, module, window_start, window_end)
    query[cfg["gmt_begin"]] = to_epoch_ms(archive_start)
    query[cfg["gmt_end"]] = to_epoch_ms(archive_end)
    fields = archive_fields(dataset, module, curl_info)
    all_rows: list[dict[str, Any]] = []
    page_index = 0
    with requests.Session() as session:
        while True:
            rows = post_archive_page(
                session, dataset, curl_info, query, fields, page_index, page_size, retries
            )
            all_rows.extend(rows)
            print(
                f"[PAGE] {DATASETS[dataset]['label']} "
                f"{window_start:%Y-%m} page={page_index} rows={len(rows)} "
                f"total={len(all_rows)}",
                flush=True,
            )
            if len(rows) < page_size:
                break
            page_index += 1
    return all_rows


def scalar_value(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def api_rows_to_frame(
    dataset: str,
    module,
    rows: list[dict[str, Any]],
    window_start: datetime,
) -> pd.DataFrame:
    records = []
    if dataset == "order":
        paths = module.EXPORT_CONDITION_FIELDS
        for row in rows:
            record = {
                name: scalar_value(get_nested(row, path))
                for name, path in zip(module.ORDERED_COLUMNS, paths)
            }
            records.append(record)
    else:
        for row in rows:
            record = {
                name: scalar_value(get_nested(row, field))
                for name, field in zip(module.ORDERED_COLUMNS, module.EXPORT_FIELDS)
            }
            records.append(record)
    df = pd.DataFrame(records, columns=module.ORDERED_COLUMNS)
    update_time = datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    df["updatetime"] = update_time
    if dataset == "detail":
        df["统计时间"] = window_start.strftime("%Y-%m-%d %H:%M:%S")
    for column in df.columns:
        if column in module.NUMERIC_COLUMNS_HINT:
            df[column] = pd.to_numeric(
                df[column].astype(str).str.replace(",", "", regex=False), errors="coerce"
            )
            if column in module.INTEGER_COLUMNS_HINT:
                df[column] = df[column].round(0)
            else:
                df[column] = df[column].round(2)
        elif column in module.DATETIME_COLUMNS_HINT:
            df[column] = normalize_datetime_series(df[column])
    return df[module.FINAL_COLUMNS]


def normalize_datetime_series(series: pd.Series) -> pd.Series:
    """批量解析接口时间，避免对十几万行逐格调用 pd.to_datetime。"""
    text = series.astype("string").str.strip()
    empty = text.isna() | text.isin(("", "None", "nan", "NaT", "<NA>"))
    numeric = pd.to_numeric(text, errors="coerce")
    numeric_mask = numeric.notna() & text.str.fullmatch(r"-?\d+(?:\.\d+)?", na=False)
    milliseconds = numeric_mask & numeric.abs().ge(10_000_000_000)
    seconds = numeric_mask & ~milliseconds

    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    if milliseconds.any():
        parsed.loc[milliseconds] = pd.to_datetime(
            numeric.loc[milliseconds], unit="ms", errors="coerce"
        )
    if seconds.any():
        parsed.loc[seconds] = pd.to_datetime(
            numeric.loc[seconds], unit="s", errors="coerce"
        )
    ordinary = ~(empty | numeric_mask)
    if ordinary.any():
        parsed.loc[ordinary] = pd.to_datetime(
            text.loc[ordinary], errors="coerce", format="mixed"
        )
    return parsed.dt.strftime("%Y-%m-%d %H:%M:%S").astype("string")


def normalize_datetime_value(value: Any) -> Any:
    if value is None or value is pd.NA:
        return pd.NA
    if isinstance(value, (int, float)) and not pd.isna(value):
        number = float(value)
        unit = "ms" if abs(number) >= 10_000_000_000 else "s"
        parsed = pd.to_datetime(number, unit=unit, errors="coerce")
    else:
        text = str(value).strip()
        if not text or text in {"None", "nan", "NaT", "<NA>"}:
            return pd.NA
        if re.fullmatch(r"\d{10,13}", text):
            number = int(text)
            unit = "ms" if len(text) >= 13 else "s"
            parsed = pd.to_datetime(number, unit=unit, errors="coerce")
        else:
            parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return pd.NA
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def process_dataset(
    dataset: str,
    curl_path: str,
    start: datetime,
    end: datetime,
    archive_start: datetime,
    archive_end: datetime,
    table: str,
    page_size: int,
    retries: int,
    no_db: bool,
) -> int:
    cfg = DATASETS[dataset]
    module = load_module(cfg["module_file"])
    curl_info = parse_init_curl(curl_path)
    validate_curl(dataset, curl_info)
    total = 0
    for window_start, window_end in month_windows(start, end):
        print(
            f"[WINDOW] {cfg['label']} {window_start} ~ {window_end}",
            flush=True,
        )
        rows = fetch_window(
            dataset,
            module,
            curl_info,
            window_start,
            window_end,
            archive_start,
            archive_end,
            page_size,
            retries,
        )
        df = api_rows_to_frame(dataset, module, rows, window_start)
        if not df.empty and "下单时间" in df.columns:
            in_window = (
                (pd.to_datetime(df["下单时间"], errors="coerce") >= window_start)
                & (pd.to_datetime(df["下单时间"], errors="coerce") < window_end)
            )
            bad = int((~in_window).sum())
            if bad:
                print(
                    f"[WARN] {cfg['label']}过滤 {bad} 行窗口端点外数据；"
                    "该数据会由所属月份窗口写入",
                    flush=True,
                )
                df = df.loc[in_window].copy()
        if not no_db:
            write_window_with_retry(
                module,
                df,
                table,
                window_start,
                window_end,
                cfg["label"],
                retries,
            )
        print(
            f"[WINDOW DONE] {cfg['label']} {window_start:%Y-%m} rows={len(df)} "
            f"table={table if not no_db else '(no-db)'}",
            flush=True,
        )
        total += len(df)
    return total


def write_window_with_retry(
    module,
    df: pd.DataFrame,
    table: str,
    window_start: datetime,
    window_end: datetime,
    label: str,
    retries: int,
) -> None:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            module.write_window_to_mysql(
                df, table, window_start, window_end, "archive-api", label
            )
            return
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                delay = attempt * 5
                print(
                    f"[WARN] {label}写库第 {attempt}/{retries} 次失败：{exc}；"
                    f"{delay}s 后重试",
                    flush=True,
                )
                time.sleep(delay)
    raise RuntimeError(f"{label}写库重试失败：{last_error}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="回补吉客云销售归档数据到独立暂存表")
    parser.add_argument("--dataset", choices=("order", "detail", "both"), default="both")
    parser.add_argument("--order-curl", help="销售单查询归档 getExportMode cURL 文件")
    parser.add_argument("--detail-curl", help="销售单明细账归档 getExportMode cURL 文件")
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default="2026-01-01")
    parser.add_argument("--archive-start", default="2025-01-01")
    parser.add_argument("--archive-end", default="2025-11-08 05:32:47")
    parser.add_argument("--order-table", default=DATASETS["order"]["default_table"])
    parser.add_argument("--detail-table", default=DATASETS["detail"]["default_table"])
    parser.add_argument("--page-size", type=int, default=5000)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not 1 <= args.page_size <= 10000:
        raise ValueError("--page-size 必须在 1~10000")
    start = parse_datetime(args.start)
    end = parse_datetime(args.end)
    archive_start = parse_datetime(args.archive_start)
    archive_end = parse_datetime(args.archive_end)
    if end <= start or archive_end <= archive_start:
        raise ValueError("结束时间必须晚于开始时间")
    datasets = ("order", "detail") if args.dataset == "both" else (args.dataset,)
    grand_total = 0
    for dataset in datasets:
        curl_path = args.order_curl if dataset == "order" else args.detail_curl
        if not curl_path:
            raise ValueError(f"--{dataset}-curl 未提供")
        table = args.order_table if dataset == "order" else args.detail_table
        total = process_dataset(
            dataset,
            curl_path,
            start,
            end,
            archive_start,
            archive_end,
            table,
            args.page_size,
            args.retries,
            args.no_db,
        )
        print(f"[DONE] {DATASETS[dataset]['label']} total={total}", flush=True)
        grand_total += total
    print(f"[ALL DONE] total={grand_total}", flush=True)


if __name__ == "__main__":
    main()
