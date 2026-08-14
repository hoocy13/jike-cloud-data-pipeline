"""
Sync Jike Cloud sales order detail ledger via the web export task flow.

This script is designed for DolphinScheduler:
- Put a recent detail-list cURL into JKY_SALES_ORDER_DETAIL_CURL, or
  paste it into START_EXPORT_CURL_TEXT for local PyCharm runs.
- The script rewrites the cURL's time window for each batch, starts an export
  task, downloads the xlsx, normalizes columns, and imports rows into MySQL.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import sys
import tempfile
import time
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode

import pandas as pd
import pymysql
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATA_DIR, DB_CONFIG

BASE_URL = "https://env3.jkyservice.com"
WEB_APP_KEY = "jackyun_web_browser_2024"
WEB_SIGN_SECRET = os.getenv("JKY_WEB_SIGN_SECRET", "")

TABLE_NAME = "销售单明细账"
START_EXPORT_CURL_ENV = "JKY_SALES_ORDER_DETAIL_CURL"
COMMON_VERIFY_ENV = "JKY_SALES_ORDER_DETAIL_COMMON_VERIFY"
DEFAULT_CURL_FILE = Path(__file__).resolve().parent.parent / "curl" / "销售单明细账_curl.txt"

# ============================================================
# Paste cURL Here
# ============================================================
# 本地 PyCharm 调试时，把销售单明细账导出的 startExcelExport 完整 cURL 粘到这里。
# DolphinScheduler 上建议用环境变量 JKY_SALES_ORDER_DETAIL_CURL 覆盖，避免 token/cookie 写死。
START_EXPORT_CURL_TEXT = "".strip()
COMMON_VERIFY_TEXT = ""

DEFAULT_XLSX_DIR = os.path.join(DATA_DIR, "销售单明细账_web_exports")
DEFAULT_CSV = os.path.join(DATA_DIR, "销售单明细账_web.csv")

MAX_EXPORT_ROWS = 500000
SPLIT_ROW_THRESHOLD = 400000
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_WINDOW_HOURS = 24 * 30
MIN_WINDOW_HOURS = 1
CONNECT_RETRIES = 5
CONNECT_RETRY_DELAY_SECONDS = 10

# These filters were captured from a temporary UI selection. Scheduled exports
# must cover every sales channel and brand unless the script is changed explicitly.
FULL_SYNC_FILTERS = {
    "shopId": [],
    "brandId": [],
    "shopCateIds": [],
}

ORDERED_COLUMNS = [
    "订单编号", "网店订单号", "销售渠道", "渠道分类", "货品编号",
    "货品名称", "货品条码", "物流公司", "发货仓库", "品牌",
    "规格", "数量", "单价", "优惠", "折扣", "金额", "货品成本",
    "分摊后单价", "分摊金额", "分摊后金额", "费用分摊", "毛利",
    "毛利率", "价格-零售价", "价格-含税价", "价格-不含税价",
    "下单时间", "付款时间", "货品分类", "市", "客户编号",
]
EXPORT_FIELDS = [
    "tradeNo", "sourceTradeNo", "shopName", "shopCateName", "goodsNo",
    "goodsName", "barcode", "logisticName", "warehouseName", "brandName",
    "specName", "sellCount", "sellPrice", "discountFee", "discountRate", "sellTotal",
    "cost", "afterShareUnitFee", "shareFavourableFee", "afterShareFee",
    "otherShareFavourableFee", "grossProfit", "grossProfitRate", "price1",
    "price6", "price7", "tradeTime", "payTime", "cateName", "city",
    "customerCode",
]
FINAL_COLUMNS = ORDERED_COLUMNS + ["updatetime", "统计时间"]
EXTRA_COLUMNS = ["updatetime", "统计时间"]
TEXT_COLUMNS_HINT = {
    "网店订单号", "订单编号", "销售渠道", "物流公司", "物流单号",
    "客户编号", "客户名称", "货品编号", "货品名称", "规格", "条码",
    "货品条码", "品牌", "交易名称", "商品链接Id", "渠道分类",
    "发货仓库", "货品分类", "市", "客户编号",
}
DATETIME_COLUMNS_HINT = {"下单时间", "付款时间", "updatetime", "统计时间"}
INTEGER_COLUMNS_HINT = {"数量"}
DECIMAL_COLUMNS_HINT = {
    "单价", "优惠", "折扣", "金额", "货品成本", "分摊后单价",
    "分摊金额", "分摊后金额", "费用分摊", "毛利", "毛利率",
    "价格-零售价", "价格-含税价", "价格-不含税价",
}
NUMERIC_COLUMNS_HINT = INTEGER_COLUMNS_HINT | DECIMAL_COLUMNS_HINT


def normalize_curl_text(text: str) -> str:
    return text.replace("^\r\n", " ").replace("^\n", " ").replace("^", "")


def parse_curl_text(raw: str) -> dict[str, Any]:
    tokens = shlex.split(normalize_curl_text(raw), posix=True)
    if not tokens or tokens[0].lower() != "curl":
        raise ValueError("The input does not look like a curl command")

    url = ""
    headers: dict[str, str] = {}
    cookie = ""
    data_raw = ""
    i = 1
    while i < len(tokens):
        token = tokens[i]
        if token in ("-H", "--header"):
            i += 1
            name, value = tokens[i].split(":", 1)
            headers[name.strip().lower()] = value.strip()
        elif token in ("-b", "--cookie"):
            i += 1
            cookie = tokens[i]
        elif token in ("--data-raw", "--data", "--data-binary", "-d"):
            i += 1
            data_raw = tokens[i]
        elif token.startswith("--data-raw="):
            data_raw = token.split("=", 1)[1]
        elif not token.startswith("-") and not url:
            url = token
        i += 1

    if not data_raw:
        raise ValueError("Could not find --data-raw in cURL")
    if not any(part in url for part in
               ("startExcelExport", "validateExcelExport", "oms/trade/tradeOrderDetialList")):
        raise ValueError(
            "Please provide a sales-order-detail startExcelExport, validateExcelExport, or tradeOrderDetialList cURL")

    params = dict(parse_qsl(data_raw, keep_blank_values=True))
    return {"url": url, "headers": headers, "cookie": cookie, "params": params}


def parse_curl_file(curl_path: str) -> dict[str, Any]:
    return parse_curl_text(Path(curl_path).read_text(encoding="utf-8-sig"))


def load_curl_info(curl_path: str | None) -> dict[str, Any]:
    env_curl = os.getenv(START_EXPORT_CURL_ENV, "").strip()
    if env_curl:
        print(f"[INFO] using cURL from environment variable {START_EXPORT_CURL_ENV}", flush=True)
        return parse_curl_text(env_curl)
    if curl_path and os.path.exists(curl_path):
        print(f"[INFO] using cURL file: {curl_path}", flush=True)
        return parse_curl_file(curl_path)
    if DEFAULT_CURL_FILE.exists():
        print(f"[INFO] using default cURL file: {DEFAULT_CURL_FILE}", flush=True)
        return parse_curl_file(str(DEFAULT_CURL_FILE))
    if START_EXPORT_CURL_TEXT.strip():
        print("[INFO] using cURL from START_EXPORT_CURL_TEXT", flush=True)
        return parse_curl_text(START_EXPORT_CURL_TEXT)
    raise FileNotFoundError(
        f"No cURL configured. Set {START_EXPORT_CURL_ENV}, update {DEFAULT_CURL_FILE}, "
        "paste START_EXPORT_CURL_TEXT, or pass --curl."
    )


def signed_params(params: dict[str, Any], authorization: str, exclude: set[str] | None = None) -> dict[str, str]:
    out = {key: "" if value is None else str(value) for key, value in params.items()}
    out["timestamp"] = str(int(time.time() * 1000))
    out["access_token"] = authorization
    out["appkey"] = WEB_APP_KEY
    out.pop("sign", None)

    excluded = exclude or set()
    sign_items = [(key, value) for key, value in out.items() if key not in excluded and value != ""]
    sign_items.sort(key=lambda item: item[0])
    payload = "".join(key + value for key, value in sign_items)
    out["sign"] = hashlib.md5(
        (WEB_SIGN_SECRET + payload + WEB_SIGN_SECRET).encode("utf-8")
    ).hexdigest().upper()
    return out


def web_headers(curl_info: dict[str, Any], module_code: str, referer: str | None = None) -> dict[str, str]:
    source = curl_info["headers"]
    authorization = source.get("authorization")
    if not authorization:
        raise ValueError("Missing authorization header in cURL")
    headers = {
        "accept": "*/*",
        "accept-language": source.get("accept-language", "zh-CN,zh;q=0.9"),
        "authorization": authorization,
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "module_code": module_code,
        "origin": BASE_URL,
        "referer": referer or source.get("referer", f"{BASE_URL}/"),
        "user-agent": source.get("user-agent", "Mozilla/5.0"),
        "x-requested-with": "XMLHttpRequest",
    }
    if source.get("ati"):
        headers["ati"] = source["ati"]
    if source.get("bx-v"):
        headers["bx-v"] = source["bx-v"]
    if curl_info.get("cookie"):
        headers["cookie"] = curl_info["cookie"]
    return headers


def request_json(session: requests.Session, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    response = None
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            response = session.request(method, url, timeout=90, **kwargs)
            break
        except (requests.ConnectTimeout, requests.ConnectionError):
            if attempt >= CONNECT_RETRIES:
                raise
            print(
                f"[WARN] connection failed, retry {attempt}/{CONNECT_RETRIES} "
                f"after {CONNECT_RETRY_DELAY_SECONDS}s: {url}",
                flush=True,
            )
            time.sleep(CONNECT_RETRY_DELAY_SECONDS)
    if response is None:
        raise RuntimeError(f"Request did not return a response: {url}")
    text = response.text
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Non-JSON response from {url}: {text[:500]}") from exc
    if response.status_code >= 400 or data.get("code") not in (None, 200):
        raise RuntimeError(f"Request failed {response.status_code}: {text[:1000]}")
    return data


def parse_json(value: str, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def detail_list_params_to_export_params(params: dict[str, str]) -> dict[str, str]:
    json_str = parse_json(params.get("jsonStr", ""), {})
    json_str.pop("subTradeIds", None)
    json_str.update(FULL_SYNC_FILTERS)
    has_verify = bool(params.get("commonVerify"))
    condition = {
        "pageInfo": {
            "pageIndex": int(params.get("pageIndex") or 0),
            "pageSize": MAX_EXPORT_ROWS,
            "sortField": params.get("sortField", ""),
            "sortOrder": params.get("sortOrder", ""),
        },
        "filterOrderDetailDto": json_str,
        "cols": EXPORT_FIELDS,
        "plaintext": 1 if has_verify else 0,
        "version": "2.0",
    }
    out = {
        "serverName": "oms/oms/excel",
        "excelType": "11",
        "headersJson": json.dumps({"enName": EXPORT_FIELDS, "showName": ORDERED_COLUMNS}, ensure_ascii=False,
                                  separators=(",", ":")),
        "conditionJson": json.dumps(condition, ensure_ascii=False, separators=(",", ":")),
        "datasource": "",
        "isMerge": "true",
        "typeName": "销售单明细账",
        "multiSheet": "false",
        "exportTotal": "",
        "isSyn": "false",
    }
    if params.get("commonVerify"):
        out["commonVerify"] = params["commonVerify"]
        out["plaintext"] = "true"
    else:
        print(
            "[WARN] tradeOrderDetialList cURL has no commonVerify; "
            "using non-plaintext export to avoid phone verification.",
            flush=True,
        )
    return out


def export_params_from_curl(curl_info: dict[str, Any]) -> dict[str, str]:
    url = curl_info["url"]
    params = dict(curl_info["params"])
    if "oms/trade/tradeOrderDetialList" in url:
        print("[INFO] input cURL is tradeOrderDetialList; converting filter params to export params", flush=True)
        return detail_list_params_to_export_params(params)
    if "validateExcelExport" in url:
        params["isSyn"] = "false"
    return params


def force_standard_columns(params: dict[str, str]) -> dict[str, str]:
    out = dict(params)
    out["headersJson"] = json.dumps(
        {"enName": EXPORT_FIELDS, "showName": ORDERED_COLUMNS},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    condition = parse_json(out.get("conditionJson", ""), {})
    condition["cols"] = EXPORT_FIELDS
    out["conditionJson"] = json.dumps(condition, ensure_ascii=False, separators=(",", ":"))
    return out


def add_common_verify(params: dict[str, str]) -> dict[str, str]:
    out = dict(params)
    verify = out.get("commonVerify") or os.getenv(COMMON_VERIFY_ENV, "").strip() or COMMON_VERIFY_TEXT.strip()
    if verify:
        out["commonVerify"] = verify
    return out


def has_common_verify(params: dict[str, str]) -> bool:
    return bool(params.get("commonVerify") or os.getenv(COMMON_VERIFY_ENV, "").strip() or COMMON_VERIFY_TEXT.strip())


def set_export_time_window(params: dict[str, str], start_dt: datetime, end_dt: datetime) -> dict[str, str]:
    out = force_standard_columns(params)
    plaintext = has_common_verify(out)
    condition = parse_json(out.get("conditionJson", ""), {})
    filter_key = "filterOrderDetailDto" if "filterOrderDetailDto" in condition else "jsonStr"
    filter_data = condition.get(filter_key) or {}
    filter_data.pop("subTradeIds", None)
    filter_data.update(FULL_SYNC_FILTERS)
    filter_data["timeType"] = int(filter_data.get("timeType") or 0)
    filter_data["timeBegin"] = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    filter_data["timeEnd"] = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    condition[filter_key] = filter_data
    condition.setdefault("pageInfo", {})
    condition["pageInfo"]["pageIndex"] = 0
    condition["pageInfo"]["pageSize"] = MAX_EXPORT_ROWS
    condition["plaintext"] = 1 if plaintext else 0
    condition.setdefault("version", "2.0")
    out["conditionJson"] = json.dumps(condition, ensure_ascii=False, separators=(",", ":"))
    if plaintext:
        out["plaintext"] = "true"
    else:
        out.pop("plaintext", None)
        out.pop("commonVerify", None)
    out["isSyn"] = "false"
    return out


def post_export_endpoint(
        session: requests.Session,
        curl_info: dict[str, Any],
        endpoint: str,
        params: dict[str, str],
) -> dict[str, Any]:
    headers = web_headers(curl_info, "order_detail_list")
    body = signed_params(add_common_verify(params), headers["authorization"])
    return request_json(session, "POST", BASE_URL + endpoint, headers=headers, data=urlencode(body))


def validate_and_start_export(session: requests.Session, curl_info: dict[str, Any], params: dict[str, str]) -> str:
    validate = post_export_endpoint(
        session,
        curl_info,
        "/jkyun/excel-service/manager/validateExcelExport",
        params,
    )
    print(f"[INFO] validateExcelExport: {validate.get('msg', 'OK')}", flush=True)
    start = post_export_endpoint(
        session,
        curl_info,
        "/jkyun/excel-service/manager/startExcelExport",
        params,
    )
    task_id = start.get("result", {}).get("data") or start.get("data") or start.get("result")
    if isinstance(task_id, dict) and task_id.get("verifyType"):
        raise RuntimeError(
            "销售单明细账导出触发手机验证，当前 cURL 缺少可用 commonVerify。"
            "请在吉客云页面完成手机验证后，复制验证后的 startExcelExport cURL，"
            "粘到 curl/每日更新_curl.txt，然后运行 py -3 scripts\\sync_curl_auth.py。"
        )
    if not task_id:
        raise RuntimeError(f"Could not find task id in startExcelExport response: {start}")
    print(f"[INFO] export task id: {task_id}", flush=True)
    return str(task_id)


def find_download(task_payload: dict[str, Any], task_id: str) -> tuple[str, str, str] | None:
    rows = task_payload.get("result", {}).get("data") or task_payload.get("data") or []
    for row in rows:
        if task_id and str(row.get("taskId")) != str(task_id):
            continue
        for item in row.get("attachmentList") or []:
            url = item.get("attachmentUrl")
            if url:
                return url, item.get("attachmentName", ""), row.get("taskTitle", "")
    return None


def poll_task_download(
        session: requests.Session,
        curl_info: dict[str, Any],
        task_id: str,
        timeout_seconds: int,
        interval_seconds: int,
) -> tuple[str, str, str]:
    headers = web_headers(curl_info, "task_list", f"{BASE_URL}/system/taskList.html")
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        now = str(int(time.time() * 1000))
        params = signed_params(
            {"pageIndex": "0", "pageSize": "10", "timeStamp": now, "_": now},
            headers["authorization"],
            exclude={"_"},
        )
        url = BASE_URL + "/jkyun/tms/taskmanage/sysTaskInfoList?" + urlencode(params)
        payload = request_json(session, "GET", url, headers=headers)
        found = find_download(payload, task_id)
        if found:
            print("[INFO] export task completed", flush=True)
            return found
        print("[INFO] waiting for export task...", flush=True)
        time.sleep(interval_seconds)
    raise TimeoutError(f"Timed out waiting for task {task_id}")


def download_file(session: requests.Session, url: str, out_path: str) -> str:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    urls = [url]
    if url.startswith("http://"):
        urls.append(url.replace("http://", "https://", 1))
    elif url.startswith("https://"):
        urls.append(url.replace("https://", "http://", 1))

    last_error: Exception | None = None
    for candidate in urls:
        for attempt in range(1, 4):
            try:
                response = session.get(
                    candidate,
                    headers={"referer": BASE_URL + "/", "user-agent": "Mozilla/5.0"},
                    timeout=(30, 300),
                )
                response.raise_for_status()
                Path(out_path).write_bytes(response.content)
                with zipfile.ZipFile(out_path) as workbook_zip:
                    bad_member = workbook_zip.testzip()
                    if bad_member:
                        raise zipfile.BadZipFile(f"Bad member in xlsx: {bad_member}")
                print(f"[INFO] downloaded xlsx: {out_path} ({len(response.content)} bytes)", flush=True)
                return out_path
            except Exception as exc:
                last_error = exc
                print(f"[WARN] download failed attempt {attempt}/3: {exc}", flush=True)
                time.sleep(attempt * 3)
    raise RuntimeError(f"Download failed after retries: {last_error}")


def normalize_column_name(name: Any) -> str:
    text = "" if name is None else str(name)
    text = text.strip().replace("\n", "")
    return re.sub(r"\.\d+$", "", text)


def load_sales_excel(
        xlsx_path: str,
        window_start: datetime,
        window_end: datetime,
        task_id: str,
        update_time: datetime,
) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, dtype=object)
    df.columns = [normalize_column_name(col) for col in df.columns]
    df = df.dropna(how="all")
    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[ORDERED_COLUMNS].copy()
    df["updatetime"] = update_time.strftime("%Y-%m-%d %H:%M:%S")
    df["统计时间"] = window_start.strftime("%Y-%m-%d %H:%M:%S")

    for col in df.columns:
        if col in NUMERIC_COLUMNS_HINT:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "", regex=False), errors="coerce")
            if col in INTEGER_COLUMNS_HINT:
                df[col] = df[col].round(0)
            elif col in DECIMAL_COLUMNS_HINT:
                df[col] = df[col].round(2)
        elif col in DATETIME_COLUMNS_HINT:
            parsed = pd.to_datetime(df[col], errors="coerce")
            df[col] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S").where(parsed.notna(), pd.NA)
    df = df[FINAL_COLUMNS]
    try:
        from 销售品牌补全 import enrich_dataframe_from_brand_mapping

        df, _ = enrich_dataframe_from_brand_mapping(df)
    except Exception as exc:
        print(f"[WARN] 品牌补全步骤失败，保留导出原值：{exc}", flush=True)
    return df[FINAL_COLUMNS]


def mysql_type_for_column(column: str, series: pd.Series) -> str:
    if column in DATETIME_COLUMNS_HINT:
        return "DATETIME"
    if column in INTEGER_COLUMNS_HINT:
        return "DECIMAL(18,0)"
    if column in DECIMAL_COLUMNS_HINT or pd.api.types.is_numeric_dtype(series):
        return "DECIMAL(18,2)"
    if column in TEXT_COLUMNS_HINT or series.astype(str).str.len().max() > 255:
        return "TEXT"
    return "VARCHAR(255)"


def ensure_table(
        cursor,
        table_name: str,
        df: pd.DataFrame,
        window_start: datetime,
) -> None:
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    exists = cursor.fetchone() is not None
    if not exists:
        columns_sql = []
        for col in df.columns:
            columns_sql.append(f"`{col}` {mysql_type_for_column(col, df[col])}")
        indexes = []
        if "订单编号" in df.columns:
            indexes.append("INDEX `idx_订单编号` (`订单编号`(64))")
        if "下单时间" in df.columns:
            indexes.append("INDEX `idx_下单时间` (`下单时间`)")
        if "updatetime" in df.columns:
            indexes.append("INDEX `idx_updatetime` (`updatetime`)")
        if "统计时间" in df.columns:
            indexes.append("INDEX `idx_统计时间` (`统计时间`)")
        cursor.execute(f"""
            CREATE TABLE `{table_name}` (
                {", ".join(columns_sql + indexes)}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='销售单明细账网页导出'
        """)
        return

    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    existing_order = [row[0] for row in cursor.fetchall()]
    existing = set(existing_order)
    expected_order = list(df.columns)
    if existing_order != expected_order and "下单时间" in existing:
        cursor.execute(f"SELECT MIN(`下单时间`) FROM `{table_name}`")
        earliest_order_time = cursor.fetchone()[0]
        if earliest_order_time and window_start > earliest_order_time:
            raise RuntimeError(
                "销售单明细账字段结构已变化，不能只刷新部分历史数据。"
                f"请使用 --start {earliest_order_time:%Y-%m-%d} 从现表最早日期全量回补。"
            )
    stale_columns = [col for col in existing_order if col not in expected_order]
    missing_columns = [col for col in expected_order if col not in existing]
    schema_clauses = [f"DROP COLUMN `{col}`" for col in stale_columns]
    schema_clauses.extend(
        f"ADD COLUMN `{col}` {mysql_type_for_column(col, df[col])}"
        for col in missing_columns
    )
    if schema_clauses:
        cursor.execute(f"ALTER TABLE `{table_name}` {', '.join(schema_clauses)}")
        existing.difference_update(stale_columns)
        existing.update(missing_columns)
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    actual_order = [row[0] for row in cursor.fetchall()]
    if actual_order != expected_order:
        reorder_clauses = []
        for index, col in enumerate(expected_order):
            position = " FIRST" if index == 0 else f" AFTER `{expected_order[index - 1]}`"
            reorder_clauses.append(
                f"MODIFY COLUMN `{col}` {mysql_type_for_column(col, df[col])}{position}"
            )
        cursor.execute(f"ALTER TABLE `{table_name}` {', '.join(reorder_clauses)}")
        print(f"[INFO] reordered columns in {table_name} to match the export definition", flush=True)
    cursor.execute(f"SHOW INDEX FROM `{table_name}`")
    existing_indexes = {row[2] for row in cursor.fetchall()}
    if "订单编号" in df.columns and "idx_订单编号" not in existing_indexes:
        cursor.execute(f"ALTER TABLE `{table_name}` ADD INDEX `idx_订单编号` (`订单编号`(64))")
    if "下单时间" in df.columns and "idx_下单时间" not in existing_indexes:
        cursor.execute(f"ALTER TABLE `{table_name}` ADD INDEX `idx_下单时间` (`下单时间`)")
    if "updatetime" in df.columns and "idx_updatetime" not in existing_indexes:
        cursor.execute(f"ALTER TABLE `{table_name}` ADD INDEX `idx_updatetime` (`updatetime`)")
    if "统计时间" in df.columns and "idx_统计时间" not in existing_indexes:
        cursor.execute(f"ALTER TABLE `{table_name}` ADD INDEX `idx_统计时间` (`统计时间`)")


def clean_for_mysql(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()
    for col in df_clean.columns:
        series = df_clean[col]
        if pd.api.types.is_numeric_dtype(series):
            df_clean[col] = series.where(pd.notna(series), "\\N")
        else:
            df_clean[col] = series.fillna("\\N").astype(str)
            df_clean.loc[df_clean[col].isin(["", "nan", "None", "NaT", "<NA>"]), col] = "\\N"
    return df_clean


def connect_db() -> Any:
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            return pymysql.connect(**DB_CONFIG)
        except pymysql.MySQLError:
            if attempt >= CONNECT_RETRIES:
                raise
            print(
                f"[WARN] MySQL connection failed; retrying in "
                f"{CONNECT_RETRY_DELAY_SECONDS}s ({attempt}/{CONNECT_RETRIES})",
                flush=True,
            )
            time.sleep(CONNECT_RETRY_DELAY_SECONDS)
    raise RuntimeError("unreachable")


def write_window_to_mysql(
        df: pd.DataFrame,
        table_name: str,
        window_start: datetime,
        window_end: datetime,
        task_id: str,
        file_name: str,
) -> None:
    tmp_table = f"{table_name}_stage_{os.getpid()}"
    tmp_file = os.path.join(tempfile.gettempdir(), f"{tmp_table}.csv")
    conn = None
    cursor = None
    try:
        df_clean = clean_for_mysql(df)
        df_clean.to_csv(tmp_file, index=False, header=False, encoding="utf-8")
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute("SET GLOBAL local_infile = 1")
        ensure_table(cursor, table_name, df_clean, window_start)
        cursor.execute(f"DROP TABLE IF EXISTS `{tmp_table}`")
        cursor.execute(f"CREATE TABLE `{tmp_table}` LIKE `{table_name}`")

        columns = ", ".join(f"`{col}`" for col in df_clean.columns)
        tmp_path = tmp_file.replace("\\", "/")
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{tmp_path}'
            INTO TABLE `{tmp_table}`
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            ({columns})
        """)
        loaded = cursor.rowcount
        if "下单时间" in df_clean.columns:
            start_str = window_start.strftime("%Y-%m-%d %H:%M:%S")
            end_str = window_end.strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(
                f"DELETE FROM `{table_name}` WHERE `下单时间` >= %s AND `下单时间` < %s",
                (start_str, end_str),
            )
        elif "订单编号" in df_clean.columns:
            cursor.execute(f"""
                DELETE target FROM `{table_name}` AS target
                INNER JOIN (
                    SELECT DISTINCT `订单编号`
                    FROM `{tmp_table}`
                    WHERE `订单编号` IS NOT NULL AND `订单编号` <> '\\N'
                ) AS source ON target.`订单编号` = source.`订单编号`
            """)
        else:
            raise RuntimeError("Cannot replace rows safely because neither `下单时间` nor `订单编号` exists.")
        deleted = cursor.rowcount
        cursor.execute(f"INSERT INTO `{table_name}` ({columns}) SELECT {columns} FROM `{tmp_table}`")
        cursor.execute(f"DROP TABLE IF EXISTS `{tmp_table}`")
        conn.commit()
        print(
            f"[INFO] deleted {deleted} old rows, imported {loaded} rows into "
            f"{DB_CONFIG['database']}.{table_name}",
            flush=True,
        )
    except Exception as exc:
        if conn:
            conn.rollback()
        raise exc
    finally:
        if cursor:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS `{tmp_table}`")
                if conn:
                    conn.commit()
            except Exception:
                pass
            cursor.close()
        if conn:
            conn.close()
        try:
            os.remove(tmp_file)
        except OSError:
            pass


def split_windows(start_dt: datetime, end_dt: datetime, window_hours: int) -> list[tuple[datetime, datetime]]:
    windows = []
    current = start_dt
    step = timedelta(hours=window_hours)
    while current < end_dt:
        nxt = min(current + step, end_dt)
        windows.append((current, nxt))
        current = nxt
    return windows


def safe_file_stamp(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def process_window(
        session: requests.Session,
        curl_info: dict[str, Any],
        base_params: dict[str, str],
        window_start: datetime,
        window_end: datetime,
        args: argparse.Namespace,
) -> int:
    print(f"[WINDOW] {window_start} ~ {window_end}", flush=True)
    update_time = datetime.now().replace(microsecond=0)
    params = set_export_time_window(base_params, window_start, window_end)
    task_id = validate_and_start_export(session, curl_info, params)
    url, attachment_name, task_title = poll_task_download(session, curl_info, task_id, args.timeout, args.interval)
    if task_title:
        print(f"[INFO] task title: {task_title}", flush=True)
    os.makedirs(args.xlsx_dir, exist_ok=True)
    out_xlsx = os.path.join(
        args.xlsx_dir,
        f"销售单明细账_{safe_file_stamp(window_start)}_{safe_file_stamp(window_end)}_{task_id}.xlsx",
    )
    xlsx_path = download_file(session, url, out_xlsx)
    df = load_sales_excel(xlsx_path, window_start, window_end, task_id, update_time)
    rows = len(df)
    print(f"[INFO] normalized rows: {rows}", flush=True)

    duration_hours = (window_end - window_start).total_seconds() / 3600
    if rows >= args.split_threshold and duration_hours > args.min_window_hours:
        print(
            f"[WARN] row count {rows} reached split threshold {args.split_threshold}; "
            "splitting this window",
            flush=True,
        )
        mid = window_start + (window_end - window_start) / 2
        return (
                process_window(session, curl_info, base_params, window_start, mid, args)
                + process_window(session, curl_info, base_params, mid, window_end, args)
        )

    if args.csv:
        os.makedirs(os.path.dirname(args.csv), exist_ok=True)
        mode = "a" if os.path.exists(args.csv) else "w"
        df.to_csv(args.csv, index=False, encoding="utf-8-sig", mode=mode, header=(mode == "w"))
    if not args.no_db:
        write_window_to_mysql(df, args.table, window_start, window_end, task_id, os.path.basename(xlsx_path))
    return rows


def parse_datetime(value: str, end_of_day: bool = False) -> datetime:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        suffix = "23:59:59" if end_of_day else "00:00:00"
        value = f"{value} {suffix}"
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export filtered Jike sales order details and sync them into MySQL.")
    parser.add_argument("--curl",
                        help="sales-order-detail startExcelExport/tradeOrderDetialList Copy-as-cURL text file")
    parser.add_argument("--table", default=TABLE_NAME, help="target MySQL table")
    parser.add_argument("--start", help="window start, YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end", help="window end, YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                        help="optional rolling window; default is the latest 30 days")
    parser.add_argument("--window-hours", type=int, default=DEFAULT_WINDOW_HOURS)
    parser.add_argument("--min-window-hours", type=int, default=MIN_WINDOW_HOURS)
    parser.add_argument("--max-rows", type=int, default=MAX_EXPORT_ROWS)
    parser.add_argument("--split-threshold", type=int, default=SPLIT_ROW_THRESHOLD,
                        help="split a completed export window when row count reaches this threshold")
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--interval", type=int, default=10)
    parser.add_argument("--xlsx-dir", default=DEFAULT_XLSX_DIR)
    parser.add_argument("--csv", default="", help="optional cumulative csv output path")
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.start:
        start_dt = parse_datetime(args.start)
    elif args.lookback_days is not None:
        default_end_dt = datetime.now().replace(microsecond=0)
        start_dt = default_end_dt - timedelta(days=args.lookback_days)
    else:
        today = date.today()
        start_dt = datetime(today.year, today.month, 1, 0, 0, 0)
    if args.end:
        end_dt = parse_datetime(args.end, end_of_day=True)
    elif args.start:
        today = date.today()
        end_dt = datetime(today.year, today.month, today.day, 23, 59, 59)
    elif args.lookback_days is not None:
        end_dt = datetime.now().replace(microsecond=0)
    else:
        today = date.today()
        end_dt = datetime(today.year, today.month, today.day, 23, 59, 59)
    if end_dt <= start_dt:
        raise ValueError("--end must be later than --start")

    curl_info = load_curl_info(args.curl)
    base_params = export_params_from_curl(curl_info)
    windows = split_windows(start_dt, end_dt, args.window_hours)
    print(f"[INFO] windows: {len(windows)}", flush=True)
    total_rows = 0
    with requests.Session() as session:
        for window_start, window_end in windows:
            total_rows += process_window(session, curl_info, base_params, window_start, window_end, args)
    print(f"[DONE] total rows: {total_rows}", flush=True)


if __name__ == "__main__":
    main()
