"""
Sync Jike Cloud sales orders via the web export task flow.

This script is designed for DolphinScheduler:
- Put a recent sales-order startExcelExport cURL into JKY_SALES_ORDER_CURL, or
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

TABLE_NAME = "销售单查询"
START_EXPORT_CURL_ENV = "JKY_SALES_ORDER_CURL"
COMMON_VERIFY_ENV = "JKY_SALES_ORDER_COMMON_VERIFY"

# ============================================================
# Paste cURL Here
# ============================================================
# 本地 PyCharm 调试时，把销售单查询的 startExcelExport 完整 cURL 粘到这里。
# DolphinScheduler 上建议用环境变量 JKY_SALES_ORDER_CURL 覆盖，避免 token/cookie 写死。
START_EXPORT_CURL_TEXT = "".strip()
COMMON_VERIFY_TEXT = ""

DEFAULT_XLSX_DIR = os.path.join(DATA_DIR, "销售单查询_web_exports")
DEFAULT_CSV = os.path.join(DATA_DIR, "销售单查询_web.csv")

MAX_EXPORT_ROWS = 500000
DEFAULT_LOOKBACK_DAYS = 40
DEFAULT_WINDOW_HOURS = 24 * 30
MIN_WINDOW_HOURS = 1
CONNECT_RETRIES = 5
CONNECT_RETRY_DELAY_SECONDS = 10

ORDERED_COLUMNS = [
    "标记", "订单编号", "订单状态", "结算状态", "销售渠道", "处理时间",
    "付款时间", "发货仓库", "物流公司", "物流单号", "网店订单号",
    "发货时间", "订单类型", "应收合计", "货品数量", "货品摘要",
    "合并备注", "下单时间", "渠道分类", "实付金额", "市", "仓库编码（平台）",
    "客户编号", "客户账号", "客户名称",
]
EXPORT_FIELDS = [
    "flagIds", "tradeNo", "tradeStatusExplain", "settleStatusExplain",
    "shopName", "handleTime", "payTime", "warehouseName", "logisticName",
    "mainPostid", "sourceTradeNo", "consignTime", "tradeTypeExplain",
    "payment", "tradeCount", "goodslist", "mergeRemarks", "tradeTime",
    "shopCateName", "realFee", "city", "platWarehouseCode", "customerCode",
    "customerAccount", "customerName",
]
EXPORT_CONDITION_FIELDS = [
    "tradeOrder.flagIds", "tradeOrder.tradeNo", "tradeOrder.tradeStatusExplain",
    "tradeOrder.settleStatusExplain", "tradeOrder.shopName", "tradeOrder.handleTime",
    "tradeOrder.payTime", "tradeOrder.warehouseName", "tradeOrder.logisticName",
    "tradeOrder.mainPostid", "sourceTradeNo", "tradeOrder.consignTime",
    "tradeOrder.tradeTypeExplain", "tradeOrder.payment", "tradeOrder.tradeCount",
    "tradeOrder.goodslist", "mergeRemarks", "tradeOrder.tradeTime",
    "shopCateName", "realFee", "tradeOrderReceiver.city", "platWarehouseCode",
    "tradeOrderCustomer.customerCode", "tradeOrderCustomer.customerAccount",
    "tradeOrderCustomer.customerName",
]
FINAL_COLUMNS = ORDERED_COLUMNS + ["updatetime"]
EXTRA_COLUMNS = ["updatetime"]
TEXT_COLUMNS_HINT = {
    "标记", "订单编号", "订单状态", "结算状态", "销售渠道", "发货仓库",
    "物流公司", "物流单号", "网店订单号", "订单类型", "货品摘要",
    "合并备注", "渠道分类", "市", "仓库编码（平台）", "客户编号",
    "客户账号", "客户名称",
}
DATETIME_COLUMNS_HINT = {"处理时间", "付款时间", "发货时间", "下单时间", "updatetime"}
INTEGER_COLUMNS_HINT = {"货品数量"}
DECIMAL_COLUMNS_HINT = {"应收合计", "实付金额"}
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
               ("startExcelExport", "validateExcelExport", "oms-flow/trade/queryIdList", "oms-flow/trade/queryList")):
        raise ValueError(
            "Please provide a sales-order startExcelExport, validateExcelExport, queryIdList, or queryList cURL")

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
    if START_EXPORT_CURL_TEXT.strip():
        print("[INFO] using cURL from START_EXPORT_CURL_TEXT", flush=True)
        return parse_curl_text(START_EXPORT_CURL_TEXT)
    raise FileNotFoundError(
        "No cURL configured. Set JKY_SALES_ORDER_CURL, paste START_EXPORT_CURL_TEXT, or pass --curl."
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


def query_id_list_params_to_export_params(params: dict[str, str]) -> dict[str, str]:
    json_str = parse_json(params.get("jsonStr", ""), {})
    # queryList carries the 50 ids of the current page. Export must keep the
    # filter criteria, not this page-only id list.
    json_str.pop("tradeIds", None)
    has_verify = bool(params.get("commonVerify"))
    condition = {
        "pageInfo": {
            "pageIndex": int(params.get("pageIndex") or 0),
            "pageSize": MAX_EXPORT_ROWS,
            "sortField": params.get("sortField", ""),
            "sortOrder": params.get("sortOrder", ""),
        },
        "jsonStr": json_str,
        "cols": EXPORT_CONDITION_FIELDS,
        "plaintext": 1 if has_verify else 0,
        "version": "2.0",
    }
    out = {
        "serverName": "oms/oms/excel",
        "excelType": "2",
        "headersJson": json.dumps({"enName": EXPORT_FIELDS, "showName": ORDERED_COLUMNS}, ensure_ascii=False,
                                  separators=(",", ":")),
        "conditionJson": json.dumps(condition, ensure_ascii=False, separators=(",", ":")),
        "datasource": "",
        "isMerge": "true",
        "typeName": "销售单查询",
        "multiSheet": "false",
        "exportTotal": "",
        "isSyn": "false",
    }
    if params.get("commonVerify"):
        out["commonVerify"] = params["commonVerify"]
        out["plaintext"] = "true"
    else:
        print(
            "[WARN] queryList/queryIdList cURL has no commonVerify; "
            "using non-plaintext export to avoid phone verification.",
            flush=True,
        )
    return out


def export_params_from_curl(curl_info: dict[str, Any]) -> dict[str, str]:
    url = curl_info["url"]
    params = dict(curl_info["params"])
    if "oms-flow/trade/queryIdList" in url or "oms-flow/trade/queryList" in url:
        print("[INFO] input cURL is queryList/queryIdList; converting filter params to export params", flush=True)
        return query_id_list_params_to_export_params(params)
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
    condition["cols"] = EXPORT_CONDITION_FIELDS
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
    json_str = condition.get("jsonStr") or {}
    json_str.pop("tradeIds", None)
    json_str["timeType"] = int(json_str.get("timeType") or 0)
    json_str["timeBegin"] = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    json_str["timeEnd"] = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    condition["jsonStr"] = json_str
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
    headers = web_headers(curl_info, "order_queryv2")
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
            "销售单导出触发手机验证，当前 cURL 缺少可用 commonVerify。"
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


def ensure_table(cursor, table_name: str, df: pd.DataFrame) -> None:
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    exists = cursor.fetchone() is not None
    if not exists:
        columns_sql = []
        for col in df.columns:
            columns_sql.append(f"`{col}` {mysql_type_for_column(col, df[col])}")
        indexes = [
            "INDEX `idx_订单编号` (`订单编号`(64))",
            "INDEX `idx_下单时间` (`下单时间`)",
            "INDEX `idx_updatetime` (`updatetime`)",
        ]
        cursor.execute(f"""
            CREATE TABLE `{table_name}` (
                {", ".join(columns_sql + indexes)}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='销售单查询网页导出'
        """)
        return

    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    existing = {row[0] for row in cursor.fetchall()}
    for col in list(existing):
        if col not in df.columns:
            cursor.execute(f"ALTER TABLE `{table_name}` DROP COLUMN `{col}`")
            existing.remove(col)
    for index, col in enumerate(df.columns):
        if col not in existing:
            position = " FIRST" if index == 0 else f" AFTER `{df.columns[index - 1]}`"
            cursor.execute(
                f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` "
                f"{mysql_type_for_column(col, df[col])}{position}"
            )
            existing.add(col)

    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    actual_order = [row[0] for row in cursor.fetchall()]
    expected_order = list(df.columns)
    if actual_order != expected_order:
        reorder_clauses = []
        for index, col in enumerate(expected_order):
            position = " FIRST" if index == 0 else f" AFTER `{expected_order[index - 1]}`"
            reorder_clauses.append(
                f"MODIFY COLUMN `{col}` {mysql_type_for_column(col, df[col])}{position}"
            )
        cursor.execute(f"ALTER TABLE `{table_name}` {', '.join(reorder_clauses)}")
        print(f"[INFO] reordered columns in {table_name} to match the export definition", flush=True)


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
        ensure_table(cursor, table_name, df_clean)
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
        start_str = window_start.strftime("%Y-%m-%d %H:%M:%S")
        end_str = window_end.strftime("%Y-%m-%d %H:%M:%S")
        if "下单时间" not in df_clean.columns:
            raise RuntimeError("Cannot replace window safely because column `下单时间` is missing.")
        cursor.execute(
            f"DELETE FROM `{table_name}` WHERE `下单时间` >= %s AND `下单时间` < %s",
            (start_str, end_str),
        )
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
        f"销售单查询_{safe_file_stamp(window_start)}_{safe_file_stamp(window_end)}_{task_id}.xlsx",
    )
    xlsx_path = download_file(session, url, out_xlsx)
    df = load_sales_excel(xlsx_path, window_start, window_end, task_id, update_time)
    rows = len(df)
    print(f"[INFO] normalized rows: {rows}", flush=True)

    duration_hours = (window_end - window_start).total_seconds() / 3600
    if rows >= args.max_rows and duration_hours > args.min_window_hours:
        print("[WARN] row count reached export cap; splitting this window", flush=True)
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
    parser = argparse.ArgumentParser(description="Export filtered Jike sales orders and sync them into MySQL.")
    parser.add_argument("--curl", help="sales-order startExcelExport/queryIdList Copy-as-cURL text file")
    parser.add_argument("--table", default=TABLE_NAME, help="target MySQL table")
    parser.add_argument("--start", help="window start, YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end", help="window end, YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"days to refresh when --start is omitted (default: {DEFAULT_LOOKBACK_DAYS})",
    )
    parser.add_argument("--window-hours", type=int, default=DEFAULT_WINDOW_HOURS)
    parser.add_argument("--min-window-hours", type=int, default=MIN_WINDOW_HOURS)
    parser.add_argument("--max-rows", type=int, default=MAX_EXPORT_ROWS)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--interval", type=int, default=10)
    parser.add_argument("--xlsx-dir", default=DEFAULT_XLSX_DIR)
    parser.add_argument("--csv", default="", help="optional cumulative csv output path")
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.lookback_days <= 0:
        raise ValueError("--lookback-days must be greater than 0")
    if args.start:
        start_dt = parse_datetime(args.start)
    else:
        default_end_dt = datetime.now().replace(microsecond=0)
        start_dt = default_end_dt - timedelta(days=args.lookback_days)
    if args.end:
        end_dt = parse_datetime(args.end, end_of_day=True)
    else:
        end_dt = datetime.now().replace(microsecond=0)
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
