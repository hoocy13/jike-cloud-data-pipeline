"""
Sync Jike Cloud branch warehouse stock from the web export flow.

Input is a DevTools "Copy as cURL" request for startExcelExport after the
page filters have been set. The script keeps conditionJson from that cURL, so
the exported rows match the filtered page, then imports the downloaded xlsx
into MySQL.
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
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pymysql

from config import DATA_DIR, DB_CONFIG

BASE_URL = "https://env3.jkyservice.com"
WEB_APP_KEY = "jackyun_web_browser_2024"
WEB_SIGN_SECRET = os.getenv("JKY_WEB_SIGN_SECRET", "")
TABLE_NAME = "分仓库查询"

# ============================================================
# Paste cURL Here
# ============================================================
# 粘贴 DevTools 复制的完整 cURL。推荐粘 stockSkuList，也兼容 startExcelExport。
# DolphinScheduler 上更推荐配置环境变量 JKY_WAREHOUSE_STOCK_CURL 覆盖这里，
# 避免把登录 token/cookie 固定在资源文件里。
START_EXPORT_CURL_TEXT = "".strip()

START_EXPORT_CURL_ENV = "JKY_WAREHOUSE_STOCK_CURL"
DEFAULT_CURL = os.path.join(DATA_DIR, "warehouse_stock_startExcelExport_curl.txt")
DEFAULT_XLSX = os.path.join(DATA_DIR, "分仓库查询_web.xlsx")
DEFAULT_CSV = os.path.join(DATA_DIR, "分仓库查询_web.csv")

FIELD_MAP = {
    "warehouseName": "仓库",
    "goodsNo": "货品编号",
    "goodsName": "货品名称",
    "brandName": "品牌",
    "skuName": "规格",
    "skuBarcode": "条码",
    "cateName": "货品分类",
    "shelfLifeStr": "货品保质期",
    "price1": "零售价",
    "price6": "含税价",
    "price7": "不含税价",
    "currentQuantity": "库存数量",
    "canUseQuantity": "可用库存",
    "inQuantitySum": "今日入库",
    "yesterdayQuantity": "昨天销量",
    "weekQuantity": "近7天销量",
    "threedayQuantity": "近30天销量",
    "lastStockInTime": "最近入库时间",
    "costValue": "库存金额",
    "costPrice": "当前成本价",
    "distrubuteQuantity": "渠道预留",
    "purchasingQuantity": "采购在途",
    "allocateQuantity": "调拨在途",
    "salesReturnQuantity": "退货在途",
    "field5": "近90天销量(库存公式)",
    "outQuantitySum": "今日出库",
    "stockInQuantity": "入库申请",
    "stockOutQuantity": "出库申请",
    "waitLaunchStock": "待上架库存",
    "waitQualityTestQuantity": "待质检量",
    "productWaitQuantity": "生产待下达",
    "defectiveQuanity": "残次品",
}

EXPORT_FIELDS = list(FIELD_MAP.keys())
EXPORT_DISPLAY_MAP = dict(FIELD_MAP)
DEFAULT_EXPORT_HEADER_FIELDS = [
    "warehouseName",
    "goodsNo",
    "goodsName",
    "brandName",
    "skuName",
    "skuBarcode",
    "cateName",
    "shelfLifeStr",
    "price1",
    "price6",
    "price7",
    "currentQuantity",
    "canUseQuantity",
    "inQuantitySum",
    "yesterdayQuantity",
    "weekQuantity",
    "threedayQuantity",
    "lastStockInTime",
    "costValue",
    "costPrice",
    "distrubuteQuantity",
    "purchasingQuantity",
    "allocateQuantity",
    "salesReturnQuantity",
    "field5",
    "outQuantitySum",
    "stockInQuantity",
    "stockOutQuantity",
    "waitQualityTestQuantity",
    "productWaitQuantity",
    "defectiveQuanity",
]
FINAL_COLUMNS = list(FIELD_MAP.values()) + ["updatetime"]
QUANTITY_COLUMNS = [
    "库存数量",
    "可用库存",
    "今日入库",
    "昨天销量",
    "近7天销量",
    "近30天销量",
    "渠道预留",
    "采购在途",
    "调拨在途",
    "退货在途",
    "近90天销量(库存公式)",
    "今日出库",
    "入库申请",
    "出库申请",
    "待上架库存",
    "待质检量",
    "生产待下达",
    "残次品",
]
MONEY_COLUMNS = [
    "零售价",
    "含税价",
    "不含税价",
    "库存金额",
    "当前成本价",
]
ID_COLUMNS: list[str] = []
NUMERIC_COLUMNS = QUANTITY_COLUMNS + MONEY_COLUMNS + ID_COLUMNS


def normalize_curl_text(text: str) -> str:
    return text.replace("^\r\n", " ").replace("^\n", " ").replace("^", "")


def parse_curl_text(raw: str) -> dict[str, Any]:
    tokens = shlex.split(normalize_curl_text(raw), posix=True)
    if not tokens or tokens[0].lower() != "curl":
        raise ValueError("The input file does not look like a curl command")

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

    supported = ("startExcelExport", "validateExcelExport", "warehouseStock/stockSkuList")
    if not any(item in url for item in supported):
        raise ValueError(
            "Please provide a cURL for startExcelExport, validateExcelExport, or warehouseStock/stockSkuList"
        )
    if not data_raw:
        raise ValueError("Could not find --data-raw in the cURL")

    params = dict(parse_qsl(data_raw, keep_blank_values=True))
    return {"url": url, "headers": headers, "cookie": cookie, "params": params}


def parse_curl_file(curl_path: str) -> dict[str, Any]:
    raw = Path(curl_path).read_text(encoding="utf-8-sig")
    return parse_curl_text(raw)


def signed_params(params: dict[str, Any], authorization: str, exclude: set[str] | None = None) -> dict[str, str]:
    out = {key: "" if value is None else str(value) for key, value in params.items()}
    out["timestamp"] = str(int(time.time() * 1000))
    out["access_token"] = authorization
    out["appkey"] = WEB_APP_KEY
    out.pop("sign", None)

    excluded = exclude or set()
    sign_items = [
        (key, value)
        for key, value in out.items()
        if key not in excluded and value != ""
    ]
    sign_items.sort(key=lambda item: item[0])
    payload = "".join(key + value for key, value in sign_items)
    out["sign"] = hashlib.md5(
        (WEB_SIGN_SECRET + payload + WEB_SIGN_SECRET).encode("utf-8")
    ).hexdigest().upper()
    return out


def export_headers(curl_info: dict[str, Any], module_code: str, referer: str | None = None) -> dict[str, str]:
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
    if curl_info.get("cookie"):
        headers["cookie"] = curl_info["cookie"]
    return headers


def force_export_columns(params: dict[str, str]) -> dict[str, str]:
    out = dict(params)
    headers_json = {
        "enName": EXPORT_FIELDS,
        "showName": [FIELD_MAP[field] for field in EXPORT_FIELDS],
    }
    out["headersJson"] = json.dumps(headers_json, ensure_ascii=False, separators=(",", ":"))

    condition = json.loads(out.get("conditionJson", "{}"))
    condition["cols"] = EXPORT_FIELDS
    out["conditionJson"] = json.dumps(condition, ensure_ascii=False, separators=(",", ":"))
    return out


def build_headers_json(fields: list[str]) -> str:
    headers_json = {
        "enName": fields,
        "showName": [EXPORT_DISPLAY_MAP[field] for field in fields],
    }
    return json.dumps(headers_json, ensure_ascii=False, separators=(",", ":"))


def parse_json_param(value: str, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def stock_list_params_to_export_params(params: dict[str, str]) -> dict[str, str]:
    auth_keys = {"timestamp", "access_token", "appkey", "sign"}
    condition: dict[str, Any] = {
        key: value
        for key, value in params.items()
        if key not in auth_keys
    }

    condition["pageIndex"] = int(condition.get("pageIndex") or 0)
    condition["pageSize"] = int(condition.get("pageSize") or 50)
    condition["cols"] = EXPORT_FIELDS
    condition.setdefault("outType", 1)
    condition.setdefault("useCostCalOrganization", 1)
    condition.setdefault("ids", [])
    condition.setdefault("version", "2.0")

    return {
        "serverName": "erp-stock/erp-stock/export",
        "excelType": "warehouse.stock.sku.export",
        "headersJson": build_headers_json(DEFAULT_EXPORT_HEADER_FIELDS),
        "conditionJson": json.dumps(condition, ensure_ascii=False, separators=(",", ":")),
        "datasource": "",
        "typeName": "分仓库存查询",
        "multiSheet": "false",
        "exportTotal": "",
        "isSyn": "false",
    }


def export_params_from_curl(curl_info: dict[str, Any]) -> dict[str, str]:
    url = curl_info["url"]
    params = dict(curl_info["params"])
    if "warehouseStock/stockSkuList" in url:
        print("[INFO] input cURL is stockSkuList; converting filter params to export params", flush=True)
        return stock_list_params_to_export_params(params)
    return params


def request_json(session: requests.Session, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    response = session.request(method, url, timeout=60, **kwargs)
    text = response.text
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Non-JSON response from {url}: {text[:500]}") from exc
    if response.status_code >= 400 or data.get("code") not in (None, 200):
        raise RuntimeError(f"Request failed {response.status_code}: {text[:800]}")
    return data


def list_headers(curl_info: dict[str, Any]) -> dict[str, str]:
    source = curl_info["headers"]
    authorization = source.get("authorization")
    if not authorization:
        raise ValueError("Missing authorization header in cURL")

    headers = {
        "accept": source.get("accept", "text/plain, */*; q=0.01"),
        "accept-language": source.get("accept-language", "zh-CN,zh;q=0.9"),
        "authorization": authorization,
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "module_code": source.get("module_code", "branch_stock"),
        "origin": BASE_URL,
        "referer": source.get("referer", f"{BASE_URL}/"),
        "user-agent": source.get("user-agent", "Mozilla/5.0"),
        "x-requested-with": "XMLHttpRequest",
    }
    if source.get("ati"):
        headers["ati"] = source["ati"]
    if curl_info.get("cookie"):
        headers["cookie"] = curl_info["cookie"]
    return headers


def request_list_json(session: requests.Session, curl_info: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    headers = list_headers(curl_info)
    body = signed_params(params, headers["authorization"])
    return request_json(session, "POST", curl_info["url"], headers=headers, data=urlencode(body))


def find_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    result = payload.get("result") or {}
    candidates = [result.get("data"), payload.get("data"), result]
    for candidate in candidates:
        if isinstance(candidate, list):
            return candidate
        if isinstance(candidate, dict):
            rows = (
                candidate.get("data")
                or candidate.get("list")
                or candidate.get("rows")
                or candidate.get("records")
                or candidate.get("stockSkuList")
            )
            if isinstance(rows, list):
                return rows
    raise RuntimeError(f"Cannot locate row list in response: {str(payload)[:1000]}")


def fetch_all_stock_rows(curl_info: dict[str, Any], page_size: int) -> list[dict[str, Any]]:
    if "warehouseStock/stockSkuList" not in curl_info["url"]:
        raise ValueError("Direct paging requires a stockSkuList cURL")

    base_params = dict(curl_info["params"])
    base_params["pageSize"] = str(page_size)
    base_params["cols"] = json.dumps(EXPORT_FIELDS, ensure_ascii=False, separators=(",", ":"))
    base_params.setdefault("serviceType", "stock.search.v2")

    rows_all: list[dict[str, Any]] = []
    with requests.Session() as session:
        page_index = 0
        while True:
            params = dict(base_params)
            params["pageIndex"] = str(page_index)
            payload = request_list_json(session, curl_info, params)
            rows = find_rows(payload)
            print(f"[INFO] page {page_index}: {len(rows)} rows", flush=True)
            rows_all.extend(rows)
            if not rows or len(rows) < page_size:
                break
            page_index += 1
    return rows_all


def post_export_endpoint(
        session: requests.Session,
        curl_info: dict[str, Any],
        endpoint: str,
        params: dict[str, str],
) -> dict[str, Any]:
    headers = export_headers(curl_info, "branch_stock")
    body = signed_params(params, headers["authorization"])
    return request_json(session, "POST", BASE_URL + endpoint, headers=headers, data=urlencode(body))


def validate_and_start_export(
        session: requests.Session,
        curl_info: dict[str, Any],
        force_standard_columns: bool,
) -> str:
    params = export_params_from_curl(curl_info)
    if force_standard_columns:
        params = force_export_columns(params)

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
    task_id = (
            start.get("result", {}).get("data")
            or start.get("data")
            or start.get("result")
    )
    if not task_id:
        raise RuntimeError(f"Could not find task id in startExcelExport response: {start}")
    print(f"[INFO] export task id: {task_id}", flush=True)
    return str(task_id)


def find_download(task_payload: dict[str, Any], task_id: str) -> tuple[str, str] | None:
    rows = task_payload.get("result", {}).get("data") or task_payload.get("data") or []
    for row in rows:
        if task_id and str(row.get("taskId")) != str(task_id):
            continue
        for item in row.get("attachmentList") or []:
            url = item.get("attachmentUrl")
            if url:
                return url.replace("http://", "https://"), item.get("attachmentName", "")
    return None


def poll_task_download(
        session: requests.Session,
        curl_info: dict[str, Any],
        task_id: str,
        timeout_seconds: int,
        interval_seconds: int,
) -> tuple[str, str]:
    headers = export_headers(curl_info, "task_list", f"{BASE_URL}/system/taskList.html")
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        now = str(int(time.time() * 1000))
        params = signed_params(
            {
                "pageIndex": "0",
                "pageSize": "10",
                "timeStamp": now,
                "_": now,
            },
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
    response = session.get(
        url,
        headers={"referer": BASE_URL + "/", "user-agent": "Mozilla/5.0"},
        timeout=180,
    )
    response.raise_for_status()
    Path(out_path).write_bytes(response.content)
    print(f"[INFO] downloaded xlsx: {out_path} ({len(response.content)} bytes)", flush=True)
    return out_path


def normalize_column_name(name: Any) -> str:
    text = "" if name is None else str(name)
    text = text.strip().replace("\n", "")
    return re.sub(r"\.\d+$", "", text)


def load_stock_excel(xlsx_path: str, update_time: datetime) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, dtype=object)
    df.columns = [normalize_column_name(col) for col in df.columns]
    df = df.rename(columns=FIELD_MAP)

    for noisy_col in ("序号", "行号", ""):
        if noisy_col in df.columns:
            df = df.drop(columns=[noisy_col])

    for col in FIELD_MAP.values():
        if col not in df.columns:
            df[col] = pd.NA

    df = df[list(FIELD_MAP.values())].copy()
    df = df.dropna(how="all", subset=["仓库", "货品编号", "货品名称", "条码"])

    for col in NUMERIC_COLUMNS:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")
        if col in QUANTITY_COLUMNS:
            df[col] = df[col].round(0)
        elif col in MONEY_COLUMNS:
            df[col] = df[col].round(2)

    for col in [
        "仓库",
        "货品编号",
        "货品名称",
        "品牌",
        "规格",
        "条码",
        "货品分类",
        "货品保质期",
        "最近入库时间",
    ]:
        df[col] = df[col].where(pd.notna(df[col]), pd.NA)

    df["updatetime"] = update_time.strftime("%Y-%m-%d %H:%M:%S")
    return df[FINAL_COLUMNS]


def normalize_stock_rows(rows: list[dict[str, Any]], update_time: datetime) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for source_col in EXPORT_FIELDS:
        if source_col not in df.columns:
            df[source_col] = pd.NA

    df = df[EXPORT_FIELDS].rename(columns=FIELD_MAP).copy()
    df = df.dropna(how="all", subset=["仓库", "货品编号", "货品名称", "条码"])

    for col in NUMERIC_COLUMNS:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")
        if col in QUANTITY_COLUMNS:
            df[col] = df[col].round(0)
        elif col in MONEY_COLUMNS:
            df[col] = df[col].round(2)

    for col in [
        "仓库",
        "货品编号",
        "货品名称",
        "品牌",
        "规格",
        "条码",
        "货品分类",
        "货品保质期",
        "最近入库时间",
    ]:
        df[col] = df[col].where(pd.notna(df[col]), pd.NA)

    df["updatetime"] = update_time.strftime("%Y-%m-%d %H:%M:%S")
    return df[FINAL_COLUMNS]


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


def mysql_type_for_column(column: str) -> str:
    if column == "updatetime":
        return "DATETIME"
    if column in ID_COLUMNS:
        return "BIGINT"
    if column in QUANTITY_COLUMNS:
        return "DECIMAL(18,0)"
    if column in MONEY_COLUMNS:
        return "DECIMAL(18,2)"
    if column == "货品名称":
        return "TEXT"
    return "VARCHAR(255)"


def create_sql_for_table(table_name: str) -> str:
    columns_sql = [f"    `{col}` {mysql_type_for_column(col)}" for col in FINAL_COLUMNS]
    indexes = [
        "    INDEX `idx_仓库` (`仓库`)",
        "    INDEX `idx_货品编号` (`货品编号`)",
        "    INDEX `idx_品牌` (`品牌`)",
        "    INDEX `idx_条码` (`条码`)",
        "    INDEX `idx_updatetime` (`updatetime`)",
    ]
    definition = ",\n".join(columns_sql + indexes)
    return f"""
CREATE TABLE `{table_name}` (
{definition}
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='分仓库查询';
"""


def write_exact_snapshot_to_mysql(df: pd.DataFrame, table_name: str) -> None:
    tmp_table = f"{table_name}_tmp_web_{os.getpid()}"
    old_table = f"{table_name}_old_web_{os.getpid()}"
    tmp_file = os.path.join(tempfile.gettempdir(), f"{tmp_table}.csv")
    conn = None
    cursor = None

    try:
        df_clean = clean_for_mysql(df)
        df_clean.to_csv(tmp_file, index=False, header=False, encoding="utf-8")

        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SET GLOBAL local_infile = 1")
        cursor.execute(f"DROP TABLE IF EXISTS `{tmp_table}`")
        cursor.execute(create_sql_for_table(tmp_table))

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
        conn.commit()

        cursor.execute(f"DROP TABLE IF EXISTS `{old_table}`")
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if cursor.fetchone():
            cursor.execute(f"""
                RENAME TABLE
                `{table_name}` TO `{old_table}`,
                `{tmp_table}` TO `{table_name}`
            """)
            cursor.execute(f"DROP TABLE IF EXISTS `{old_table}`")
        else:
            cursor.execute(f"RENAME TABLE `{tmp_table}` TO `{table_name}`")
        conn.commit()
        print(f"[INFO] imported {loaded} rows into {DB_CONFIG['database']}.{table_name}", flush=True)
    except Exception:
        if conn:
            conn.rollback()
        if cursor:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS `{tmp_table}`")
                conn.commit()
            except Exception:
                pass
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        try:
            os.remove(tmp_file)
        except OSError:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export filtered Jike branch stock rows and sync them into MySQL.",
    )
    parser.add_argument("--curl", default=DEFAULT_CURL, help="startExcelExport Copy-as-cURL text file")
    parser.add_argument("--table", default=TABLE_NAME, help="target MySQL table")
    parser.add_argument("--xlsx", default=DEFAULT_XLSX, help="downloaded xlsx output path")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="normalized csv output path")
    parser.add_argument(
        "--mode",
        choices=["auto", "export", "page"],
        default="auto",
        help="auto: startExcelExport uses export task, stockSkuList uses paging",
    )
    parser.add_argument("--page-size", type=int, default=200, help="web list page size")
    parser.add_argument("--timeout", type=int, default=300, help="task polling timeout seconds")
    parser.add_argument("--interval", type=int, default=5, help="task polling interval seconds")
    parser.add_argument(
        "--force-standard-columns",
        action="store_true",
        help="replace headersJson and conditionJson.cols with the configured columns",
    )
    parser.add_argument("--no-db", action="store_true", help="only export and normalize files")
    parser.add_argument("--from-xlsx", help="skip web export and import an existing xlsx")
    return parser.parse_args()


def load_curl_info(curl_path: str) -> dict[str, Any]:
    env_curl = os.getenv(START_EXPORT_CURL_ENV, "").strip()
    if env_curl:
        print(f"[INFO] using cURL from environment variable {START_EXPORT_CURL_ENV}", flush=True)
        return parse_curl_text(env_curl)
    if curl_path and curl_path != DEFAULT_CURL and os.path.exists(curl_path):
        print(f"[INFO] using cURL file: {curl_path}", flush=True)
        return parse_curl_file(curl_path)
    if START_EXPORT_CURL_TEXT.strip():
        print("[INFO] using cURL from START_EXPORT_CURL_TEXT", flush=True)
        return parse_curl_text(START_EXPORT_CURL_TEXT)
    if os.path.exists(curl_path):
        print(f"[INFO] using cURL file: {curl_path}", flush=True)
        return parse_curl_file(curl_path)
    raise FileNotFoundError(f"Cannot find cURL file: {curl_path}")


def main() -> None:
    args = parse_args()
    update_time = datetime.now().replace(microsecond=0)

    if args.from_xlsx:
        df = load_stock_excel(args.from_xlsx, update_time)
    else:
        curl_info = load_curl_info(args.curl)
        use_export = args.mode == "export" or (
            args.mode == "auto" and "warehouseStock/stockSkuList" not in curl_info["url"]
        )
        if use_export:
            with requests.Session() as session:
                task_id = validate_and_start_export(session, curl_info, args.force_standard_columns)
                download_url, file_name = poll_task_download(
                    session,
                    curl_info,
                    task_id,
                    args.timeout,
                    args.interval,
                )
                if file_name:
                    print(f"[INFO] attachment: {file_name}", flush=True)
                xlsx_path = download_file(session, download_url, args.xlsx)
            df = load_stock_excel(xlsx_path, update_time)
        else:
            rows = fetch_all_stock_rows(curl_info, args.page_size)
            df = normalize_stock_rows(rows, update_time)

    os.makedirs(os.path.dirname(args.csv), exist_ok=True)
    df.to_csv(args.csv, index=False, encoding="utf-8-sig")
    print(f"[INFO] normalized rows: {len(df)}", flush=True)
    print(f"[INFO] normalized csv: {args.csv}", flush=True)

    if args.no_db:
        print("[INFO] --no-db set, skip MySQL import", flush=True)
        return

    write_exact_snapshot_to_mysql(df, args.table)


if __name__ == "__main__":
    main()
