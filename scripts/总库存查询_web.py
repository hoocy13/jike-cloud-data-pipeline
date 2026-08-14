"""
Sync Jike Cloud total stock list from the web list API.

Paste a fresh allStockSkuList cURL into START_LIST_CURL_TEXT for local PyCharm
runs, or set JKY_TOTAL_STOCK_CURL in DolphinScheduler.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shlex
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode

import pandas as pd
import pymysql
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG

BASE_URL = "https://env3.jkyservice.com"
WEB_APP_KEY = "jackyun_web_browser_2024"
WEB_SIGN_SECRET = os.getenv("JKY_WEB_SIGN_SECRET", "")

TABLE_NAME = "总库存查询"
START_LIST_CURL_ENV = "JKY_TOTAL_STOCK_CURL"

# ============================================================
# Paste cURL Here
# ============================================================
START_LIST_CURL_TEXT = "".strip()

FIELD_COLUMNS = [
    "warehouseName",
    "goodsNo",
    "goodsName",
    "skuName",
    "skuBarcode",
    "brandName",
    "currentQuantity",
    "lockingQuantity",
    "canUseQuantity",
    "orderingQuantity",
    "orderAbleQuantity",
    "salesReturnQuantity",
    "distrubuteQuantity",
    "yesterdayQuantity",
    "weekQuantity",
    "threedayQuantity",
    "shelfLifeStr",
    "stockInQuantity",
    "stockOutQuantity",
    "price1",
    "price3",
    "price6",
    "price7",
    "cateName",
    "defectiveQuanity",
    "warehouseId",
    "goodsId",
    "id",
    "skuId",
    "unitName",
    "goodsAlias",
    "totalSaleQuantity",
    "warehouseIds",
    "isBlockup",
    "isStopSelling",
    "isStopPurchasing",
    "isBatchManagement",
    "isSerialManagement",
    "aduitStatus",
    "warehouseTypeCode",
    "isPositonStock",
    "maxValue",
    "minValue",
]

FIELD_MAP = {
    "warehouseName": "仓库",
    "goodsNo": "货品编号",
    "goodsName": "货品名称",
    "skuName": "规格",
    "skuBarcode": "条码",
    "brandName": "品牌",
    "currentQuantity": "库存数量",
    "lockingQuantity": "锁定待发",
    "canUseQuantity": "可用库存",
    "orderingQuantity": "订购量",
    "orderAbleQuantity": "可订购量",
    "salesReturnQuantity": "退货在途",
    "distrubuteQuantity": "渠道预留",
    "yesterdayQuantity": "昨天销量",
    "weekQuantity": "近7天销量",
    "threedayQuantity": "近30天销量",
    "shelfLifeStr": "货品保质期",
    "stockInQuantity": "入库申请",
    "stockOutQuantity": "出库申请",
    "price1": "零售价",
    "price3": "会员价",
    "price6": "含税价",
    "price7": "不含税价",
    "cateName": "货品分类",
    "defectiveQuanity": "残次品",
    "warehouseId": "仓库ID",
    "goodsId": "货品ID",
    "id": "库存ID",
    "skuId": "规格ID",
    "unitName": "单位",
    "goodsAlias": "货品别名",
    "totalSaleQuantity": "总销量",
    "warehouseIds": "仓库ID集合",
    "isBlockup": "是否停用",
    "isStopSelling": "是否停售",
    "isStopPurchasing": "是否停购",
    "isBatchManagement": "是否批次管理",
    "isSerialManagement": "是否序列号管理",
    "aduitStatus": "审核状态",
    "warehouseTypeCode": "仓库类型编码",
    "isPositonStock": "是否库位库存",
    "maxValue": "库存上限",
    "minValue": "库存下限",
}

PRIMARY_SOURCE_COLUMNS = [
    "warehouseName",
    "goodsNo",
    "goodsName",
    "skuName",
    "skuBarcode",
    "brandName",
    "currentQuantity",
    "lockingQuantity",
    "canUseQuantity",
    "orderingQuantity",
    "orderAbleQuantity",
    "salesReturnQuantity",
    "distrubuteQuantity",
    "yesterdayQuantity",
    "weekQuantity",
    "threedayQuantity",
    "shelfLifeStr",
    "stockInQuantity",
    "stockOutQuantity",
    "price1",
    "price3",
    "price6",
    "price7",
    "cateName",
    "defectiveQuanity",
]
SECONDARY_SOURCE_COLUMNS = [col for col in FIELD_COLUMNS if col not in PRIMARY_SOURCE_COLUMNS]
PRIMARY_COLUMNS = [FIELD_MAP.get(col, col) for col in PRIMARY_SOURCE_COLUMNS]
SECONDARY_COLUMNS = [FIELD_MAP.get(col, col) for col in SECONDARY_SOURCE_COLUMNS]
FINAL_COLUMNS = PRIMARY_COLUMNS + SECONDARY_COLUMNS + ["updatetime"]
QUANTITY_COLUMNS = [
    "库存数量", "锁定待发", "可用库存", "订购量", "可订购量",
    "退货在途", "渠道预留", "昨天销量", "近7天销量", "近30天销量",
    "入库申请", "出库申请", "残次品", "总销量", "库存上限", "库存下限",
]
MONEY_COLUMNS = ["零售价", "会员价", "含税价", "不含税价"]
ID_COLUMNS = ["仓库ID", "货品ID", "库存ID", "规格ID"]
NUMERIC_COLUMNS = QUANTITY_COLUMNS + MONEY_COLUMNS + ID_COLUMNS


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

    if "allStockSkuList" not in url:
        raise ValueError("Please provide an allStockSkuList Copy-as-cURL")
    if not data_raw:
        raise ValueError("Could not find --data-raw in cURL")
    params = dict(parse_qsl(data_raw, keep_blank_values=True))
    return {"url": url, "headers": headers, "cookie": cookie, "params": params}


def parse_curl_file(curl_path: str) -> dict[str, Any]:
    return parse_curl_text(Path(curl_path).read_text(encoding="utf-8-sig"))


def load_curl_info(curl_path: str | None) -> dict[str, Any]:
    env_curl = os.getenv(START_LIST_CURL_ENV, "").strip()
    if env_curl:
        print(f"[INFO] using cURL from environment variable {START_LIST_CURL_ENV}", flush=True)
        return parse_curl_text(env_curl)
    if curl_path and os.path.exists(curl_path):
        print(f"[INFO] using cURL file: {curl_path}", flush=True)
        return parse_curl_file(curl_path)
    if START_LIST_CURL_TEXT.strip():
        print("[INFO] using cURL from START_LIST_CURL_TEXT", flush=True)
        return parse_curl_text(START_LIST_CURL_TEXT)
    raise FileNotFoundError(f"No cURL configured. Set {START_LIST_CURL_ENV}, paste START_LIST_CURL_TEXT, or pass --curl.")


def signed_params(params: dict[str, Any], authorization: str) -> dict[str, str]:
    out = {key: "" if value is None else str(value) for key, value in params.items()}
    out["timestamp"] = str(int(time.time() * 1000))
    out["access_token"] = authorization
    out["appkey"] = WEB_APP_KEY
    out.pop("sign", None)
    sign_items = [(key, value) for key, value in out.items() if value != ""]
    sign_items.sort(key=lambda item: item[0])
    payload = "".join(key + value for key, value in sign_items)
    out["sign"] = hashlib.md5((WEB_SIGN_SECRET + payload + WEB_SIGN_SECRET).encode("utf-8")).hexdigest().upper()
    return out


def web_headers(curl_info: dict[str, Any]) -> dict[str, str]:
    source = curl_info["headers"]
    authorization = source.get("authorization")
    if not authorization:
        raise ValueError("Missing authorization header in cURL")
    headers = {
        "accept": source.get("accept", "text/plain, */*; q=0.01"),
        "accept-language": source.get("accept-language", "zh-CN,zh;q=0.9"),
        "authorization": authorization,
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "module_code": source.get("module_code", "total_stock"),
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


def request_json(session: requests.Session, curl_info: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    headers = web_headers(curl_info)
    body = signed_params(params, headers["authorization"])
    response = session.post(curl_info["url"], headers=headers, data=urlencode(body), timeout=90)
    text = response.text
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Non-JSON response: {text[:500]}") from exc
    if response.status_code >= 400 or data.get("code") not in (None, 200):
        raise RuntimeError(f"Request failed {response.status_code}: {text[:1000]}")
    return data


def find_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    result = payload.get("result") or {}
    # 吉客云翻到最后一页后会返回成功响应，但 result.data/pageInfo 均为 null。
    # 这是正常的空页，不应中断整条 DolphinScheduler 工作流。
    raw_result = payload.get("result")
    if isinstance(raw_result, dict) and "data" in raw_result and raw_result["data"] is None:
        return []
    candidates = [result.get("data"), payload.get("data"), result]
    for candidate in candidates:
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
        elif isinstance(candidate, list):
            return candidate
    raise RuntimeError(f"Cannot locate row list in response: {str(payload)[:1000]}")


def fetch_all_stock(curl_info: dict[str, Any], page_size: int) -> list[dict[str, Any]]:
    base_params = dict(curl_info["params"])
    base_params["pageSize"] = str(page_size)
    rows_all: list[dict[str, Any]] = []
    page_index = 0
    with requests.Session() as session:
        while True:
            params = dict(base_params)
            params["pageIndex"] = str(page_index)
            payload = request_json(session, curl_info, params)
            rows = find_rows(payload)
            print(f"[INFO] page {page_index}: {len(rows)} rows", flush=True)
            rows_all.extend(rows)
            if not rows or len(rows) < page_size:
                break
            page_index += 1
    return rows_all


def normalize_dataframe(rows: list[dict[str, Any]], update_time: datetime) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for source_col in FIELD_COLUMNS:
        if source_col not in df.columns:
            df[source_col] = pd.NA
    df = df[FIELD_COLUMNS].rename(columns=FIELD_MAP).copy()
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "", regex=False), errors="coerce")
        if col in QUANTITY_COLUMNS + ID_COLUMNS:
            df[col] = df[col].round(0)
        elif col in MONEY_COLUMNS:
            df[col] = df[col].round(2)
    df["updatetime"] = update_time.strftime("%Y-%m-%d %H:%M:%S")
    return df[FINAL_COLUMNS]


def mysql_type_for_column(column: str) -> str:
    if column == "updatetime":
        return "DATETIME"
    if column in ID_COLUMNS:
        return "BIGINT"
    if column in QUANTITY_COLUMNS:
        return "DECIMAL(18,0)"
    if column in MONEY_COLUMNS:
        return "DECIMAL(18,2)"
    if column in {"货品名称", "仓库ID集合"}:
        return "TEXT"
    return "VARCHAR(255)"


def create_table_sql(table_name: str) -> str:
    columns_sql = [f"`{col}` {mysql_type_for_column(col)}" for col in FINAL_COLUMNS]
    indexes = [
        "INDEX `idx_货品编号` (`货品编号`)",
        "INDEX `idx_条码` (`条码`)",
        "INDEX `idx_品牌` (`品牌`)",
        "INDEX `idx_updatetime` (`updatetime`)",
    ]
    return f"""
CREATE TABLE `{table_name}` (
    {", ".join(columns_sql + indexes)}
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='总库存查询';
"""


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
        cursor.execute(create_table_sql(tmp_table))
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
            cursor.execute(f"RENAME TABLE `{table_name}` TO `{old_table}`, `{tmp_table}` TO `{table_name}`")
            cursor.execute(f"DROP TABLE IF EXISTS `{old_table}`")
        else:
            cursor.execute(f"RENAME TABLE `{tmp_table}` TO `{table_name}`")
        conn.commit()
        print(f"[INFO] imported {loaded} rows into {DB_CONFIG['database']}.{table_name}", flush=True)
    except Exception:
        if conn:
            conn.rollback()
        raise
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Jike total stock list into MySQL.")
    parser.add_argument("--curl", help="allStockSkuList Copy-as-cURL text file")
    parser.add_argument("--table", default=TABLE_NAME, help="target MySQL table")
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--csv", default="", help="optional csv output path")
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    update_time = datetime.now().replace(microsecond=0)
    curl_info = load_curl_info(args.curl)
    rows = fetch_all_stock(curl_info, args.page_size)
    df = normalize_dataframe(rows, update_time)
    print(f"[INFO] normalized rows: {len(df)}", flush=True)
    if args.csv:
        os.makedirs(os.path.dirname(args.csv), exist_ok=True)
        df.to_csv(args.csv, index=False, encoding="utf-8-sig")
    if not args.no_db:
        write_exact_snapshot_to_mysql(df, args.table)
    print(f"[DONE] total rows: {len(df)}", flush=True)


if __name__ == "__main__":
    main()
