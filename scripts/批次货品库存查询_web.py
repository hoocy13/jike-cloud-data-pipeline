"""
Sync Jike Cloud batch goods stock list from the web list API.

Paste a fresh batch.stock.search/pagelist cURL into START_LIST_CURL_TEXT for
local PyCharm runs, or set JKY_BATCH_STOCK_CURL in DolphinScheduler.
"""

from __future__ import annotations

import argparse
import hashlib
import json
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
from config import DATA_DIR, DB_CONFIG

BASE_URL = "https://env3.jkyservice.com"
WEB_APP_KEY = "jackyun_web_browser_2024"
WEB_SIGN_SECRET = os.getenv("JKY_WEB_SIGN_SECRET", "")

TABLE_NAME = "批次货品库存查询"
START_LIST_CURL_ENV = "JKY_BATCH_STOCK_CURL"
CONNECT_RETRIES = 5
CONNECT_RETRY_DELAY_SECONDS = 10
DEFAULT_CSV = os.path.join(DATA_DIR, "批次货品库存查询_web.csv")

# ============================================================
# Paste cURL Here
# ============================================================
START_LIST_CURL_TEXT = "".strip()

FIELD_MAP = {
    "warehouseName": "仓库",
    "batchNo": "批次",
    "goodsNo": "货品编号",
    "goodsName": "货品名称",
    "skuName": "规格",
    "skuBarcode": "条码",
    "brandName": "品牌",
    "totalQuantity": "总库存量",
    "residualQuantity": "库存数量",
    "canUseQuantity": "可用库存",
    "productionDate": "生产日期",
    "expirationDate": "到期日期",
    "shelfLife": "保质期",
    "shelfLiftUnit": "保质期单位",
    "surShelfLife": "剩余有效天数",
    "surShelfLifeRadio": "剩余有效天数占比(%)",
    "cateName": "货品分类",
}
SOURCE_COLUMNS = list(FIELD_MAP.keys())
FINAL_COLUMNS = list(FIELD_MAP.values()) + ["updatetime"]

QUANTITY_COLUMNS = ["总库存量", "库存数量", "可用库存", "保质期", "剩余有效天数"]
RATE_COLUMNS = ["剩余有效天数占比(%)"]
DATE_COLUMNS = ["生产日期", "到期日期"]
TEXT_COLUMNS = [col for col in FIELD_MAP.values() if col not in QUANTITY_COLUMNS + RATE_COLUMNS + DATE_COLUMNS]


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

    if "batch.stock.search/pagelist" not in url:
        raise ValueError("Please provide a batch.stock.search/pagelist Copy-as-cURL")
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
        "module_code": source.get("module_code", "batch_stock"),
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
    response = None
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            response = session.post(
                curl_info["url"],
                headers=headers,
                data=urlencode(body),
                timeout=90,
            )
        except (requests.ConnectTimeout, requests.ConnectionError):
            if attempt >= CONNECT_RETRIES:
                raise
            print(
                f"[WARN] HTTP connection failed; retrying in "
                f"{CONNECT_RETRY_DELAY_SECONDS}s ({attempt}/{CONNECT_RETRIES})",
                flush=True,
            )
            time.sleep(CONNECT_RETRY_DELAY_SECONDS)
            continue

        if response.status_code not in (429, 500, 502, 503, 504):
            break
        if attempt >= CONNECT_RETRIES:
            break
        print(
            f"[WARN] HTTP {response.status_code}; retrying in "
            f"{CONNECT_RETRY_DELAY_SECONDS}s ({attempt}/{CONNECT_RETRIES})",
            flush=True,
        )
        time.sleep(CONNECT_RETRY_DELAY_SECONDS)

    if response is None:
        raise RuntimeError("HTTP request did not return a response")
    text = response.text
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Non-JSON response: {text[:500]}") from exc
    if response.status_code >= 400 or data.get("code") not in (None, 200):
        raise RuntimeError(f"Request failed {response.status_code}: {text[:1000]}")
    return data


def find_rows_and_total(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], int | None]:
    result = payload.get("result") or {}
    candidates = [result.get("data"), payload.get("data"), result]
    for candidate in candidates:
        if isinstance(candidate, list):
            return candidate, len(candidate)
        if isinstance(candidate, dict):
            rows = (
                candidate.get("data")
                or candidate.get("list")
                or candidate.get("rows")
                or candidate.get("records")
                or candidate.get("pageList")
            )
            page_info = candidate.get("pageInfo") or result.get("pageInfo") or payload.get("pageInfo") or {}
            total = page_info.get("total") or page_info.get("totalCount") or candidate.get("total") or candidate.get("totalCount")
            if isinstance(rows, list):
                return rows, int(total) if str(total or "").isdigit() else None
    raise RuntimeError(f"Cannot locate row list in response: {json.dumps(payload, ensure_ascii=False)[:1000]}")


def fetch_all_rows(curl_info: dict[str, Any], page_size: int) -> list[dict[str, Any]]:
    base_params = dict(curl_info["params"])
    base_params["pageSize"] = str(page_size)
    base_params["cols"] = json.dumps(SOURCE_COLUMNS, ensure_ascii=False, separators=(",", ":"))
    base_params.setdefault("serviceType", "batch.stock.search")

    rows_all: list[dict[str, Any]] = []
    with requests.Session() as session:
        page_index = 0
        while True:
            params = dict(base_params)
            params["pageIndex"] = str(page_index)
            payload = request_json(session, curl_info, params)
            rows, total = find_rows_and_total(payload)
            suffix = f", total={total}" if total is not None else ""
            print(f"[INFO] page {page_index}: {len(rows)} rows{suffix}", flush=True)
            rows_all.extend(rows)
            if not rows or len(rows) < page_size:
                break
            page_index += 1
    return rows_all


def normalize_date_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    parsed_ms = pd.to_datetime(numeric, unit="ms", errors="coerce")
    parsed_text = pd.to_datetime(series, errors="coerce")
    parsed = parsed_ms.where(numeric.notna(), parsed_text)
    return parsed.dt.strftime("%Y-%m-%d").where(parsed.notna(), pd.NA)


def normalize_dataframe(rows: list[dict[str, Any]], update_time: datetime) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for source_col in SOURCE_COLUMNS:
        if source_col not in df.columns:
            df[source_col] = pd.NA

    df = df[SOURCE_COLUMNS].rename(columns=FIELD_MAP).copy()
    df = df.dropna(how="all", subset=["仓库", "批次", "货品编号", "条码"])

    for col in QUANTITY_COLUMNS:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
        )
        df[col] = pd.to_numeric(df[col], errors="coerce").round(0)

    for col in RATE_COLUMNS:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", "", regex=False)
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
        )
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    for col in DATE_COLUMNS:
        df[col] = normalize_date_series(df[col])

    for col in TEXT_COLUMNS:
        df[col] = df[col].where(pd.notna(df[col]), pd.NA)

    df["updatetime"] = update_time.strftime("%Y-%m-%d %H:%M:%S")
    return df[FINAL_COLUMNS]


def mysql_type_for_column(column: str) -> str:
    if column == "updatetime":
        return "DATETIME"
    if column in DATE_COLUMNS:
        return "DATE"
    if column in QUANTITY_COLUMNS:
        return "DECIMAL(18,0)"
    if column in RATE_COLUMNS:
        return "DECIMAL(18,2)"
    if column == "货品名称":
        return "TEXT"
    return "VARCHAR(255)"


def create_table_sql(table_name: str) -> str:
    columns_sql = [f"    `{col}` {mysql_type_for_column(col)}" for col in FINAL_COLUMNS]
    indexes = [
        "    INDEX `idx_仓库` (`仓库`)",
        "    INDEX `idx_批次` (`批次`)",
        "    INDEX `idx_货品编号` (`货品编号`)",
        "    INDEX `idx_条码` (`条码`)",
        "    INDEX `idx_updatetime` (`updatetime`)",
    ]
    definition = ",\n".join(columns_sql + indexes)
    return f"""
CREATE TABLE `{table_name}` (
{definition}
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='批次货品库存查询';
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


def write_exact_snapshot_to_mysql(df: pd.DataFrame, table_name: str) -> None:
    tmp_table = f"{table_name}_tmp_web_{os.getpid()}"
    old_table = f"{table_name}_old_web_{os.getpid()}"
    tmp_file = os.path.join(tempfile.gettempdir(), f"{tmp_table}.csv")
    conn = None
    cursor = None
    try:
        df_clean = clean_for_mysql(df)
        df_clean.to_csv(tmp_file, index=False, header=False, encoding="utf-8")

        conn = connect_db()
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
    parser = argparse.ArgumentParser(description="Sync Jike batch goods stock list into MySQL.")
    parser.add_argument("--curl", help="batch.stock.search/pagelist Copy-as-cURL text file")
    parser.add_argument("--table", default=TABLE_NAME, help="target MySQL table")
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--csv", default=DEFAULT_CSV, help="normalized csv output path")
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    update_time = datetime.now().replace(microsecond=0)
    curl_info = load_curl_info(args.curl)
    rows = fetch_all_rows(curl_info, args.page_size)
    df = normalize_dataframe(rows, update_time)

    os.makedirs(os.path.dirname(args.csv), exist_ok=True)
    df.to_csv(args.csv, index=False, encoding="utf-8-sig")
    print(f"[INFO] normalized rows: {len(df)}", flush=True)
    print(f"[INFO] normalized csv: {args.csv}", flush=True)

    if args.no_db:
        print("[INFO] --no-db set, skip MySQL import", flush=True)
        return

    write_exact_snapshot_to_mysql(df, args.table)
    print(f"[DONE] total rows: {len(df)}", flush=True)


if __name__ == "__main__":
    main()
