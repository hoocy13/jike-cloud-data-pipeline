"""
Sync Jike Cloud sales channel list from the web list API.

Paste a fresh getsaleschannelinfoforcols cURL into START_LIST_CURL_TEXT for
local PyCharm runs, or set JKY_SALES_CHANNEL_CURL in DolphinScheduler.
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
import pymysql
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG

BASE_URL = "https://env3.jkyservice.com"
WEB_APP_KEY = "jackyun_web_browser_2024"
WEB_SIGN_SECRET = os.getenv("JKY_WEB_SIGN_SECRET", "")

TABLE_NAME = "渠道列表"
START_LIST_CURL_ENV = "JKY_SALES_CHANNEL_CURL"

# ============================================================
# Paste cURL Here
# ============================================================
START_LIST_CURL_TEXT = "".strip()

FIELD_MAP = {
    "channelCode": "渠道编号",
    "channelName": "渠道名称",
    "cateName": "分类",
    "channelTypeName": "渠道类型",
    "assistantCode": "助记码",
    "companyName": "公司",
    "channelDepartName": "部门",
    "linkMan": "联系人",
    "linkTel": "联系电话",
    "gmtCreate": "创建时间",
    "commissionFormula": "提成公式",
    "responsibleUserName": "负责人",
    "groupName": "分组",
    "onlinePlatTypeName": "线上平台",
    "isAuth": "是否授权",
    "warehouseName": "默认仓库",
    "chargeName": "结算账户",
    "salesName": "业务员",
    "memo": "备注",
    "officialAccountsName": "公众号名称",
    "officialAccountsUrl": "公众号链接",
    "ownerName": "货主",
    "channelId": "渠道ID",
    "channelType": "渠道类型编码",
    "chargeType": "结算类型",
    "onlinePlatTypeCode": "线上平台编码",
    "companyId": "公司ID",
    "authToken": "授权Token",
    "isTokenExpire": "Token是否过期",
    "provinceName": "省",
    "cityName": "市",
    "townName": "区县",
    "streetName": "街道",
    "officeAddress": "办公地址",
}
FINAL_COLUMNS = list(FIELD_MAP.values()) + ["updatetime"]
ID_COLUMNS = {"渠道ID", "公司ID"}
DATETIME_COLUMNS = {"创建时间", "updatetime"}
TEXT_COLUMNS = {"备注", "公众号链接", "授权Token", "提成公式", "办公地址"}


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

    if "getsaleschannelinfoforcols" not in url:
        raise ValueError("Please provide a getsaleschannelinfoforcols Copy-as-cURL")
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
    raise FileNotFoundError(
        f"No cURL configured. Set {START_LIST_CURL_ENV}, paste START_LIST_CURL_TEXT, or pass --curl.")


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
        "module_code": source.get("module_code", "sale_way_list"),
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


def find_list_and_total(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], int | None]:
    result = payload.get("result") or {}
    candidates = [result.get("data"), payload.get("data"), result]
    for candidate in candidates:
        if isinstance(candidate, dict):
            rows = (
                    candidate.get("data")
                    or candidate.get("list")
                    or candidate.get("rows")
                    or candidate.get("records")
                    or candidate.get("salesChannelInfoList")
            )
            page_info = candidate.get("pageInfo") or result.get("pageInfo") or {}
            total = page_info.get("total") or page_info.get("totalCount") or candidate.get("total") or candidate.get(
                "totalCount")
            if isinstance(rows, list):
                return rows, int(total) if str(total or "").isdigit() else None
        elif isinstance(candidate, list):
            return candidate, len(candidate)
    raise RuntimeError(f"Cannot locate row list in response: {json.dumps(payload, ensure_ascii=False)[:1000]}")


def fetch_all_channels(curl_info: dict[str, Any], page_size: int) -> list[dict[str, Any]]:
    base_params = dict(curl_info["params"])
    base_params["pageSize"] = str(page_size)
    rows_all: list[dict[str, Any]] = []
    page_index = 0
    with requests.Session() as session:
        while True:
            params = dict(base_params)
            params["pageIndex"] = str(page_index)
            payload = request_json(session, curl_info, params)
            rows, total = find_list_and_total(payload)
            print(f"[INFO] page {page_index}: {len(rows)} rows" + (f", total={total}" if total is not None else ""),
                  flush=True)
            rows_all.extend(rows)
            if not rows:
                break
            if len(rows) < page_size:
                break
            page_index += 1
    return rows_all


def normalize_dataframe(rows: list[dict[str, Any]], update_time: datetime) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for source_col in FIELD_MAP:
        if source_col not in df.columns:
            df[source_col] = pd.NA
    df = df[list(FIELD_MAP)].rename(columns=FIELD_MAP).copy()
    df["updatetime"] = update_time.strftime("%Y-%m-%d %H:%M:%S")
    for col in DATETIME_COLUMNS:
        numeric = pd.to_numeric(df[col], errors="coerce")
        parsed = pd.to_datetime(numeric, unit="ms", errors="coerce")
        fallback = pd.to_datetime(df[col], errors="coerce")
        parsed = parsed.where(numeric.notna(), fallback)
        df[col] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S").where(parsed.notna(), pd.NA)
    for col in ID_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[FINAL_COLUMNS]


def create_table_sql(table_name: str) -> str:
    return f"""
CREATE TABLE `{table_name}` (
    `渠道编号` VARCHAR(128),
    `渠道名称` VARCHAR(255),
    `分类` VARCHAR(255),
    `渠道类型` VARCHAR(128),
    `助记码` VARCHAR(128),
    `公司` VARCHAR(255),
    `部门` VARCHAR(255),
    `联系人` VARCHAR(128),
    `联系电话` VARCHAR(128),
    `创建时间` DATETIME,
    `提成公式` TEXT,
    `负责人` VARCHAR(128),
    `分组` VARCHAR(255),
    `线上平台` VARCHAR(128),
    `是否授权` VARCHAR(64),
    `默认仓库` VARCHAR(255),
    `结算账户` VARCHAR(255),
    `业务员` VARCHAR(128),
    `备注` TEXT,
    `公众号名称` VARCHAR(255),
    `公众号链接` TEXT,
    `货主` VARCHAR(128),
    `渠道ID` BIGINT,
    `渠道类型编码` VARCHAR(64),
    `结算类型` VARCHAR(64),
    `线上平台编码` VARCHAR(64),
    `公司ID` BIGINT,
    `授权Token` TEXT,
    `Token是否过期` VARCHAR(64),
    `省` VARCHAR(128),
    `市` VARCHAR(128),
    `区县` VARCHAR(128),
    `街道` VARCHAR(255),
    `办公地址` TEXT,
    `updatetime` DATETIME,
    INDEX `idx_渠道编号` (`渠道编号`),
    INDEX `idx_渠道名称` (`渠道名称`),
    INDEX `idx_渠道ID` (`渠道ID`),
    INDEX `idx_创建时间` (`创建时间`),
    INDEX `idx_updatetime` (`updatetime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='渠道列表';
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
    parser = argparse.ArgumentParser(description="Sync Jike sales channel list into MySQL.")
    parser.add_argument("--curl", help="getsaleschannelinfoforcols Copy-as-cURL text file")
    parser.add_argument("--table", default=TABLE_NAME, help="target MySQL table")
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--csv", default="", help="optional csv output path")
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    update_time = datetime.now().replace(microsecond=0)
    curl_info = load_curl_info(args.curl)
    rows = fetch_all_channels(curl_info, args.page_size)
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
