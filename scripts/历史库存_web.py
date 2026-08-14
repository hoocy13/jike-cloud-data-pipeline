"""
Fetch Jike Cloud historical warehouse stock and persist idempotent snapshots.

The Copy-as-cURL may come from either the history page list request or the
specialExcelExport request.  specialExcelExport only contains the visible
page rows; this script deliberately ignores its ``datas`` payload and calls
the real paginated history endpoint instead.
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import os
import shlex
import sys
import tempfile
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import pymysql
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATA_DIR, DB_CONFIG


BASE_URL = "https://env3.jkyservice.com"
HISTORY_URL = f"{BASE_URL}/jkyun/birc/stock/history"
WEB_APP_KEY = "jackyun_web_browser_2024"
WEB_SIGN_SECRET = os.getenv("JKY_WEB_SIGN_SECRET", "")

TABLE_NAME = "历史库存"
BATCH_TABLE_NAME = "历史库存快照批次"
HISTORY_CURL_ENV = "JKY_HISTORY_STOCK_CURL"
DEFAULT_CSV_DIR = os.path.join(DATA_DIR, "历史库存快照")

CONNECT_RETRIES = 5
CONNECT_RETRY_DELAY_SECONDS = 10
SHRINK_GUARD_RATIO = Decimal("0.80")

FIELD_MAP = {
    "warehouseId": "仓库ID",
    "warehouseName": "仓库",
    "goodsId": "货品ID",
    "goodsNo": "货品编号",
    "goodsName": "货品名称",
    "goodsAlias": "别名",
    "skuId": "SKUID",
    "skuPropertiesName": "规格",
    "baseUnitName": "单位",
    "skuBarcode": "条码",
    "isCertified": "正品标记",
    "isCertifiedName": "正品",
    "brandName": "品牌",
    "cateName": "分类",
    "defaultVendName": "默认供应商",
    "quantity": "库存量",
    "accountingQuantity": "核算数量",
    "costAmt": "成本金额",
    "unAccountingQuantity": "未核算数量",
    "unCostAmt": "未核算金额",
    "noAccountingQuantity": "非核算数量",
    "noCostAmt": "非核算金额",
    "stockAmt": "库存金额",
    "skuVolumeVal": "体积(cm³)",
    "skuWeightVal": "重量(g)",
    "inventoryVolume": "总体积(cm³)",
    "inventoryWeight": "总重量(g)",
    "goodsTypeName": "货品类型",
    "assistInfo": "辅助显示",
    "goodsField1": "货位字段",
    "skuField1": "自定义字段1",
}

ID_COLUMNS = ["仓库ID", "货品ID", "SKUID"]
TEXT_COLUMNS = [
    "仓库",
    "货品编号",
    "货品名称",
    "别名",
    "规格",
    "单位",
    "条码",
    "正品",
    "品牌",
    "分类",
    "默认供应商",
    "货品类型",
    "辅助显示",
    "货位字段",
    "自定义字段1",
]
QUANTITY_COLUMNS = [
    "库存量",
    "核算数量",
    "未核算数量",
    "非核算数量",
    "体积(cm³)",
    "重量(g)",
    "总体积(cm³)",
    "总重量(g)",
]
AMOUNT_COLUMNS = ["成本金额", "未核算金额", "非核算金额", "库存金额"]
FINAL_COLUMNS = (
    ["快照日期"]
    + ID_COLUMNS
    + TEXT_COLUMNS[:2]
    + ["货品名称", "别名", "规格", "单位", "条码", "正品标记", "正品"]
    + TEXT_COLUMNS[8:]
    + QUANTITY_COLUMNS[:4]
    + AMOUNT_COLUMNS
    + QUANTITY_COLUMNS[4:]
    + ["updatetime"]
)


def normalize_curl_text(text: str) -> str:
    return text.replace("^\r\n", " ").replace("^\n", " ").replace("^", "")


def parse_curl_text(raw: str) -> dict[str, Any]:
    tokens = shlex.split(normalize_curl_text(raw), posix=True)
    if not tokens or tokens[0].lower() != "curl":
        raise ValueError("The input does not look like a curl command")

    url = ""
    headers: dict[str, str] = {}
    cookie = ""
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
        elif not token.startswith("-") and not url:
            url = token
        i += 1

    if not any(part in url for part in ("specialExcelExport", "/birc/stock/history")):
        raise ValueError(
            "Please provide a history_warehouse specialExcelExport or history list Copy-as-cURL"
        )
    if not headers.get("authorization"):
        raise ValueError("Missing authorization header in cURL")
    if not cookie:
        raise ValueError("Missing cookie in cURL")
    return {"url": HISTORY_URL, "headers": headers, "cookie": cookie}


def load_curl_info(curl_path: str | None) -> dict[str, Any]:
    env_curl = os.getenv(HISTORY_CURL_ENV, "").strip()
    if env_curl:
        print(f"[INFO] using cURL from {HISTORY_CURL_ENV}", flush=True)
        return parse_curl_text(env_curl)
    if curl_path and os.path.exists(curl_path):
        print(f"[INFO] using cURL file: {curl_path}", flush=True)
        return parse_curl_text(Path(curl_path).read_text(encoding="utf-8-sig"))
    raise FileNotFoundError(
        f"No cURL configured. Set {HISTORY_CURL_ENV} or pass --curl."
    )


def signed_params(params: dict[str, Any], authorization: str) -> dict[str, str]:
    out = {key: "" if value is None else str(value) for key, value in params.items()}
    out["timestamp"] = str(int(time.time() * 1000))
    out["access_token"] = authorization
    out["appkey"] = WEB_APP_KEY
    out.pop("sign", None)
    sign_items = sorted((key, value) for key, value in out.items() if value != "")
    payload = "".join(key + value for key, value in sign_items)
    out["sign"] = hashlib.md5(
        (WEB_SIGN_SECRET + payload + WEB_SIGN_SECRET).encode("utf-8")
    ).hexdigest().upper()
    return out


def web_headers(curl_info: dict[str, Any]) -> dict[str, str]:
    source = curl_info["headers"]
    return {
        "accept": source.get("accept", "text/plain, */*; q=0.01"),
        "accept-language": source.get("accept-language", "zh-CN,zh;q=0.9"),
        "authorization": source["authorization"],
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "module_code": "history_warehouse",
        "origin": BASE_URL,
        "referer": source.get(
            "referer",
            f"{BASE_URL}/erp_stock/goods_stock/history_warehouse.html",
        ),
        "user-agent": source.get("user-agent", "Mozilla/5.0"),
        "x-requested-with": "XMLHttpRequest",
        "cookie": curl_info["cookie"],
    }


def request_json(
    session: requests.Session,
    curl_info: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, Any]:
    headers = web_headers(curl_info)
    response = None
    for attempt in range(1, CONNECT_RETRIES + 1):
        body = signed_params(params, headers["authorization"])
        try:
            response = session.post(
                HISTORY_URL,
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
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Non-JSON response: {response.text[:500]}") from exc
    if response.status_code >= 400 or payload.get("code") != 200:
        raise RuntimeError(
            f"History request failed {response.status_code}: {response.text[:1000]}"
        )
    return payload


def fetch_snapshot(
    curl_info: dict[str, Any],
    snapshot_date: date,
    warehouse_id: str,
    page_size: int,
    interval: float,
    max_pages: int,
) -> tuple[list[dict[str, Any]], str]:
    base_params: dict[str, Any] = {
        "warehouseId": warehouse_id,
        "endDate": snapshot_date.isoformat(),
        "serviceType": "history.stock.search",
        "blockUp": "1",
        "filterZeroSku": "1",
        "isFilterDeleted": "1",
        "pageSize": str(page_size),
        "sortField": "",
        "sortOrder": "",
    }
    all_rows: list[dict[str, Any]] = []
    context_id = ""

    with requests.Session() as session:
        for page_index in range(max_pages):
            params = dict(base_params)
            params["pageIndex"] = str(page_index)
            if context_id:
                params["contextId"] = context_id
            payload = request_json(session, curl_info, params)
            result = payload.get("result") or {}
            rows = result.get("data") or []
            if not isinstance(rows, list):
                raise RuntimeError(
                    f"History response data is not a list: "
                    f"{json.dumps(payload, ensure_ascii=False)[:1000]}"
                )
            if not context_id and result.get("contextId"):
                context_id = str(result["contextId"])
            all_rows.extend(rows)
            print(
                f"[INFO] {snapshot_date} page {page_index}: {len(rows)} rows; "
                f"accumulated={len(all_rows)}",
                flush=True,
            )
            if len(rows) < page_size:
                return all_rows, context_id
            if interval > 0:
                time.sleep(interval)
    raise RuntimeError(
        f"Reached --max-pages={max_pages} for {snapshot_date}; refusing partial snapshot"
    )


def flatten_row(row: dict[str, Any]) -> dict[str, Any]:
    flattened = dict(row)
    for nested_name in ("goodsExtendMap", "skuExtendMap"):
        nested = row.get(nested_name)
        if isinstance(nested, dict):
            for key, value in nested.items():
                if flattened.get(key) in (None, ""):
                    flattened[key] = value
    return flattened


def normalize_dataframe(
    rows: list[dict[str, Any]],
    snapshot_date: date,
    update_time: datetime,
) -> pd.DataFrame:
    flattened = [flatten_row(row) for row in rows]
    df = pd.DataFrame(flattened)
    for source_name in FIELD_MAP:
        if source_name not in df.columns:
            df[source_name] = pd.NA
    df = df[list(FIELD_MAP)].rename(columns=FIELD_MAP).copy()
    df.insert(0, "快照日期", snapshot_date.isoformat())
    df["updatetime"] = update_time.strftime("%Y-%m-%d %H:%M:%S")

    for col in ID_COLUMNS:
        df[col] = (
            df[col]
            .fillna("")
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )
    missing_key = (df["仓库ID"] == "") | (df["SKUID"] == "")
    if missing_key.any():
        raise RuntimeError(
            f"{int(missing_key.sum())} rows have no warehouseId or skuId; "
            "refusing an incomplete-key snapshot"
        )

    for col in TEXT_COLUMNS:
        df[col] = df[col].where(pd.notna(df[col]), pd.NA)
    df["正品标记"] = pd.to_numeric(df["正品标记"], errors="coerce").astype("Int64")
    for col in QUANTITY_COLUMNS + AMOUNT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    duplicate_mask = df.duplicated(["快照日期", "仓库ID", "SKUID"], keep=False)
    if duplicate_mask.any():
        examples = (
            df.loc[duplicate_mask, ["仓库ID", "SKUID"]]
            .drop_duplicates()
            .head(5)
            .to_dict("records")
        )
        raise RuntimeError(
            f"Duplicate warehouse/SKU keys in {snapshot_date}: {examples}"
        )

    df = df[FINAL_COLUMNS].sort_values(
        ["快照日期", "仓库ID", "SKUID"],
        kind="stable",
    )
    return df.reset_index(drop=True)


def clean_for_mysql(df: pd.DataFrame) -> pd.DataFrame:
    clean = df.copy()
    for col in clean.columns:
        series = clean[col]
        if pd.api.types.is_numeric_dtype(series):
            clean[col] = series.where(pd.notna(series), "\\N")
        else:
            clean[col] = series.fillna("\\N").astype(str)
            clean.loc[
                clean[col].isin(["", "nan", "None", "NaT", "<NA>"]),
                col,
            ] = "\\N"
    return clean


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


def snapshot_table_sql(table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS `{table_name}` (
    `快照日期` DATE NOT NULL,
    `仓库ID` VARCHAR(64) NOT NULL,
    `货品ID` VARCHAR(64) NOT NULL,
    `SKUID` VARCHAR(64) NOT NULL,
    `仓库` VARCHAR(255),
    `货品编号` VARCHAR(255),
    `货品名称` VARCHAR(500),
    `别名` VARCHAR(500),
    `规格` VARCHAR(500),
    `单位` VARCHAR(100),
    `条码` VARCHAR(255),
    `正品标记` TINYINT,
    `正品` VARCHAR(100),
    `品牌` VARCHAR(255),
    `分类` VARCHAR(500),
    `默认供应商` VARCHAR(500),
    `货品类型` VARCHAR(100),
    `辅助显示` VARCHAR(500),
    `货位字段` VARCHAR(500),
    `自定义字段1` VARCHAR(500),
    `库存量` DECIMAL(24,6),
    `核算数量` DECIMAL(24,6),
    `未核算数量` DECIMAL(24,6),
    `非核算数量` DECIMAL(24,6),
    `成本金额` DECIMAL(24,4),
    `未核算金额` DECIMAL(24,4),
    `非核算金额` DECIMAL(24,4),
    `库存金额` DECIMAL(24,4),
    `体积(cm³)` DECIMAL(24,6),
    `重量(g)` DECIMAL(24,6),
    `总体积(cm³)` DECIMAL(24,6),
    `总重量(g)` DECIMAL(24,6),
    `updatetime` DATETIME NOT NULL,
    PRIMARY KEY (`快照日期`, `仓库ID`, `SKUID`),
    INDEX `idx_历史库存_仓库日期` (`仓库ID`, `快照日期`),
    INDEX `idx_历史库存_货品编号` (`货品编号`),
    INDEX `idx_历史库存_条码` (`条码`),
    INDEX `idx_历史库存_日期` (`快照日期`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='吉客云历史库存月末快照';
"""


def batch_table_sql(table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS `{table_name}` (
    `快照日期` DATE NOT NULL,
    `状态` VARCHAR(20) NOT NULL,
    `行数` BIGINT,
    `库存量合计` DECIMAL(30,6),
    `核算数量合计` DECIMAL(30,6),
    `成本金额合计` DECIMAL(30,4),
    `库存金额合计` DECIMAL(30,4),
    `未核算金额合计` DECIMAL(30,4),
    `数据哈希` CHAR(64),
    `开始时间` DATETIME,
    `完成时间` DATETIME,
    `源接口` VARCHAR(500),
    `上下文ID` VARCHAR(100),
    `错误信息` TEXT,
    `updatetime` DATETIME NOT NULL,
    PRIMARY KEY (`快照日期`),
    INDEX `idx_历史库存批次_状态` (`状态`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='历史库存快照运行与质量记录';
"""


def ensure_tables(cursor: Any, table_name: str, batch_table_name: str) -> None:
    cursor.execute(snapshot_table_sql(table_name))
    cursor.execute(batch_table_sql(batch_table_name))
    cursor.execute(f"SHOW COLUMNS FROM `{batch_table_name}`")
    existing_columns = {row[0] for row in cursor.fetchall()}
    if "库存金额合计" not in existing_columns:
        cursor.execute(
            f"ALTER TABLE `{batch_table_name}` "
            f"ADD COLUMN `库存金额合计` DECIMAL(30,4) AFTER `成本金额合计`"
        )
    if "未核算金额合计" not in existing_columns:
        cursor.execute(
            f"ALTER TABLE `{batch_table_name}` "
            f"ADD COLUMN `未核算金额合计` DECIMAL(30,4) AFTER `库存金额合计`"
        )


def record_status(
    snapshot_date: date,
    status: str,
    batch_table_name: str,
    started_at: datetime,
    error_message: str | None = None,
) -> None:
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute(batch_table_sql(batch_table_name))
            cursor.execute(
                f"""
                INSERT INTO `{batch_table_name}`
                    (`快照日期`, `状态`, `开始时间`, `完成时间`, `源接口`,
                     `错误信息`, `updatetime`)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    `状态` = VALUES(`状态`),
                    `开始时间` = VALUES(`开始时间`),
                    `完成时间` = VALUES(`完成时间`),
                    `源接口` = VALUES(`源接口`),
                    `错误信息` = VALUES(`错误信息`),
                    `updatetime` = NOW()
                """,
                (
                    snapshot_date,
                    status,
                    started_at,
                    datetime.now() if status == "FAILED" else None,
                    HISTORY_URL,
                    error_message,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def dataframe_hash(df: pd.DataFrame) -> str:
    payload = df.drop(columns=["updatetime"]).to_csv(
        index=False,
        lineterminator="\n",
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def decimal_sum(df: pd.DataFrame, column: str) -> Decimal:
    value = pd.to_numeric(df[column], errors="coerce").sum()
    if pd.isna(value):
        return Decimal("0")
    return Decimal(str(value))


def write_snapshot_to_mysql(
    df: pd.DataFrame,
    snapshot_date: date,
    table_name: str,
    batch_table_name: str,
    context_id: str,
    started_at: datetime,
    force: bool,
) -> None:
    stage_table = f"{table_name}_stage_{snapshot_date:%Y%m%d}_{os.getpid()}"
    tmp_file = os.path.join(tempfile.gettempdir(), f"{stage_table}.csv")
    conn = None
    cursor = None
    try:
        clean = clean_for_mysql(df)
        clean.to_csv(tmp_file, index=False, header=False, encoding="utf-8")
        conn = connect_db()
        cursor = conn.cursor()
        ensure_tables(cursor, table_name, batch_table_name)
        cursor.execute("SET GLOBAL local_infile = 1")
        cursor.execute(f"DROP TABLE IF EXISTS `{stage_table}`")
        cursor.execute(f"CREATE TABLE `{stage_table}` LIKE `{table_name}`")

        columns = ", ".join(f"`{column}`" for column in FINAL_COLUMNS)
        tmp_path = tmp_file.replace("\\", "/")
        cursor.execute(
            f"""
            LOAD DATA LOCAL INFILE '{tmp_path}'
            INTO TABLE `{stage_table}`
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            ({columns})
            """
        )
        loaded = cursor.rowcount
        if loaded != len(df):
            raise RuntimeError(
                f"Stage row count mismatch for {snapshot_date}: "
                f"loaded={loaded}, expected={len(df)}"
            )
        cursor.execute(
            f"SELECT COUNT(*) FROM `{table_name}` WHERE `快照日期` = %s",
            (snapshot_date,),
        )
        existing = int(cursor.fetchone()[0])
        if (
            existing > 0
            and loaded < int(Decimal(existing) * SHRINK_GUARD_RATIO)
            and not force
        ):
            raise RuntimeError(
                f"Snapshot {snapshot_date} shrank from {existing} to {loaded} rows "
                f"(< {SHRINK_GUARD_RATIO:%}); use --force only after validation"
            )

        conn.commit()
        data_hash = dataframe_hash(df)
        inventory_total = decimal_sum(df, "库存量")
        accounting_total = decimal_sum(df, "核算数量")
        cost_total = decimal_sum(df, "成本金额")
        stock_amount_total = decimal_sum(df, "库存金额")
        unaccounted_amount_total = decimal_sum(df, "未核算金额")

        conn.begin()
        cursor.execute(
            f"DELETE FROM `{table_name}` WHERE `快照日期` = %s",
            (snapshot_date,),
        )
        deleted = cursor.rowcount
        cursor.execute(
            f"INSERT INTO `{table_name}` ({columns}) "
            f"SELECT {columns} FROM `{stage_table}`"
        )
        inserted = cursor.rowcount
        cursor.execute(
            f"""
            INSERT INTO `{batch_table_name}`
                (`快照日期`, `状态`, `行数`, `库存量合计`, `核算数量合计`,
                 `成本金额合计`, `库存金额合计`, `未核算金额合计`,
                 `数据哈希`, `开始时间`, `完成时间`, `源接口`, `上下文ID`,
                 `错误信息`, `updatetime`)
            VALUES (%s, 'SUCCESS', %s, %s, %s, %s, %s, %s, %s, %s, NOW(),
                    %s, %s, NULL, NOW())
            ON DUPLICATE KEY UPDATE
                `状态` = 'SUCCESS',
                `行数` = VALUES(`行数`),
                `库存量合计` = VALUES(`库存量合计`),
                `核算数量合计` = VALUES(`核算数量合计`),
                `成本金额合计` = VALUES(`成本金额合计`),
                `库存金额合计` = VALUES(`库存金额合计`),
                `未核算金额合计` = VALUES(`未核算金额合计`),
                `数据哈希` = VALUES(`数据哈希`),
                `开始时间` = VALUES(`开始时间`),
                `完成时间` = NOW(),
                `源接口` = VALUES(`源接口`),
                `上下文ID` = VALUES(`上下文ID`),
                `错误信息` = NULL,
                `updatetime` = NOW()
            """,
            (
                snapshot_date,
                inserted,
                inventory_total,
                accounting_total,
                cost_total,
                stock_amount_total,
                unaccounted_amount_total,
                data_hash,
                started_at,
                HISTORY_URL,
                context_id or None,
            ),
        )
        conn.commit()
        print(
            f"[INFO] snapshot {snapshot_date}: deleted={deleted}, "
            f"inserted={inserted}, hash={data_hash[:12]}",
            flush=True,
        )
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS `{stage_table}`")
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


def parse_month(value: str) -> tuple[int, int]:
    try:
        parsed = datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("month must be YYYY-MM") from exc
    return parsed.year, parsed.month


def month_end_dates(start_month: str, end_month: str) -> list[date]:
    start_year, start_number = parse_month(start_month)
    end_year, end_number = parse_month(end_month)
    if (start_year, start_number) > (end_year, end_number):
        raise ValueError("--start-month must not be after --end-month")
    result: list[date] = []
    year, month = start_year, start_number
    while (year, month) <= (end_year, end_number):
        result.append(date(year, month, calendar.monthrange(year, month)[1]))
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1
    return result


def recent_completed_month_ends(count: int) -> list[date]:
    if count < 1:
        raise ValueError("--recent-months must be at least 1")
    month_end = date.today().replace(day=1) - timedelta(days=1)
    result: list[date] = []
    for _ in range(count):
        result.append(month_end)
        month_end = month_end.replace(day=1) - timedelta(days=1)
    return sorted(result)


def selected_dates(args: argparse.Namespace) -> list[date]:
    values: list[date] = []
    for raw in args.snapshot_date or []:
        try:
            values.append(date.fromisoformat(raw))
        except ValueError as exc:
            raise ValueError(f"Invalid --snapshot-date: {raw}") from exc
    if args.start_month or args.end_month:
        if not args.start_month or not args.end_month:
            raise ValueError("--start-month and --end-month must be used together")
        values.extend(month_end_dates(args.start_month, args.end_month))
    if not values:
        values.extend(recent_completed_month_ends(args.recent_months))
    yesterday = date.today() - timedelta(days=1)
    unique = sorted(set(values))
    future = [value for value in unique if value > yesterday]
    if future:
        raise ValueError(
            f"Historical stock only supports dates through yesterday: {future}"
        )
    return unique


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch and store Jike Cloud historical stock snapshots."
    )
    parser.add_argument("--curl", help="history page Copy-as-cURL file")
    parser.add_argument(
        "--snapshot-date",
        action="append",
        help="snapshot date YYYY-MM-DD; may be repeated",
    )
    parser.add_argument("--start-month", help="first month YYYY-MM")
    parser.add_argument("--end-month", help="last month YYYY-MM")
    parser.add_argument(
        "--recent-months",
        type=int,
        default=2,
        help="when no date range is supplied, refresh this many completed month ends",
    )
    parser.add_argument("--warehouse-id", default="", help="blank means all warehouses")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=10000)
    parser.add_argument("--interval", type=float, default=0.2)
    parser.add_argument("--table", default=TABLE_NAME)
    parser.add_argument("--batch-table", default=BATCH_TABLE_NAME)
    parser.add_argument("--csv-dir", default=DEFAULT_CSV_DIR)
    parser.add_argument("--no-db", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="allow replacing a snapshot that shrank below the safety threshold",
    )
    parser.add_argument("--continue-on-error", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dates = selected_dates(args)
    curl_info = load_curl_info(args.curl)
    os.makedirs(args.csv_dir, exist_ok=True)
    failures: list[tuple[date, str]] = []

    print(
        f"[INFO] snapshot dates: {', '.join(value.isoformat() for value in dates)}",
        flush=True,
    )
    for snapshot_date in dates:
        started_at = datetime.now().replace(microsecond=0)
        print(f"[SNAPSHOT] {snapshot_date}", flush=True)
        if not args.no_db:
            record_status(
                snapshot_date,
                "RUNNING",
                args.batch_table,
                started_at,
            )
        try:
            rows, context_id = fetch_snapshot(
                curl_info,
                snapshot_date,
                args.warehouse_id,
                args.page_size,
                args.interval,
                args.max_pages,
            )
            if not rows:
                raise RuntimeError(f"History API returned zero rows for {snapshot_date}")
            df = normalize_dataframe(rows, snapshot_date, started_at)
            csv_path = os.path.join(
                args.csv_dir,
                f"历史库存_{snapshot_date:%Y%m%d}.csv",
            )
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            print(
                f"[INFO] normalized rows={len(df)}, csv={csv_path}",
                flush=True,
            )
            if not args.no_db:
                write_snapshot_to_mysql(
                    df,
                    snapshot_date,
                    args.table,
                    args.batch_table,
                    context_id,
                    started_at,
                    args.force,
                )
        except Exception as exc:
            failures.append((snapshot_date, str(exc)))
            if not args.no_db:
                try:
                    record_status(
                        snapshot_date,
                        "FAILED",
                        args.batch_table,
                        started_at,
                        str(exc)[:4000],
                    )
                except Exception as status_exc:
                    print(
                        f"[WARN] failed to record snapshot status: {status_exc}",
                        flush=True,
                    )
            print(f"[ERROR] {snapshot_date}: {exc}", flush=True)
            if not args.continue_on_error:
                raise

    if failures:
        summary = "; ".join(f"{day}: {message}" for day, message in failures)
        raise RuntimeError(f"{len(failures)} snapshot(s) failed: {summary}")
    print(f"[DONE] snapshots completed: {len(dates)}", flush=True)


if __name__ == "__main__":
    main()
