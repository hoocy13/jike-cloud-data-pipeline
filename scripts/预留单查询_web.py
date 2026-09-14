"""同步吉客云预留单主表及货品明细到 ODS。"""

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

BASE_URL = os.getenv("JKY_WEB_BASE_URL", "https://web.jackyun.com").rstrip("/")
WEB_APP_KEY = "jackyun_web_browser_2024"
WEB_SIGN_SECRET = os.getenv("JKY_WEB_SIGN_SECRET", "")
MAIN_URL = BASE_URL + "/jkyun/erp-stock/search/reserve.search/pagelist"
DETAIL_URL = BASE_URL + "/jkyun/erp-stock/search/reserve.search.detail/pagelist"
DEFAULT_CURL = Path(__file__).resolve().parents[1] / "curl" / "预留单查询_curl.txt"
DEFAULT_MAIN_CSV = os.path.join(DATA_DIR, "预留单查询_web.csv")
DEFAULT_DETAIL_CSV = os.path.join(DATA_DIR, "预留单货品明细_web.csv")
CONNECT_RETRIES = 5

MAIN_FIELDS = {
    "reserveId": "预留单ID", "reserveNo": "预留单号", "status": "状态码", "statusName": "状态",
    "reason": "预留原因", "warehouseId": "仓库ID", "warehouseName": "预留仓库", "source": "来源",
    "reserveType": "预留类型码", "reserveTypeName": "预留类型", "reserveObjType": "对象类型码",
    "reserveObjTypeName": "对象类型", "reserveObj": "预留对象ID", "reserveObjName": "预留对象",
    "beginDate": "使用起始日期", "endDate": "使用结束日期", "releaseType": "释放类型码",
    "releaseTypeName": "到期释放", "applyDepartId": "申请部门ID", "applyDepartName": "申请部门",
    "applyUserId": "申请人ID", "applyUserName": "申请人", "applyDate": "申请时间",
    "flagData": "标记", "memo": "备注", "gmtCreate": "创建时间", "gmtModified": "修改时间",
}

DETAIL_FIELDS = {
    "id": "明细ID", "reserveId": "预留单ID", "status": "状态码", "statusName": "状态",
    "reserveQuantity": "预留数量", "releaseQuantity": "已释放数量", "residualQuantity": "剩余数量",
    "usedQuantity": "已用数量", "canUseQuantity": "可用数量", "memo": "备注",
    "warehouseId": "仓库ID", "goodsId": "货品ID", "goodsNo": "货品编号", "goodsName": "货品名称",
    "skuId": "规格ID", "skuName": "规格", "skuBarcode": "条码", "unitName": "单位",
    "isCertified": "正品标识", "brandId": "品牌ID", "brandName": "品牌", "cateId": "分类ID",
    "cateCode": "分类编码", "cateName": "货品分类", "commonUnitName": "基本单位", "batchList": "批次信息",
}

MAIN_DATETIMES = {"使用起始日期", "使用结束日期", "申请时间", "创建时间", "修改时间"}
DETAIL_NUMBERS = {"预留数量", "已释放数量", "剩余数量", "已用数量", "可用数量"}
TEXT_COLUMNS = {"备注", "批次信息", "货品名称"}


def normalize_curl_text(text: str) -> str:
    # Windows Copy-as-cURL may escape both '%' and line breaks with '^'.
    return text.replace("^%", "%").replace("^\r\n", " ").replace("^\n", " ").replace("^", "")


def parse_curl_text(raw: str) -> dict[str, Any]:
    tokens = shlex.split(normalize_curl_text(raw), posix=True)
    if not tokens or tokens[0].lower() != "curl":
        raise ValueError("输入内容不是有效的 cURL")
    url, cookie, data_raw = "", "", ""
    headers: dict[str, str] = {}
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
        elif not token.startswith("-") and not url:
            url = token
        i += 1
    if "startExcelExport" not in url or not data_raw:
        raise ValueError("请提供预留单查询的 startExcelExport cURL")
    params = dict(parse_qsl(data_raw, keep_blank_values=True))
    condition = json.loads(params.get("conditionJson") or "{}")
    if params.get("excelType") != "reserve.search" or condition.get("serviceType") != "reserve.search":
        raise ValueError("该 cURL 不是预留单主表 reserve.search 请求")
    return {"headers": headers, "cookie": cookie, "condition": condition}


def load_curl(path: str) -> dict[str, Any]:
    env_value = os.getenv("JKY_RESERVE_CURL", "").strip()
    if env_value:
        print("[INFO] using cURL from JKY_RESERVE_CURL", flush=True)
        return parse_curl_text(env_value)
    print(f"[INFO] using cURL file: {path}", flush=True)
    return parse_curl_text(Path(path).read_text(encoding="utf-8-sig"))


def signed_params(params: dict[str, Any], authorization: str) -> dict[str, str]:
    if not WEB_SIGN_SECRET:
        raise RuntimeError("缺少 JKY_WEB_SIGN_SECRET")
    out = {k: (json.dumps(v, ensure_ascii=False, separators=(",", ":")) if isinstance(v, (dict, list)) else "" if v is None else str(v)) for k, v in params.items()}
    out.update(timestamp=str(int(time.time() * 1000)), access_token=authorization, appkey=WEB_APP_KEY)
    out.pop("sign", None)
    payload = "".join(k + v for k, v in sorted(out.items()) if v != "")
    out["sign"] = hashlib.md5((WEB_SIGN_SECRET + payload + WEB_SIGN_SECRET).encode()).hexdigest().upper()
    return out


def request_headers(info: dict[str, Any]) -> dict[str, str]:
    source = info["headers"]
    authorization = source.get("authorization")
    if not authorization:
        raise ValueError("cURL 缺少 authorization")
    result = {
        "accept": source.get("accept", "*/*"), "accept-language": source.get("accept-language", "zh-CN,zh;q=0.9"),
        "authorization": authorization, "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "module_code": source.get("module_code", "reserve_inventory_search_main"), "origin": BASE_URL,
        "referer": source.get("referer", BASE_URL + "/"), "user-agent": source.get("user-agent", "Mozilla/5.0"),
        "x-requested-with": "XMLHttpRequest",
    }
    if source.get("ati"):
        result["ati"] = source["ati"]
    if info.get("cookie"):
        result["cookie"] = info["cookie"]
    return result


def post_json(session: requests.Session, url: str, headers: dict[str, str], params: dict[str, Any]) -> dict[str, Any]:
    response = None
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            response = session.post(url, headers=headers, data=urlencode(signed_params(params, headers["authorization"])), timeout=90)
            if response.status_code not in (429, 500, 502, 503, 504):
                break
        except (requests.ConnectTimeout, requests.ConnectionError):
            if attempt == CONNECT_RETRIES:
                raise
        if attempt < CONNECT_RETRIES:
            time.sleep(attempt * 5)
    if response is None:
        raise RuntimeError("请求未返回响应")
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"吉客云返回非 JSON 内容: {response.text[:300]}") from exc
    if response.status_code >= 400 or payload.get("code") not in (None, 200):
        raise RuntimeError(f"吉客云请求失败 {response.status_code}: {response.text[:800]}")
    return payload


def rows_from(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], int | None]:
    result = payload.get("result") or {}
    rows = result.get("data") if isinstance(result, dict) else None
    if not isinstance(rows, list):
        rows = payload.get("data")
    if not isinstance(rows, list):
        raise RuntimeError(f"响应中没有数据列表: {json.dumps(payload, ensure_ascii=False)[:500]}")
    page = result.get("pageInfo") or payload.get("pageInfo") or {}
    total = page.get("total") or page.get("totalCount")
    return rows, int(total) if str(total or "").isdigit() else None


def fetch_main(session: requests.Session, info: dict[str, Any], page_size: int) -> list[dict[str, Any]]:
    headers = request_headers(info)
    base = dict(info["condition"])
    base.update(serviceType="reserve.search", pageSize=page_size)
    base.pop("commonVerify", None)
    all_rows: list[dict[str, Any]] = []
    page = 0
    while True:
        params = dict(base, pageIndex=page)
        rows, total = rows_from(post_json(session, MAIN_URL, headers, params))
        all_rows.extend(rows)
        print(f"[INFO] main page {page}: {len(rows)} rows, accumulated={len(all_rows)}, total={total}", flush=True)
        if not rows or len(rows) < page_size or (total is not None and len(all_rows) >= total):
            return all_rows
        page += 1


def fetch_details(session: requests.Session, info: dict[str, Any], main_rows: list[dict[str, Any]], chunk_size: int) -> list[dict[str, Any]]:
    headers = request_headers(info)
    reserve_ids = [str(row["reserveId"]) for row in main_rows if row.get("reserveId")]
    reserve_no = {str(row["reserveId"]): row.get("reserveNo") for row in main_rows}
    all_rows: list[dict[str, Any]] = []
    for start in range(0, len(reserve_ids), chunk_size):
        ids = reserve_ids[start:start + chunk_size]
        page = 0
        while True:
            params = {"reserveId": ",".join(ids), "serviceType": "reserve.search.detail", "pageIndex": page, "pageSize": 10000}
            rows, _ = rows_from(post_json(session, DETAIL_URL, headers, params))
            for row in rows:
                row["reserveNo"] = reserve_no.get(str(row.get("reserveId")))
            all_rows.extend(rows)
            print(f"[INFO] detail batch {start // chunk_size + 1}: {len(rows)} rows, accumulated={len(all_rows)}", flush=True)
            if len(rows) < 10000:
                break
            page += 1
    return all_rows


def normalize_frame(rows: list[dict[str, Any]], mapping: dict[str, str], update_time: datetime, detail: bool = False) -> pd.DataFrame:
    source_columns = list(mapping)
    if detail:
        source_columns.insert(2, "reserveNo")
        mapping = dict(mapping)
        mapping["reserveNo"] = "预留单号"
    df = pd.DataFrame(rows)
    for col in source_columns:
        if col not in df:
            df[col] = pd.NA
    if detail and "batchList" in df:
        df["batchList"] = df["batchList"].map(lambda v: json.dumps(v, ensure_ascii=False, separators=(",", ":")) if isinstance(v, (list, dict)) else v)
    df = df[source_columns].rename(columns=mapping).copy()
    for col in MAIN_DATETIMES.intersection(df.columns):
        numeric = pd.to_numeric(df[col], errors="coerce")
        parsed_ms = (
            pd.to_datetime(numeric, unit="ms", errors="coerce", utc=True)
            .dt.tz_convert("Asia/Shanghai")
            .dt.tz_localize(None)
        )
        parsed = parsed_ms.where(numeric.notna(), pd.to_datetime(df[col], errors="coerce"))
        df[col] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S").where(parsed.notna(), pd.NA)
    for col in DETAIL_NUMBERS.intersection(df.columns):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["updatetime"] = update_time.strftime("%Y-%m-%d %H:%M:%S")
    return df


def validate_frames(main_df: pd.DataFrame, detail_df: pd.DataFrame) -> None:
    if main_df.empty or detail_df.empty:
        raise RuntimeError("预留单主表或明细为空，保留数据库现有快照")
    if main_df["预留单ID"].isna().any() or main_df["预留单ID"].duplicated().any():
        raise RuntimeError("预留单ID 缺失或重复，保留数据库现有快照")
    if detail_df["明细ID"].isna().any() or detail_df["明细ID"].duplicated().any():
        raise RuntimeError("预留单明细ID 缺失或重复，保留数据库现有快照")
    main_ids = set(main_df["预留单ID"].astype(str))
    detail_ids = set(detail_df["预留单ID"].astype(str))
    orphan_ids = detail_ids - main_ids
    if orphan_ids:
        raise RuntimeError(f"发现 {len(orphan_ids)} 个无法关联主表的预留单ID，保留数据库现有快照")
    print("[INFO] validation passed: IDs unique and all details reference the main snapshot", flush=True)


def mysql_type(column: str) -> str:
    if column in MAIN_DATETIMES or column == "updatetime":
        return "DATETIME"
    if column in DETAIL_NUMBERS:
        return "DECIMAL(20,4)"
    if column in TEXT_COLUMNS:
        return "TEXT"
    return "VARCHAR(255)"


def create_sql(table: str, columns: list[str], indexes: list[str]) -> str:
    definitions = [f"`{c}` {mysql_type(c)}" for c in columns]
    definitions.extend(f"INDEX `idx_{c}` (`{c}`)" for c in indexes)
    return f"CREATE TABLE `{table}` ({','.join(definitions)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"


def clean_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]):
            out[col] = out[col].where(pd.notna(out[col]), "\\N")
        else:
            out[col] = out[col].fillna("\\N").astype(str)
            out.loc[out[col].isin(["", "nan", "None", "NaT", "<NA>"]), col] = "\\N"
    return out


def connect_db():
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            return pymysql.connect(**DB_CONFIG)
        except pymysql.MySQLError:
            if attempt == CONNECT_RETRIES:
                raise
            time.sleep(attempt * 5)


def write_pair(main_df: pd.DataFrame, detail_df: pd.DataFrame, main_table: str, detail_table: str) -> None:
    nonce = f"{os.getpid()}_{int(time.time())}"
    specs = [(main_df, main_table, ["预留单ID", "预留单号", "状态", "申请时间"]),
             (detail_df, detail_table, ["明细ID", "预留单ID", "预留单号", "货品编号", "条码"])]
    conn = connect_db()
    cursor = conn.cursor()
    staged: list[tuple[str, str, str]] = []
    files: list[str] = []
    try:
        cursor.execute("SET GLOBAL local_infile = 1")
        for df, table, indexes in specs:
            stage, old = f"{table}_tmp_{nonce}", f"{table}_old_{nonce}"
            path = os.path.join(tempfile.gettempdir(), stage + ".csv")
            clean_frame(df).to_csv(path, index=False, header=False, encoding="utf-8")
            files.append(path)
            cursor.execute(f"DROP TABLE IF EXISTS `{stage}`")
            cursor.execute(create_sql(stage, list(df.columns), indexes))
            cols = ",".join(f"`{c}`" for c in df.columns)
            cursor.execute(f"LOAD DATA LOCAL INFILE '{path.replace(os.sep, '/')}' INTO TABLE `{stage}` CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' ({cols})")
            staged.append((table, stage, old))
        rename_parts = []
        for table, stage, old in staged:
            cursor.execute(f"DROP TABLE IF EXISTS `{old}`")
            cursor.execute(f"SHOW TABLES LIKE %s", (table,))
            if cursor.fetchone():
                rename_parts.extend([f"`{table}` TO `{old}`", f"`{stage}` TO `{table}`"])
            else:
                rename_parts.append(f"`{stage}` TO `{table}`")
        cursor.execute("RENAME TABLE " + ",".join(rename_parts))
        for _, _, old in staged:
            cursor.execute(f"DROP TABLE IF EXISTS `{old}`")
        conn.commit()
        print(f"[INFO] imported {len(main_df)} rows into {DB_CONFIG['database']}.{main_table}", flush=True)
        print(f"[INFO] imported {len(detail_df)} rows into {DB_CONFIG['database']}.{detail_table}", flush=True)
    except Exception:
        conn.rollback()
        raise
    finally:
        for _, stage, _ in staged:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS `{stage}`")
            except Exception:
                pass
        cursor.close()
        conn.close()
        for path in files:
            try:
                os.remove(path)
            except OSError:
                pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同步吉客云预留单及货品明细")
    parser.add_argument("--curl", default=str(DEFAULT_CURL))
    parser.add_argument("--main-table", default="预留单查询")
    parser.add_argument("--detail-table", default="预留单货品明细")
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--detail-chunk-size", type=int, default=50)
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    updated = datetime.now().replace(microsecond=0)
    info = load_curl(args.curl)
    with requests.Session() as session:
        main_rows = fetch_main(session, info, args.page_size)
        detail_rows = fetch_details(session, info, main_rows, args.detail_chunk_size)
    main_df = normalize_frame(main_rows, MAIN_FIELDS, updated)
    detail_df = normalize_frame(detail_rows, DETAIL_FIELDS, updated, detail=True)
    validate_frames(main_df, detail_df)
    os.makedirs(DATA_DIR, exist_ok=True)
    main_df.to_csv(DEFAULT_MAIN_CSV, index=False, encoding="utf-8-sig")
    detail_df.to_csv(DEFAULT_DETAIL_CSV, index=False, encoding="utf-8-sig")
    print(f"[INFO] normalized main={len(main_df)}, detail={len(detail_df)}", flush=True)
    if not args.no_db:
        write_pair(main_df, detail_df, args.main_table, args.detail_table)
    print(f"[DONE] main={len(main_df)}, detail={len(detail_df)}", flush=True)


if __name__ == "__main__":
    main()
