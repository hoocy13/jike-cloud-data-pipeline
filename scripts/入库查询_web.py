"""Sync Jike purchase-inbound headers/details and expose a monthly brand view."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dt_time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode

import pymysql
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG

BASE_URL = "https://env3.jkyservice.com"
DETAIL_URL = f"{BASE_URL}/jkyun/erp-busiorder/goodsdoc/listGoodsDocDetail"
WEB_APP_KEY = "jackyun_web_browser_2024"
WEB_SIGN_SECRET = os.getenv("JKY_WEB_SIGN_SECRET", "")
ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CURL = ROOT / "curl" / "入库查询_curl.txt"
HEADER_TABLE = "入库查询"
DETAIL_TABLE = "入库查询明细"
MONTHLY_TABLE = "品牌月度到货"

HEADER_FIELDS = {
    "docId": "docId", "goodsdocNo": "入库单号", "outBillNo": "外部单号",
    "deliveryNo": "收货单号", "inOutDate": "入库时间", "inouttype": "入库类型编码",
    "inouttypeName": "入库类型", "warehouseId": "仓库ID", "warehouseName": "入库仓库",
    "channelName": "销售渠道", "billNo": "关联单号", "sourceBillNo": "来源单号",
    "financeBillStatus": "核算状态编码", "financeStatusName": "核算状态", "companyName": "公司",
    "createUserName": "制单人", "vendCustomerName": "往来单位", "inOutReason": "入库原因",
    "callbackStatus": "回传状态编码", "callbackStatusName": "回传状态", "goodsdocRemark": "备注",
    "receiveGoodsRemark": "收货备注", "gmtCreate": "系统入库时间", "priceStatus": "改价状态编码",
    "priceStatusName": "修改过入库价", "logisticName": "物流公司", "logisticNo": "物流单号",
    "projName": "辅助核算项目", "totalQuantity": "数量合计", "baseCostTotalAmount": "入库成本总金额",
    "baseNoTaxTotalAmount": "无税总金额本币", "baseHasTaxTotalAmount": "含税总金额本币",
    "baseTaxTotalAmount": "总税额本币", "baseTotalFee": "入库总费用本币", "currencyCodeName": "原币币种",
    "baceCurrencyName": "本币币种", "currencyRate": "汇率", "redStatus": "红冲状态", "source": "来源",
}

DETAIL_FIELDS = {
    "recId": "recId", "headId": "docId", "goodsdocNo": "入库单号", "goodsId": "货品ID",
    "goodsNo": "货品编号", "goodsName": "货品名称", "skuId": "SKU_ID", "skuName": "规格",
    "skuBarcode": "条码", "brandId": "品牌ID", "brandName": "品牌", "cateId": "货品分类ID",
    "cateName": "货品分类", "orderNum": "采购单号", "unitName": "单位", "quantity": "数量",
    "defectiveType": "次品类型编码", "defectiveTypeName": "次品类型",
    "baceCurrencyCostPrice": "入库成本单价", "baceCurrencyCostAmount": "入库成本金额",
    "baceCurrencyNoTaxPrice": "无税单价本币", "baceCurrencyNoTaxAmount": "无税金额本币",
    "baceCurrencyWithTaxPrice": "含税单价本币", "baceCurrencyWithTaxAmount": "含税金额本币",
    "baceCurrencyTaxAmount": "税额本币", "taxRate": "税率", "taxRateName": "税率名称",
    "isCertified": "正品标识", "isCertifiedName": "正品", "batchNo": "批次",
    "productionDate": "生产日期", "expirationDate": "到期日期", "shelfLife": "保质期",
    "shelfLiftUnit": "保质期单位", "manufacturer": "生产厂家", "goodsDetailRemark": "备注",
    "costCalcStatus": "成本计算状态", "detailSettStatus": "明细结算状态", "warehouseId": "仓库ID",
}

HEADER_DECIMALS = {"数量合计", "入库成本总金额", "无税总金额本币", "含税总金额本币", "总税额本币", "入库总费用本币", "汇率"}
DETAIL_DECIMALS = {"数量", "入库成本单价", "入库成本金额", "无税单价本币", "无税金额本币", "含税单价本币", "含税金额本币", "税额本币", "税率", "保质期"}
DATETIME_COLS = {"入库时间", "系统入库时间"}
DATE_COLS = {"生产日期", "到期日期"}


def normalize_curl_text(text: str) -> str:
    return text.replace("^\r\n", " ").replace("^\n", " ").replace("^", "")


def parse_curl(path: Path) -> dict[str, Any]:
    tokens = shlex.split(normalize_curl_text(path.read_text(encoding="utf-8-sig")), posix=True)
    headers: dict[str, str] = {}
    url = cookie = raw_data = ""
    i = 1
    while i < len(tokens):
        token = tokens[i]
        if token in ("-H", "--header"):
            i += 1
            key, value = tokens[i].split(":", 1)
            headers[key.strip().lower()] = value.strip()
        elif token in ("-b", "--cookie"):
            i += 1; cookie = tokens[i]
        elif token in ("--data-raw", "--data", "-d"):
            i += 1; raw_data = tokens[i]
        elif not token.startswith("-") and not url:
            url = token
        i += 1
    if "listGoodsDoc" not in url or not raw_data:
        raise ValueError("请提供 listGoodsDoc 的 Copy-as-cURL")
    return {"url": url, "headers": headers, "cookie": cookie, "params": dict(parse_qsl(raw_data, keep_blank_values=True))}


def signed_params(params: dict[str, Any], authorization: str) -> dict[str, str]:
    out = {k: "" if v is None else str(v) for k, v in params.items()}
    out.update(timestamp=str(int(time.time() * 1000)), access_token=authorization, appkey=WEB_APP_KEY)
    out.pop("sign", None)
    payload = "".join(k + v for k, v in sorted(out.items()) if v != "")
    out["sign"] = hashlib.md5((WEB_SIGN_SECRET + payload + WEB_SIGN_SECRET).encode()).hexdigest().upper()
    return out


def web_headers(info: dict[str, Any]) -> dict[str, str]:
    source = info["headers"]
    authorization = source.get("authorization")
    if not authorization:
        raise ValueError("cURL 缺少 authorization")
    result = {
        "accept": source.get("accept", "text/plain, */*; q=0.01"), "authorization": authorization,
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "module_code": source.get("module_code", "stockIn_List"), "origin": BASE_URL,
        "referer": source.get("referer", f"{BASE_URL}/"), "user-agent": source.get("user-agent", "Mozilla/5.0"),
        "x-requested-with": "XMLHttpRequest",
    }
    if source.get("ati"): result["ati"] = source["ati"]
    if info.get("cookie"): result["cookie"] = info["cookie"]
    return result


def request_rows(url: str, info: dict[str, Any], params: dict[str, Any], retries: int = 4) -> list[dict[str, Any]]:
    headers = web_headers(info)
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(url, headers=headers, data=urlencode(signed_params(params, headers["authorization"])), timeout=90)
            payload = response.json()
            if response.status_code >= 400:
                raise RuntimeError(f"HTTP {response.status_code}: {response.text[:300]}")
            if payload.get("code") not in (None, 0, 200, "0", "200"):
                raise RuntimeError(f"业务请求失败: {json.dumps(payload, ensure_ascii=False)[:500]}")
            result = payload.get("result")
            rows = result.get("data") if isinstance(result, dict) else None
            if isinstance(result, dict) and rows is None:
                return []
            if not isinstance(rows, list):
                raise RuntimeError(f"响应中没有 result.data: {json.dumps(payload, ensure_ascii=False)[:500]}")
            return rows
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(min(2 ** (attempt - 1), 8))
    raise RuntimeError(str(last_error))


def paged_rows(url: str, info: dict[str, Any], base: dict[str, Any], page_size: int, label: str) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    previous_keys: tuple[str, ...] | None = None
    for page in range(10000):
        params = dict(base); params.update(pageIndex=str(page), pageSize=str(page_size))
        rows = request_rows(url, info, params)
        keys = tuple(str(r.get("docId") or r.get("recId") or "") for r in rows)
        if rows and keys == previous_keys:
            raise RuntimeError(f"{label} 第 {page} 页与上页重复，停止以避免死循环")
        previous_keys = keys
        all_rows.extend(rows)
        if len(rows) < page_size: break
    else:
        raise RuntimeError(f"{label} 超过最大页数")
    return all_rows


def api_end(end: datetime) -> str:
    return (end - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")


def fetch_headers(info: dict[str, Any], start: datetime, end: datetime, page_size: int) -> list[dict[str, Any]]:
    params = dict(info["params"])
    params.update(inouttypes="101", inOrOut="1", redDoc="1",
                  archived=str(info["params"].get("archived", "0")), serviceType="goodsdoc.search",
                  inOutDateStart=start.strftime("%Y-%m-%d %H:%M:%S"), inOutDateEnd=api_end(end),
                  cols=json.dumps(list(HEADER_FIELDS), ensure_ascii=False, separators=(",", ":")))
    rows = paged_rows(info["url"], info, params, page_size, "入库主单")
    unique = {str(row.get("docId")): row for row in rows if row.get("docId") is not None}
    if len(unique) != len(rows):
        print(f"[WARN] 主单去重: {len(rows)} -> {len(unique)}", flush=True)
    return list(unique.values())


def detail_params(info: dict[str, Any], doc_id: str) -> dict[str, Any]:
    source = info["params"]
    return {
        "archived": source.get("archived", "0"), "docId": doc_id, "ownerId": source.get("ownerId", ""),
        "ownerName": source.get("ownerName", ""), "serviceType": "goodsdoc.detail.search", "sortField": "", "sortOrder": "",
        "cols": json.dumps(list(DETAIL_FIELDS), ensure_ascii=False, separators=(",", ":")), "isShowForSerial": "0", "jlinkId": "",
    }


def fetch_one_detail(info: dict[str, Any], doc_id: str, page_size: int) -> tuple[str, list[dict[str, Any]]]:
    rows = paged_rows(DETAIL_URL, info, detail_params(info, doc_id), page_size, f"明细 docId={doc_id}")
    for row in rows:
        row.setdefault("headId", doc_id)
    return doc_id, rows


def fetch_details(info: dict[str, Any], doc_ids: list[str], page_size: int, workers: int) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_one_detail, info, doc_id, page_size): doc_id for doc_id in doc_ids}
        done = 0
        for future in as_completed(futures):
            doc_id, rows = future.result()
            result.extend(rows); done += 1
            if done % 100 == 0 or done == len(doc_ids):
                print(f"[INFO] 明细进度 {done}/{len(doc_ids)}, 累计 {len(result)} 行", flush=True)
    unique = {str(row.get("recId")): row for row in result if row.get("recId") is not None}
    if len(unique) != len(result):
        print(f"[WARN] 明细去重: {len(result)} -> {len(unique)}", flush=True)
    return list(unique.values())


def as_datetime(value: Any, date_only: bool = False) -> str | None:
    if value in (None, "", 0, "0"): return None
    try:
        if isinstance(value, (int, float)) or str(value).isdigit():
            number = float(value); parsed = datetime.fromtimestamp(number / 1000 if number > 10_000_000_000 else number)
        else:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
        return parsed.strftime("%Y-%m-%d" if date_only else "%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError, OverflowError):
        return None


def as_decimal(value: Any) -> Decimal | None:
    if value in (None, ""): return None
    try: return Decimal(str(value).replace(",", ""))
    except InvalidOperation: return None


def normalize(rows: list[dict[str, Any]], mapping: dict[str, str], decimals: set[str], updated: str) -> list[dict[str, Any]]:
    output = []
    for row in rows:
        item = {target: row.get(source) for source, target in mapping.items()}
        for col in decimals: item[col] = as_decimal(item.get(col))
        for col in DATETIME_COLS & item.keys(): item[col] = as_datetime(item.get(col))
        for col in DATE_COLS & item.keys(): item[col] = as_datetime(item.get(col), True)
        item["updatetime"] = updated
        output.append(item)
    return output


def column_type(name: str, decimals: set[str]) -> str:
    if name in ("docId", "recId", "货品ID", "SKU_ID", "品牌ID", "货品分类ID", "仓库ID"): return "VARCHAR(32)"
    if name in decimals: return "DECIMAL(24,6)"
    if name in DATETIME_COLS or name == "updatetime": return "DATETIME"
    if name in DATE_COLS: return "DATE"
    if name in ("货品名称", "备注", "收货备注"): return "TEXT"
    return "VARCHAR(255)"


def create_table_sql(table: str, mapping: dict[str, str], decimals: set[str], primary: str, indexes: list[tuple[str, str]]) -> str:
    cols = list(mapping.values()) + ["updatetime"]
    definitions = [f"`{c}` {column_type(c, decimals)}" for c in cols]
    definitions.append(f"PRIMARY KEY (`{primary}`)")
    definitions.extend(f"INDEX `{name}` (`{col}`)" for name, col in indexes)
    return f"CREATE TABLE IF NOT EXISTS `{table}` ({', '.join(definitions)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"


def insert_rows(cursor: Any, table: str, rows: list[dict[str, Any]], columns: list[str]) -> None:
    if not rows: return
    sql = f"INSERT INTO `{table}` ({', '.join(f'`{c}`' for c in columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
    cursor.executemany(sql, [tuple(row.get(c) for c in columns) for row in rows])


def ensure_schema(cursor: Any) -> None:
    cursor.execute(create_table_sql(HEADER_TABLE, HEADER_FIELDS, HEADER_DECIMALS, "docId", [("idx_入库时间", "入库时间"), ("idx_入库单号", "入库单号"), ("idx_往来单位", "往来单位")]))
    cursor.execute(create_table_sql(DETAIL_TABLE, DETAIL_FIELDS, DETAIL_DECIMALS, "recId", [("idx_docId", "docId"), ("idx_货品编号", "货品编号"), ("idx_品牌", "品牌"), ("idx_批次", "批次")]))
    cursor.execute(
        "SELECT `TABLE_TYPE` FROM `information_schema`.`TABLES` "
        "WHERE `TABLE_SCHEMA` = DATABASE() AND `TABLE_NAME` = %s",
        (MONTHLY_TABLE,),
    )
    monthly_object = cursor.fetchone()
    if monthly_object and monthly_object[0] == "VIEW":
        cursor.execute(f"DROP VIEW `{MONTHLY_TABLE}`")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS `{MONTHLY_TABLE}` (
            `到货月份` DATE NOT NULL,
            `品牌ID` VARCHAR(32) NULL,
            `品牌` VARCHAR(255) NOT NULL,
            `毛到货数量` DECIMAL(30,6) NOT NULL DEFAULT 0,
            `红冲数量` DECIMAL(30,6) NOT NULL DEFAULT 0,
            `净到货数量` DECIMAL(30,6) NOT NULL DEFAULT 0,
            `净到货成本金额` DECIMAL(30,6) NULL,
            `入库单数` BIGINT UNSIGNED NOT NULL DEFAULT 0,
            `SKU数` BIGINT UNSIGNED NOT NULL DEFAULT 0,
            `供应商数` BIGINT UNSIGNED NOT NULL DEFAULT 0,
            `updatetime` DATETIME NULL,
            INDEX `idx_到货月份` (`到货月份`),
            INDEX `idx_品牌ID` (`品牌ID`),
            INDEX `idx_品牌` (`品牌`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


def refresh_monthly_table(cursor: Any) -> None:
    cursor.execute(f"DELETE FROM `{MONTHLY_TABLE}`")
    cursor.execute(f"""
        INSERT INTO `{MONTHLY_TABLE}` (
            `到货月份`, `品牌ID`, `品牌`, `毛到货数量`, `红冲数量`,
            `净到货数量`, `净到货成本金额`, `入库单数`, `SKU数`,
            `供应商数`, `updatetime`
        )
        SELECT CAST(DATE_FORMAT(h.`入库时间`, '%Y-%m-01') AS DATE) AS `到货月份`,
               d.`品牌ID`, COALESCE(NULLIF(d.`品牌`, ''), '未维护品牌') AS `品牌`,
               SUM(CASE WHEN d.`数量` > 0 THEN d.`数量` ELSE 0 END) AS `毛到货数量`,
               SUM(CASE WHEN d.`数量` < 0 THEN -d.`数量` ELSE 0 END) AS `红冲数量`,
               SUM(d.`数量`) AS `净到货数量`, SUM(d.`入库成本金额`) AS `净到货成本金额`,
               COUNT(DISTINCT h.`docId`) AS `入库单数`, COUNT(DISTINCT d.`货品编号`) AS `SKU数`,
               COUNT(DISTINCT h.`往来单位`) AS `供应商数`, MAX(GREATEST(h.`updatetime`, d.`updatetime`)) AS `updatetime`
        FROM `{HEADER_TABLE}` h JOIN `{DETAIL_TABLE}` d ON d.`docId` = h.`docId`
        WHERE h.`入库类型编码` = '101'
        GROUP BY CAST(DATE_FORMAT(h.`入库时间`, '%Y-%m-01') AS DATE), d.`品牌ID`, COALESCE(NULLIF(d.`品牌`, ''), '未维护品牌')
    """)


def write_window(headers: list[dict[str, Any]], details: list[dict[str, Any]], start: datetime, end: datetime) -> None:
    suffix = uuid.uuid4().hex[:10]
    hs, ds, ids = f"{HEADER_TABLE}_stage_{suffix}", f"{DETAIL_TABLE}_stage_{suffix}", f"{HEADER_TABLE}_ids_{suffix}"
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            ensure_schema(cur)
            cur.execute(f"CREATE TABLE `{hs}` LIKE `{HEADER_TABLE}`")
            cur.execute(f"CREATE TABLE `{ds}` LIKE `{DETAIL_TABLE}`")
            cur.execute(f"CREATE TABLE `{ids}` (`docId` VARCHAR(32) PRIMARY KEY) ENGINE=InnoDB")
            insert_rows(cur, hs, headers, list(HEADER_FIELDS.values()) + ["updatetime"])
            insert_rows(cur, ds, details, list(DETAIL_FIELDS.values()) + ["updatetime"])
            cur.executemany(f"INSERT INTO `{ids}` (`docId`) VALUES (%s)", [(h["docId"],) for h in headers])
            conn.commit()

            conn.begin()
            cur.execute(f"DELETE d FROM `{DETAIL_TABLE}` d JOIN `{ids}` i ON i.`docId`=d.`docId`")
            # Also clear children of headers that used to be in the window but are no
            # longer returned (deleted/moved upstream). This makes an empty window safe.
            cur.execute(
                f"DELETE d FROM `{DETAIL_TABLE}` d JOIN `{HEADER_TABLE}` h ON h.`docId`=d.`docId` "
                f"WHERE h.`入库时间` >= %s AND h.`入库时间` < %s",
                (start, end),
            )
            cur.execute(f"DELETE FROM `{HEADER_TABLE}` WHERE `入库时间` >= %s AND `入库时间` < %s", (start, end))
            cur.execute(f"INSERT INTO `{HEADER_TABLE}` SELECT * FROM `{hs}`")
            cur.execute(f"INSERT INTO `{DETAIL_TABLE}` SELECT * FROM `{ds}`")
            refresh_monthly_table(cur)
            conn.commit()
            print(f"[INFO] 已写入 {len(headers)} 张主单、{len(details)} 行明细", flush=True)
    except Exception:
        conn.rollback(); raise
    finally:
        try:
            with conn.cursor() as cur:
                for table in (hs, ds, ids): cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            conn.commit()
        finally: conn.close()


def parse_datetime(value: str, end: bool = False) -> datetime:
    try: return datetime.fromisoformat(value)
    except ValueError:
        parsed = date.fromisoformat(value)
        return datetime.combine(parsed + (timedelta(days=1) if end else timedelta()), dt_time.min)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同步采购入库主单、明细及品牌月度到货汇总表")
    parser.add_argument("--curl", default=str(DEFAULT_CURL)); parser.add_argument("--start")
    parser.add_argument("--end", help="结束时间（不含）；仅日期时自动取次日0点")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--page-size", type=int, default=200); parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    now = datetime.now().replace(microsecond=0)
    end = parse_datetime(args.end, True) if args.end else now
    start = parse_datetime(args.start) if args.start else datetime.combine(end.date() - timedelta(days=args.lookback_days - 1), dt_time.min)
    if start >= end: raise ValueError("start 必须早于 end")
    if args.lookback_days <= 0 or args.page_size <= 0 or args.workers <= 0: raise ValueError("数值参数必须大于0")
    info = parse_curl(Path(args.curl))
    print(f"[INFO] 范围 [{start}, {end})，固定入库类型=101(采购入库)", flush=True)
    raw_headers = fetch_headers(info, start, end, args.page_size)
    doc_ids = [str(row["docId"]) for row in raw_headers]
    print(f"[INFO] 主单 {len(doc_ids)} 张，开始抓取明细", flush=True)
    raw_details = fetch_details(info, doc_ids, args.page_size, args.workers) if doc_ids else []
    updated = now.strftime("%Y-%m-%d %H:%M:%S")
    headers = normalize(raw_headers, HEADER_FIELDS, HEADER_DECIMALS, updated)
    details = normalize(raw_details, DETAIL_FIELDS, DETAIL_DECIMALS, updated)
    if args.no_db:
        print(f"[DONE] 校验通过：{len(headers)} 张主单，{len(details)} 行明细（未入库）", flush=True); return
    write_window(headers, details, start, end)
    print(f"[DONE] 采购入库同步完成：{len(headers)} 张主单，{len(details)} 行明细", flush=True)


if __name__ == "__main__": main()
