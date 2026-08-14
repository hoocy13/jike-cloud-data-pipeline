"""同步抖音货权转移采购单，并构建销售单金额补全 DWD 表。"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import sys
import time
import zipfile
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pymysql
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from config import DB_CONFIG


DEFAULT_LIST_CURL = ROOT / "curl" / "进口超市上海仓_货权转移采购单_curl.txt"
DEFAULT_EXPORT_CURL = ROOT / "curl" / "进口超市上海仓_货权转移采购单导出_curl.txt"
DEFAULT_PROGRESS_CURL = ROOT / "curl" / "进口超市上海仓_货权转移采购单进度_curl.txt"
DEFAULT_EXPORT_DIR = ROOT / "data" / "进口超市上海仓_货权转移采购单_exports"
ODS_PO_TABLE = "进口超市上海仓_货权转移采购单"
ODS_PO_DETAIL_TABLE = "进口超市上海仓_货权转移采购单明细"
DWD_TABLE = "销售单查询_进口超市上海仓补全"
TMALL_MAPPING_TABLE = "天猫国际自营_销售单金额补全映射"
SOURCE_CHANNEL = "进口超市上海仓"
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_PAGE_SIZE = 200
DEFAULT_WINDOW_DAYS = 7
EXPORT_LOCK_TIMEOUT_SECONDS = 3600
BUILD_LOCK_NAME = "jike_trade_export:build_import_supermarket_dwd"
DB_CONNECT_RETRIES = 5
DB_TRANSACTION_RETRIES = 5
DB_RETRY_DELAY_SECONDS = 10
RETRYABLE_DB_ERROR_CODES = {1205, 1213, 2003, 2006, 2013}


class ExportLimitError(RuntimeError):
    pass


def connect_mysql(schema: str, retries: int = DB_CONNECT_RETRIES) -> Any:
    """Connect with bounded retries and longer socket timeouts for remote MySQL."""
    config = db_config(schema)
    config.setdefault("connect_timeout", 20)
    config.setdefault("read_timeout", 300)
    config.setdefault("write_timeout", 300)
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return pymysql.connect(**config)
        except pymysql.MySQLError as exc:
            last_error = exc
            if attempt >= retries:
                raise
            print(
                f"[WARN] MySQL {schema} connection failed; retrying in "
                f"{DB_RETRY_DELAY_SECONDS}s ({attempt}/{retries}): {exc}",
                flush=True,
            )
            time.sleep(DB_RETRY_DELAY_SECONDS)
    raise RuntimeError("unreachable") from last_error


def retryable_db_error(exc: BaseException) -> bool:
    return (
        isinstance(exc, pymysql.MySQLError)
        and bool(exc.args)
        and exc.args[0] in RETRYABLE_DB_ERROR_CODES
    )

FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "采购单号": ("orderNo", "purchaseOrderNo", "poNo"),
    "业务单号": ("salesOrderNo", "bizOrderNo", "businessOrderNo", "businessNo", "bizNo", "sourceOrderNo", "tradeNo"),
    "状态编码": ("status", "orderStatus", "statusCode"),
    "状态": ("statusDesc", "statusName", "orderStatusName"),
    "供应商ID": ("supplierId", "vendorId"),
    "供应商": ("supplierName", "vendorName"),
    "仓库ID": ("destLocationCode", "warehouseId", "warehouseCode", "actualWarehouseId"),
    "实际送货仓库": ("destLocationName", "warehouseName", "actualWarehouseName", "deliveryWarehouseName"),
    "采购SKU数": ("skuAmount", "skuCount", "skuNum", "purchaseSkuCount", "cargoCount"),
    "采购总数量": ("totalQuantity", "totalCount", "purchaseQuantity", "quantity"),
    "采购总金额": ("totalPriceInCent", "totalAmount", "purchaseTotalAmount", "purchaseAmount", "orderAmount", "totalPrice"),
    "补贴金额": ("subsidyAmount", "allowanceAmount", "subsidyFee"),
    "采购单标签": ("tags", "tagList", "orderTags", "labelList"),
    "创建时间": ("createTimeInSec", "createTime", "createdAt", "gmtCreate"),
    "完结时间": ("finishTimeInSec", "finishTime", "completeTime", "completedAt", "gmtFinished"),
}

DETAIL_COLUMNS = (
    "采购单明细号", "采购单号", "业务单号", "cargoId", "productId", "spuId", "条码", "品牌ID", "品牌",
    "货品名称", "规格", "数量", "含税单价", "不含税单价", "是否赠品", "源数据JSON", "updatetime",
)


def normalize_curl_text(text: str) -> str:
    return text.replace("^\r\n", " ").replace("^\n", " ").replace("^", "")


def parse_generic_curl(raw: str) -> dict[str, Any]:
    tokens = shlex.split(normalize_curl_text(raw), posix=True)
    if not tokens or tokens[0].lower() != "curl":
        raise ValueError("文件内容不是完整的 Copy-as-cURL 命令")
    url = ""
    headers: dict[str, str] = {}
    cookie = ""
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
        elif not token.startswith("-") and not url:
            url = token
        index += 1
    if not url:
        raise ValueError("cURL 中缺少 URL")
    if not cookie:
        raise ValueError("cURL 中缺少 Cookie")
    return {"url": url, "headers": headers, "cookie": cookie}


def parse_curl_text(raw: str) -> dict[str, Any]:
    info = parse_generic_curl(raw)
    if "/api/procurement/po/list" not in info["url"]:
        raise ValueError("请提供货权转移采购单 po/list 的 cURL")
    return info


def load_curl(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"请把 po/list cURL 保存到 {path}")
    return parse_curl_text(path.read_text(encoding="utf-8-sig"))


def load_endpoint_curl(path: Path, endpoint: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"请把 {endpoint} cURL 保存到 {path}")
    info = parse_generic_curl(path.read_text(encoding="utf-8-sig"))
    if endpoint not in info["url"]:
        raise ValueError(f"{path} 不是 {endpoint} 的 cURL")
    return info


def inherit_auth(endpoint_info: dict[str, Any], auth_info: dict[str, Any]) -> dict[str, Any]:
    """保留目标接口 URL，但统一使用已更新导出请求的登录态。"""
    result = {
        "url": endpoint_info["url"],
        "headers": dict(endpoint_info["headers"]),
        "cookie": auth_info["cookie"],
    }
    for name in ("accept", "accept-language", "menukey", "referer", "user-agent"):
        if auth_info["headers"].get(name):
            result["headers"][name] = auth_info["headers"][name]
    return result


def request_headers(info: dict[str, Any]) -> dict[str, str]:
    source = info["headers"]
    return {
        "accept": source.get("accept", "*/*"),
        "accept-language": source.get("accept-language", "zh-CN,zh;q=0.9"),
        "cookie": info["cookie"],
        "menukey": source.get("menukey", "/cargo-right-transfer/list"),
        "referer": source.get("referer", "https://bscm.jinritemai.com/views/cargo-right-transfer/list"),
        "user-agent": source.get("user-agent", "Mozilla/5.0"),
    }


def build_url(info: dict[str, Any], start: datetime, end: datetime, page: int, page_size: int) -> str:
    parsed = urlparse(info["url"])
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update(
        orderType="5",
        createTimeStart=str(int(start.timestamp())),
        createTimeEnd=str(int(end.timestamp())),
        page=str(page),
        pageSize=str(page_size),
    )
    return urlunparse(parsed._replace(query=urlencode(query)))


def api_json(session: requests.Session, url: str, headers: dict[str, str], retries: int = 4) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, headers=headers, timeout=(30, 120))
            payload = response.json()
            if response.status_code >= 400:
                raise RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")
            if isinstance(payload, dict) and payload.get("code") not in (None, 0, "0", 200, "200"):
                raise RuntimeError(f"接口返回失败: {json.dumps(payload, ensure_ascii=False)[:800]}")
            return payload.get("data", payload) if isinstance(payload, dict) else payload
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt == retries:
                break
            wait = min(2 ** attempt, 15)
            print(f"[WARN] 请求失败，{wait} 秒后重试 {attempt}/{retries}: {exc}", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"接口请求最终失败: {last_error}")


def subject_aid(url: str) -> str:
    return dict(parse_qsl(urlparse(url).query, keep_blank_values=True)).get("subject_aid", "305219")


def build_export_url(info: dict[str, Any], start: datetime, end: datetime) -> str:
    parsed = urlparse(info["url"])
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params = json.loads(query.get("queryParams") or "{}")
    params.update(
        orderType=5,
        createTimeStart=str(int(start.timestamp())),
        createTimeEnd=str(int(end.timestamp())),
    )
    query["bizType"] = "TransferProcurementOrder"
    query["queryParams"] = json.dumps(params, ensure_ascii=False, separators=(",", ":"))
    return urlunparse(parsed._replace(query=urlencode(query)))


def build_progress_url(info: dict[str, Any], task_id: str) -> str:
    parsed = urlparse(info["url"])
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["taskId"] = str(task_id)
    return urlunparse(parsed._replace(query=urlencode(query)))


def start_export(session: requests.Session, info: dict[str, Any], start: datetime, end: datetime) -> str:
    payload = api_json(session, build_export_url(info, start, end), request_headers(info))
    task_id = payload.get("taskId") if isinstance(payload, dict) else None
    if not task_id:
        raise RuntimeError(f"创建导出任务后未找到 taskId: {payload}")
    print(f"[INFO] export task={task_id}, window={start} ~ {end}", flush=True)
    return str(task_id)


def poll_export(
    session: requests.Session,
    info: dict[str, Any],
    task_id: str,
    timeout_seconds: int,
    interval_seconds: int,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        payload = api_json(session, build_progress_url(info, task_id), request_headers(info))
        if not isinstance(payload, dict):
            raise RuntimeError(f"任务进度格式异常: {payload}")
        status = int(payload.get("taskStatus", 0))
        if status == 30:
            print(
                f"[INFO] export completed: task={task_id}, rows={payload.get('totalRecordCount')}",
                flush=True,
            )
            return payload
        if status == -10:
            reason = str(payload.get("failReason") or "")
            if "100000" in reason or "超出限制数量" in reason:
                raise ExportLimitError(reason)
            raise RuntimeError(f"导出任务失败: {payload}")
        print(f"[INFO] waiting export task={task_id}, status={status}", flush=True)
        time.sleep(interval_seconds)
    raise TimeoutError(f"等待导出任务超时: {task_id}")


def download_export(
    session: requests.Session,
    progress_info: dict[str, Any],
    task_id: str,
    file_name: str,
    export_dir: Path,
) -> Path:
    parsed = urlparse(progress_info["url"])
    url = f"{parsed.scheme}://{parsed.netloc}/api/gei/downloadFile"
    response = session.get(
        url,
        headers=request_headers(progress_info),
        params={"taskId": task_id, "subject_aid": subject_aid(progress_info["url"])},
        timeout=(30, 300),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"下载失败 ({response.status_code}): {response.text[:500]}")
    if response.headers.get("content-type", "").lower().startswith("application/json"):
        raise RuntimeError(f"下载接口返回 JSON: {response.text[:800]}")
    export_dir.mkdir(parents=True, exist_ok=True)
    safe_name = Path(file_name or f"货权转移采购单_{task_id}.xlsx").name
    path = export_dir / f"{task_id}_{safe_name}"
    path.write_bytes(response.content)
    print(f"[INFO] downloaded {path} ({len(response.content)} bytes)", flush=True)
    return path


def walk_dicts(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_dicts(child)


def find_rows(payload: Any) -> list[dict[str, Any]]:
    candidates: list[list[dict[str, Any]]] = []
    for node in walk_dicts(payload):
        for key in ("list", "records", "items", "rows", "data", "result"):
            value = node.get(key)
            if isinstance(value, list) and (not value or isinstance(value[0], dict)):
                candidates.append(value)
    if not candidates:
        return []
    return max(candidates, key=lambda rows: sum(1 for row in rows if any(str(v).startswith("PO") for v in row.values())))


def find_total(payload: Any, rows: list[dict[str, Any]]) -> int:
    for node in walk_dicts(payload):
        for key in ("total", "totalCount", "count", "totalNum"):
            value = node.get(key)
            if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
                number = int(value)
                if number >= len(rows):
                    return number
    return len(rows)


def flatten_dict(value: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, child in value.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(child, dict):
            result.update(flatten_dict(child, path))
        else:
            result[path] = child
            result.setdefault(key, child)
    return result


def pick(flat: dict[str, Any], aliases: tuple[str, ...]) -> Any:
    lower = {key.lower(): value for key, value in flat.items()}
    for alias in aliases:
        if alias.lower() in lower and lower[alias.lower()] not in (None, ""):
            return lower[alias.lower()]
    for alias in aliases:
        suffix = "." + alias.lower()
        for key, value in lower.items():
            if key.endswith(suffix) and value not in (None, ""):
                return value
    return None


def infer_po_no(flat: dict[str, Any]) -> str | None:
    value = pick(flat, FIELD_ALIASES["采购单号"])
    if value:
        return str(value)
    for value in flat.values():
        if re.fullmatch(r"PO\d{12,}", str(value or "")):
            return str(value)
    return None


def normalize_time(value: Any) -> datetime | None:
    if value in (None, "", "-"):
        return None
    if isinstance(value, (int, float)) or str(value).isdigit():
        number = int(value)
        if number > 10_000_000_000:
            number //= 1000
        try:
            return datetime.fromtimestamp(number)
        except (OSError, OverflowError, ValueError):
            return None
    text = str(value).replace("T", " ").replace("Z", "")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def normalize_decimal(value: Any) -> Decimal | None:
    if value in (None, "", "-"):
        return None
    text = re.sub(r"[^0-9.\-]", "", str(value))
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def normalize_label(value: Any) -> str | None:
    if value in (None, "", []):
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def normalize_po_row(row: dict[str, Any], synced_at: datetime) -> tuple[Any, ...]:
    flat = flatten_dict(row)
    values: dict[str, Any] = {}
    for name, aliases in FIELD_ALIASES.items():
        values[name] = pick(flat, aliases)
    values["采购单号"] = infer_po_no(flat)
    if not values["采购单号"]:
        raise RuntimeError(f"采购单记录中未找到 PO 单号，字段为: {sorted(row.keys())}")
    for name in ("采购总金额", "补贴金额", "采购SKU数", "采购总数量"):
        values[name] = normalize_decimal(values[name])
    if pick(flat, ("totalPriceInCent",)) is not None and values["采购总金额"] is not None:
        values["采购总金额"] /= Decimal("100")
    for name in ("创建时间", "完结时间"):
        values[name] = normalize_time(values[name])
    values["采购单标签"] = normalize_label(values["采购单标签"])
    return tuple(values[name] for name in FIELD_ALIASES) + (
        json.dumps(row, ensure_ascii=False, separators=(",", ":")),
        synced_at,
    )


def normalize_detail_rows(row: dict[str, Any], synced_at: datetime) -> list[tuple[Any, ...]]:
    order_no = str(row.get("orderNo") or "")
    business_no = str(row.get("salesOrderNo") or "")
    result = []
    for detail in row.get("detailVOs") or []:
        if not isinstance(detail, dict):
            continue
        price = normalize_decimal(detail.get("priceInCent"))
        no_tax_price = normalize_decimal(detail.get("priceInCentWithoutTaxrate"))
        result.append((
            detail.get("detailNo"), order_no, business_no, detail.get("cargoId"), detail.get("productId"),
            detail.get("spuId"), detail.get("barCode"), detail.get("brandId"), detail.get("brandName"),
            detail.get("cargoName"), detail.get("specsDesc"), normalize_decimal(detail.get("quantity")),
            price / Decimal("100") if price is not None else None,
            no_tax_price / Decimal("100") if no_tax_price is not None else None,
            1 if detail.get("isGift") else 0,
            json.dumps(detail, ensure_ascii=False, separators=(",", ":")), synced_at,
        ))
    return result


def db_config(database: str) -> dict[str, Any]:
    config = dict(DB_CONFIG)
    config["database"] = database
    return config


def ensure_ods_table(cursor: Any) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `ods`.`{ODS_PO_TABLE}` (
          `采购单号` VARCHAR(40) NOT NULL,
          `业务单号` VARCHAR(80) NULL,
          `状态编码` VARCHAR(40) NULL,
          `状态` VARCHAR(80) NULL,
          `供应商ID` VARCHAR(80) NULL,
          `供应商` VARCHAR(255) NULL,
          `仓库ID` VARCHAR(80) NULL,
          `实际送货仓库` VARCHAR(255) NULL,
          `采购SKU数` DECIMAL(18,0) NULL,
          `采购总数量` DECIMAL(18,0) NULL,
          `采购总金额` DECIMAL(18,2) NULL,
          `补贴金额` DECIMAL(18,2) NULL,
          `采购单标签` TEXT NULL,
          `创建时间` DATETIME NULL,
          `完结时间` DATETIME NULL,
          `源数据JSON` JSON NOT NULL,
          `updatetime` DATETIME NOT NULL,
          PRIMARY KEY (`采购单号`),
          INDEX `idx_业务单号` (`业务单号`),
          INDEX `idx_创建时间` (`创建时间`),
          INDEX `idx_仓库` (`实际送货仓库`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='抖音进口超市上海仓货权转移采购单原始接口数据'
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `ods`.`{ODS_PO_DETAIL_TABLE}` (
          `采购单明细号` VARCHAR(160) NOT NULL, `采购单号` VARCHAR(40) NOT NULL, `业务单号` VARCHAR(80) NULL,
          `cargoId` VARCHAR(40) NULL, `productId` VARCHAR(40) NULL, `spuId` VARCHAR(40) NULL,
          `条码` VARCHAR(100) NULL, `品牌ID` VARCHAR(40) NULL, `品牌` VARCHAR(255) NULL,
          `货品名称` TEXT NULL, `规格` TEXT NULL, `数量` DECIMAL(18,0) NULL,
          `含税单价` DECIMAL(18,2) NULL, `不含税单价` DECIMAL(18,2) NULL, `是否赠品` TINYINT(1) NOT NULL,
          `源数据JSON` JSON NOT NULL, `updatetime` DATETIME NOT NULL,
          PRIMARY KEY (`采购单明细号`), INDEX `idx_采购单号` (`采购单号`), INDEX `idx_业务单号` (`业务单号`),
          INDEX `idx_cargoId` (`cargoId`), INDEX `idx_productId` (`productId`), INDEX `idx_品牌` (`品牌`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='抖音进口超市上海仓货权转移采购单货品明细'
        """
    )
    # 导出中的商品 ID 偶尔是一对多的逗号组合，明细幂等键可能超过早期设计的 80 字符。
    cursor.execute(
        "SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.columns "
        "WHERE table_schema='ods' AND table_name=%s AND column_name='采购单明细号'",
        (ODS_PO_DETAIL_TABLE,),
    )
    detail_key_length = cursor.fetchone()
    if not detail_key_length or int(detail_key_length[0] or 0) < 160:
        cursor.execute(
            f"ALTER TABLE `ods`.`{ODS_PO_DETAIL_TABLE}` MODIFY `采购单明细号` VARCHAR(160) NOT NULL"
        )


def upsert_po_rows(rows: list[tuple[Any, ...]], details: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    columns = list(FIELD_ALIASES) + ["源数据JSON", "updatetime"]
    placeholders = ",".join(["%s"] * len(columns))
    updates = ",".join(f"`{column}`=VALUES(`{column}`)" for column in columns[1:])
    sql = (
        f"INSERT INTO `ods`.`{ODS_PO_TABLE}` ({','.join(f'`{c}`' for c in columns)}) "
        f"VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
    )
    for attempt in range(1, DB_TRANSACTION_RETRIES + 1):
        connection = None
        try:
            connection = connect_mysql("ods")
            with connection.cursor() as cursor:
                ensure_ods_table(cursor)
                cursor.executemany(sql, rows)
                if details:
                    detail_placeholders = ",".join(["%s"] * len(DETAIL_COLUMNS))
                    detail_updates = ",".join(
                        f"`{column}`=VALUES(`{column}`)" for column in DETAIL_COLUMNS[1:]
                    )
                    cursor.executemany(
                        f"INSERT INTO `ods`.`{ODS_PO_DETAIL_TABLE}` "
                        f"({','.join(f'`{c}`' for c in DETAIL_COLUMNS)}) "
                        f"VALUES ({detail_placeholders}) "
                        f"ON DUPLICATE KEY UPDATE {detail_updates}",
                        details,
                    )
            connection.commit()
            return
        except Exception as exc:
            if connection:
                try:
                    connection.rollback()
                except Exception:
                    pass
            if not retryable_db_error(exc) or attempt >= DB_TRANSACTION_RETRIES:
                raise
            print(
                f"[WARN] MySQL transaction interrupted; retrying the same "
                f"idempotent export window in {DB_RETRY_DELAY_SECONDS}s "
                f"({attempt}/{DB_TRANSACTION_RETRIES}): {exc}",
                flush=True,
            )
            time.sleep(DB_RETRY_DELAY_SECONDS)
        finally:
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass


def sync_po_list(info: dict[str, Any], start: datetime, end: datetime, page_size: int) -> int:
    headers = request_headers(info)
    synced_at = datetime.now().replace(microsecond=0)
    total_written = 0
    with requests.Session() as session:
        page = 1
        total = None
        while total is None or total_written < total:
            payload = api_json(session, build_url(info, start, end, page, page_size), headers)
            raw_rows = find_rows(payload)
            if total is None:
                total = find_total(payload, raw_rows)
                print(f"[INFO] po/list total={total}, pageSize={page_size}", flush=True)
                if raw_rows:
                    print(f"[INFO] po/list fields={sorted(raw_rows[0].keys())}", flush=True)
            if not raw_rows:
                break
            normalized = [normalize_po_row(row, synced_at) for row in raw_rows]
            details = [detail for row in raw_rows for detail in normalize_detail_rows(row, synced_at)]
            upsert_po_rows(normalized, details)
            total_written += len(normalized)
            print(f"[INFO] po/list page={page}, synced={total_written}/{total}", flush=True)
            if len(raw_rows) < page_size:
                break
            page += 1
            time.sleep(0.15)
    return total_written


def clean_header(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "").strip())


def load_export_rows(path: Path, synced_at: datetime) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    if zipfile.is_zipfile(path):
        sheets = pd.read_excel(path, sheet_name=None, dtype=object)
        frames = [frame for frame in sheets.values() if not frame.dropna(how="all").empty]
        frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    else:
        frame = pd.read_excel(path, dtype=object)
    frame = frame.dropna(how="all")
    frame.columns = [clean_header(column) for column in frame.columns]
    print(f"[INFO] export columns={list(frame.columns)}", flush=True)
    records = frame.to_dict(orient="records")
    grouped: dict[str, dict[str, Any]] = {}
    detail_rows: list[tuple[Any, ...]] = []

    def cell(record: dict[str, Any], *names: str) -> Any:
        for name in names:
            value = record.get(name)
            if value is not None and not pd.isna(value) and str(value).strip() not in ("", "-"):
                return value
        return None

    for index, record in enumerate(records):
        po_no = cell(record, "采购单号")
        if not po_no:
            continue
        po_no = str(po_no)
        business_no = cell(record, "业务单号", "销售单号")
        quantity = normalize_decimal(cell(record, "采购数量", "采购总数量")) or Decimal("0")
        unit_price = normalize_decimal(cell(record, "单价"))
        subsidy = normalize_decimal(cell(record, "补贴金额(元)", "补贴金额")) or Decimal("0")
        sku_id = cell(record, "货品SKUID", "cargoId")
        product_id = cell(record, "商品ID", "productId")
        aggregate = grouped.setdefault(po_no, {
            "业务单号": business_no, "状态": cell(record, "状态"),
            "供应商ID": cell(record, "供应商编码"), "供应商": cell(record, "供应商名称", "供应商"),
            "实际送货仓库": cell(record, "入库仓库", "实际送货仓库"),
            "采购单标签": cell(record, "采购单标签"), "创建时间": cell(record, "创建时间"),
            "完结时间": cell(record, "完结时间"), "sku": set(), "数量": Decimal("0"),
            "金额": Decimal("0"), "补贴": Decimal("0"), "行数": 0,
        })
        if sku_id:
            aggregate["sku"].add(str(sku_id))
        aggregate["数量"] += quantity
        aggregate["金额"] += (unit_price or Decimal("0")) * quantity
        aggregate["补贴"] += subsidy
        aggregate["行数"] += 1

        detail_no = f"{po_no}:{sku_id or ''}:{product_id or ''}:{index}"
        raw = {key: (None if pd.isna(value) else str(value)) for key, value in record.items()}
        detail_rows.append((
            detail_no, po_no, str(business_no or ""), str(sku_id or "") or None,
            str(product_id or "") or None, None, cell(record, "条形码", "条码"), None,
            cell(record, "品牌"), cell(record, "货品名称"), cell(record, "规格"), quantity,
            unit_price, None, 0, json.dumps(raw, ensure_ascii=False, separators=(",", ":")), synced_at,
        ))

    main_rows: list[tuple[Any, ...]] = []
    for po_no, aggregate in grouped.items():
        values = {
            "采购单号": po_no, "业务单号": aggregate["业务单号"], "状态编码": None,
            "状态": aggregate["状态"], "供应商ID": aggregate["供应商ID"], "供应商": aggregate["供应商"],
            "仓库ID": None, "实际送货仓库": aggregate["实际送货仓库"],
            "采购SKU数": Decimal(len(aggregate["sku"])), "采购总数量": aggregate["数量"],
            "采购总金额": aggregate["金额"], "补贴金额": aggregate["补贴"],
            "采购单标签": normalize_label(aggregate["采购单标签"]),
            "创建时间": normalize_time(aggregate["创建时间"]), "完结时间": normalize_time(aggregate["完结时间"]),
        }
        raw_summary = {"source": "generalExport", "lineCount": aggregate["行数"]}
        main_rows.append(tuple(values[name] for name in FIELD_ALIASES) + (
            json.dumps(raw_summary, ensure_ascii=False, separators=(",", ":")), synced_at,
        ))
    return main_rows, detail_rows


def process_export_window(
    session: requests.Session,
    export_info: dict[str, Any],
    progress_info: dict[str, Any],
    start: datetime,
    end: datetime,
    timeout_seconds: int,
    interval_seconds: int,
    export_dir: Path,
    min_window_hours: float,
) -> int:
    task_id = start_export(session, export_info, start, end)
    try:
        progress = poll_export(session, progress_info, task_id, timeout_seconds, interval_seconds)
    except ExportLimitError as exc:
        hours = (end - start).total_seconds() / 3600
        if hours <= min_window_hours:
            raise RuntimeError(f"窗口已缩小至 {hours:.2f} 小时仍超过10万条: {exc}") from exc
        midpoint = start + (end - start) / 2
        print(f"[WARN] {exc}; split window at {midpoint}", flush=True)
        return (
            process_export_window(session, export_info, progress_info, midpoint, end, timeout_seconds, interval_seconds, export_dir, min_window_hours)
            + process_export_window(session, export_info, progress_info, start, midpoint, timeout_seconds, interval_seconds, export_dir, min_window_hours)
        )
    path = download_export(session, progress_info, task_id, str(progress.get("fileName") or ""), export_dir)
    rows, details = load_export_rows(path, datetime.now().replace(microsecond=0))
    upsert_po_rows(rows, details)
    print(f"[INFO] imported purchase orders={len(rows)}, detail rows={len(details)}", flush=True)
    return len(rows)


def sync_po_exports(
    export_info: dict[str, Any],
    progress_info: dict[str, Any],
    start: datetime,
    end: datetime,
    window_days: int,
    timeout_seconds: int,
    interval_seconds: int,
    export_dir: Path,
    min_window_hours: float,
) -> int:
    total = 0
    windows: list[tuple[datetime, datetime]] = []
    current = start
    while current < end:
        window_end = min(current + timedelta(days=window_days), end)
        windows.append((current, window_end))
        current = window_end

    with requests.Session() as session:
        for window_start, window_end in reversed(windows):
            total += process_export_window(
                session, export_info, progress_info, window_start, window_end,
                timeout_seconds, interval_seconds, export_dir, min_window_hours,
            )
    return total


def table_exists(cursor: Any, schema: str, table: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        (schema, table),
    )
    return cursor.fetchone() is not None


def relation_type(cursor: Any, schema: str, name: str) -> str | None:
    cursor.execute(
        "SELECT TABLE_TYPE FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        (schema, name),
    )
    row = cursor.fetchone()
    return str(row[0]) if row else None


def swap_dwd_stage(cursor: Any, dwd_stage: str, dwd_old: str) -> None:
    """Atomically publish a completed DWD stage table."""
    current_dwd_type = relation_type(cursor, "dwd", DWD_TABLE)
    cursor.execute(f"DROP TABLE IF EXISTS `dwd`.`{dwd_old}`")
    if current_dwd_type == "VIEW":
        cursor.execute(f"DROP VIEW `dwd`.`{DWD_TABLE}`")
        cursor.execute(f"RENAME TABLE `dwd`.`{dwd_stage}` TO `dwd`.`{DWD_TABLE}`")
    elif current_dwd_type == "BASE TABLE":
        cursor.execute(
            f"RENAME TABLE `dwd`.`{DWD_TABLE}` TO `dwd`.`{dwd_old}`, "
            f"`dwd`.`{dwd_stage}` TO `dwd`.`{DWD_TABLE}`"
        )
        cursor.execute(f"DROP TABLE `dwd`.`{dwd_old}`")
    else:
        cursor.execute(f"RENAME TABLE `dwd`.`{dwd_stage}` TO `dwd`.`{DWD_TABLE}`")


def completed_dwd_stage(cursor: Any, dwd_stage: str) -> bool:
    """Return true when a prior attempt finished building and indexing the stage."""
    if relation_type(cursor, "dwd", dwd_stage) != "BASE TABLE":
        return False
    cursor.execute(f"SELECT COUNT(*) FROM `dwd`.`{dwd_stage}`")
    stage_count = int(cursor.fetchone()[0])
    cursor.execute("SELECT COUNT(*) FROM `ods`.`销售单查询`")
    source_count = int(cursor.fetchone()[0])
    cursor.execute(
        "SELECT DISTINCT INDEX_NAME FROM information_schema.statistics "
        "WHERE table_schema='dwd' AND table_name=%s",
        (dwd_stage,),
    )
    indexes = {str(row[0]) for row in cursor.fetchall()}
    required = {"idx_销售渠道", "idx_发货仓库", "idx_物流单号", "idx_付款时间"}
    return stage_count == source_count and required.issubset(indexes)


def collect_dwd_metrics(cursor: Any, mapping: str) -> dict[str, int]:
    cursor.execute("SELECT COUNT(*) FROM `ods`.`销售单查询`")
    total = int(cursor.fetchone()[0])
    cursor.execute(
        f"SELECT COUNT(*), SUM(`订单类型` <> '零售业务') FROM `dwd`.`{DWD_TABLE}` WHERE `销售渠道`=%s",
        (SOURCE_CHANNEL,),
    )
    target, non_retail = [int(value or 0) for value in cursor.fetchone()]
    cursor.execute(
        f"SELECT SUM(`平台订单数`=1 AND `销售单数`=1), "
        f"SUM(`销售单数`>0 AND NOT (`平台订单数`=1 AND `销售单数`=1)) "
        f"FROM `dwd`.`{mapping}`"
    )
    matched, merged = [int(value or 0) for value in cursor.fetchone()]
    unmatched = max(target - non_retail - matched - merged, 0)
    return {"total": total, "target": target, "matched": matched, "merged": merged, "unmatched": unmatched}


def _build_dwd_once() -> dict[str, int]:
    """重建金额映射和实体 DWD 表，ODS 原始销售单保持不变。"""
    mapping = "进口超市上海仓_销售单金额补全映射"
    stage = f"{mapping}_stage"
    old = f"{mapping}_old"
    chain_stage = f"{mapping}_chain_stage"
    po_stage = f"{mapping}_po_stage"
    sales_stage = f"{mapping}_sales_stage"
    dwd_stage = f"{DWD_TABLE}_stage"
    dwd_old = f"{DWD_TABLE}_old"
    connection = connect_mysql("dwd")
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT GET_LOCK(%s, %s)", (BUILD_LOCK_NAME, EXPORT_LOCK_TIMEOUT_SECONDS))
            if cursor.fetchone()[0] != 1:
                raise TimeoutError("another import-supermarket DWD build is still running")
            if completed_dwd_stage(cursor, dwd_stage):
                print("[INFO] 检测到已完成的销售单 DWD stage，跳过全量重算并继续发布", flush=True)
                swap_dwd_stage(cursor, dwd_stage, dwd_old)
                metrics = collect_dwd_metrics(cursor, mapping)
                connection.commit()
                return metrics
            has_po = table_exists(cursor, "ods", ODS_PO_TABLE)
            for helper_table in (stage, chain_stage, po_stage, sales_stage):
                cursor.execute(f"DROP TABLE IF EXISTS `dwd`.`{helper_table}`")

            print("[INFO] 构建并索引全链路订单汇总", flush=True)
            cursor.execute(
                f"""
                CREATE TABLE `dwd`.`{chain_stage}` AS
                SELECT NULLIF(TRIM(`运单号`),'') AS `运单号`,
                       NULLIF(TRIM(`店铺单号`),'') AS `店铺单号`,
                       GROUP_CONCAT(DISTINCT NULLIF(TRIM(`履约单号`),'') ORDER BY `履约单号` SEPARATOR ',') AS `履约单号`,
                       MAX(CAST(COALESCE(NULLIF(`实际支付gmv`,''),'0') AS DECIMAL(20,2))) / 100 AS `平台支付GMV`,
                       MAX(`updatetime`) AS `全链路更新时间`
                FROM `ods`.`进口超市上海仓_正向全链路数据`
                WHERE NULLIF(TRIM(`运单号`),'') IS NOT NULL
                  AND NULLIF(TRIM(`店铺单号`),'') IS NOT NULL
                GROUP BY NULLIF(TRIM(`运单号`),''), NULLIF(TRIM(`店铺单号`),'')
                """
            )
            cursor.execute(
                f"ALTER TABLE `dwd`.`{chain_stage}` "
                "ADD INDEX `idx_运单店铺` (`运单号`(100), `店铺单号`(100))"
            )

            print("[INFO] 构建并索引货权采购单汇总", flush=True)
            if has_po:
                cursor.execute(
                    f"""
                    CREATE TABLE `dwd`.`{po_stage}` AS
                    SELECT `业务单号`, COUNT(DISTINCT `采购单号`) AS `货权转移采购单数`,
                           SUM(COALESCE(`采购总金额`,0)) AS `货权转移采购总金额`,
                           SUM(COALESCE(`补贴金额`,0)) AS `货权转移补贴金额`,
                           MAX(`updatetime`) AS `采购单更新时间`
                    FROM `ods`.`{ODS_PO_TABLE}`
                    WHERE NULLIF(`业务单号`,'') IS NOT NULL
                    GROUP BY `业务单号`
                    """
                )
            else:
                cursor.execute(
                    f"""
                    CREATE TABLE `dwd`.`{po_stage}` AS
                    SELECT CAST(NULL AS CHAR(255)) AS `业务单号`,
                           0 AS `货权转移采购单数`,
                           CAST(0 AS DECIMAL(20,2)) AS `货权转移采购总金额`,
                           CAST(0 AS DECIMAL(20,2)) AS `货权转移补贴金额`,
                           CAST(NULL AS DATETIME) AS `采购单更新时间`
                    WHERE FALSE
                    """
                )
            cursor.execute(
                f"ALTER TABLE `dwd`.`{po_stage}` "
                "ADD INDEX `idx_业务单号` (`业务单号`)"
            )

            print("[INFO] 构建并索引销售单物流单号汇总", flush=True)
            cursor.execute(
                f"""
                CREATE TABLE `dwd`.`{sales_stage}` AS
                SELECT NULLIF(TRIM(`物流单号`),'') AS `运单号`, COUNT(*) AS `销售单数`
                FROM `ods`.`销售单查询`
                WHERE `销售渠道` = '{SOURCE_CHANNEL}' AND `订单类型` = '零售业务'
                  AND NULLIF(TRIM(`物流单号`),'') IS NOT NULL
                GROUP BY NULLIF(TRIM(`物流单号`),'')
                """
            )
            cursor.execute(
                f"ALTER TABLE `dwd`.`{sales_stage}` "
                "ADD INDEX `idx_运单号` (`运单号`(100))"
            )

            print("[INFO] 使用已索引汇总表生成金额补全映射", flush=True)
            cursor.execute(
                f"""
                CREATE TABLE `dwd`.`{stage}` AS
                SELECT c.`运单号`,
                       GROUP_CONCAT(DISTINCT c.`店铺单号` ORDER BY c.`店铺单号` SEPARATOR ',') AS `平台店铺单号`,
                       GROUP_CONCAT(DISTINCT c.`履约单号` ORDER BY c.`履约单号` SEPARATOR ',') AS `履约单号`,
                       COUNT(DISTINCT c.`店铺单号`) AS `平台订单数`,
                       SUM(c.`平台支付GMV`) AS `平台支付GMV`,
                       MAX(c.`全链路更新时间`) AS `全链路更新时间`,
                       COALESCE(sc.`销售单数`,0) AS `销售单数`,
                       SUM(COALESCE(p.`货权转移采购单数`,0)) AS `货权转移采购单数`,
                       SUM(COALESCE(p.`货权转移采购总金额`,0)) AS `货权转移采购总金额`,
                       SUM(COALESCE(p.`货权转移补贴金额`,0)) AS `货权转移补贴金额`,
                       MAX(p.`采购单更新时间`) AS `采购单更新时间`,
                       NOW() AS `金额补全时间`
                FROM `dwd`.`{chain_stage}` c
                LEFT JOIN `dwd`.`{sales_stage}` sc ON c.`运单号` = sc.`运单号`
                LEFT JOIN `dwd`.`{po_stage}` p ON p.`业务单号` = c.`店铺单号`
                GROUP BY c.`运单号`, sc.`销售单数`
                """
            )
            cursor.execute(f"ALTER TABLE `dwd`.`{stage}` MODIFY `运单号` VARCHAR(255) NOT NULL, ADD PRIMARY KEY (`运单号`)")
            for helper_table in (chain_stage, po_stage, sales_stage):
                cursor.execute(f"DROP TABLE `dwd`.`{helper_table}`")
            cursor.execute(f"DROP TABLE IF EXISTS `dwd`.`{old}`")
            if table_exists(cursor, "dwd", mapping):
                cursor.execute(f"RENAME TABLE `dwd`.`{mapping}` TO `dwd`.`{old}`, `dwd`.`{stage}` TO `dwd`.`{mapping}`")
                cursor.execute(f"DROP TABLE `dwd`.`{old}`")
            else:
                cursor.execute(f"RENAME TABLE `dwd`.`{stage}` TO `dwd`.`{mapping}`")

            cursor.execute(
                "SELECT COLUMN_NAME FROM information_schema.columns "
                "WHERE table_schema='ods' AND table_name='销售单查询' ORDER BY ORDINAL_POSITION"
            )
            sales_columns = [str(row[0]) for row in cursor.fetchall()]
            has_tmall_mapping = table_exists(cursor, "dwd", TMALL_MAPPING_TABLE)
            fill_condition = (
                f"s.`销售渠道` = '{SOURCE_CHANNEL}' AND s.`订单类型` = '零售业务' "
                "AND m.`平台支付GMV` IS NOT NULL AND m.`平台订单数` = 1 AND m.`销售单数` = 1"
            )
            tmall_fill_condition = (
                "s.`销售渠道` = '天猫国际自营' "
                "AND tm.`匹配状态` = '完整匹配' AND tm.`修正金额` IS NOT NULL"
            )
            selected_sales_columns = []
            for column in sales_columns:
                if column in ("应收合计", "实付金额"):
                    if has_tmall_mapping:
                        selected_sales_columns.append(
                            f"CASE WHEN {tmall_fill_condition} THEN tm.`修正金额` "
                            f"WHEN {fill_condition} THEN m.`平台支付GMV` "
                            f"ELSE s.`{column}` END AS `{column}`"
                        )
                    else:
                        selected_sales_columns.append(
                            f"CASE WHEN {fill_condition} THEN m.`平台支付GMV` "
                            f"ELSE s.`{column}` END AS `{column}`"
                        )
                else:
                    selected_sales_columns.append(f"s.`{column}`")

            cursor.execute(f"DROP TABLE IF EXISTS `dwd`.`{dwd_stage}`")
            cursor.execute(
                f"""
                CREATE TABLE `dwd`.`{dwd_stage}` AS
                SELECT {','.join(selected_sales_columns)}
                FROM `ods`.`销售单查询` s
                LEFT JOIN `dwd`.`{mapping}` m
                  ON s.`销售渠道` = '{SOURCE_CHANNEL}'
                 AND s.`订单类型` = '零售业务'
                 AND NULLIF(TRIM(s.`物流单号`),'') = m.`运单号`
                {"LEFT JOIN `dwd`.`" + TMALL_MAPPING_TABLE + "` tm ON s.`订单编号` = tm.`订单编号`" if has_tmall_mapping else ""}
                """
            )
            cursor.execute(
                f"ALTER TABLE `dwd`.`{dwd_stage}` "
                "ADD INDEX `idx_销售渠道` (`销售渠道`(100)), "
                "ADD INDEX `idx_发货仓库` (`发货仓库`(100)), "
                "ADD INDEX `idx_物流单号` (`物流单号`(100)), "
                "ADD INDEX `idx_付款时间` (`付款时间`)"
            )
            swap_dwd_stage(cursor, dwd_stage, dwd_old)
            metrics = collect_dwd_metrics(cursor, mapping)
        connection.commit()
        return metrics
    finally:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT RELEASE_LOCK(%s)", (BUILD_LOCK_NAME,))
        except Exception:
            pass
        connection.close()


def build_dwd() -> dict[str, int]:
    """Build DWD with retries; completed stage tables are resumed instead of rebuilt."""
    for attempt in range(1, DB_TRANSACTION_RETRIES + 1):
        try:
            return _build_dwd_once()
        except pymysql.MySQLError as exc:
            if not retryable_db_error(exc) or attempt >= DB_TRANSACTION_RETRIES:
                raise
            print(
                f"[WARN] DWD build lost MySQL connection; retrying/resuming in "
                f"{DB_RETRY_DELAY_SECONDS}s ({attempt}/{DB_TRANSACTION_RETRIES}): {exc}",
                flush=True,
            )
            time.sleep(DB_RETRY_DELAY_SECONDS)
    raise RuntimeError("unreachable")


def parse_datetime(value: str, end_of_day: bool = False) -> datetime:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        result = datetime.strptime(value, "%Y-%m-%d")
        return result + timedelta(days=1) if end_of_day else result
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同步进口超市货权转移采购单并构建销售单金额补全 DWD")
    parser.add_argument("--list-curl", type=Path, default=DEFAULT_LIST_CURL)
    parser.add_argument("--export-curl", type=Path, default=DEFAULT_EXPORT_CURL)
    parser.add_argument("--progress-curl", type=Path, default=DEFAULT_PROGRESS_CURL)
    parser.add_argument("--start", help="开始时间，默认最近30天")
    parser.add_argument("--end", help="结束日期（含）或结束时间（不含）")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--min-window-hours", type=float, default=1.0)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--interval", type=int, default=2)
    parser.add_argument("--export-dir", type=Path, default=DEFAULT_EXPORT_DIR)
    parser.add_argument("--use-list-api", action="store_true", help="仅调试：使用有分页签名限制的 po/list")
    parser.add_argument("--build-only", action="store_true", help="不请求平台，只重建 DWD")
    parser.add_argument("--sync-only", action="store_true", help="只同步采购单，不重建 DWD")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if min(args.page_size, args.lookback_days, args.window_days, args.min_window_hours, args.timeout, args.interval) <= 0:
        raise ValueError("分页、窗口及超时参数必须大于0")
    end = parse_datetime(args.end, end_of_day=True) if args.end else datetime.now().replace(microsecond=0)
    start = parse_datetime(args.start) if args.start else end - timedelta(days=args.lookback_days)
    if end <= start:
        raise ValueError("结束时间必须晚于开始时间")

    if not args.build_only:
        print(f"[INFO] 同步货权转移采购单: {start} ~ {end}", flush=True)
        lock_connection = connect_mysql("ods")
        try:
            with lock_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT GET_LOCK(%s, %s)",
                    ("jike_trade_export:bscm_export", EXPORT_LOCK_TIMEOUT_SECONDS),
                )
                if cursor.fetchone()[0] != 1:
                    raise TimeoutError("another BSCM export process is still running")
            if args.use_list_api:
                rows = sync_po_list(load_curl(args.list_curl), start, end, args.page_size)
            else:
                export_info = load_endpoint_curl(args.export_curl, "generalExport")
                progress_info = inherit_auth(
                    load_endpoint_curl(args.progress_curl, "queryTaskProgress"),
                    export_info,
                )
                print("[INFO] 进度查询沿用导出 cURL 登录态", flush=True)
                rows = sync_po_exports(
                    export_info,
                    progress_info,
                    start, end, args.window_days, args.timeout, args.interval,
                    args.export_dir, args.min_window_hours,
                )
        finally:
            lock_connection.close()
        print(f"[INFO] 采购单同步完成: {rows} rows", flush=True)

    if not args.sync_only:
        metrics = build_dwd()
        print(f"[DONE] dwd.{DWD_TABLE}: {json.dumps(metrics, ensure_ascii=False)}", flush=True)


if __name__ == "__main__":
    main()
