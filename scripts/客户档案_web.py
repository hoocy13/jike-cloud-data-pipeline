"""
Sync Jike Cloud customer files through the web export task flow.

This script is designed for large exports. It starts the Jike export task,
downloads the attachment, streams rows from xlsx/csv/zip files into a staging
CSV, then atomically swaps the MySQL table.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shlex
import shutil
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode
from xml.etree import ElementTree as ET

import openpyxl
import pymysql
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATA_DIR, DB_CONFIG

BASE_URL = "https://env3.jkyservice.com"
WEB_APP_KEY = "jackyun_web_browser_2024"
WEB_SIGN_SECRET = os.getenv("JKY_WEB_SIGN_SECRET", "")

TABLE_NAME = "客户档案"
START_EXPORT_CURL_ENV = "JKY_CUSTOMER_FILE_CURL"
COMMON_VERIFY_ENV = "JKY_CUSTOMER_FILE_COMMON_VERIFY"

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CURL = ROOT / "curl" / "客户档案_curl.txt"
DEFAULT_XLSX_DIR = os.path.join(DATA_DIR, "客户档案_web_exports")
REQUEST_TIMEOUT = 90
REQUEST_RETRIES = 5
SPLIT_ROW_THRESHOLD = 400000
DB_CONNECT_RETRIES = 5

EXPORT_FIELDS = [
    "channelName",
    "customerCreateSourceDesc",
    "customerCode",
    "nickname",
    "customerStatusDesc",
    "customerTypeName",
    "contacts",
    "phone",
    "detailedAddress",
    "vipLevelName",
    "tagArr",
    "integralBalance",
    "customerManagerName",
    "merchandiserName",
    "lastConsumptionTime",
    "lastConsumptionShopName",
    "remark",
    "debtAmountMax",
    "debtAmountMaxExpireTime",
    "specialReminding",
    "blackList",
    "settlementExtraRuleExplain",
]
CONDITION_FIELDS = [
    "createChannelInfo.channelName",
    "customerCreateSourceDesc",
    "customerCode",
    "nickname",
    "customerStatusDesc",
    "customerTypeName",
    "contacts",
    "phone",
    "detailedAddress",
    "vipLevelName",
    "tagArr",
    "integralBalance",
    "customerManagerName",
    "merchandiserName",
    "lastConsumptionTime",
    "lastConsumptionShopName",
    "remark",
    "debtAmountMax",
    "debtAmountMaxExpireTime",
    "specialReminding",
    "blackList",
    "settlementExtraRuleExplain",
]
ORDERED_COLUMNS = [
    "建档渠道",
    "客户来源",
    "客户编号",
    "客户名称",
    "状态",
    "客户分类",
    "联系人",
    "联系电话",
    "联系地址",
    "会员等级",
    "客户标签",
    "积分余额",
    "业务员",
    "跟单员",
    "最近消费时间",
    "最近消费店铺",
    "备注",
    "欠款额度",
    "额度到期日",
    "特别提醒",
    "黑名单",
    "账期例外规则",
]
FINAL_COLUMNS = ORDERED_COLUMNS + ["updatetime"]
TEXT_COLUMNS = {"联系地址", "客户标签", "备注", "特别提醒", "账期例外规则"}
DATETIME_COLUMNS = {"最近消费时间", "额度到期日", "updatetime"}
DECIMAL_COLUMNS = {"积分余额", "欠款额度"}


def mysql_connect_with_retry():
    last_error: Exception | None = None
    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            return pymysql.connect(**DB_CONFIG)
        except pymysql.MySQLError as exc:
            last_error = exc
            if attempt >= DB_CONNECT_RETRIES:
                break
            wait_seconds = attempt * 10
            print(
                f"[WARN] MySQL connect failed, retry {attempt}/{DB_CONNECT_RETRIES} after {wait_seconds}s: {exc}",
                flush=True,
            )
            time.sleep(wait_seconds)
    raise last_error


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
    if not any(part in url for part in ("startExcelExport", "validateExcelExport")):
        raise ValueError("Please provide a customer-file startExcelExport or validateExcelExport cURL")
    params = dict(parse_qsl(data_raw, keep_blank_values=True))
    return {"url": url, "headers": headers, "cookie": cookie, "params": params}


def load_curl_info(curl_path: str | None) -> dict[str, Any]:
    env_curl = os.getenv(START_EXPORT_CURL_ENV, "").strip()
    if env_curl:
        print(f"[INFO] using cURL from environment variable {START_EXPORT_CURL_ENV}", flush=True)
        return parse_curl_text(env_curl)
    path = Path(curl_path) if curl_path else DEFAULT_CURL
    if path.exists():
        print(f"[INFO] using cURL file: {path}", flush=True)
        return parse_curl_text(path.read_text(encoding="utf-8-sig"))
    raise FileNotFoundError(f"No cURL configured. Save a fresh cURL to {DEFAULT_CURL}, or pass --curl.")


def parse_json(value: str, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


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


def web_headers(curl_info: dict[str, Any], module_code: str | None = None, referer: str | None = None) -> dict[str, str]:
    source = curl_info["headers"]
    authorization = source.get("authorization")
    if not authorization:
        raise ValueError("Missing authorization header in cURL")
    headers = {
        "accept": source.get("accept", "*/*"),
        "accept-language": source.get("accept-language", "zh-CN,zh;q=0.9"),
        "authorization": authorization,
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "module_code": module_code or source.get("module_code", "oms_customer_file"),
        "origin": BASE_URL,
        "referer": referer or source.get("referer", f"{BASE_URL}/"),
        "user-agent": source.get("user-agent", "Mozilla/5.0"),
        "x-requested-with": "XMLHttpRequest",
    }
    for name in ("ati", "bx-v"):
        if source.get(name):
            headers[name] = source[name]
    if curl_info.get("cookie"):
        headers["cookie"] = curl_info["cookie"]
    return headers


def request_json(session: requests.Session, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            response = session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            text = response.text
            try:
                data = response.json()
            except ValueError as exc:
                raise RuntimeError(f"Non-JSON response from {url}: {text[:500]}") from exc
            if response.status_code >= 400 or data.get("code") not in (None, 200):
                raise RuntimeError(f"Request failed {response.status_code}: {text[:1000]}")
            return data
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as exc:
            last_error = exc
            if attempt >= REQUEST_RETRIES:
                break
            wait_seconds = attempt * 10
            print(
                f"[WARN] request timeout/network error, retry {attempt}/{REQUEST_RETRIES} after {wait_seconds}s: {exc}",
                flush=True,
            )
            time.sleep(wait_seconds)
    raise RuntimeError(f"Request failed after {REQUEST_RETRIES} retries: {last_error}")


def parse_datetime(value: str, end_of_day: bool = False) -> datetime:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        suffix = "23:59:59" if end_of_day else "00:00:00"
        value = f"{value} {suffix}"
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def apply_customer_time_window(
    condition: dict[str, Any],
    args: argparse.Namespace,
    start_dt: datetime | None = None,
    end_dt: datetime | None = None,
    all_time: bool = False,
) -> None:
    customer = condition.setdefault("customerDto", {})
    if all_time:
        customer["gmtCreateBegin"] = ""
        customer["gmtCreateEnd"] = ""
        print("[INFO] customer create time filter: ALL", flush=True)
        return

    now = datetime.now().replace(microsecond=0)
    end_dt = end_dt or (parse_datetime(args.end, end_of_day=True) if args.end else now.replace(hour=23, minute=59, second=59))
    start_dt = start_dt or (parse_datetime(args.start) if args.start else (
        end_dt - timedelta(days=args.lookback_days)
    ).replace(hour=0, minute=0, second=0))
    customer["gmtCreateBegin"] = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    customer["gmtCreateEnd"] = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"[INFO] customer create time filter: {customer['gmtCreateBegin']} ~ {customer['gmtCreateEnd']}",
        flush=True,
    )


def force_export_params(
    params: dict[str, str],
    args: argparse.Namespace,
    start_dt: datetime | None = None,
    end_dt: datetime | None = None,
    all_time: bool = False,
) -> dict[str, str]:
    out = dict(params)
    out["serverName"] = "crm/crm/excel"
    out["excelType"] = "1"
    out["headersJson"] = json.dumps(
        {"enName": EXPORT_FIELDS, "showName": ORDERED_COLUMNS},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    condition = parse_json(out.get("conditionJson", ""), {})
    condition.setdefault("pageInfo", {})
    condition["pageInfo"]["pageIndex"] = 0
    condition["pageInfo"]["pageSize"] = 100000
    condition.pop("ids", None)
    condition["cols"] = CONDITION_FIELDS
    condition.setdefault("version", "2.0")
    apply_customer_time_window(condition, args, start_dt=start_dt, end_dt=end_dt, all_time=all_time)
    out["conditionJson"] = json.dumps(condition, ensure_ascii=False, separators=(",", ":"))
    out["datasource"] = ""
    out["typeName"] = "客户档案"
    out["multiSheet"] = "false"
    out["exportTotal"] = ""
    out["isSyn"] = "false"
    verify = out.get("commonVerify") or os.getenv(COMMON_VERIFY_ENV, "").strip()
    if verify:
        out["commonVerify"] = verify
    return out


def post_export_endpoint(
    session: requests.Session,
    curl_info: dict[str, Any],
    endpoint: str,
    params: dict[str, str],
) -> dict[str, Any]:
    headers = web_headers(curl_info, "oms_customer_file")
    body = signed_params(params, headers["authorization"])
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
        raise RuntimeError("客户档案导出触发手机验证。请先在吉客云页面验证后，重新复制 startExcelExport cURL。")
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


def safe_suffix(name: str, url: str) -> str:
    source = name or url.split("?", 1)[0]
    suffix = Path(source).suffix.lower()
    return suffix if suffix in {".xlsx", ".xlsm", ".csv", ".zip"} else ".xlsx"


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
                    timeout=(30, 1800),
                )
                response.raise_for_status()
                Path(out_path).write_bytes(response.content)
                print(f"[INFO] downloaded export: {out_path} ({len(response.content)} bytes)", flush=True)
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


def normalize_value(value: Any, column: str) -> Any:
    if value is None:
        return "\\N"
    text = str(value).strip()
    if text in {"", "nan", "None", "NaT", "<NA>"}:
        return "\\N"
    if text.startswith("0000-00-00"):
        return "\\N"
    if column in DECIMAL_COLUMNS:
        number = re.sub(r",", "", text)
        return number if number else "\\N"
    if column in DATETIME_COLUMNS:
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return text
    return text


def excel_column_index(cell_ref: str) -> int:
    letters = re.match(r"([A-Z]+)", cell_ref or "")
    if not letters:
        return 0
    index = 0
    for char in letters.group(1):
        index = index * 26 + ord(char) - ord("A") + 1
    return index - 1


def load_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    strings: list[str] = []
    with archive.open("xl/sharedStrings.xml") as handle:
        for event, elem in ET.iterparse(handle, events=("end",)):
            if elem.tag == ns + "si":
                strings.append("".join(node.text or "" for node in elem.iter(ns + "t")))
                elem.clear()
    return strings


def cell_text(cell: ET.Element, shared_strings: list[str]) -> str:
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    cell_type = cell.attrib.get("t", "")
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.iter(ns + "t"))
    value = cell.find(ns + "v")
    if value is None or value.text is None:
        return ""
    if cell_type == "s":
        try:
            return shared_strings[int(value.text)]
        except (ValueError, IndexError):
            return ""
    return value.text


def iter_xlsx_rows(path: str) -> Iterable[dict[str, Any]]:
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    with zipfile.ZipFile(path) as archive:
        shared_strings = load_shared_strings(archive)
        sheet_names = sorted(name for name in archive.namelist() if re.fullmatch(r"xl/worksheets/sheet\d+\.xml", name))
        for sheet_name in sheet_names:
            index_by_name: dict[str, int] | None = None
            with archive.open(sheet_name) as handle:
                for event, elem in ET.iterparse(handle, events=("end",)):
                    if elem.tag != ns + "row":
                        continue
                    row_values: dict[int, str] = {}
                    for cell in elem.findall(ns + "c"):
                        idx = excel_column_index(cell.attrib.get("r", ""))
                        row_values[idx] = cell_text(cell, shared_strings)
                    if index_by_name is None:
                        header = {normalize_column_name(value): idx for idx, value in row_values.items() if value}
                        if any(name in header for name in ORDERED_COLUMNS):
                            index_by_name = header
                        elem.clear()
                        continue
                    if not any(value not in (None, "") for value in row_values.values()):
                        elem.clear()
                        continue
                    yield {
                        col: row_values.get(index_by_name[col], None) if col in index_by_name else None
                        for col in ORDERED_COLUMNS
                    }
                    elem.clear()


def iter_csv_rows(path: str) -> Iterable[dict[str, Any]]:
    for encoding in ("utf-8-sig", "gb18030"):
        try:
            with open(path, "r", encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle)
                if not reader.fieldnames:
                    return
                field_map = {normalize_column_name(name): name for name in reader.fieldnames}
                for row in reader:
                    yield {col: row.get(field_map.get(col, ""), None) for col in ORDERED_COLUMNS}
            return
        except UnicodeDecodeError:
            continue


def iter_export_rows(path: str, extract_dir: str) -> Iterable[dict[str, Any]]:
    suffix = Path(path).suffix.lower()
    if suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            archive.extractall(extract_dir)
        for file_path in sorted(Path(extract_dir).rglob("*")):
            if file_path.suffix.lower() in {".xlsx", ".xlsm", ".csv"}:
                yield from iter_export_rows(str(file_path), extract_dir)
        return
    if suffix in {".xlsx", ".xlsm"}:
        yield from iter_xlsx_rows(path)
        return
    if suffix == ".csv":
        yield from iter_csv_rows(path)
        return
    raise ValueError(f"Unsupported export file type: {path}")


def create_table_sql(table_name: str) -> str:
    columns_sql = []
    for col in FINAL_COLUMNS:
        if col in DATETIME_COLUMNS:
            col_type = "DATETIME"
        elif col in DECIMAL_COLUMNS:
            col_type = "DECIMAL(18,2)"
        elif col in TEXT_COLUMNS:
            col_type = "TEXT"
        else:
            col_type = "VARCHAR(255)"
        columns_sql.append(f"`{col}` {col_type}")
    indexes = [
        "INDEX `idx_客户编号` (`客户编号`)",
        "INDEX `idx_客户名称` (`客户名称`)",
        "INDEX `idx_客户分类` (`客户分类`)",
        "INDEX `idx_最近消费时间` (`最近消费时间`)",
        "INDEX `idx_updatetime` (`updatetime`)",
    ]
    return f"""
CREATE TABLE `{table_name}` (
    {", ".join(columns_sql + indexes)}
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户档案网页导出';
"""


def build_stage_csv(export_path: str, update_time: datetime) -> tuple[str, int]:
    extract_dir = tempfile.mkdtemp(prefix="jky_customer_extract_")
    csv_path = os.path.join(tempfile.gettempdir(), f"客户档案_stage_{os.getpid()}.csv")
    rows_count = 0
    try:
        with open(csv_path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            for row in iter_export_rows(export_path, extract_dir):
                output = [normalize_value(row.get(col), col) for col in ORDERED_COLUMNS]
                output.append(update_time.strftime("%Y-%m-%d %H:%M:%S"))
                writer.writerow(output)
                rows_count += 1
                if rows_count % 100000 == 0:
                    print(f"[INFO] prepared {rows_count} rows", flush=True)
        return csv_path, rows_count
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)


def write_snapshot_to_mysql(csv_path: str, rows_count: int, table_name: str) -> None:
    tmp_table = f"{table_name}_tmp_web_{os.getpid()}"
    old_table = f"{table_name}_old_web_{os.getpid()}"
    conn = None
    cursor = None
    try:
        conn = mysql_connect_with_retry()
        cursor = conn.cursor()
        cursor.execute("SET GLOBAL local_infile = 1")
        cursor.execute(f"DROP TABLE IF EXISTS `{tmp_table}`")
        cursor.execute(create_table_sql(tmp_table))
        columns = ", ".join(f"`{col}`" for col in FINAL_COLUMNS)
        tmp_path = csv_path.replace("\\", "/")
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{tmp_path}'
            INTO TABLE `{tmp_table}`
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            ({columns})
        """)
        loaded = cursor.rowcount
        clean_stage_datetime_columns(cursor, tmp_table)
        if loaded != rows_count:
            print(f"[WARN] prepared rows={rows_count}, MySQL loaded rows={loaded}", flush=True)
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


def ensure_customer_table(cursor, table_name: str) -> None:
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    if not cursor.fetchone():
        cursor.execute(create_table_sql(table_name))


def clean_stage_datetime_columns(cursor, table_name: str) -> None:
    for col in DATETIME_COLUMNS:
        cursor.execute(f"""
            UPDATE `{table_name}`
            SET `{col}` = NULL
            WHERE CAST(`{col}` AS CHAR) = '0000-00-00 00:00:00'
               OR CAST(`{col}` AS CHAR) = '0000-00-00'
        """)


def write_incremental_to_mysql(csv_path: str, rows_count: int, table_name: str) -> None:
    stage_table = f"{table_name}_stage_web_{os.getpid()}"
    conn = None
    cursor = None
    try:
        conn = mysql_connect_with_retry()
        cursor = conn.cursor()
        cursor.execute("SET GLOBAL local_infile = 1")
        ensure_customer_table(cursor, table_name)
        cursor.execute(f"DROP TABLE IF EXISTS `{stage_table}`")
        cursor.execute(f"CREATE TABLE `{stage_table}` LIKE `{table_name}`")
        columns = ", ".join(f"`{col}`" for col in FINAL_COLUMNS)
        tmp_path = csv_path.replace("\\", "/")
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{tmp_path}'
            INTO TABLE `{stage_table}`
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            ({columns})
        """)
        loaded = cursor.rowcount
        clean_stage_datetime_columns(cursor, stage_table)
        if loaded != rows_count:
            print(f"[WARN] prepared rows={rows_count}, MySQL loaded rows={loaded}", flush=True)
        cursor.execute(f"""
            DELETE target
            FROM `{table_name}` AS target
            INNER JOIN (
                SELECT DISTINCT `客户编号`
                FROM `{stage_table}`
                WHERE `客户编号` IS NOT NULL AND `客户编号` <> ''
            ) AS stage
            ON target.`客户编号` = stage.`客户编号`
        """)
        deleted = cursor.rowcount
        cursor.execute(f"INSERT INTO `{table_name}` ({columns}) SELECT {columns} FROM `{stage_table}`")
        inserted = cursor.rowcount
        cursor.execute(f"DROP TABLE IF EXISTS `{stage_table}`")
        conn.commit()
        print(
            f"[INFO] incremental imported {inserted} rows into {DB_CONFIG['database']}.{table_name}; "
            f"replaced {deleted} old rows",
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


def reset_full_stage_table(table_name: str) -> str:
    stage_table = f"{table_name}_full_stage"
    conn = None
    cursor = None
    try:
        conn = mysql_connect_with_retry()
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS `{stage_table}`")
        cursor.execute(create_table_sql(stage_table))
        conn.commit()
        print(f"[INFO] reset full stage table: {DB_CONFIG['database']}.{stage_table}", flush=True)
        return stage_table
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def swap_full_stage_to_target(stage_table: str, table_name: str) -> None:
    old_table = f"{table_name}_old_full_{os.getpid()}"
    conn = None
    cursor = None
    try:
        conn = mysql_connect_with_retry()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM `{stage_table}`")
        stage_rows = cursor.fetchone()[0]
        if stage_rows <= 0:
            raise RuntimeError(f"Full stage table `{stage_table}` is empty; refuse to replace `{table_name}`.")
        cursor.execute(f"DROP TABLE IF EXISTS `{old_table}`")
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if cursor.fetchone():
            cursor.execute(f"RENAME TABLE `{table_name}` TO `{old_table}`, `{stage_table}` TO `{table_name}`")
            cursor.execute(f"DROP TABLE IF EXISTS `{old_table}`")
        else:
            cursor.execute(f"RENAME TABLE `{stage_table}` TO `{table_name}`")
        conn.commit()
        print(f"[INFO] swapped full stage into {DB_CONFIG['database']}.{table_name}; rows={stage_rows}", flush=True)
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def add_month(dt: datetime) -> datetime:
    if dt.month == 12:
        return dt.replace(year=dt.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return dt.replace(month=dt.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)


def monthly_windows(start_dt: datetime, end_dt: datetime) -> list[tuple[datetime, datetime]]:
    windows: list[tuple[datetime, datetime]] = []
    current = start_dt
    while current < end_dt:
        next_month = add_month(current)
        nxt = min(next_month, end_dt)
        windows.append((current, nxt))
        current = nxt
    return windows


def safe_file_stamp(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def run_export_once(
    curl_info: dict[str, Any],
    args: argparse.Namespace,
    start_dt: datetime | None,
    end_dt: datetime | None,
    replace_snapshot: bool,
    all_time: bool = False,
) -> int:
    update_time = datetime.now().replace(microsecond=0)
    params = force_export_params(dict(curl_info["params"]), args, start_dt=start_dt, end_dt=end_dt, all_time=all_time)
    label = "ALL" if all_time else f"{start_dt} ~ {end_dt}"
    print(f"[WINDOW] {label}", flush=True)
    with requests.Session() as session:
        task_id = validate_and_start_export(session, curl_info, params)
        url, attachment_name, task_title = poll_task_download(session, curl_info, task_id, args.timeout, args.interval)
        if task_title:
            print(f"[INFO] task title: {task_title}", flush=True)
        suffix = safe_suffix(attachment_name, url)
        if start_dt and end_dt:
            stamp = f"{safe_file_stamp(start_dt)}_{safe_file_stamp(end_dt)}"
        else:
            stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        out_path = os.path.join(args.xlsx_dir, f"客户档案_{stamp}_{task_id}{suffix}")
        export_path = download_file(session, url, out_path)

    csv_path, rows_count = build_stage_csv(export_path, update_time)
    print(f"[INFO] normalized rows: {rows_count}", flush=True)
    try:
        if not args.no_db:
            if replace_snapshot:
                write_snapshot_to_mysql(csv_path, rows_count, args.table)
            else:
                write_incremental_to_mysql(csv_path, rows_count, args.table)
    finally:
        try:
            os.remove(csv_path)
        except OSError:
            pass
    return rows_count


def run_full_export_recursive(
    curl_info: dict[str, Any],
    args: argparse.Namespace,
    start_dt: datetime,
    end_dt: datetime,
) -> int:
    stage_table = f"{args.table}_full_stage"
    if args.no_db:
        stage_table = args.table
    elif args.append_full:
        print(f"[INFO] append full stage table: {DB_CONFIG['database']}.{stage_table}", flush=True)
    else:
        stage_table = reset_full_stage_table(args.table)
    total_rows = 0

    def process(start: datetime, end: datetime) -> int:
        update_time = datetime.now().replace(microsecond=0)
        params = force_export_params(dict(curl_info["params"]), args, start_dt=start, end_dt=end)
        print(f"[WINDOW] {start} ~ {end}", flush=True)
        with requests.Session() as session:
            task_id = validate_and_start_export(session, curl_info, params)
            url, attachment_name, task_title = poll_task_download(session, curl_info, task_id, args.timeout, args.interval)
            if task_title:
                print(f"[INFO] task title: {task_title}", flush=True)
            suffix = safe_suffix(attachment_name, url)
            stamp = f"{safe_file_stamp(start)}_{safe_file_stamp(end)}"
            out_path = os.path.join(args.xlsx_dir, f"客户档案_{stamp}_{task_id}{suffix}")
            export_path = download_file(session, url, out_path)

        csv_path, rows_count = build_stage_csv(export_path, update_time)
        print(f"[INFO] normalized rows: {rows_count}", flush=True)
        if rows_count >= args.max_rows and (end - start) <= timedelta(days=args.min_window_days):
            try:
                os.remove(csv_path)
            except OSError:
                pass
            raise RuntimeError(
                f"Window {start} ~ {end} still reached export cap {args.max_rows}; "
                "increase split precision before loading this data."
            )
        if rows_count >= args.split_threshold and (end - start) > timedelta(days=args.min_window_days):
            print(
                f"[WARN] row count {rows_count} reached split threshold {args.split_threshold}; "
                "splitting this window and discarding capped slice",
                flush=True,
            )
            try:
                os.remove(csv_path)
            except OSError:
                pass
            mid = (start + (end - start) / 2).replace(microsecond=0)
            return process(start, mid) + process(mid, end)

        try:
            if not args.no_db:
                if rows_count == 0:
                    print("[INFO] empty window, skip database write", flush=True)
                else:
                    write_incremental_to_mysql(csv_path, rows_count, stage_table)
        finally:
            try:
                os.remove(csv_path)
            except OSError:
                pass
        return rows_count

    windows = monthly_windows(start_dt, end_dt)
    print(f"[INFO] monthly windows: {len(windows)}", flush=True)
    for window_start, window_end in windows:
        total_rows += process(window_start, window_end)

    if not args.no_db:
        swap_full_stage_to_target(stage_table, args.table)
    return total_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Jike customer files and sync them into MySQL.")
    parser.add_argument("--curl", default=str(DEFAULT_CURL), help="customer-file startExcelExport Copy-as-cURL text file")
    parser.add_argument("--table", default=TABLE_NAME, help="target MySQL table")
    parser.add_argument("--all", action="store_true", help="export all customer files and replace the table")
    parser.add_argument("--append-full", action="store_true", help="continue a split full export without replacing the table")
    parser.add_argument("--single-all", action="store_true", help="send one no-date export request without splitting")
    parser.add_argument("--replace-snapshot", action="store_true", help="replace the whole table with this export")
    parser.add_argument("--start", help="customer create start, YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end", help="customer create end, YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--full-start", default="2019-11-01", help="full export lower bound")
    parser.add_argument("--full-end", help="full export upper bound")
    parser.add_argument("--max-rows", type=int, default=500000)
    parser.add_argument("--split-threshold", type=int, default=SPLIT_ROW_THRESHOLD)
    parser.add_argument("--min-window-days", type=float, default=1)
    parser.add_argument("--timeout", type=int, default=7200)
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--xlsx-dir", default=DEFAULT_XLSX_DIR)
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    curl_info = load_curl_info(args.curl)
    if args.single_all:
        rows_count = run_export_once(curl_info, args, None, None, replace_snapshot=True, all_time=True)
    elif args.all:
        full_start = parse_datetime(args.full_start)
        full_end = parse_datetime(args.full_end, end_of_day=True) if args.full_end else datetime.now().replace(
            hour=23, minute=59, second=59, microsecond=0
        )
        rows_count = run_full_export_recursive(curl_info, args, full_start, full_end)
    else:
        rows_count = run_export_once(curl_info, args, None, None, replace_snapshot=args.replace_snapshot)
    print(f"[DONE] total rows: {rows_count}", flush=True)


if __name__ == "__main__":
    main()
