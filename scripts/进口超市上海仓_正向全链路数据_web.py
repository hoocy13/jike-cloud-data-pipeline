"""Export Douyin BSCM forward fulfillment data and sync it to MySQL."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urlparse

import pandas as pd
import pymysql
import requests

sys_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)
from config import DATA_DIR, DB_CONFIG


TABLE_NAME = "进口超市上海仓_正向全链路数据"
DEFAULT_CURL = os.path.join(sys_path, "curl", "进口超市上海仓_正向全链路数据_curl.txt")
DEFAULT_EXPORT_DIR = os.path.join(DATA_DIR, "进口超市上海仓_正向全链路数据_exports")
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_WINDOW_DAYS = 1
DEFAULT_SUBJECT_AID = "305219"
CONNECT_RETRIES = 5
CONNECT_RETRY_DELAY = 10
DEFAULT_MIN_WINDOW_HOURS = 1
DOWNLOAD_STABILIZE_SECONDS = 5
WRITE_LOCK_TIMEOUT_SECONDS = 180
EXPORT_LOCK_TIMEOUT_SECONDS = 3600
RESPONSE_RETRY_DELAY_SECONDS = 3

PAY_TIME_COLUMNS = (
    "履约单创建时间",
    "履约创建时间",
    "支付时间",
)


class ExportLimitError(RuntimeError):
    """The requested time window exceeds BSCM's 50,000-row export limit."""


class IncompleteExportError(RuntimeError):
    """The generated export contains fewer rows than the stored window."""


def normalize_curl_text(text: str) -> str:
    return text.replace("^\r\n", " ").replace("^\n", " ").replace("^", "")


def parse_curl_text(raw: str) -> dict[str, Any]:
    tokens = shlex.split(normalize_curl_text(raw), posix=True)
    if not tokens or tokens[0].lower() != "curl":
        raise ValueError("文件内容不是完整的 cURL 命令")

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

    if "exportFulfillOrderList" not in url:
        raise ValueError("请提供 exportFulfillOrderList 的 cURL")
    if not cookie:
        raise ValueError("cURL 中缺少 cookie")
    try:
        payload = json.loads(data_raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"cURL 中的 JSON 请求体无法解析: {exc}") from exc
    return {"url": url, "headers": headers, "cookie": cookie, "payload": payload}


def load_curl(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"请把完整 cURL 保存到: {path}")
    return parse_curl_text(Path(path).read_text(encoding="utf-8-sig"))


def request_headers(curl_info: dict[str, Any], json_request: bool = True) -> dict[str, str]:
    source = curl_info["headers"]
    headers = {
        "accept": source.get("accept", "*/*"),
        "accept-language": source.get("accept-language", "zh-CN,zh;q=0.9"),
        "cookie": curl_info["cookie"],
        "menukey": source.get("menukey", "/fulfillment-center/fulfillment-monitor/forward-report"),
        "origin": source.get("origin", "https://bscm.jinritemai.com"),
        "referer": source.get(
            "referer",
            "https://bscm.jinritemai.com/views/fulfillment-center/fulfillment-monitor/forward-report",
        ),
        "user-agent": source.get("user-agent", "Mozilla/5.0"),
    }
    if json_request:
        headers["content-type"] = "application/json"
    if source.get("x-secsdk-csrf-token"):
        headers["x-secsdk-csrf-token"] = source["x-secsdk-csrf-token"]
    return headers


def request_with_connect_retry(session: requests.Session, method: str, url: str, **kwargs: Any) -> requests.Response:
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            return session.request(method, url, timeout=(30, 300), **kwargs)
        except requests.ConnectTimeout:
            if attempt == CONNECT_RETRIES:
                raise
            print(f"[WARN] 连接超时，{CONNECT_RETRY_DELAY}秒后重试 {attempt}/{CONNECT_RETRIES}", flush=True)
            time.sleep(CONNECT_RETRY_DELAY)
    raise RuntimeError(f"请求未返回: {url}")


def unwrap_api_data(data: Any) -> Any:
    current = data
    for _ in range(4):
        if not isinstance(current, dict):
            break
        if "data" in current and current["data"] is not None:
            current = current["data"]
            continue
        if "result" in current and current["result"] is not None:
            current = current["result"]
            continue
        break
    return current


def request_json(session: requests.Session, method: str, url: str, **kwargs: Any) -> Any:
    for attempt in range(1, CONNECT_RETRIES + 1):
        response = request_with_connect_retry(session, method, url, **kwargs)
        text = response.text
        if response.status_code >= 400:
            if response.status_code < 500 or attempt == CONNECT_RETRIES:
                raise RuntimeError(f"接口请求失败 ({response.status_code}): {text[:1000]}")
        else:
            try:
                data = response.json()
            except ValueError:
                data = None
            if data is not None:
                if isinstance(data, dict):
                    code = data.get("code", data.get("status_code"))
                    if code not in (None, 0, "0", 200, "200"):
                        raise RuntimeError(f"接口返回失败: {text[:1000]}")
                return unwrap_api_data(data)
            if attempt == CONNECT_RETRIES:
                raise RuntimeError(f"接口未返回 JSON ({response.status_code}): {text[:500]}")
        print(
            f"[WARN] temporary empty/error response; retrying in "
            f"{RESPONSE_RETRY_DELAY_SECONDS}s ({attempt}/{CONNECT_RETRIES})",
            flush=True,
        )
        time.sleep(RESPONSE_RETRY_DELAY_SECONDS)
    raise RuntimeError(f"request did not return JSON: {url}")


def find_nested_value(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        if key in value:
            return value[key]
        for child in value.values():
            found = find_nested_value(child, key)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = find_nested_value(child, key)
            if found is not None:
                return found
    return None


def subject_aid_from_url(url: str) -> str:
    query = dict(parse_qsl(urlparse(url).query, keep_blank_values=True))
    return query.get("subject_aid") or DEFAULT_SUBJECT_AID


def api_url(curl_info: dict[str, Any], endpoint: str) -> str:
    parsed = urlparse(curl_info["url"])
    return f"{parsed.scheme}://{parsed.netloc}/scm/visualized/{endpoint}"


def iso_utc(dt: datetime) -> str:
    # DolphinScheduler and the source page use China Standard Time.
    cst = timezone(timedelta(hours=8))
    aware = dt.replace(tzinfo=cst)
    return aware.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_window_payload(template: dict[str, Any], start_dt: datetime, end_dt: datetime) -> dict[str, Any]:
    payload = json.loads(json.dumps(template, ensure_ascii=False))
    request_data = payload.setdefault("fulfillRequest", {})
    inclusive_end = end_dt - timedelta(milliseconds=1)
    start_text = start_dt.strftime("%Y/%m/%d %H:%M:%S")
    end_text = inclusive_end.strftime("%Y/%m/%d %H:%M:%S")
    request_data.update(
        {
            "startCreateTime": start_text,
            "endCreateTime": end_text,
            "createTime": [iso_utc(start_dt), iso_utc(inclusive_end)],
            "onlySearchException": False,
        }
    )
    # The database window is replaced by fulfillment creation time. Do not
    # constrain payment time as well, because cross-day payments would fall
    # through the intersection of two daily ranges.
    for key in ("startPayTime", "endPayTime", "payTime"):
        request_data.pop(key, None)
    payload["direction"] = 1
    payload.setdefault("bizType", "domestic_spot")
    payload["pageNo"] = 1
    return payload


def start_export(session: requests.Session, curl_info: dict[str, Any], payload: dict[str, Any]) -> str:
    data = request_json(
        session,
        "POST",
        curl_info["url"],
        headers=request_headers(curl_info),
        json=payload,
    )
    task_id = find_nested_value(data, "taskId")
    if not task_id:
        raise RuntimeError(f"创建导出任务后未找到 taskId: {data}")
    print(f"[INFO] export task id: {task_id}", flush=True)
    return str(task_id)


def poll_export(
    session: requests.Session,
    curl_info: dict[str, Any],
    task_id: str,
    timeout_seconds: int,
    interval_seconds: int,
) -> None:
    deadline = time.time() + timeout_seconds
    params = {"taskId": task_id, "subject_aid": subject_aid_from_url(curl_info["url"])}
    while time.time() < deadline:
        data = request_json(
            session,
            "GET",
            api_url(curl_info, "queryStatus"),
            headers=request_headers(curl_info, json_request=False),
            params=params,
        )
        status = find_nested_value(data, "Status") or find_nested_value(data, "status")
        status_text = str(status or "EXECUTING").upper()
        if status_text == "SUCCESS":
            print("[INFO] export task completed", flush=True)
            return
        if status_text == "FAILED":
            annotation = str(
                find_nested_value(data, "Annotation")
                or find_nested_value(data, "annotation")
                or ""
            )
            if "超出上限" in annotation or "50000" in annotation:
                raise ExportLimitError(annotation)
            raise RuntimeError(f"导出任务失败: {data}")
        print(f"[INFO] waiting for export task... status={status_text}", flush=True)
        time.sleep(interval_seconds)
    raise TimeoutError(f"等待导出任务超时: {task_id}")


def response_filename(response: requests.Response, task_id: str) -> str:
    disposition = response.headers.get("content-disposition", "")
    match = re.search(r"filename\*=UTF-8''([^;]+)", disposition, flags=re.I)
    if match:
        return os.path.basename(unquote(match.group(1)))
    match = re.search(r'filename="?([^";]+)', disposition, flags=re.I)
    if match:
        return os.path.basename(match.group(1))
    return f"进口超市上海仓_正向全链路数据_{task_id}.xlsx"


def download_export(
    session: requests.Session,
    curl_info: dict[str, Any],
    task_id: str,
    export_dir: str,
    start_dt: datetime,
    end_dt: datetime,
) -> str:
    params = {"taskId": task_id, "subject_aid": subject_aid_from_url(curl_info["url"])}
    response = request_with_connect_retry(
        session,
        "GET",
        api_url(curl_info, "download"),
        headers=request_headers(curl_info, json_request=False),
        params=params,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"下载失败 ({response.status_code}): {response.text[:1000]}")
    if response.headers.get("content-type", "").lower().startswith("application/json"):
        raise RuntimeError(f"下载接口返回 JSON: {response.text[:1000]}")
    os.makedirs(export_dir, exist_ok=True)
    source_name = response_filename(response, task_id)
    suffix = Path(source_name).suffix or ".xlsx"
    file_name = (
        f"进口超市上海仓_正向全链路数据_"
        f"{start_dt:%Y%m%d%H%M%S}_{end_dt:%Y%m%d%H%M%S}_{task_id}{suffix}"
    )
    out_path = os.path.join(export_dir, file_name)
    Path(out_path).write_bytes(response.content)
    print(f"[INFO] downloaded: {out_path} ({len(response.content)} bytes)", flush=True)
    return out_path


def unique_columns(columns: list[Any]) -> list[str]:
    result: list[str] = []
    counts: dict[str, int] = {}
    for value in columns:
        base = re.sub(r"\s+", "", str(value or "").strip()) or "未命名字段"
        counts[base] = counts.get(base, 0) + 1
        result.append(base if counts[base] == 1 else f"{base}_{counts[base]}")
    return result


def load_export(path: str, update_time: datetime) -> pd.DataFrame:
    if zipfile.is_zipfile(path):
        sheets = pd.read_excel(path, sheet_name=None, dtype=object)
        frames = [frame for frame in sheets.values() if not frame.dropna(how="all").empty]
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    else:
        last_error: Exception | None = None
        for encoding in ("utf-8-sig", "gb18030"):
            try:
                df = pd.read_csv(path, dtype=object, encoding=encoding)
                break
            except UnicodeDecodeError as exc:
                last_error = exc
        else:
            raise RuntimeError(f"导出文件无法读取: {last_error}")
    df = df.dropna(how="all")
    df.columns = unique_columns(list(df.columns))
    df["updatetime"] = update_time.strftime("%Y-%m-%d %H:%M:%S")
    return df


def find_pay_time_column(columns: list[str]) -> str:
    for candidate in PAY_TIME_COLUMNS:
        if candidate in columns:
            return candidate
    fuzzy = [col for col in columns if "支付时间" in col]
    if len(fuzzy) == 1:
        return fuzzy[0]
    raise RuntimeError(f"导出文件中未找到唯一的支付时间列，实际字段: {columns}")


def valid_identifier(value: str) -> str:
    if not value or "`" in value or "\x00" in value:
        raise ValueError(f"不安全的 MySQL 标识符: {value!r}")
    return value


def ensure_table(cursor: Any, table: str, df: pd.DataFrame, pay_col: str) -> None:
    table = valid_identifier(table)
    cursor.execute("SHOW TABLES LIKE %s", (table,))
    exists = cursor.fetchone() is not None
    if not exists:
        definitions = []
        for col in df.columns:
            col = valid_identifier(col)
            sql_type = "DATETIME" if col in (pay_col, "updatetime") else "TEXT"
            definitions.append(f"`{col}` {sql_type}")
        definitions.append(f"INDEX `idx_pay_time` (`{pay_col}`)")
        definitions.append("INDEX `idx_updatetime` (`updatetime`)")
        cursor.execute(
            f"CREATE TABLE `{table}` ({', '.join(definitions)}) "
            "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='进口超市上海仓正向全链路数据'"
        )
        return
    cursor.execute(f"SHOW COLUMNS FROM `{table}`")
    existing = {row[0] for row in cursor.fetchall()}
    for col in df.columns:
        if col not in existing:
            sql_type = "DATETIME" if col in (pay_col, "updatetime") else "TEXT"
            cursor.execute(f"ALTER TABLE `{table}` ADD COLUMN `{valid_identifier(col)}` {sql_type}")


def clean_for_mysql(df: pd.DataFrame, pay_col: str) -> pd.DataFrame:
    out = df.copy()
    parsed = pd.to_datetime(out[pay_col], errors="coerce")
    out[pay_col] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S").where(parsed.notna(), pd.NA)
    for col in out.columns:
        out[col] = out[col].fillna("\\N").astype(str)
        out.loc[out[col].isin(("", "nan", "None", "NaT", "<NA>")), col] = "\\N"
    return out


def connect_db() -> Any:
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            return pymysql.connect(**DB_CONFIG)
        except pymysql.MySQLError:
            if attempt == CONNECT_RETRIES:
                raise
            print(
                f"[WARN] MySQL connection failed; retrying in "
                f"{CONNECT_RETRY_DELAY}s ({attempt}/{CONNECT_RETRIES})",
                flush=True,
            )
            time.sleep(CONNECT_RETRY_DELAY)
    raise RuntimeError("unreachable")


def acquire_export_lock() -> Any:
    conn = connect_db()
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT GET_LOCK(%s, %s)",
            ("jike_trade_export:bscm_export", EXPORT_LOCK_TIMEOUT_SECONDS),
        )
        if cursor.fetchone()[0] != 1:
            conn.close()
            raise TimeoutError("another BSCM forward export process is still running")
    return conn


def write_window(df: pd.DataFrame, table: str, start_dt: datetime, end_dt: datetime) -> None:
    if df.empty:
        print("[WARN] export contains zero rows; preserving any existing window", flush=True)
        conn = connect_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT GET_LOCK(%s, %s)",
                    (f"jike_trade_export:{table}:write", WRITE_LOCK_TIMEOUT_SECONDS),
                )
                if cursor.fetchone()[0] != 1:
                    raise TimeoutError(f"timed out waiting for the ods.{table} write lock")
                cursor.execute("SHOW TABLES LIKE %s", (table,))
                if cursor.fetchone() is None:
                    return
                cursor.execute(f"SHOW COLUMNS FROM `{valid_identifier(table)}`")
                pay_col = find_pay_time_column([row[0] for row in cursor.fetchall()])
                cursor.execute(
                    f"SELECT COUNT(*) FROM `{table}` WHERE `{pay_col}` >= %s AND `{pay_col}` < %s",
                    (start_dt, end_dt),
                )
                existing = int(cursor.fetchone()[0])
                if existing:
                    raise IncompleteExportError(
                        f"empty export would replace {existing} existing rows for "
                        f"{start_dt} ~ {end_dt}"
                    )
        finally:
            conn.close()
        return

    pay_col = find_pay_time_column(list(df.columns))
    clean = clean_for_mysql(df, pay_col)
    stage = f"{table}_stage_{os.getpid()}"
    tmp_path = os.path.join(tempfile.gettempdir(), f"{stage}.csv")
    clean.to_csv(tmp_path, index=False, header=False, encoding="utf-8", lineterminator="\n")
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT GET_LOCK(%s, %s)",
            (f"jike_trade_export:{table}:write", WRITE_LOCK_TIMEOUT_SECONDS),
        )
        if cursor.fetchone()[0] != 1:
            raise TimeoutError(f"timed out waiting for the ods.{table} write lock")
        cursor.execute("SET GLOBAL local_infile = 1")
        ensure_table(cursor, table, clean, pay_col)
        cursor.execute(f"DROP TABLE IF EXISTS `{valid_identifier(stage)}`")
        cursor.execute(f"CREATE TABLE `{stage}` LIKE `{valid_identifier(table)}`")
        columns = ", ".join(f"`{valid_identifier(col)}`" for col in clean.columns)
        mysql_path = tmp_path.replace("\\", "/")
        cursor.execute(
            f"LOAD DATA LOCAL INFILE '{mysql_path}' INTO TABLE `{stage}` "
            f"CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' "
            f"LINES TERMINATED BY '\\n' ({columns})"
        )
        loaded = cursor.rowcount
        cursor.execute(
            f"SELECT COUNT(*) FROM `{table}` WHERE `{pay_col}` >= %s AND `{pay_col}` < %s",
            (start_dt, end_dt),
        )
        existing = int(cursor.fetchone()[0])
        if loaded < existing:
            raise IncompleteExportError(
                f"export rows {loaded} are fewer than existing rows {existing} for "
                f"{start_dt} ~ {end_dt}; refusing to replace the window"
            )
        cursor.execute(
            f"DELETE FROM `{table}` WHERE `{pay_col}` >= %s AND `{pay_col}` < %s",
            (start_dt, end_dt),
        )
        deleted = cursor.rowcount
        cursor.execute(f"INSERT INTO `{table}` ({columns}) SELECT {columns} FROM `{stage}`")
        cursor.execute(f"DROP TABLE IF EXISTS `{stage}`")
        conn.commit()
        print(f"[INFO] deleted {deleted} old rows, imported {loaded} rows into ods.{table}", flush=True)
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS `{stage}`")
            conn.commit()
        except Exception:
            pass
        cursor.close()
        conn.close()
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def split_windows(start_dt: datetime, end_dt: datetime, days: int) -> list[tuple[datetime, datetime]]:
    windows = []
    current = start_dt
    while current < end_dt:
        nxt = min(current + timedelta(days=days), end_dt)
        windows.append((current, nxt))
        current = nxt
    return windows


def parse_datetime(value: str, date_as_exclusive_end: bool = False) -> datetime:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        result = datetime.strptime(value, "%Y-%m-%d")
        return result + timedelta(days=1) if date_as_exclusive_end else result
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def process_window(
    session: requests.Session,
    curl_info: dict[str, Any],
    window_start: datetime,
    window_end: datetime,
    args: argparse.Namespace,
    incomplete_attempt: int = 1,
) -> int:
    print(f"[WINDOW] {window_start} ~ {window_end} (end exclusive)", flush=True)
    payload = build_window_payload(curl_info["payload"], window_start, window_end)
    task_id = start_export(session, curl_info, payload)
    try:
        poll_export(session, curl_info, task_id, args.timeout, args.interval)
    except ExportLimitError as exc:
        duration_hours = (window_end - window_start).total_seconds() / 3600
        if duration_hours <= args.min_window_hours:
            raise RuntimeError(
                f"导出窗口已经缩小到 {duration_hours:.2f} 小时，仍然超过 50000 条: {exc}"
            ) from exc
        midpoint = window_start + (window_end - window_start) / 2
        print(
            f"[WARN] {exc}; automatically splitting into "
            f"{window_start} ~ {midpoint} and {midpoint} ~ {window_end}",
            flush=True,
        )
        return (
            process_window(session, curl_info, window_start, midpoint, args)
            + process_window(session, curl_info, midpoint, window_end, args)
        )

    # BSCM can report COMPLETED slightly before the generated file is fully
    # available. A short grace period prevents downloading a valid but partial
    # JSON workbook.
    time.sleep(DOWNLOAD_STABILIZE_SECONDS)
    path = download_export(session, curl_info, task_id, args.export_dir, window_start, window_end)
    df = load_export(path, datetime.now().replace(microsecond=0))
    rows = len(df)
    print(f"[INFO] normalized rows: {rows}, columns: {len(df.columns)}", flush=True)
    if not df.empty:
        time_column = find_pay_time_column(list(df.columns))
        exported_times = pd.to_datetime(df[time_column], errors="coerce")
        invalid_rows = (
            exported_times.isna()
            | (exported_times < window_start)
            | (exported_times >= window_end)
        )
        if invalid_rows.any():
            actual_min = exported_times.min()
            actual_max = exported_times.max()
            raise RuntimeError(
                f"export window mismatch for {time_column}: requested "
                f"{window_start} ~ {window_end}, actual {actual_min} ~ {actual_max}, "
                f"invalid rows {int(invalid_rows.sum())}/{rows}; refusing to write"
            )
    if not args.no_db:
        try:
            write_window(df, args.table, window_start, window_end)
        except IncompleteExportError as exc:
            if incomplete_attempt <= args.incomplete_retries:
                print(
                    f"[WARN] {exc}; retrying export "
                    f"({incomplete_attempt}/{args.incomplete_retries})",
                    flush=True,
                )
                return process_window(
                    session,
                    curl_info,
                    window_start,
                    window_end,
                    args,
                    incomplete_attempt=incomplete_attempt + 1,
                )

            duration_hours = (window_end - window_start).total_seconds() / 3600
            half_duration_hours = duration_hours / 2
            if half_duration_hours >= args.min_window_hours:
                midpoint = window_start + (window_end - window_start) / 2
                print(
                    f"[WARN] {exc}; retries exhausted, splitting incomplete window into "
                    f"{window_start} ~ {midpoint} and {midpoint} ~ {window_end}",
                    flush=True,
                )
                return (
                    process_window(session, curl_info, window_start, midpoint, args)
                    + process_window(session, curl_info, midpoint, window_end, args)
                )

            skipped = {
                "start": window_start.strftime("%Y-%m-%d %H:%M:%S"),
                "end": window_end.strftime("%Y-%m-%d %H:%M:%S"),
                "reason": str(exc),
            }
            args.incomplete_skips.append(skipped)
            print(
                f"[DEGRADED] {exc}; minimum safe split would be "
                f"{half_duration_hours:.2f} hours, preserving existing rows and "
                "skipping this window so downstream DWD can continue",
                flush=True,
            )
            return 0
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Douyin BSCM forward fulfillment exports into MySQL.")
    parser.add_argument("--curl", default=DEFAULT_CURL, help="exportFulfillOrderList cURL file")
    parser.add_argument("--table", default=TABLE_NAME)
    parser.add_argument("--start", help="YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end", help="inclusive date or exclusive datetime")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--min-window-hours", type=float, default=DEFAULT_MIN_WINDOW_HOURS)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--interval", type=int, default=2)
    parser.add_argument(
        "--incomplete-retries",
        type=int,
        default=3,
        help="retry when an export would shrink an existing daily window",
    )
    parser.add_argument("--export-dir", default=DEFAULT_EXPORT_DIR)
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.lookback_days <= 0 or args.window_days <= 0 or args.min_window_hours <= 0:
        raise ValueError("lookback, window, and minimum window values must be greater than 0")
    if args.incomplete_retries < 0:
        raise ValueError("--incomplete-retries must not be negative")
    if args.window_days > 1:
        raise ValueError(
            "BSCM multi-day forward exports can return incomplete files; "
            "--window-days must not exceed 1"
        )
    end_dt = parse_datetime(args.end, date_as_exclusive_end=True) if args.end else datetime.now().replace(microsecond=0)
    start_dt = parse_datetime(args.start) if args.start else end_dt - timedelta(days=args.lookback_days)
    if end_dt <= start_dt:
        raise ValueError("--end must be later than --start")

    curl_info = load_curl(args.curl)
    windows = list(reversed(split_windows(start_dt, end_dt, args.window_days)))
    args.incomplete_skips = []
    print(
        f"[INFO] windows: {len(windows)}, newest first, "
        f"target: {DB_CONFIG['database']}.{args.table}",
        flush=True,
    )
    total = 0
    lock_conn = acquire_export_lock()
    try:
        with requests.Session() as session:
            for window_start, window_end in windows:
                total += process_window(session, curl_info, window_start, window_end, args)
    finally:
        lock_conn.close()
    if args.incomplete_skips:
        print(
            f"[DEGRADED] skipped {len(args.incomplete_skips)} incomplete minimum "
            f"window(s); existing database rows were preserved: "
            f"{json.dumps(args.incomplete_skips, ensure_ascii=False)}",
            flush=True,
        )
    print(f"[DONE] total rows: {total}", flush=True)


if __name__ == "__main__":
    main()
