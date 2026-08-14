"""
吉客云数据导出工具 - 公共工具模块
提供签名、API 请求、MySQL 写入等通用功能
"""

import hashlib
import json
import os
import tempfile
import time
from datetime import datetime, date

import numpy as np
import pandas as pd
import pymysql
import requests

from config import (
    APP_KEY, APP_SECRET, API_URL, VERSION, CONTENT_TYPE,
    DB_CONFIG, REQUEST_INTERVAL, REQUEST_TIMEOUT, MAX_RETRIES, MYSQL_TMP_FILE,
)


# ============================================================
# 签名
# ============================================================
def generate_sign(params: dict, secret: str = APP_SECRET) -> str:
    """吉客云 API 签名算法（MD5）"""
    sorted_keys = sorted(params.keys())
    sign_str = secret
    for key in sorted_keys:
        sign_str += key + str(params[key])
    sign_str += secret
    sign_str = sign_str.lower()
    return hashlib.md5(sign_str.encode("utf-8")).hexdigest()


# ============================================================
# API 请求
# ============================================================
def build_params(method: str, biz_content: str) -> dict:
    """构建完整的请求参数（含签名）"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    params = {
        "appkey": APP_KEY,
        "bizcontent": biz_content,
        "contenttype": CONTENT_TYPE,
        "method": method,
        "timestamp": timestamp,
        "version": VERSION,
    }
    params["sign"] = generate_sign(params)
    return params


def api_request(method: str, biz_content: str, max_retries: int = MAX_RETRIES) -> dict | None:
    """
    通用 API 请求（带重试）
    成功返回 result dict，失败返回 None
    """
    for attempt in range(1, max_retries + 1):
        params = build_params(method, biz_content)

        try:
            resp = requests.post(API_URL, data=params, timeout=REQUEST_TIMEOUT)
            result = resp.json()
        except Exception as e:
            wait = attempt * 3
            print(f"    [重试 {attempt}/{max_retries}] 请求异常: {e}")
            if attempt < max_retries:
                time.sleep(wait)
                continue
            return None

        code = result.get("code")
        if code != 200:
            wait = attempt * 3
            sub_code = result.get("subCode", "")
            msg = result.get("msg", "")
            print(f"    [重试 {attempt}/{max_retries}] code={code}, subCode={sub_code}, msg={msg}")
            if attempt < max_retries:
                time.sleep(wait)
                continue
            return None

        return result

    return None


def sleep_between_requests():
    """请求间隔"""
    time.sleep(REQUEST_INTERVAL)


def _mysql_type_for_series(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DECIMAL(18,6)"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "DATETIME"
    if pd.api.types.is_bool_dtype(series):
        return "TINYINT(1)"
    if series.name and ("日期" in str(series.name) or str(series.name).endswith("date")):
        return "DATE"
    return "TEXT"


def ensure_table_columns(cursor, table_name: str, df: pd.DataFrame):
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    existing_columns = {row[0] for row in cursor.fetchall()}
    for column in df.columns:
        if column in existing_columns:
            continue
        column_type = _mysql_type_for_series(df[column])
        cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{column}` {column_type}")
        print(f"[INFO] Added missing column `{column}` {column_type} to {table_name}")


# ============================================================
# MySQL 写入（影子表秒级原子切换方案）
# ============================================================
def init_database(table_name: str, create_table_sql: str = None):
    """
    初始化数据库，确保数据库和正式表存在
    - 幂等操作，不会删除已有数据
    - 如果正式表不存在，则用 create_table_sql 创建
    """
    conn = None
    cursor = None
    try:
        conn = pymysql.connect(
            host=DB_CONFIG["host"], port=DB_CONFIG["port"],
            user=DB_CONFIG["user"], password=DB_CONFIG["password"],
            charset=DB_CONFIG["charset"], local_infile=True,
        )
        cursor = conn.cursor()
        cursor.execute("SET GLOBAL local_infile = 1")
        db_name = DB_CONFIG["database"]

        # 创建数据库（如果不存在）
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
            "DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_0900_ai_ci"
        )
        cursor.execute(f"USE `{db_name}`")

        # 检查正式表是否存在
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone() is not None

        if not table_exists:
            if create_table_sql:
                cursor.execute(create_table_sql)
                conn.commit()
                print(f"[完成] 数据库 {db_name}，正式表 {table_name} 已创建")
            else:
                print(f"[警告] 表 {table_name} 不存在，且未提供建表 SQL")
        else:
            print(f"[完成] 数据库 {db_name} 和表 {table_name} 已就绪")

    except Exception as e:
        print(f"[错误] 初始化数据库失败: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def write_to_mysql(df: pd.DataFrame, table_name: str, create_table_sql: str = None):
    """
    将 DataFrame 写入 MySQL（影子表秒级原子切换方案）

    核心逻辑：
    1. 检查正式表是否存在，不存在则创建
    2. 创建临时影子表（CREATE TABLE ... LIKE）
    3. 使用 LOAD DATA LOCAL INFILE 将数据写入临时表
    4. 使用 RENAME TABLE 原子切换（毫秒级，报表无感知）
    5. 清理旧表

    优势：
    - 正式表始终有数据，彻底消除空表期
    - RENAME TABLE 是原子操作，报表查询不会中断
    - 异常时自动清理临时表，不会留下垃圾数据
    """
    conn = None
    cursor = None
    tmp_file = None
    tmp_table = f"{table_name}_tmp"
    old_table = f"{table_name}_old"

    try:
        # ============================================================
        # 步骤 1：连接数据库，确保正式表存在
        # ============================================================
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SET GLOBAL local_infile = 1")

        # 检查正式表是否存在
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone() is not None

        if not table_exists:
            if create_table_sql:
                cursor.execute(create_table_sql)
                conn.commit()
                print(f"[步骤 1/5] 创建正式表 {table_name}")
            else:
                raise ValueError(f"正式表 {table_name} 不存在，且未提供建表 SQL")
        else:
            print(f"[步骤 1/5] 正式表 {table_name} 已存在")

        ensure_table_columns(cursor, table_name, df)
        conn.commit()

        # ============================================================
        # 步骤 2：创建临时影子表
        # ============================================================
        cursor.execute(f"DROP TABLE IF EXISTS `{tmp_table}`")
        cursor.execute(f"CREATE TABLE `{tmp_table}` LIKE `{table_name}`")
        conn.commit()
        print(f"[步骤 2/5] 创建临时影子表 {tmp_table}")

        # ============================================================
        # 步骤 3：清洗数据
        # ============================================================
        col_list = df.columns.tolist()
        df_clean = df.copy()

        for col_name in col_list:
            series = df_clean[col_name]
            if series.dtype == object:
                df_clean[col_name] = series.fillna("\\N").astype(str)
                df_clean.loc[df_clean[col_name].isin(["nan", "None", "NaT", ""]), col_name] = "\\N"
            elif pd.api.types.is_numeric_dtype(series):
                df_clean[col_name] = series.apply(
                    lambda x: "\\N" if pd.isna(x) or (isinstance(x, float) and np.isinf(x)) else x
                )

        # 写入临时 CSV
        tmp_file = os.path.join(tempfile.gettempdir(), MYSQL_TMP_FILE)
        df_clean.to_csv(tmp_file, index=False, header=False, encoding="utf-8")
        print(f"[步骤 3/5] 数据清洗完成，{len(df_clean)} 行")

        # ============================================================
        # 步骤 4：LOAD DATA 写入临时表
        # ============================================================
        columns = df_clean.columns.tolist()
        col_names = ", ".join([f"`{c}`" for c in columns])

        tmp_path = tmp_file.replace("\\", "/")
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{tmp_path}'
            INTO TABLE `{tmp_table}`
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            ({col_names})
        """)
        affected = cursor.rowcount
        conn.commit()
        print(f"[步骤 4/5] 写入临时表 {tmp_table}：{affected} 行")

        # ============================================================
        # 步骤 5：原子切换（RENAME TABLE，毫秒级）
        # ============================================================
        # 先清理可能残留的旧表
        cursor.execute(f"DROP TABLE IF EXISTS `{old_table}`")

        # 原子切换：正式表 → 旧表，临时表 → 正式表
        cursor.execute(f"""
            RENAME TABLE
            `{table_name}` TO `{old_table}`,
            `{tmp_table}` TO `{table_name}`
        """)
        conn.commit()
        print(f"[步骤 5/5] 原子切换完成（RENAME TABLE）")

        # 清理旧表
        cursor.execute(f"DROP TABLE IF EXISTS `{old_table}`")
        conn.commit()

        print(f"\n[完成] 导入 {affected} 行到 {DB_CONFIG['database']}.{table_name}")
        print(f"[优势] 报表查询零中断，切换仅需毫秒级")

    except Exception as e:
        print(f"\n[错误] 写入失败: {e}")
        # 异常时清理临时表
        if cursor:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS `{tmp_table}`")
                conn.commit()
                print(f"[清理] 已删除临时表 {tmp_table}")
            except Exception as cleanup_err:
                print(f"[警告] 清理临时表失败: {cleanup_err}")
        raise

    finally:
        # 显式关闭连接，防止泄漏
        if cursor:
            cursor.close()
        if conn:
            conn.close()

        # 清理临时文件
        if tmp_file:
            try:
                os.remove(tmp_file)
            except OSError:
                pass


def write_to_mysql_replace_by_keys(
        df: pd.DataFrame,
        table_name: str,
        create_table_sql: str = None,
        replace_key_columns: list[str] = None,
):
    """
    Batch upsert for incremental sync.

    The batch is loaded into a staging table first. Existing rows that share
    the configured replace keys are deleted from the target table, then the
    staging rows are inserted. This keeps memory bounded and also removes
    stale detail rows when an order's line items changed.
    """
    if df.empty:
        print("[INFO] Empty batch, skip MySQL write")
        return 0
    if not replace_key_columns:
        raise ValueError("replace_key_columns is required")

    conn = None
    cursor = None
    tmp_file = None
    stage_table = f"{table_name}_stage_{os.getpid()}"

    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SET GLOBAL local_infile = 1")

        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone() is not None
        if not table_exists:
            if create_table_sql:
                cursor.execute(create_table_sql)
                conn.commit()
                print(f"[INFO] Created target table {table_name}")
            else:
                raise ValueError(f"Target table {table_name} does not exist")

        ensure_table_columns(cursor, table_name, df)
        conn.commit()

        cursor.execute(f"DROP TABLE IF EXISTS `{stage_table}`")
        cursor.execute(f"CREATE TABLE `{stage_table}` LIKE `{table_name}`")
        conn.commit()

        columns = df.columns.tolist()
        df_clean = df.copy()
        for col_name in columns:
            series = df_clean[col_name]
            if series.dtype == object:
                df_clean[col_name] = series.fillna("\\N").astype(str)
                df_clean.loc[df_clean[col_name].isin(["nan", "None", "NaT", ""]), col_name] = "\\N"
            elif pd.api.types.is_numeric_dtype(series):
                df_clean[col_name] = series.apply(
                    lambda x: "\\N" if pd.isna(x) or (isinstance(x, float) and np.isinf(x)) else x
                )

        tmp_file = os.path.join(tempfile.gettempdir(), f"{os.getpid()}_{MYSQL_TMP_FILE}")
        df_clean.to_csv(tmp_file, index=False, header=False, encoding="utf-8")

        col_names = ", ".join([f"`{c}`" for c in columns])
        tmp_path = tmp_file.replace("\\", "/")
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{tmp_path}'
            INTO TABLE `{stage_table}`
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            ({col_names})
        """)
        loaded = cursor.rowcount

        join_clause = " AND ".join([f"target.`{c}` = stage.`{c}`" for c in replace_key_columns])
        cursor.execute(f"""
            DELETE target
            FROM `{table_name}` AS target
            INNER JOIN (
                SELECT DISTINCT {", ".join([f"`{c}`" for c in replace_key_columns])}
                FROM `{stage_table}`
            ) AS stage
            ON {join_clause}
        """)
        deleted = cursor.rowcount

        cursor.execute(f"""
            INSERT INTO `{table_name}` ({col_names})
            SELECT {col_names}
            FROM `{stage_table}`
        """)
        inserted = cursor.rowcount
        conn.commit()
        print(f"[INFO] Batch loaded={loaded}, replaced={deleted}, inserted={inserted}")
        return inserted

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[ERROR] Batch replace write failed: {e}")
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
        if tmp_file:
            try:
                os.remove(tmp_file)
            except OSError:
                pass
