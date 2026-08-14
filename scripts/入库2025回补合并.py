#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""校验、合并并提交 2025 年采购入库主单和明细。"""

from __future__ import annotations

import argparse
import importlib.util
import sys
import time
from datetime import datetime
from pathlib import Path

import pymysql


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from config import DB_CONFIG


SCHEMA = "ods"
START = "2025-01-01 00:00:00"
END = "2026-01-01 00:00:00"
LOCK_NAME = "jky_inbound_2025_backfill_merge"
TABLES = {
    "formal_header": "入库查询",
    "formal_detail": "入库查询明细",
    "archive_header": "入库查询_2025归档_stage",
    "archive_detail": "入库查询明细_2025归档_stage",
    "active_header": "入库查询_2025未归档_stage",
    "active_detail": "入库查询明细_2025未归档_stage",
    "merge_header": "入库查询_2025_merge_stage",
    "merge_detail": "入库查询明细_2025_merge_stage",
}


def load_base_module():
    path = SCRIPT_DIR / "入库查询_web.py"
    spec = importlib.util.spec_from_file_location("inbound_query_base_merge", path)
    if not spec or not spec.loader:
        raise RuntimeError(f"无法加载 {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def qt(table: str) -> str:
    return f"`{SCHEMA}`.`{table}`"


def connect(retries: int = 5):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return pymysql.connect(**DB_CONFIG, autocommit=False)
        except pymysql.MySQLError as exc:
            last_error = exc
            if attempt == retries:
                break
            print(f"[WARN] MySQL 连接失败，10 秒后重试（{attempt}/{retries}）", flush=True)
            time.sleep(10)
    raise last_error


def scalar(cur, sql: str, params=()):
    cur.execute(sql, params)
    return cur.fetchone()[0]


def table_exists(cur, table: str) -> bool:
    return bool(
        scalar(
            cur,
            """
            SELECT COUNT(*) FROM information_schema.tables
             WHERE table_schema=%s AND table_name=%s
            """,
            (SCHEMA, table),
        )
    )


def columns(cur, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
         WHERE table_schema=%s AND table_name=%s
         ORDER BY ordinal_position
        """,
        (SCHEMA, table),
    )
    return [row[0] for row in cur.fetchall()]


def build_merge(cur) -> None:
    for table in (
        TABLES["formal_header"],
        TABLES["formal_detail"],
        TABLES["archive_header"],
        TABLES["archive_detail"],
        TABLES["active_header"],
        TABLES["active_detail"],
    ):
        if not table_exists(cur, table):
            raise RuntimeError(f"缺少数据表：{SCHEMA}.{table}")

    header_cols = columns(cur, TABLES["formal_header"])
    detail_cols = columns(cur, TABLES["formal_detail"])
    for table in (TABLES["archive_header"], TABLES["active_header"]):
        if columns(cur, table) != header_cols:
            raise RuntimeError(f"{table} 与正式主表结构不一致")
    for table in (TABLES["archive_detail"], TABLES["active_detail"]):
        if columns(cur, table) != detail_cols:
            raise RuntimeError(f"{table} 与正式明细表结构不一致")

    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {qt(TABLES['merge_header'])} "
        f"LIKE {qt(TABLES['formal_header'])}"
    )
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {qt(TABLES['merge_detail'])} "
        f"LIKE {qt(TABLES['formal_detail'])}"
    )
    cur.execute(f"DELETE FROM {qt(TABLES['merge_detail'])}")
    cur.execute(f"DELETE FROM {qt(TABLES['merge_header'])}")

    header_list = ", ".join(f"`{col}`" for col in header_cols)
    detail_list = ", ".join(f"`{col}`" for col in detail_cols)
    cur.execute(
        f"""
        INSERT INTO {qt(TABLES['merge_header'])} ({header_list})
        SELECT {header_list} FROM {qt(TABLES['active_header'])}
        """
    )
    active_headers = cur.rowcount
    cur.execute(
        f"""
        INSERT INTO {qt(TABLES['merge_header'])} ({header_list})
        SELECT {", ".join("a.`" + col + "`" for col in header_cols)}
          FROM {qt(TABLES['archive_header'])} a
         WHERE NOT EXISTS (
             SELECT 1 FROM {qt(TABLES['active_header'])} u
              WHERE u.`docId`=a.`docId`
         )
        """
    )
    archive_headers = cur.rowcount

    cur.execute(
        f"""
        INSERT INTO {qt(TABLES['merge_detail'])} ({detail_list})
        SELECT {detail_list} FROM {qt(TABLES['active_detail'])}
        """
    )
    active_details = cur.rowcount
    cur.execute(
        f"""
        INSERT INTO {qt(TABLES['merge_detail'])} ({detail_list})
        SELECT {", ".join("d.`" + col + "`" for col in detail_cols)}
          FROM {qt(TABLES['archive_detail'])} d
         WHERE NOT EXISTS (
             SELECT 1 FROM {qt(TABLES['active_header'])} u
              WHERE u.`docId`=d.`docId`
         )
        """
    )
    archive_details = cur.rowcount
    print(
        f"[MERGE] 主单：未归档 {active_headers} + 归档 {archive_headers}；"
        f"明细：未归档 {active_details} + 归档 {archive_details}",
        flush=True,
    )


def validate_merge(cur) -> tuple[int, int]:
    cur.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT `docId`),
               MIN(`入库时间`), MAX(`入库时间`),
               SUM(`docId` IS NULL OR `docId`=''),
               SUM(`入库时间` IS NULL),
               SUM(`入库类型编码` <> '101')
          FROM {qt(TABLES['merge_header'])}
         WHERE `入库时间` >= %s AND `入库时间` < %s
        """,
        (START, END),
    )
    header = cur.fetchone()
    cur.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT d.`recId`),
               COUNT(DISTINCT d.`docId`),
               SUM(d.`recId` IS NULL OR d.`recId`=''),
               SUM(d.`docId` IS NULL OR d.`docId`=''),
               SUM(h.`docId` IS NULL)
          FROM {qt(TABLES['merge_detail'])} d
          LEFT JOIN {qt(TABLES['merge_header'])} h ON h.`docId`=d.`docId`
        """
    )
    detail = cur.fetchone()
    if header[0] != header[1] or header[4] or header[5] or header[6]:
        raise RuntimeError(f"主单关键字段校验失败：{header}")
    if detail[0] != detail[1] or detail[3] or detail[4] or detail[5]:
        raise RuntimeError(f"明细关键字段/孤儿记录校验失败：{detail}")

    cur.execute(
        f"""
        SELECT DATE_FORMAT(`入库时间`, '%%Y-%%m'), COUNT(*)
          FROM {qt(TABLES['merge_header'])}
         WHERE `入库时间` >= %s AND `入库时间` < %s
         GROUP BY 1 ORDER BY 1
        """,
        (START, END),
    )
    months = cur.fetchall()
    expected = {f"2025-{month:02d}" for month in range(1, 13)}
    if {row[0] for row in months} != expected:
        raise RuntimeError(f"月份覆盖不完整：{months}")
    print(
        f"[VALID] 主单 {header[0]}，明细 {detail[0]}，"
        f"时间 {header[2]} ~ {header[3]}",
        flush=True,
    )
    for month, count in months:
        print(f"  {month}: {count} 张主单", flush=True)
    return header[0], detail[0]


def backup_existing_2025(cur) -> tuple[str | None, str | None]:
    count = scalar(
        cur,
        f"""
        SELECT COUNT(*) FROM {qt(TABLES['formal_header'])}
         WHERE `入库时间` >= %s AND `入库时间` < %s
        """,
        (START, END),
    )
    if not count:
        return None, None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    header_backup = f"入库查询_2025替换前备份_{stamp}"
    detail_backup = f"入库查询明细_2025替换前备份_{stamp}"
    cur.execute(f"CREATE TABLE {qt(header_backup)} LIKE {qt(TABLES['formal_header'])}")
    cur.execute(f"CREATE TABLE {qt(detail_backup)} LIKE {qt(TABLES['formal_detail'])}")
    cur.execute(
        f"""
        INSERT INTO {qt(header_backup)}
        SELECT * FROM {qt(TABLES['formal_header'])}
         WHERE `入库时间` >= %s AND `入库时间` < %s
        """,
        (START, END),
    )
    cur.execute(
        f"""
        INSERT INTO {qt(detail_backup)}
        SELECT d.* FROM {qt(TABLES['formal_detail'])} d
        JOIN {qt(TABLES['formal_header'])} h ON h.`docId`=d.`docId`
         WHERE h.`入库时间` >= %s AND h.`入库时间` < %s
        """,
        (START, END),
    )
    print(
        f"[BACKUP] 已备份原正式表 2025 数据至 {header_backup} / {detail_backup}",
        flush=True,
    )
    return header_backup, detail_backup


def commit_formal(cur, base_module) -> None:
    cur.execute(
        f"""
        DELETE d FROM {qt(TABLES['formal_detail'])} d
        JOIN {qt(TABLES['formal_header'])} h ON h.`docId`=d.`docId`
         WHERE h.`入库时间` >= %s AND h.`入库时间` < %s
        """,
        (START, END),
    )
    cur.execute(
        f"""
        DELETE FROM {qt(TABLES['formal_header'])}
         WHERE `入库时间` >= %s AND `入库时间` < %s
        """,
        (START, END),
    )
    cur.execute(
        f"INSERT INTO {qt(TABLES['formal_header'])} "
        f"SELECT * FROM {qt(TABLES['merge_header'])}"
    )
    header_count = cur.rowcount
    cur.execute(
        f"INSERT INTO {qt(TABLES['formal_detail'])} "
        f"SELECT * FROM {qt(TABLES['merge_detail'])}"
    )
    detail_count = cur.rowcount
    base_module.refresh_monthly_table(cur)
    print(
        f"[FORMAL] 写入主单 {header_count}，明细 {detail_count}，已刷新品牌月度到货",
        flush=True,
    )


def parse_args():
    parser = argparse.ArgumentParser(description="校验并合并 2025 年采购入库回补数据")
    parser.add_argument("--commit", action="store_true", help="校验后写入正式表")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_module = load_base_module()
    conn = connect()
    locked = False
    try:
        with conn.cursor() as cur:
            locked = scalar(cur, "SELECT GET_LOCK(%s, 30)", (LOCK_NAME,)) == 1
            if not locked:
                raise RuntimeError("未取得采购入库回补锁")
            build_merge(cur)
            expected_headers, expected_details = validate_merge(cur)
            conn.commit()
            if not args.commit:
                print("[DONE] 合并暂存和校验完成，未修改正式表", flush=True)
                return

            # 旧环境中的“品牌月度到货”可能仍是 VIEW；沿用主脚本的迁移逻辑，
            # 先转换为可刷新的汇总表。该步骤包含 DDL，必须在替换事务之前完成。
            base_module.ensure_schema(cur)
            conn.commit()

            # DDL 会隐式提交，因此备份在正式替换事务之前完成。
            backup_existing_2025(cur)
            conn.commit()
            conn.begin()
            try:
                commit_formal(cur, base_module)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            formal_headers = scalar(
                cur,
                f"""
                SELECT COUNT(*) FROM {qt(TABLES['formal_header'])}
                 WHERE `入库时间` >= %s AND `入库时间` < %s
                """,
                (START, END),
            )
            formal_details = scalar(
                cur,
                f"""
                SELECT COUNT(*) FROM {qt(TABLES['formal_detail'])} d
                JOIN {qt(TABLES['formal_header'])} h ON h.`docId`=d.`docId`
                 WHERE h.`入库时间` >= %s AND h.`入库时间` < %s
                """,
                (START, END),
            )
            if (formal_headers, formal_details) != (expected_headers, expected_details):
                raise RuntimeError(
                    f"正式表复核不一致：{formal_headers}/{formal_details} != "
                    f"{expected_headers}/{expected_details}"
                )
            print(
                f"[CHECK] 正式表 2025：主单 {formal_headers}，明细 {formal_details}",
                flush=True,
            )
    finally:
        if locked:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT RELEASE_LOCK(%s)", (LOCK_NAME,))
            except Exception:
                pass
        conn.close()


if __name__ == "__main__":
    main()
