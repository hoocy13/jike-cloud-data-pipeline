#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""构建无歧义的货品品牌映射，并补全销售单明细账的空品牌。

映射优先级：条码 -> 货品编号 -> 货品名称+规格。
只保留同一映射键下品牌唯一的记录，不做模糊推断，不覆盖已有品牌。
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pymysql


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from config import DB_CONFIG


SCHEMA = "ods"
MAP_TABLE = "货品品牌映射"
SALES_TABLE = "销售单明细账"
DWD_VIEW = "销售单明细账_品牌补全"
LOCK_NAME = "jky_sales_brand_backfill"
DB_RETRIES = 5
DB_RETRY_DELAY_SECONDS = 10
RETRYABLE_DB_ERROR_CODES = {1205, 1213, 2003, 2006, 2013}
REFERENCE_TABLES = (
    ("入库查询明细", True),
    ("总库存查询", False),
    ("分仓库查询", False),
    ("批次货品库存查询", False),
    ("历史库存", False),
)
MISSING_TEXT = {"", "\\N", "None", "nan", "NaN", "<NA>"}
BRAND_NAME_RULES = (
    ("兰蔻", (r"兰蔻", r"LANCOME")),
    ("资生堂", (r"资生堂", r"SHISEIDO", r"SHESEIDO")),
    ("YSL", (r"圣罗兰", r"(?<![A-Z])YSL(?![A-Z])")),
    ("赫莲娜", (r"赫莲娜", r"(?<![A-Z])HR(?![A-Z])")),
    ("植村秀", (r"植村秀", r"SHU UEMURA")),
    ("阿玛尼", (r"阿玛尼", r"ARMANI")),
    ("肌肤之钥", (r"肌肤之钥", r"CLE DE PEAU")),
    ("雅诗兰黛", (r"雅诗兰黛",)),
    ("娇兰", (r"娇兰",)),
    ("TOM FORD", (r"TOM FORD",)),
    ("适乐肤", (r"适乐肤", r"CERAVE")),
    ("伊丽莎白雅顿", (r"伊丽莎白雅顿", r"伊丽莎白雅粉胶")),
    ("伊菲丹", (r"伊菲丹",)),
    ("修丽可", (r"修丽可",)),
    ("帕尔玛之水", (r"帕尔玛之水",)),
    ("梧颜", (r"梧颜",)),
    ("欧舒丹", (r"欧舒丹",)),
    ("希思黎", (r"希思黎",)),
    ("莱珀妮", (r"莱珀妮",)),
    ("迪奥", (r"迪奥",)),
    ("海蓝之谜", (r"海蓝之谜",)),
    ("科颜氏", (r"科颜氏",)),
    ("卡诗", (r"卡诗",)),
    ("纪梵希", (r"纪梵希",)),
    ("无品牌", (r"^分装罐(?:\s|$)", r"^补差价专用$")),
)

# 货品名称本身不足以识别品牌，但已通过同编码前缀及同款历史销售记录人工核验。
MANUAL_GOODS_BRANDS = {
    "DC-1121": "兰蔻",
    "HR-2401061107": "赫莲娜",
    "LC-20246211129": "兰蔻",
    "LC-20424621": "兰蔻",
    "LC-20246202032": "兰蔻",
}


def connect(retries: int = 5):
    last_error = None
    config = dict(DB_CONFIG)
    config.setdefault("connect_timeout", 20)
    config.setdefault("read_timeout", 300)
    config.setdefault("write_timeout", 300)
    for attempt in range(1, retries + 1):
        try:
            return pymysql.connect(**config, autocommit=False)
        except pymysql.MySQLError as exc:
            last_error = exc
            if attempt == retries:
                break
            print(f"[WARN] MySQL 连接失败，10 秒后重试（{attempt}/{retries}）", flush=True)
            time.sleep(10)
    raise last_error


def acquire_lock(conn, timeout_seconds: int = 30) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT GET_LOCK(%s, %s)", (LOCK_NAME, timeout_seconds))
        if cur.fetchone()[0] != 1:
            raise RuntimeError("未取得销售品牌回填锁")


def execute_update_with_retry(conn, sql: str, params: tuple[Any, ...]):
    """Run an idempotent blank-brand update and reconnect after transient failures."""
    for attempt in range(1, DB_RETRIES + 1):
        try:
            # Do not let PyMySQL reconnect silently: a new session must reacquire
            # the MySQL named lock before it is allowed to continue writing.
            conn.ping(reconnect=False)
            with conn.cursor() as cur:
                cur.execute(sql, params)
                changed = cur.rowcount
            conn.commit()
            return conn, changed
        except pymysql.MySQLError as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            if not exc.args or exc.args[0] not in RETRYABLE_DB_ERROR_CODES or attempt >= DB_RETRIES:
                raise
            try:
                conn.close()
            except Exception:
                pass
            print(
                f"[WARN] 品牌月度更新连接中断，{DB_RETRY_DELAY_SECONDS}秒后从当前步骤续跑 "
                f"({attempt}/{DB_RETRIES}): {exc}",
                flush=True,
            )
            time.sleep(DB_RETRY_DELAY_SECONDS)
            conn = connect()
            acquire_lock(conn)
    raise RuntimeError("unreachable")


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text in MISSING_TEXT else text


def key_hash(mapping_type: str, key1: str, key2: str = "") -> str:
    return hashlib.sha256(f"{mapping_type}\x1f{key1}\x1f{key2}".encode("utf-8")).hexdigest()


def load_reference_rows(cur) -> list[tuple[str, str, str, str, str, str, str]]:
    rows: list[tuple[str, str, str, str, str, str, str]] = []
    for table, has_brand_id in REFERENCE_TABLES:
        brand_id_sql = "`品牌ID`" if has_brand_id else "NULL"
        cur.execute(
            f"""
            SELECT `条码`, `货品编号`, `货品名称`, `规格`,
                   {brand_id_sql} AS `品牌ID`, `品牌`
              FROM `{SCHEMA}`.`{table}`
             WHERE LENGTH(TRIM(COALESCE(`品牌`, ''))) > 0
            """
        )
        source_rows = cur.fetchall()
        for barcode, goods_no, goods_name, spec, brand_id, brand in source_rows:
            rows.append(
                (
                    clean_text(barcode),
                    clean_text(goods_no),
                    clean_text(goods_name),
                    clean_text(spec),
                    clean_text(brand_id),
                    clean_text(brand),
                    table,
                )
            )
        print(f"[SOURCE] {table}: {len(source_rows)} 行有效品牌参考", flush=True)
    return rows


def build_unambiguous_maps(
    rows: Iterable[tuple[str, str, str, str, str, str, str]],
) -> tuple[list[tuple[Any, ...]], dict[str, tuple[int, int]]]:
    candidates: dict[tuple[str, str, str], dict[str, dict[str, set[str]]]] = defaultdict(
        lambda: defaultdict(lambda: {"brand_ids": set(), "sources": set()})
    )
    for barcode, goods_no, goods_name, spec, brand_id, brand, source in rows:
        keys = []
        if barcode:
            keys.append(("条码", barcode, ""))
        if goods_no:
            keys.append(("货品编号", goods_no, ""))
        if goods_name:
            keys.append(("名称规格", goods_name, spec))
        for key in keys:
            item = candidates[key][brand]
            if brand_id:
                item["brand_ids"].add(brand_id)
            item["sources"].add(source)

    output: list[tuple[Any, ...]] = []
    stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    now = datetime.now().replace(microsecond=0)
    for (mapping_type, key1, key2), brands in candidates.items():
        if len(brands) != 1:
            stats[mapping_type][1] += 1
            continue
        brand, evidence = next(iter(brands.items()))
        brand_ids = evidence["brand_ids"]
        brand_id = next(iter(brand_ids)) if len(brand_ids) == 1 else None
        sources = sorted(evidence["sources"])
        output.append(
            (
                key_hash(mapping_type, key1, key2),
                mapping_type,
                key1,
                key2 or None,
                brand_id,
                brand,
                len(sources),
                ",".join(sources),
                now,
            )
        )
        stats[mapping_type][0] += 1
    return output, {key: tuple(value) for key, value in stats.items()}


def refresh_mapping(conn) -> dict[str, tuple[int, int]]:
    with conn.cursor() as cur:
        rows = load_reference_rows(cur)
        mappings, stats = build_unambiguous_maps(rows)
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{SCHEMA}`.`{MAP_TABLE}` (
                `映射键哈希` CHAR(64) NOT NULL,
                `映射类型` VARCHAR(20) NOT NULL,
                `映射键1` VARCHAR(768) NOT NULL,
                `映射键2` VARCHAR(768) NULL,
                `品牌ID` VARCHAR(64) NULL,
                `品牌` VARCHAR(255) NOT NULL,
                `来源数` INT NOT NULL,
                `来源表` TEXT NULL,
                `updatetime` DATETIME NOT NULL,
                PRIMARY KEY (`映射键哈希`),
                INDEX `idx_映射类型_键1` (`映射类型`, `映射键1`(100))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        cur.execute(f"DELETE FROM `{SCHEMA}`.`{MAP_TABLE}`")
        cur.executemany(
            f"""
            INSERT INTO `{SCHEMA}`.`{MAP_TABLE}` (
                `映射键哈希`, `映射类型`, `映射键1`, `映射键2`,
                `品牌ID`, `品牌`, `来源数`, `来源表`, `updatetime`
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            mappings,
        )
    conn.commit()
    for mapping_type, (valid, conflicts) in sorted(stats.items()):
        print(f"[MAP] {mapping_type}: 有效 {valid}，冲突排除 {conflicts}", flush=True)
    return stats


def augment_mapping_from_sales(conn) -> dict[str, tuple[int, int, int]]:
    """仅当外部参考表没有该键时，补入销售表内部始终一致的品牌映射。"""
    rules = (
        ("条码", "TRIM(`货品条码`)", "''", "LENGTH(TRIM(COALESCE(`货品条码`,'')))>0"),
        ("货品编号", "TRIM(`货品编号`)", "''", "LENGTH(TRIM(COALESCE(`货品编号`,'')))>0"),
        (
            "名称规格",
            "TRIM(`货品名称`)",
            "TRIM(COALESCE(`规格`,''))",
            "LENGTH(TRIM(COALESCE(`货品名称`,'')))>0",
        ),
    )
    now = datetime.now().replace(microsecond=0)
    result: dict[str, tuple[int, int, int]] = {}
    with conn.cursor() as cur:
        for mapping_type, key1_sql, key2_sql, key_filter in rules:
            cur.execute(
                f"""
                SELECT {key1_sql} AS key1, {key2_sql} AS key2,
                       MIN(TRIM(`品牌`)) AS brand,
                       COUNT(DISTINCT TRIM(`品牌`)) AS brand_variants
                  FROM `{SCHEMA}`.`{SALES_TABLE}`
                 WHERE LENGTH(TRIM(COALESCE(`品牌`,'')))>0
                   AND `品牌` <> '\\N'
                   AND {key_filter}
                 GROUP BY {key1_sql}, {key2_sql}
                """
            )
            valid_rows = []
            conflicts = 0
            for key1, key2, brand, variants in cur.fetchall():
                if variants != 1:
                    conflicts += 1
                    continue
                valid_rows.append(
                    (
                        key_hash(mapping_type, key1, key2 or ""),
                        mapping_type,
                        key1,
                        key2 or None,
                        None,
                        brand,
                        1,
                        SALES_TABLE,
                        now,
                    )
                )
            inserted = 0
            if valid_rows:
                cur.executemany(
                    f"""
                    INSERT IGNORE INTO `{SCHEMA}`.`{MAP_TABLE}` (
                        `映射键哈希`, `映射类型`, `映射键1`, `映射键2`,
                        `品牌ID`, `品牌`, `来源数`, `来源表`, `updatetime`
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    valid_rows,
                )
                inserted = cur.rowcount
            conn.commit()
            result[mapping_type] = (len(valid_rows), conflicts, inserted)
            print(
                f"[SALES MAP] {mapping_type}: 无歧义 {len(valid_rows)}，"
                f"冲突排除 {conflicts}，新增 {inserted}",
                flush=True,
            )
    return result


def load_mapping_dicts(conn=None) -> dict[str, dict[Any, str]]:
    owns_connection = conn is None
    if conn is None:
        conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT `映射类型`, `映射键1`, COALESCE(`映射键2`, ''), `品牌`
                  FROM `{SCHEMA}`.`{MAP_TABLE}`
                """
            )
            result: dict[str, dict[Any, str]] = {
                "条码": {},
                "货品编号": {},
                "名称规格": {},
            }
            for mapping_type, key1, key2, brand in cur.fetchall():
                key = (key1, key2) if mapping_type == "名称规格" else key1
                result[mapping_type][key] = brand
            return result
    finally:
        if owns_connection:
            conn.close()


def enrich_dataframe_from_brand_mapping(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """只填充 DataFrame 中的空品牌；映射表不可用时保留原数据。"""
    if "品牌" not in df.columns:
        return df, {}
    try:
        mappings = load_mapping_dicts()
    except Exception as exc:
        print(f"[WARN] 品牌映射加载失败，保留导出原值：{exc}", flush=True)
        return df, {}

    out = df.copy()
    brand_text = out["品牌"].astype("string").str.strip()
    missing = brand_text.isna() | brand_text.isin(MISSING_TEXT)
    counts: dict[str, int] = {}

    if "货品条码" in out.columns:
        keys = out["货品条码"].astype("string").str.strip()
        values = keys.map(mappings["条码"])
        hit = missing & values.notna()
        out.loc[hit, "品牌"] = values.loc[hit]
        counts["条码"] = int(hit.sum())
        missing &= ~hit

    if "货品编号" in out.columns:
        keys = out["货品编号"].astype("string").str.strip()
        values = keys.map(mappings["货品编号"])
        hit = missing & values.notna()
        out.loc[hit, "品牌"] = values.loc[hit]
        counts["货品编号"] = int(hit.sum())
        missing &= ~hit

    if "货品名称" in out.columns and "规格" in out.columns:
        names = out["货品名称"].astype("string").str.strip()
        specs = out["规格"].astype("string").fillna("").str.strip()
        values = pd.Series(
            [mappings["名称规格"].get((clean_text(n), clean_text(s))) for n, s in zip(names, specs)],
            index=out.index,
            dtype="string",
        )
        hit = missing & values.notna()
        out.loc[hit, "品牌"] = values.loc[hit]
        counts["名称规格"] = int(hit.sum())
        missing &= ~hit

    if "货品编号" in out.columns:
        keys = out["货品编号"].astype("string").str.strip()
        values = keys.map(MANUAL_GOODS_BRANDS)
        hit = missing & values.notna()
        out.loc[hit, "品牌"] = values.loc[hit]
        counts["人工核验"] = int(hit.sum())
        missing &= ~hit

    if "货品名称" in out.columns:
        names_upper = out["货品名称"].astype("string").fillna("").str.upper()
        dictionary_hits = 0
        for brand, patterns in BRAND_NAME_RULES:
            rule_hit = pd.Series(False, index=out.index)
            for pattern in patterns:
                rule_hit |= names_upper.str.contains(pattern, regex=True, na=False)
            hit = missing & rule_hit
            out.loc[hit, "品牌"] = brand
            dictionary_hits += int(hit.sum())
            missing &= ~hit
        counts["品牌词典"] = dictionary_hits

    counts["未命中"] = int(missing.sum())
    print(f"[BRAND] 导入品牌补全：{counts}", flush=True)
    return out, counts


def month_ranges(cur, lookback_days: int | None = None):
    cur.execute(
        f"SELECT MIN(`下单时间`), MAX(`下单时间`) FROM `{SCHEMA}`.`{SALES_TABLE}`"
    )
    min_time, max_time = cur.fetchone()
    if not min_time or not max_time:
        return
    current = min_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if lookback_days is not None:
        cutoff = datetime.now().replace(microsecond=0) - timedelta(days=lookback_days)
        current = max(current, cutoff)
    end = max_time
    while current <= end:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1)
        else:
            next_month = current.replace(month=current.month + 1)
        yield current, next_month
        current = next_month


def backfill_sales(conn, lookback_days: int | None = None):
    totals = {"条码": 0, "货品编号": 0, "名称规格": 0, "人工核验": 0, "品牌词典": 0}
    with conn.cursor() as cur:
        ranges = list(month_ranges(cur, lookback_days))
    if lookback_days is not None:
        print(f"[INFO] 品牌日常增量回填范围: 最近 {lookback_days} 天", flush=True)
    for start, end in ranges:
        rules = (
            (
                "条码",
                "t.`货品条码` = m.`映射键1`",
                "m.`映射类型` = '条码'",
            ),
            (
                "货品编号",
                "t.`货品编号` = m.`映射键1`",
                "m.`映射类型` = '货品编号'",
            ),
            (
                "名称规格",
                "t.`货品名称` = m.`映射键1` AND COALESCE(t.`规格`, '') = COALESCE(m.`映射键2`, '')",
                "m.`映射类型` = '名称规格'",
            ),
        )
        for label, join_condition, map_filter in rules:
            conn, changed = execute_update_with_retry(
                conn,
                f"""
                    UPDATE `{SCHEMA}`.`{SALES_TABLE}` t
                    JOIN `{SCHEMA}`.`{MAP_TABLE}` m ON {join_condition}
                       SET t.`品牌` = m.`品牌`
                     WHERE {map_filter}
                       AND t.`下单时间` >= %s AND t.`下单时间` < %s
                       AND (t.`品牌` IS NULL OR LENGTH(TRIM(t.`品牌`))=0 OR t.`品牌`='\\N')
                    """,
                (start, end),
            )
            totals[label] += changed

        manual_case = """CASE `货品编号`
            WHEN 'DC-1121' THEN '兰蔻'
            WHEN 'HR-2401061107' THEN '赫莲娜'
            WHEN 'LC-20246211129' THEN '兰蔻'
            WHEN 'LC-20424621' THEN '兰蔻'
            WHEN 'LC-20246202032' THEN '兰蔻'
            ELSE NULL END"""
        conn, changed = execute_update_with_retry(
            conn,
            f"""
                UPDATE `{SCHEMA}`.`{SALES_TABLE}`
                   SET `品牌` = {manual_case}
                 WHERE `下单时间` >= %s AND `下单时间` < %s
                   AND (`品牌` IS NULL OR LENGTH(TRIM(`品牌`))=0 OR `品牌`='\\N')
                   AND ({manual_case}) IS NOT NULL
                """,
            (start, end),
        )
        totals["人工核验"] += changed

        dictionary_case = """CASE
            WHEN `货品名称` LIKE '%%兰蔻%%' OR UPPER(`货品名称`) LIKE '%%LANCOME%%' THEN '兰蔻'
            WHEN `货品名称` LIKE '%%资生堂%%' OR UPPER(`货品名称`) LIKE '%%SHISEIDO%%' OR UPPER(`货品名称`) LIKE '%%SHESEIDO%%' THEN '资生堂'
            WHEN `货品名称` LIKE '%%圣罗兰%%' OR UPPER(`货品名称`) REGEXP '(^|[^A-Z])YSL([^A-Z]|$)' THEN 'YSL'
            WHEN `货品名称` LIKE '%%赫莲娜%%' OR UPPER(`货品名称`) REGEXP '(^|[^A-Z])HR([^A-Z]|$)' THEN '赫莲娜'
            WHEN `货品名称` LIKE '%%植村秀%%' OR UPPER(`货品名称`) LIKE '%%SHU UEMURA%%' THEN '植村秀'
            WHEN `货品名称` LIKE '%%阿玛尼%%' OR UPPER(`货品名称`) LIKE '%%ARMANI%%' THEN '阿玛尼'
            WHEN `货品名称` LIKE '%%肌肤之钥%%' OR UPPER(`货品名称`) LIKE '%%CLE DE PEAU%%' THEN '肌肤之钥'
            WHEN `货品名称` LIKE '%%雅诗兰黛%%' THEN '雅诗兰黛'
            WHEN `货品名称` LIKE '%%娇兰%%' THEN '娇兰'
            WHEN UPPER(`货品名称`) LIKE '%%TOM FORD%%' THEN 'TOM FORD'
            WHEN `货品名称` LIKE '%%适乐肤%%' OR UPPER(`货品名称`) LIKE '%%CERAVE%%' THEN '适乐肤'
            WHEN `货品名称` LIKE '%%伊丽莎白雅顿%%' OR `货品名称` LIKE '%%伊丽莎白雅粉胶%%' THEN '伊丽莎白雅顿'
            WHEN `货品名称` LIKE '%%伊菲丹%%' THEN '伊菲丹'
            WHEN `货品名称` LIKE '%%修丽可%%' THEN '修丽可'
            WHEN `货品名称` LIKE '%%帕尔玛之水%%' THEN '帕尔玛之水'
            WHEN `货品名称` LIKE '%%梧颜%%' THEN '梧颜'
            WHEN `货品名称` LIKE '%%欧舒丹%%' THEN '欧舒丹'
            WHEN `货品名称` LIKE '%%希思黎%%' THEN '希思黎'
            WHEN `货品名称` LIKE '%%莱珀妮%%' THEN '莱珀妮'
            WHEN `货品名称` LIKE '%%迪奥%%' THEN '迪奥'
            WHEN `货品名称` LIKE '%%海蓝之谜%%' THEN '海蓝之谜'
            WHEN `货品名称` LIKE '%%科颜氏%%' THEN '科颜氏'
            WHEN `货品名称` LIKE '%%卡诗%%' THEN '卡诗'
            WHEN `货品名称` LIKE '%%纪梵希%%' THEN '纪梵希'
            WHEN `货品名称` REGEXP '^分装罐([[:space:]]|$)' OR `货品名称` = '补差价专用' THEN '无品牌'
            ELSE NULL END"""
        conn, changed = execute_update_with_retry(
            conn,
            f"""
                UPDATE `{SCHEMA}`.`{SALES_TABLE}`
                   SET `品牌` = {dictionary_case}
                 WHERE `下单时间` >= %s AND `下单时间` < %s
                   AND (`品牌` IS NULL OR LENGTH(TRIM(`品牌`))=0 OR `品牌`='\\N')
                   AND ({dictionary_case}) IS NOT NULL
                """,
            (start, end),
        )
        totals["品牌词典"] += changed
        print(f"[UPDATE] {start:%Y-%m} 完成", flush=True)
    print(f"[DONE] 品牌回填行数：{totals}", flush=True)
    return conn, totals


def ensure_dwd_view(conn) -> None:
    """提供稳定的 DWD 消费入口；现阶段保留 ODS 兼容写法，不复制四百万行数据。"""
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE IF NOT EXISTS `dwd` DEFAULT CHARACTER SET utf8mb4")
        cur.execute(
            f"""
            CREATE OR REPLACE SQL SECURITY INVOKER VIEW `dwd`.`{DWD_VIEW}` AS
            SELECT * FROM `{SCHEMA}`.`{SALES_TABLE}`
            """
        )
    conn.commit()
    print(f"[DWD] 已刷新视图 dwd.{DWD_VIEW}", flush=True)


def parse_args():
    parser = argparse.ArgumentParser(description="构建品牌映射并回填销售单明细账")
    parser.add_argument("--refresh-map", action="store_true", help="刷新货品品牌映射")
    parser.add_argument("--include-sales", action="store_true", help="补入销售表内部无歧义映射")
    parser.add_argument("--backfill", action="store_true", help="回填销售单明细账空品牌")
    parser.add_argument(
        "--lookback-days",
        type=int,
        help="仅回填最近N天；不传时执行全部历史月份",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.lookback_days is not None and args.lookback_days <= 0:
        raise ValueError("--lookback-days 必须大于0")
    if not args.refresh_map and not args.include_sales and not args.backfill:
        args.refresh_map = True
    conn = connect()
    locked = False
    try:
        acquire_lock(conn, 5)
        locked = True
        if args.refresh_map:
            refresh_mapping(conn)
        if args.include_sales:
            augment_mapping_from_sales(conn)
        if args.backfill:
            conn, _ = backfill_sales(conn, args.lookback_days)
        ensure_dwd_view(conn)
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
