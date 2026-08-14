#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""吉客云日常同步后的核心数据质量闸门。

结果写入 ops.dq_check_result；任一关键检查失败时返回非零状态，阻止 ADS 刷新。
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pymysql


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import DB_CONFIG


@dataclass
class Check:
    name: str
    passed: bool
    actual: Any
    expected: str
    severity: str = "CRITICAL"


def connect(database: str | None = None):
    config = dict(DB_CONFIG)
    if database:
        config["database"] = database
    return pymysql.connect(**config, autocommit=False)


def scalar(cur, sql: str, params: tuple[Any, ...] = ()) -> Any:
    cur.execute(sql, params)
    row = cur.fetchone()
    return None if row is None else row[0]


def ensure_result_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE IF NOT EXISTS `ops` DEFAULT CHARACTER SET utf8mb4")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS `ops`.`dq_check_result` (
              `run_id` VARCHAR(36) NOT NULL,
              `check_time` DATETIME NOT NULL,
              `check_name` VARCHAR(128) NOT NULL,
              `passed` TINYINT(1) NOT NULL,
              `severity` VARCHAR(16) NOT NULL,
              `actual_value` TEXT NULL,
              `expected_rule` TEXT NOT NULL,
              PRIMARY KEY (`run_id`, `check_name`),
              INDEX `idx_check_time` (`check_time`),
              INDEX `idx_passed_time` (`passed`, `check_time`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='吉客云数据质量闸门结果'
            """
        )
    conn.commit()


def table_exists(cur, schema: str, table: str) -> bool:
    return bool(
        scalar(
            cur,
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            (schema, table),
        )
    )


def run_checks(conn, max_age_hours: int) -> list[Check]:
    checks: list[Check] = []
    required = (
        ("ods", "销售单查询"),
        ("ods", "销售单明细账"),
        ("ods", "入库查询"),
        ("ods", "入库查询明细"),
        ("ods", "总库存查询"),
        ("ods", "分仓库查询"),
        ("ods", "批次货品库存查询"),
        ("dwd", "销售单查询_进口超市上海仓补全"),
        ("dwd", "销售单明细账_品牌补全"),
    )
    with conn.cursor() as cur:
        existence = {(s, t): table_exists(cur, s, t) for s, t in required}
        for schema, table in required:
            checks.append(Check(f"表存在:{schema}.{table}", existence[(schema, table)], existence[(schema, table)], "必须存在"))

        for schema, table in required:
            if not existence[(schema, table)]:
                continue
            if (schema, table) == ("dwd", "销售单明细账_品牌补全"):
                rows = scalar(cur, f"SELECT EXISTS(SELECT 1 FROM `{schema}`.`{table}` LIMIT 1)")
                expected = "至少存在1行"
            else:
                rows = scalar(cur, f"SELECT COUNT(*) FROM `{schema}`.`{table}`")
                expected = "> 0"
            checks.append(Check(f"表非空:{schema}.{table}", rows > 0, rows, expected))

        freshness_tables = (
            ("ods", "销售单查询"),
            ("ods", "销售单明细账"),
            ("ods", "入库查询"),
            ("ods", "总库存查询"),
            ("ods", "分仓库查询"),
            ("ods", "批次货品库存查询"),
        )
        for schema, table in freshness_tables:
            if not existence.get((schema, table)):
                continue
            age = scalar(
                cur,
                f"SELECT TIMESTAMPDIFF(HOUR, MAX(`updatetime`), NOW()) FROM `{schema}`.`{table}`",
            )
            passed = age is not None and age <= max_age_hours
            checks.append(Check(f"新鲜度:{schema}.{table}", passed, age, f"<= {max_age_hours}小时"))

        if existence[("ods", "销售单明细账")]:
            blanks = scalar(
                cur,
                """SELECT COUNT(*) FROM `ods`.`销售单明细账`
                   WHERE `品牌` IS NULL OR LENGTH(TRIM(`品牌`))=0 OR `品牌`='\\N'""",
            )
            checks.append(Check("销售明细品牌完整", blanks == 0, blanks, "空品牌行数 = 0"))

        if existence[("ods", "入库查询")] and existence[("ods", "入库查询明细")]:
            orphans = scalar(
                cur,
                """SELECT COUNT(*) FROM `ods`.`入库查询明细` d
                   LEFT JOIN `ods`.`入库查询` h ON h.`docId`=d.`docId`
                   WHERE h.`docId` IS NULL""",
            )
            checks.append(Check("入库主明细完整", orphans == 0, orphans, "孤儿明细行数 = 0"))

        if existence[("ods", "销售单查询")] and existence[("dwd", "销售单查询_进口超市上海仓补全")]:
            ods_rows = scalar(cur, "SELECT COUNT(*) FROM `ods`.`销售单查询`")
            dwd_rows = scalar(cur, "SELECT COUNT(*) FROM `dwd`.`销售单查询_进口超市上海仓补全`")
            checks.append(Check("销售DWD行数一致", ods_rows == dwd_rows, {"ods": ods_rows, "dwd": dwd_rows}, "ODS行数 = DWD行数"))
    return checks


def persist(conn, run_id: str, checks: list[Check]) -> None:
    now = datetime.now().replace(microsecond=0)
    rows = [
        (
            run_id,
            now,
            check.name,
            int(check.passed),
            check.severity,
            json.dumps(check.actual, ensure_ascii=False, default=str),
            check.expected,
        )
        for check in checks
    ]
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO `ops`.`dq_check_result`
               (`run_id`,`check_time`,`check_name`,`passed`,`severity`,`actual_value`,`expected_rule`)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            rows,
        )
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="检查吉客云核心表是否可以刷新 ADS")
    parser.add_argument("--max-age-hours", type=int, default=36)
    args = parser.parse_args()
    if args.max_age_hours <= 0:
        raise ValueError("--max-age-hours 必须大于0")

    run_id = str(uuid.uuid4())
    conn = connect()
    try:
        ensure_result_table(conn)
        checks = run_checks(conn, args.max_age_hours)
        persist(conn, run_id, checks)
    finally:
        conn.close()

    for check in checks:
        status = "PASS" if check.passed else "FAIL"
        print(f"[{status}] {check.name}: actual={check.actual}; expected={check.expected}", flush=True)
    failed = [check for check in checks if not check.passed and check.severity == "CRITICAL"]
    print(f"[SUMMARY] run_id={run_id}; checks={len(checks)}; failed={len(failed)}", flush=True)
    if failed:
        raise SystemExit(3)


if __name__ == "__main__":
    main()
