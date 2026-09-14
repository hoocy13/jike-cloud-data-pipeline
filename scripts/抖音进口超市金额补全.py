#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按货权转移采购金额补全“进口超市上海仓”销售金额。

公司是供应商，销售收入按平台货权转移采购单的含税采购金额计算，
不使用消费者支付 GMV。不新增 ODS 字段，只补全原有为 0/空的金额字段。
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pymysql

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from config import DB_CONFIG

SOURCE_CHANNEL = "进口超市上海仓"
PO_TABLE = "进口超市上海仓_货权转移采购单"
PO_DETAIL_TABLE = "进口超市上海仓_货权转移采购单明细"
CHAIN_TABLE = "进口超市上海仓_正向全链路数据"
ZERO = Decimal("0.00")


@dataclass(frozen=True)
class SalesLine:
    order_no: str
    goods_no: str
    name: str
    quantity: Decimal


@dataclass(frozen=True)
class PoLine:
    business_no: str
    product_id: str
    cargo_id: str
    name: str
    quantity: Decimal
    unit_price: Decimal

    @property
    def amount(self) -> Decimal:
        return (self.quantity * self.unit_price).quantize(Decimal("0.01"))


def db_config(database: str) -> dict[str, Any]:
    result = dict(DB_CONFIG)
    result["database"] = database
    result.setdefault("connect_timeout", 20)
    result.setdefault("read_timeout", 900)
    result.setdefault("write_timeout", 900)
    return result


def connect_mysql(database: str, retries: int = 5):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return pymysql.connect(**db_config(database), autocommit=False)
        except pymysql.MySQLError as exc:
            last_error = exc
            if attempt >= retries:
                raise
            print(f"[WARN] MySQL 连接失败，{attempt}/5，10秒后重试", flush=True)
            time.sleep(10)
    raise last_error  # pragma: no cover


def money(value: Any) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"))


def normalize_name(value: Any) -> str:
    value = str(value or "").lower()
    value = re.sub(r"(?:商仓|旅行装|默认规格|瓶|支|有盒|无盒|样品|小样|极光洁面|型)", "", value)
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", value)


def unique_mapping(pairs: list[tuple[str, str]]) -> dict[str, str]:
    values: dict[str, set[str]] = defaultdict(set)
    for source, target in pairs:
        if source and target:
            values[source].add(target)
    return {key: next(iter(items)) for key, items in values.items() if len(items) == 1}


def fetch_candidates(
    cursor: Any,
    lookback_days: int,
    order_no: str | None = None,
    order_date: date | None = None,
):
    if order_no:
        order_filter = " AND `订单编号`=%s"
        params = (SOURCE_CHANNEL, lookback_days, order_no)
    elif order_date:
        order_filter = " AND `下单时间` >= %s AND `下单时间` < DATE_ADD(%s, INTERVAL 1 DAY)"
        params = (SOURCE_CHANNEL, lookback_days, order_date, order_date)
    else:
        order_filter = ""
        params = (SOURCE_CHANNEL, lookback_days)
    cursor.execute(
        f"""SELECT `订单编号`,`货品编号`,`货品名称`,`数量`
           FROM `ods`.`销售单明细账`
           WHERE `销售渠道`=%s AND COALESCE(`分摊后金额`,0)=0 AND `数量`>0
             AND `下单时间` >= DATE_SUB(CURDATE(), INTERVAL %s DAY){order_filter}""",
        params,
    )
    sales: dict[str, list[SalesLine]] = defaultdict(list)
    for order_no, goods_no, name, quantity in cursor.fetchall():
        sales[str(order_no)].append(SalesLine(str(order_no), str(goods_no or ""), str(name or ""), money(quantity)))
    if not sales:
        return {}, {}, {}

    sales_by_logistics: dict[str, list[str]] = defaultdict(list)
    order_numbers = sorted(sales)
    for offset in range(0, len(order_numbers), 500):
        batch = order_numbers[offset:offset + 500]
        placeholders = ",".join(["%s"] * len(batch))
        cursor.execute(
            f"""SELECT `订单编号`,NULLIF(TRIM(`物流单号`),'') FROM `ods`.`销售单查询`
                WHERE `订单编号` IN ({placeholders}) AND `销售渠道`=%s
                  AND `订单类型`='零售业务' AND NULLIF(TRIM(`物流单号`),'') IS NOT NULL""",
            (*batch, SOURCE_CHANNEL),
        )
        for order_no, logistics_no in cursor.fetchall():
            sales_by_logistics[str(logistics_no)].append(str(order_no))
    if not sales_by_logistics:
        return {}, {}, {}

    logistics_numbers = sorted(sales_by_logistics)
    placeholders = ",".join(["%s"] * len(logistics_numbers))
    cursor.execute(
        f"""SELECT NULLIF(TRIM(`运单号`),''),NULLIF(TRIM(`店铺单号`),'')
            FROM `ods`.`{CHAIN_TABLE}`
            WHERE NULLIF(TRIM(`运单号`),'') IN ({placeholders})
              AND NULLIF(TRIM(`店铺单号`),'') IS NOT NULL
              AND `支付时间` >= DATE_SUB(CURDATE(), INTERVAL %s DAY)""",
        (*logistics_numbers, lookback_days),
    )
    businesses_by_logistics: dict[str, set[str]] = defaultdict(set)
    for logistics_no, business_no in cursor.fetchall():
        if str(logistics_no) in sales_by_logistics:
            businesses_by_logistics[str(logistics_no)].add(str(business_no))

    po_by_business: dict[str, list[tuple[str, Decimal]]] = defaultdict(list)
    wanted_businesses = sorted({item for values in businesses_by_logistics.values() for item in values})
    for offset in range(0, len(wanted_businesses), 500):
        batch = wanted_businesses[offset:offset + 500]
        placeholders = ",".join(["%s"] * len(batch))
        cursor.execute(
            f"SELECT `采购单号`,`业务单号`,`采购总金额` FROM `ods`.`{PO_TABLE}` WHERE `业务单号` IN ({placeholders})",
            batch,
        )
        for po_no, business_no, po_total in cursor.fetchall():
            po_by_business[str(business_no)].append((str(po_no), money(po_total)))

    orders = {}
    for logistics_no, order_nos in sales_by_logistics.items():
        businesses = businesses_by_logistics.get(logistics_no, set())
        if len(order_nos) != 1 or len(businesses) != 1:
            continue
        business_no = next(iter(businesses))
        purchase_orders = po_by_business.get(business_no, [])
        if len(purchase_orders) != 1:
            continue
        po_no, po_total = purchase_orders[0]
        orders[order_nos[0]] = {"business_no": business_no, "po_no": po_no, "po_total": po_total}
    if not orders:
        return {}, {}, {}

    sales = {order_no: lines for order_no, lines in sales.items() if order_no in orders}

    wanted = sorted({meta["business_no"] for meta in orders.values()})
    po: dict[str, list[PoLine]] = defaultdict(list)
    for offset in range(0, len(wanted), 500):
        batch = wanted[offset:offset + 500]
        placeholders = ",".join(["%s"] * len(batch))
        cursor.execute(
            f"""SELECT `业务单号`,`productId`,`cargoId`,`货品名称`,`数量`,`含税单价`
                FROM `ods`.`{PO_DETAIL_TABLE}`
                WHERE `业务单号` IN ({placeholders}) AND `含税单价` IS NOT NULL AND `数量`>0""",
            batch,
        )
        for business_no, product_id, cargo_id, name, quantity, unit_price in cursor.fetchall():
            business_no = str(business_no or "")
            po[business_no].append(PoLine(business_no, str(product_id or ""), str(cargo_id or ""), str(name or ""), money(quantity), money(unit_price)))
    return orders, sales, po


def build_matches(orders, sales, po):
    matches: dict[tuple[str, str], tuple[SalesLine, PoLine]] = {}
    product_pairs: list[tuple[str, str]] = []
    cargo_pairs: list[tuple[str, str]] = []
    stats: dict[str, int] = defaultdict(int)

    for order_no, meta in orders.items():
        slines, plines = sales.get(order_no, []), po.get(meta["business_no"], [])
        if len(slines) == len(plines) == 1 and slines[0].quantity == plines[0].quantity:
            matches[(order_no, slines[0].goods_no)] = (slines[0], plines[0])
            product_pairs.append((plines[0].product_id, slines[0].goods_no))
            cargo_pairs.append((plines[0].cargo_id, slines[0].goods_no))
            stats["single_sku_seed"] += 1

    for order_no, meta in orders.items():
        slines, plines = sales.get(order_no, []), po.get(meta["business_no"], [])
        sby, pby = defaultdict(list), defaultdict(list)
        for line in slines:
            sby[normalize_name(line.name)].append(line)
        for line in plines:
            pby[normalize_name(line.name)].append(line)
        for key in set(sby) & set(pby):
            if key and len(sby[key]) == len(pby[key]) == 1 and sby[key][0].quantity == pby[key][0].quantity:
                sline, pline = sby[key][0], pby[key][0]
                matches[(order_no, sline.goods_no)] = (sline, pline)
                product_pairs.append((pline.product_id, sline.goods_no))
                cargo_pairs.append((pline.cargo_id, sline.goods_no))
                stats["exact_name_seed"] += 1

    product_map, cargo_map = unique_mapping(product_pairs), unique_mapping(cargo_pairs)
    for order_no, meta in orders.items():
        slines, plines = sales.get(order_no, []), po.get(meta["business_no"], [])
        by_goods = {line.goods_no: line for line in slines}
        if len(by_goods) != len(slines):
            stats["duplicate_sales_sku"] += 1
            continue
        for pline in plines:
            candidates = {x for x in (product_map.get(pline.product_id), cargo_map.get(pline.cargo_id)) if x}
            if len(candidates) == 1 and (sline := by_goods.get(next(iter(candidates)))) and sline.quantity == pline.quantity:
                matches[(order_no, sline.goods_no)] = (sline, pline)
                stats["stable_id_match"] += 1

    accepted = []
    for order_no, meta in orders.items():
        slines, plines = sales.get(order_no, []), po.get(meta["business_no"], [])
        pairs = [matches[(order_no, line.goods_no)] for line in slines if (order_no, line.goods_no) in matches]
        if not slines or len(pairs) != len(slines) or len(pairs) != len(plines) or len({p.product_id for _, p in pairs}) != len(plines):
            stats["incomplete_order"] += 1
            continue
        if sum((pline.amount for _, pline in pairs), ZERO) != meta["po_total"]:
            stats["amount_mismatch"] += 1
            continue
        accepted.extend(pairs)
        stats["accepted_orders"] += 1

    stats.update(candidate_orders=len(orders), accepted_lines=len(accepted), product_map_size=len(product_map), cargo_map_size=len(cargo_map))
    return accepted, dict(stats)


def apply_matches(cursor, orders, matches):
    cursor.execute("DROP TEMPORARY TABLE IF EXISTS `tmp_import_supermarket_sales_amount`")
    cursor.execute("""CREATE TEMPORARY TABLE `tmp_import_supermarket_sales_amount` (
        `订单编号` VARCHAR(100) NOT NULL, `货品编号` VARCHAR(255) NOT NULL,
        `单价` DECIMAL(20,2) NOT NULL, `金额` DECIMAL(20,2) NOT NULL,
        PRIMARY KEY (`订单编号`,`货品编号`)) ENGINE=InnoDB""")
    cursor.executemany(
        "INSERT INTO `tmp_import_supermarket_sales_amount` VALUES (%s,%s,%s,%s)",
        [(s.order_no, s.goods_no, p.unit_price, p.amount) for s, p in matches],
    )
    cursor.execute("""UPDATE `ods`.`销售单明细账` d
        JOIN `tmp_import_supermarket_sales_amount` m ON m.`订单编号`=d.`订单编号` AND m.`货品编号`=d.`货品编号`
        SET d.`单价`=IF(COALESCE(d.`单价`,0)=0,m.`单价`,d.`单价`),
            d.`金额`=IF(COALESCE(d.`金额`,0)=0,m.`金额`,d.`金额`),
            d.`分摊后单价`=IF(COALESCE(d.`分摊后单价`,0)=0,m.`单价`,d.`分摊后单价`),
            d.`分摊后金额`=IF(COALESCE(d.`分摊后金额`,0)=0,m.`金额`,d.`分摊后金额`)
        WHERE d.`销售渠道`=%s AND d.`数量`>0""", (SOURCE_CHANNEL,))
    detail_rows = int(cursor.rowcount)
    header_rows = 0
    for order_no in {s.order_no for s, _ in matches}:
        total = orders[order_no]["po_total"]
        cursor.execute("""UPDATE `ods`.`销售单查询`
            SET `应收合计`=IF(COALESCE(`应收合计`,0)=0,%s,`应收合计`),
                `实付金额`=IF(COALESCE(`实付金额`,0)=0,%s,`实付金额`)
            WHERE `订单编号`=%s AND `销售渠道`=%s""", (total, total, order_no, SOURCE_CHANNEL))
        header_rows += int(cursor.rowcount)
    return {"detail_rows": detail_rows, "header_rows": header_rows}


def run(apply: bool, lookback_days: int, order_no: str | None = None):
    connection = connect_mysql("ods")
    try:
        dates = [None] if order_no else [date.today() - timedelta(days=offset) for offset in range(lookback_days)]
        total_quality: dict[str, int] = defaultdict(int)
        total_changed = {"detail_rows": 0, "header_rows": 0}
        sample: list[dict[str, str]] = []
        processed_days = 0
        for current_date in dates:
            for attempt in range(1, 4):
                try:
                    connection.ping(reconnect=True)
                    with connection.cursor() as cursor:
                        orders, sales, po = fetch_candidates(cursor, lookback_days, order_no, current_date)
                        matches, quality = build_matches(orders, sales, po)
                        changed = apply_matches(cursor, orders, matches) if apply and matches else {"detail_rows": 0, "header_rows": 0}
                    if apply:
                        connection.commit()
                    else:
                        connection.rollback()
                    break
                except pymysql.MySQLError:
                    try:
                        connection.rollback()
                    except pymysql.MySQLError:
                        pass
                    connection.close()
                    if attempt >= 3:
                        raise
                    print(f"[WARN] {current_date or order_no}: 查询连接中断，{attempt}/3，10秒后重试", flush=True)
                    time.sleep(10)
                    connection = connect_mysql("ods")
            processed_days += 1
            for key, value in quality.items():
                total_quality[key] += value
            for key, value in changed.items():
                total_changed[key] += value
            for sales_line, po_line in matches:
                if len(sample) >= 10:
                    break
                sample.append({
                    "order_no": sales_line.order_no,
                    "goods_no": sales_line.goods_no,
                    "unit_price": str(po_line.unit_price),
                    "amount": str(po_line.amount),
                })
            if current_date:
                print(f"[PROGRESS] {current_date}: accepted_orders={quality.get('accepted_orders', 0)}, changed_lines={changed['detail_rows']}", flush=True)
        return {"applied": apply, "processed_days": processed_days, "quality": dict(total_quality), "changed": total_changed, "sample": sample}
    except Exception:
        try:
            connection.rollback()
        except pymysql.MySQLError:
            pass
        raise
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description="按货权转移采购含税金额补全进口超市上海仓销售金额")
    parser.add_argument("--apply", action="store_true", help="写入 ODS；默认只预演")
    parser.add_argument("--lookback-days", type=int, default=45, help="回看天数，默认 45")
    parser.add_argument("--order-no", help="只预演/补全一个吉客云订单，用于核验")
    args = parser.parse_args()
    if args.lookback_days < 1:
        parser.error("--lookback-days 必须大于 0")
    print("[DONE] " + json.dumps(run(args.apply, args.lookback_days, args.order_no), ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    main()
