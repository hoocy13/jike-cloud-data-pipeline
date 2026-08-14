#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DolphinScheduler 日常同步前置检查。

只检查本地运行条件和 cURL 关键认证材料，不发起导出，也不修改数据库。
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CURL_DIR = ROOT / "curl"

REQUIRED_CURLS = (
    "销售单查询_curl.txt",
    "销售单明细账_curl.txt",
    "入库查询_curl.txt",
    "渠道列表_curl.txt",
    "总库存查询_curl.txt",
    "分仓库查询_curl.txt",
    "批次货品库存查询_curl.txt",
    "进口超市上海仓_正向全链路数据_curl.txt",
    "进口超市上海仓_货权转移采购单_curl.txt",
    "进口超市上海仓_货权转移采购单导出_curl.txt",
    "进口超市上海仓_货权转移采购单进度_curl.txt",
)

SALES_EXPORT_CURLS = {
    "销售单查询_curl.txt",
    "销售单明细账_curl.txt",
}

COOKIE_ONLY_CURLS = {
    "进口超市上海仓_正向全链路数据_curl.txt",
    "进口超市上海仓_货权转移采购单_curl.txt",
    "进口超市上海仓_货权转移采购单导出_curl.txt",
    "进口超市上海仓_货权转移采购单进度_curl.txt",
}


def normalized(text: str) -> str:
    return text.replace("^\r\n", "").replace("^\n", "").replace("^", "")


def inspect_curl(path: Path) -> list[str]:
    problems: list[str] = []
    if not path.exists():
        return ["文件不存在"]
    text = normalized(path.read_text(encoding="utf-8", errors="ignore"))
    if len(text.strip()) < 100:
        problems.append("文件内容为空或过短")
    if path.name not in COOKIE_ONLY_CURLS and not re.search(
        r"authorization\s*:\s*Bearer\s+\S+|access_token=Bearer", text, re.I
    ):
        problems.append("缺少 authorization/access_token")
    if not re.search(r"(?:^|[;\s])token=|(?:^|[;\s])_ati=|\bati\s*:|(?:-b|--cookie)\s+", text, re.I):
        problems.append("缺少 cookie/ati 登录态")
    if path.name in SALES_EXPORT_CURLS and "commonVerify=" not in text:
        problems.append("缺少 commonVerify，销售导出可能触发手机验证")
    return problems


def main() -> None:
    parser = argparse.ArgumentParser(description="检查吉客云日常同步所需 cURL")
    parser.add_argument("--curl-dir", type=Path, default=CURL_DIR)
    args = parser.parse_args()

    failed = False
    for name in REQUIRED_CURLS:
        path = args.curl_dir / name
        problems = inspect_curl(path)
        if problems:
            failed = True
            print(f"[FAIL] {name}: {'；'.join(problems)}", flush=True)
        else:
            print(f"[OK] {name}", flush=True)
    if failed:
        raise SystemExit(2)
    print("[DONE] 吉客云同步前置检查通过", flush=True)


if __name__ == "__main__":
    main()
