#!/usr/bin/env python3
"""静态检查 BSCM 采集所需的 cURL 模板，不发起网络请求。"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlparse

from sync_curl_auth import parse_curl_text


ROOT = Path(__file__).resolve().parents[1]
CURL_DIR = ROOT / "curl"


@dataclass(frozen=True)
class CurlSpec:
    filename: str
    endpoint: str
    required_query: tuple[str, ...]


SECURITY_QUERY = ("subject_aid", "msToken", "a_bogus", "verifyFp", "fp")
SPECS = (
    CurlSpec(
        "进口超市上海仓_正向全链路数据_curl.txt",
        "exportFulfillOrderList",
        ("subject_aid",),
    ),
    CurlSpec(
        "进口超市上海仓_货权转移采购单_curl.txt",
        "/api/procurement/po/list",
        ("orderType", "createTimeStart", "createTimeEnd", "page", "pageSize") + SECURITY_QUERY,
    ),
    CurlSpec(
        "进口超市上海仓_货权转移采购单导出_curl.txt",
        "/api/gei/generalExport",
        ("bizType", "queryParams") + SECURITY_QUERY,
    ),
)


def inspect_curl(path: Path, spec: CurlSpec) -> list[str]:
    if not path.exists():
        return ["文件不存在"]

    info = parse_curl_text(path.read_text(encoding="utf-8-sig"))
    parsed = urlparse(info["url"])
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    problems: list[str] = []

    if parsed.hostname != "bscm.jinritemai.com":
        problems.append("不是 BSCM 域名")
    if spec.endpoint not in parsed.path:
        problems.append(f"接口应为 {spec.endpoint}")
    if not info["cookie"]:
        problems.append("缺少 Cookie")

    missing = [name for name in spec.required_query if not query.get(name)]
    if missing:
        problems.append("缺少参数: " + ", ".join(missing))
    return problems


def main() -> None:
    parser = argparse.ArgumentParser(description="静态检查 BSCM cURL 三件套")
    parser.add_argument("--curl-dir", type=Path, default=CURL_DIR)
    args = parser.parse_args()

    failed = False
    for spec in SPECS:
        problems = inspect_curl(args.curl_dir / spec.filename, spec)
        if problems:
            failed = True
            print(f"[FAIL] {spec.filename}: {'；'.join(problems)}", flush=True)
        else:
            print(f"[OK] {spec.filename}: {spec.endpoint}", flush=True)

    if failed:
        raise SystemExit(2)
    print("[DONE] BSCM cURL 静态预检通过（未发起导出请求）", flush=True)


if __name__ == "__main__":
    main()
