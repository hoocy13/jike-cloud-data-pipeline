#!/usr/bin/env python3
"""监听“复制为 cURL”的剪贴板内容，并安全路由到对应认证文件。"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from sync_curl_auth import load_source_auth_text, parse_curl_text, read_clipboard


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CURL_DIR = ROOT / "curl"
SYNC_SCRIPT = Path(__file__).with_name("sync_curl_auth.py")
BSCM_PREFLIGHT = Path(__file__).with_name("BSCM认证预检.py")
CAPTURE_HOST = "127.0.0.1"
CAPTURE_PORT = 18765
ALLOWED_EXTENSION_ORIGIN = "chrome-extension://johajbablbcjmpdnhbkbegmbcmhdhnhe"


@dataclass(frozen=True)
class CaptureRoute:
    name: str
    host: str
    endpoint: str
    filename: str
    system: str


ROUTES = (
    CaptureRoute(
        "吉客云销售导出",
        "web.jackyun.com",
        "startExcelExport",
        "每日更新_curl.txt",
        "jackyun",
    ),
    CaptureRoute(
        "BSCM正向全链路",
        "bscm.jinritemai.com",
        "exportFulfillOrderList",
        "进口超市上海仓_正向全链路数据_curl.txt",
        "bscm",
    ),
    CaptureRoute(
        "BSCM货权采购查询",
        "bscm.jinritemai.com",
        "/api/procurement/po/list",
        "进口超市上海仓_货权转移采购单_curl.txt",
        "bscm",
    ),
    CaptureRoute(
        "BSCM货权采购导出",
        "bscm.jinritemai.com",
        "/api/gei/generalExport",
        "进口超市上海仓_货权转移采购单导出_curl.txt",
        "bscm",
    ),
)


def identify_capture(raw: str) -> tuple[CaptureRoute, dict]:
    if not raw.lstrip().lower().startswith("curl "):
        raise ValueError("剪贴板内容不是 cURL")
    info = parse_curl_text(raw)
    parsed = urlparse(info["url"])
    route = next(
        (
            item
            for item in ROUTES
            if parsed.hostname == item.host and item.endpoint in parsed.path
        ),
        None,
    )
    if route is None:
        raise ValueError("不是当前助手支持的吉客云/BSCM目标接口")
    if not info["cookie"]:
        raise ValueError(f"{route.name} cURL 缺少 Cookie")
    if route.system == "jackyun":
        auth = load_source_auth_text(raw)
        if not auth.get("commonVerify"):
            raise ValueError("吉客云 startExcelExport 缺少 commonVerify，请先完成微信公众号验证")
    return route, info


def save_capture(raw: str, curl_dir: Path) -> tuple[CaptureRoute, Path]:
    route, _ = identify_capture(raw)
    curl_dir.mkdir(parents=True, exist_ok=True)
    target = curl_dir / route.filename
    if target.exists():
        shutil.copy2(target, target.with_suffix(target.suffix + ".bak"))
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(raw, encoding="utf-8", newline="")
    temporary.replace(target)
    print(f"[CAPTURED] {route.name} -> {target.name}", flush=True)
    return route, target


def print_child_output(completed: subprocess.CompletedProcess[str]) -> None:
    if completed.stdout.strip():
        print(completed.stdout.rstrip(), flush=True)
    if completed.stderr.strip():
        print(completed.stderr.rstrip(), file=sys.stderr, flush=True)


def run_follow_up(route: CaptureRoute, target: Path, curl_dir: Path, verbose: bool) -> None:
    child_env = {**os.environ, "PYTHONUTF8": "1"}
    if route.system == "jackyun":
        command = [
            sys.executable,
            str(SYNC_SCRIPT),
            "--source",
            str(target),
            "--curl-dir",
            str(curl_dir),
        ]
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=child_env,
        )
        if verbose:
            print_child_output(completed)
        if completed.returncode != 0:
            raise RuntimeError("吉客云登录态同步失败；请使用 --verbose 查看明细")
        print("[DONE] 吉客云登录态已按同域规则分发", flush=True)
        return

    completed = subprocess.run(
        [sys.executable, str(BSCM_PREFLIGHT), "--curl-dir", str(curl_dir)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=child_env,
    )
    if verbose:
        print_child_output(completed)
    if completed.returncode == 0:
        print(
            f"[INFO] 本次仅更新：{route.name}；当前磁盘文件结构预检通过，"
            "其余文件未在本次重新捕获，不能据此判断其登录态已刷新",
            flush=True,
        )
    else:
        failed_count = sum(1 for line in completed.stdout.splitlines() if line.startswith("[FAIL]"))
        print(
            f"[WARN] 本次仅更新：{route.name}；BSCM 静态预检还有 {failed_count or '若干'} 项未通过；"
            "其余文件未在本次重新捕获，可使用 --verbose 查看明细",
            flush=True,
        )


def process(raw: str, curl_dir: Path, follow_up: bool, verbose: bool) -> None:
    route, target = save_capture(raw, curl_dir)
    if follow_up:
        run_follow_up(route, target, curl_dir, verbose)


class CaptureHttpHandler(BaseHTTPRequestHandler):
    curl_dir = DEFAULT_CURL_DIR
    follow_up = True
    verbose = False

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def send_json(self, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("content-length", str(len(body)))
        self.send_header("access-control-allow-origin", ALLOWED_EXTENSION_ORIGIN)
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:
        if self.headers.get("origin") != ALLOWED_EXTENSION_ORIGIN:
            self.send_json(403, {"ok": False, "error": "extension origin rejected"})
            return
        self.send_response(204)
        self.send_header("access-control-allow-origin", ALLOWED_EXTENSION_ORIGIN)
        self.send_header("access-control-allow-methods", "POST, OPTIONS")
        self.send_header("access-control-allow-headers", "content-type")
        self.end_headers()

    def do_POST(self) -> None:
        if self.path != "/capture":
            self.send_json(404, {"ok": False, "error": "not found"})
            return
        if self.headers.get("origin") != ALLOWED_EXTENSION_ORIGIN:
            self.send_json(403, {"ok": False, "error": "extension origin rejected"})
            return
        try:
            length = int(self.headers.get("content-length", "0"))
            if length <= 0 or length > 2_000_000:
                raise ValueError("invalid capture size")
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            raw = payload.get("curl", "")
            if not isinstance(raw, str):
                raise ValueError("curl must be text")
            process(raw, self.curl_dir, self.follow_up, self.verbose)
        except Exception as exc:
            self.send_json(400, {"ok": False, "error": str(exc)})
            return
        self.send_json(200, {"ok": True})


def start_capture_server(curl_dir: Path, follow_up: bool, verbose: bool) -> ThreadingHTTPServer:
    handler = type(
        "ConfiguredCaptureHttpHandler",
        (CaptureHttpHandler,),
        {"curl_dir": curl_dir, "follow_up": follow_up, "verbose": verbose},
    )
    server = ThreadingHTTPServer((CAPTURE_HOST, CAPTURE_PORT), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"[READY] Chrome 扩展接收端：http://{CAPTURE_HOST}:{CAPTURE_PORT}", flush=True)
    return server


def watch_clipboard(curl_dir: Path, interval: float, follow_up: bool, verbose: bool) -> None:
    print("[READY] 正在监听剪贴板；请在 Chrome F12 中对目标请求执行“复制为 cURL”", flush=True)
    print("[READY] 按 Ctrl+C 停止", flush=True)
    previous_digest = ""
    while True:
        try:
            raw = read_clipboard()
        except (RuntimeError, ValueError):
            time.sleep(interval)
            continue
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        if digest != previous_digest:
            previous_digest = digest
            try:
                process(raw, curl_dir, follow_up, verbose)
            except ValueError as exc:
                if raw.lstrip().lower().startswith("curl "):
                    print(f"[SKIP] {exc}", flush=True)
        time.sleep(interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="监听并自动归档吉客云/BSCM Copy-as-cURL")
    parser.add_argument("--source", type=Path, help="处理指定 cURL 文本文件后退出")
    parser.add_argument("--once", action="store_true", help="处理当前剪贴板一次后退出")
    parser.add_argument("--curl-dir", type=Path, default=DEFAULT_CURL_DIR)
    parser.add_argument("--interval", type=float, default=1.0, help="剪贴板轮询秒数")
    parser.add_argument("--no-follow-up", action="store_true", help="只保存，不运行同步或预检")
    parser.add_argument("--verbose", action="store_true", help="显示同步和预检的逐文件明细")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    follow_up = not args.no_follow_up
    if args.source:
        process(args.source.read_text(encoding="utf-8-sig"), args.curl_dir, follow_up, args.verbose)
        return
    if args.once:
        process(read_clipboard(), args.curl_dir, follow_up, args.verbose)
        return
    server = start_capture_server(args.curl_dir, follow_up, args.verbose)
    try:
        watch_clipboard(args.curl_dir, args.interval, follow_up, args.verbose)
    except KeyboardInterrupt:
        print("\n[STOPPED] 剪贴板监听已停止", flush=True)
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
