"""
Copy fresh Jike web login credentials from one cURL file to the stored cURL files.

This keeps each target request's URL, filters, fields, commonVerify, and other
business parameters unchanged. Only the browser-login parts are refreshed:
authorization, access_token, cookie, ati, bx-v, and user-agent.
"""

from __future__ import annotations

import argparse
import re
import shlex
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CURL_DIR = ROOT / "curl"
DEFAULT_TARGET_GLOB = "*_curl.txt"
DEFAULT_SOURCE_FILE = DEFAULT_CURL_DIR / "每日更新_curl.txt"

SYNC_HEADERS = ("authorization", "ati", "bx-v", "user-agent")


def normalize_curl_text(text: str) -> str:
    return text.replace("^\r\n", " ").replace("^\n", " ").replace("^", "")


def parse_curl_text(raw: str) -> dict[str, Any]:
    try:
        tokens = shlex.split(normalize_curl_text(raw), posix=True)
    except ValueError as exc:
        raise ValueError(f"Could not parse cURL text: {exc}") from exc

    headers: dict[str, str] = {}
    cookie = ""
    data_raw = ""
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token in ("-H", "--header"):
            i += 1
            if i < len(tokens) and ":" in tokens[i]:
                name, value = tokens[i].split(":", 1)
                headers[name.strip().lower()] = value.strip()
        elif token in ("-b", "--cookie"):
            i += 1
            if i < len(tokens):
                cookie = tokens[i]
        elif token in ("--data-raw", "--data", "--data-binary", "-d"):
            i += 1
            if i < len(tokens):
                data_raw = tokens[i]
        elif token.startswith("--data-raw="):
            data_raw = token.split("=", 1)[1]
        i += 1

    params = dict(parse_qsl(data_raw, keep_blank_values=True)) if data_raw else {}
    return {"headers": headers, "cookie": cookie, "params": params}


def load_source_auth(source_path: Path) -> dict[str, str]:
    return load_source_auth_text(source_path.read_text(encoding="utf-8-sig"))


def load_source_auth_text(raw: str) -> dict[str, str]:
    info = parse_curl_text(raw)
    headers = info["headers"]
    params = info["params"]

    authorization = headers.get("authorization") or params.get("access_token", "")
    if not authorization:
        raise ValueError("Source cURL has no authorization header or access_token parameter.")
    if not authorization.lower().startswith("bearer "):
        authorization = f"Bearer {authorization}"

    auth = {
        "authorization": authorization,
        "cookie": info.get("cookie", ""),
    }
    for name in SYNC_HEADERS:
        if headers.get(name):
            auth[name] = headers[name]
    if params.get("commonVerify"):
        auth["commonVerify"] = params["commonVerify"]
    if not auth["cookie"]:
        raise ValueError("Source cURL has no cookie. Copy a browser request that includes cookies.")
    return auth


def read_clipboard() -> str:
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", "Get-Clipboard -Raw"],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError("Could not read clipboard. Save the cURL to a file and use --source instead.") from exc
    text = result.stdout.strip()
    if not text:
        raise ValueError("Clipboard is empty. Copy a fresh cURL first.")
    return text


def replace_header(raw: str, header_name: str, value: str) -> tuple[str, bool]:
    pattern = re.compile(
        rf'(?im)(-H\s+\^"{re.escape(header_name)}:\s*)[^\r\n"]*(\^")'
    )
    updated, count = pattern.subn(rf"\g<1>{value}\g<2>", raw)
    if count:
        return updated, True

    # Also support plain bash-style cURL snippets if they are ever used.
    plain = re.compile(
        rf"""(?im)(-H\s+['"]{re.escape(header_name)}:\s*)[^'"\r\n]*(['"])"""
    )
    updated, count = plain.subn(rf"\g<1>{value}\g<2>", raw)
    return updated, bool(count)


def replace_access_token(raw: str, authorization: str) -> tuple[str, bool]:
    pattern = re.compile(r"(?i)(access_token=)[^&\"\r\n^]*")
    updated, count = pattern.subn(rf"\g<1>{authorization}", raw)
    return updated, bool(count)


def replace_data_param(raw: str, name: str, value: str) -> tuple[str, bool]:
    pattern = re.compile(rf"(?i)({re.escape(name)}=)[^&\"\r\n^]*")
    updated, count = pattern.subn(rf"\g<1>{value}", raw)
    if count:
        return updated, True

    data_match = re.search(r'(--data-raw\s+\^"[\s\S]*?)(\^")', raw)
    if data_match:
        insert = f"^&{name}={value}"
        return raw[:data_match.end(1)] + insert + raw[data_match.end(1):], True

    plain_match = re.search(r"""(--data-raw\s+['"][\s\S]*?)(['"])""", raw)
    if plain_match:
        insert = f"&{name}={value}"
        return raw[:plain_match.end(1)] + insert + raw[plain_match.end(1):], True

    return raw, False


def remove_data_param(raw: str, name: str) -> tuple[str, bool]:
    pattern = re.compile(rf"(?i)([\^]?&)?{re.escape(name)}=[^&\"\r\n^]*")
    updated, count = pattern.subn("", raw)
    return updated, bool(count)


def replace_cookie(raw: str, cookie: str) -> tuple[str, bool]:
    pattern = re.compile(r'(?im)(-b\s+\^")[^\r\n"]*(\^")')
    updated, count = pattern.subn(rf"\g<1>{cookie}\g<2>", raw)
    if count:
        return updated, True

    plain = re.compile(r"""(?im)(-b\s+['"])[^'"\r\n]*(['"])""")
    updated, count = plain.subn(rf"\g<1>{cookie}\g<2>", raw)
    return updated, bool(count)


def sync_one(target_path: Path, auth: dict[str, str], backup: bool) -> list[str]:
    raw = target_path.read_text(encoding="utf-8-sig")
    updated = raw
    changed: list[str] = []

    for header_name in SYNC_HEADERS:
        value = auth.get(header_name)
        if not value:
            continue
        updated, did_change = replace_header(updated, header_name, value)
        if did_change:
            changed.append(header_name)

    updated, did_change = replace_access_token(updated, auth["authorization"])
    if did_change:
        changed.append("access_token")

    updated, did_change = replace_cookie(updated, auth["cookie"])
    if did_change:
        changed.append("cookie")

    # Only sales export cURLs carry commonVerify. Refresh it where the parameter
    # already exists, instead of using localized file names as logic.
    if re.search(r"(?i)commonVerify=", raw):
        if auth.get("commonVerify"):
            updated, did_change = replace_data_param(updated, "commonVerify", auth["commonVerify"])
            if did_change:
                changed.append("commonVerify")
        else:
            updated, did_change = remove_data_param(updated, "commonVerify")
            if did_change:
                changed.append("removed commonVerify")

    if updated != raw:
        if backup:
            target_path.with_suffix(target_path.suffix + ".bak").write_text(raw, encoding="utf-8")
        target_path.write_text(updated, encoding="utf-8", newline="")
    return changed


def find_targets(args: argparse.Namespace) -> list[Path]:
    if args.targets:
        return [Path(item).resolve() for item in args.targets]
    curl_dir = Path(args.curl_dir).resolve()
    return sorted(curl_dir.glob(args.glob))


def resolve_source_path(source: str) -> Path:
    source_path = Path(source)
    if source_path.exists():
        return source_path.resolve()

    fallback = DEFAULT_CURL_DIR / source_path.name
    if fallback.exists():
        return fallback.resolve()

    raise FileNotFoundError(
        f"Source cURL file not found: {source}. Use --clipboard, or save the cURL to a file first."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh stored Jike cURL login credentials from one fresh Copy-as-cURL file."
    )
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument("--source", help="Fresh cURL file copied from Jike web.")
    source_group.add_argument("--clipboard", action="store_true", help="Read the fresh cURL from clipboard.")
    parser.add_argument("--curl-dir", default=str(DEFAULT_CURL_DIR), help="Directory containing stored cURL files.")
    parser.add_argument("--glob", default=DEFAULT_TARGET_GLOB, help="Target file glob when --targets is omitted.")
    parser.add_argument("--targets", nargs="*", help="Optional explicit target cURL files.")
    parser.add_argument("--backup", action="store_true", help="Write .bak files before updating.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_path = resolve_source_path(args.source) if args.source else DEFAULT_SOURCE_FILE.resolve()
    if args.clipboard:
        source_path = None

    if source_path and not source_path.exists():
        raise FileNotFoundError(
            f"Default source cURL file not found: {source_path}. Paste a fresh cURL into it first, "
            "or use --clipboard."
        )

    auth = load_source_auth(source_path) if source_path else load_source_auth_text(read_clipboard())
    if not auth.get("commonVerify"):
        print(
            "[INFO] Source cURL has no commonVerify. Sales order scripts will use non-plaintext export mode.",
            flush=True,
        )
    targets = find_targets(args)
    if not targets:
        raise SystemExit("No target cURL files found.")

    for target_path in targets:
        if source_path and target_path == source_path:
            continue
        if target_path.name == DEFAULT_SOURCE_FILE.name:
            continue
        changed = sync_one(target_path, auth, args.backup)
        if changed:
            print(f"[OK] {target_path.name}: {', '.join(changed)}")
        else:
            print(f"[SKIP] {target_path.name}: no matching auth fields")


if __name__ == "__main__":
    main()
