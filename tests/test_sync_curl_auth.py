from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
import importlib.util
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "sync_curl_auth.py"
SPEC = importlib.util.spec_from_file_location("sync_curl_auth_for_test", SCRIPT)
sync_curl_auth = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(sync_curl_auth)


def curl_text(url: str, authorization: str, cookie: str) -> str:
    return (
        f"curl '{url}' "
        f"-H 'authorization: {authorization}' "
        "-H 'user-agent: test-agent' "
        f"-b '{cookie}' "
        "--data-raw 'pageIndex=0&pageSize=1'\n"
    )


class SyncCurlAuthCliTests(unittest.TestCase):
    def test_read_clipboard_forces_utf8_and_decodes_binary_output(self) -> None:
        completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="curl 测试".encode("utf-8"), stderr=b""
        )
        with patch.object(sync_curl_auth.subprocess, "run", return_value=completed) as run:
            value = sync_curl_auth.read_clipboard()

        self.assertEqual(value, "curl 测试")
        command = run.call_args.args[0]
        self.assertIn("OutputEncoding", command[-1])
        self.assertFalse(run.call_args.kwargs.get("text", False))

    def test_jackyun_source_does_not_modify_bscm_curl(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            curl_dir = Path(temp_dir)
            source = curl_dir / "每日更新_curl.txt"
            jackyun_target = curl_dir / "销售单查询_curl.txt"
            bscm_target = curl_dir / "进口超市上海仓_正向全链路数据_curl.txt"

            source.write_text(
                curl_text(
                    "https://web.jackyun.com/jkyun/excel-service/manager/startExcelExport",
                    "Bearer fresh-jackyun",
                    "token=fresh-jackyun-cookie",
                ),
                encoding="utf-8",
            )
            jackyun_target.write_text(
                curl_text(
                    "https://web.jackyun.com/jkyun/sales/query",
                    "Bearer old-jackyun",
                    "token=old-jackyun-cookie",
                ),
                encoding="utf-8",
            )
            original_bscm = curl_text(
                "https://bscm.jinritemai.com/api/exportFulfillOrderList?subject_aid=305219",
                "Bearer bscm-placeholder",
                "sessionid=bscm-cookie",
            )
            bscm_target.write_text(original_bscm, encoding="utf-8")

            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--source",
                    str(source),
                    "--curl-dir",
                    str(curl_dir),
                ],
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("fresh-jackyun", jackyun_target.read_text(encoding="utf-8"))
            self.assertEqual(bscm_target.read_text(encoding="utf-8"), original_bscm)


if __name__ == "__main__":
    unittest.main()
