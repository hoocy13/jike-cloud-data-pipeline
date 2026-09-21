from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "BSCM认证预检.py"


def curl_text(url: str) -> str:
    return f"curl '{url}' -H 'user-agent: test-agent' -b 'sessionid=test-cookie'\n"


class BscmAuthPreflightCliTests(unittest.TestCase):
    def test_complete_bscm_capture_set_passes_static_preflight(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            curl_dir = Path(temp_dir)
            fixtures = {
                "进口超市上海仓_正向全链路数据_curl.txt": (
                    "https://bscm.jinritemai.com/scm/visualized/"
                    "exportFulfillOrderList?subject_aid=305219"
                ),
                "进口超市上海仓_货权转移采购单_curl.txt": (
                    "https://bscm.jinritemai.com/api/procurement/po/list?"
                    "orderType=5&createTimeStart=1&createTimeEnd=2&page=1&pageSize=20&"
                    "subject_aid=305219&msToken=x&a_bogus=x&verifyFp=x&fp=x"
                ),
                "进口超市上海仓_货权转移采购单导出_curl.txt": (
                    "https://bscm.jinritemai.com/api/gei/generalExport?"
                    "bizType=TransferProcurementOrder&queryParams=%7B%7D&subject_aid=305219&"
                    "msToken=x&a_bogus=x&verifyFp=x&fp=x"
                ),
            }
            for name, url in fixtures.items():
                (curl_dir / name).write_text(curl_text(url), encoding="utf-8")

            completed = subprocess.run(
                [sys.executable, str(SCRIPT), "--curl-dir", str(curl_dir)],
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_wrong_endpoint_and_missing_cookie_fail_preflight(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            curl_dir = Path(temp_dir)
            fixtures = {
                "进口超市上海仓_正向全链路数据_curl.txt": (
                    "https://bscm.jinritemai.com/scm/visualized/"
                    "exportFulfillOrderList?subject_aid=305219"
                ),
                "进口超市上海仓_货权转移采购单_curl.txt": (
                    "https://bscm.jinritemai.com/api/procurement/po/list?"
                    "orderType=5&createTimeStart=1&createTimeEnd=2&page=1&pageSize=20&"
                    "subject_aid=305219&msToken=x&a_bogus=x&verifyFp=x&fp=x"
                ),
                "进口超市上海仓_货权转移采购单导出_curl.txt": (
                    "https://bscm.jinritemai.com/api/procurement/po/list?"
                    "subject_aid=305219&msToken=x&a_bogus=x&verifyFp=x&fp=x"
                ),
            }
            for name, url in fixtures.items():
                text = curl_text(url)
                if name == "进口超市上海仓_货权转移采购单_curl.txt":
                    text = f"curl '{url}' -H 'user-agent: test-agent'\n"
                (curl_dir / name).write_text(text, encoding="utf-8")

            completed = subprocess.run(
                [sys.executable, str(SCRIPT), "--curl-dir", str(curl_dir)],
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            self.assertEqual(completed.returncode, 2)
            self.assertIn("generalExport", completed.stdout)
            self.assertIn("Cookie", completed.stdout)


if __name__ == "__main__":
    unittest.main()
