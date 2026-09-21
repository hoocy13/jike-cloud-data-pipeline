from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "登录态捕获助手.py"
LAUNCHER = ROOT / "启动登录态捕获助手.cmd"
AUTOSTART_INSTALLER = ROOT / "scripts" / "install_auth_capture_autostart.ps1"


class AuthCaptureAssistantCliTests(unittest.TestCase):
    def test_autostart_installer_dry_run_describes_hidden_task(self) -> None:
        completed = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(AUTOSTART_INSTALLER),
                "-DryRun",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
        )

        combined = completed.stdout + completed.stderr
        self.assertEqual(completed.returncode, 0, combined)
        self.assertIn("JikeAuthCaptureAssistant", combined)
        self.assertIn("start_auth_capture_hidden.ps1", combined)
        self.assertIn(str(ROOT), combined)
        self.assertIn("logs", combined)

    def test_bscm_follow_up_does_not_claim_uncaptured_files_were_completed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace = Path(temp_dir)
            curl_dir = workspace / "curl"
            curl_dir.mkdir()
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
                (curl_dir / name).write_text(
                    f"curl '{url}' -H 'user-agent: old-agent' -b 'sessionid=old-cookie'\n",
                    encoding="utf-8",
                )
            source = workspace / "captured.txt"
            source.write_text(
                "curl 'https://bscm.jinritemai.com/scm/visualized/"
                "exportFulfillOrderList?subject_aid=305219' "
                "-H 'user-agent: new-agent' -b 'sessionid=new-cookie'\n",
                encoding="utf-8",
            )

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
                env={**os.environ, "PYTHONUTF8": "1"},
            )

            combined = completed.stdout + completed.stderr
            self.assertEqual(completed.returncode, 0, combined)
            self.assertIn("本次仅更新", combined)
            self.assertIn("其余文件未在本次重新捕获", combined)
            self.assertNotIn("四件套已齐", combined)
            self.assertNotIn("[OK]", combined)
            self.assertLessEqual(len([line for line in combined.splitlines() if line.strip()]), 3)

    def test_windows_launcher_forwards_help_without_cmd_parse_errors(self) -> None:
        completed = subprocess.run(
            ["cmd.exe", "/d", "/c", "call", str(LAUNCHER), "--help"],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=3,
        )

        combined = completed.stdout + completed.stderr
        self.assertEqual(completed.returncode, 0, combined)
        self.assertNotIn("not recognized", combined)
        self.assertIn("--once", combined)

    def test_routes_general_export_curl_to_purchase_export_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace = Path(temp_dir)
            curl_dir = workspace / "curl"
            curl_dir.mkdir()
            source = workspace / "captured.txt"
            captured = (
                "curl 'https://bscm.jinritemai.com/api/gei/generalExport?"
                "bizType=TransferProcurementOrder&queryParams=%7B%7D&subject_aid=305219&"
                "msToken=x&a_bogus=x&verifyFp=x&fp=x' "
                "-H 'user-agent: test-agent' -b 'sessionid=test-cookie'\n"
            )
            source.write_text(captured, encoding="utf-8")

            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--source",
                    str(source),
                    "--curl-dir",
                    str(curl_dir),
                    "--no-follow-up",
                ],
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            target = curl_dir / "进口超市上海仓_货权转移采购单导出_curl.txt"
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            self.assertEqual(target.read_text(encoding="utf-8"), captured)
            self.assertEqual(len(list(curl_dir.glob("*_curl.txt"))), 1)

    def test_rejects_unverified_jackyun_export_without_common_verify(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace = Path(temp_dir)
            curl_dir = workspace / "curl"
            curl_dir.mkdir()
            source = workspace / "captured.txt"
            source.write_text(
                "curl 'https://web.jackyun.com/jkyun/excel-service/manager/startExcelExport' "
                "-H 'authorization: Bearer test-token' -H 'user-agent: test-agent' "
                "-b 'token=test-cookie' --data-raw 'pageIndex=0&pageSize=50'\n",
                encoding="utf-8",
            )

            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--source",
                    str(source),
                    "--curl-dir",
                    str(curl_dir),
                    "--no-follow-up",
                ],
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("commonVerify", completed.stderr)
            self.assertEqual(list(curl_dir.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
