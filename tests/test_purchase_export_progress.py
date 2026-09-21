from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from urllib.parse import parse_qsl, urlparse


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "进口超市上海仓补全_web.py"


def load_module():
    spec = importlib.util.spec_from_file_location("purchase_export", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class PurchaseExportProgressTests(unittest.TestCase):
    def test_progress_request_is_derived_from_general_export(self) -> None:
        module = load_module()
        export_info = {
            "url": (
                "https://bscm.jinritemai.com/api/gei/generalExport?"
                "bizType=TransferProcurementOrder&queryParams=%7B%7D&subject_aid=305219&"
                "msToken=token&a_bogus=bogus&verifyFp=fingerprint&fp=fingerprint"
            ),
            "headers": {"user-agent": "test-agent", "menukey": "/cargo-right-transfer/list"},
            "cookie": "sessionid=test-cookie",
        }

        progress_info = module.derive_progress_info(export_info)
        parsed = urlparse(progress_info["url"])
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))

        self.assertEqual(parsed.path, "/api/gei/queryTaskProgress")
        self.assertEqual(query["subject_aid"], "305219")
        self.assertEqual(query["msToken"], "token")
        self.assertNotIn("bizType", query)
        self.assertNotIn("queryParams", query)
        self.assertEqual(progress_info["cookie"], "sessionid=test-cookie")


if __name__ == "__main__":
    unittest.main()
