"""
吉客云数据导出工具 - 公共配置
所有脚本共享的配置项，修改此处即可全局生效
"""

import os

# ============================================================
# 项目路径配置
# ============================================================
# 项目根目录（自动获取，无需手动设置）
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
# 数据输出目录
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# ============================================================
# 吉客云 API 配置
# ============================================================
APP_KEY = os.getenv("JKY_APP_KEY", "")
APP_SECRET = os.getenv("JKY_APP_SECRET", "")
API_URL = "https://open.jackyun.com/open/openapi/do"
VERSION = "v1.0"
CONTENT_TYPE = "json"

# ============================================================
# MySQL 数据库配置
# ============================================================
DB_CONFIG = {
    "host": os.getenv("JKY_DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("JKY_DB_PORT", "3306")),
    "user": os.getenv("JKY_DB_USER", ""),
    "password": os.getenv("JKY_DB_PASSWORD", ""),
    "database": os.getenv("JKY_DB_NAME", "ods"),
    "charset": "utf8mb4",
    "local_infile": True,
}

# ============================================================
# 通用参数
# ============================================================
# API 请求间隔（秒），官方限 1 次/秒
REQUEST_INTERVAL = 0.5
# API 请求超时（秒）
REQUEST_TIMEOUT = 60
# 最大重试次数
MAX_RETRIES = 5
# MySQL 批量写入临时文件名
MYSQL_TMP_FILE = "jike_import_tmp.csv"
