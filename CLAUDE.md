# CLAUDE.md

## 项目概述

吉客云网页版导出同步脚本集合。脚本复用浏览器 DevTools 复制的 cURL，重新计算 web 签名，创建导出任务，轮询系统任务，下载 xlsx，最后写入 MySQL。

当前主要用于 DolphinScheduler 资源目录：

```text
/dolphinscheduler/default/resources/jike-cloud-data-pipeline
```

Windows 对应目录：

```text
D:\code\anzhuanghaitun\ds_resources\default\resources\jike-cloud-data-pipeline
```

该目录才是 Git 根目录。上层目录包含 DolphinScheduler 日志、Worker 数据、PostgreSQL 和 ZooKeeper 数据卷，不得纳入本仓库。

## 安全边界

- 不读取、输出或提交 `curl/` 中的 token、cookie、`commonVerify`。
- 不提交 `manual_inputs/*.xlsx`、`output/`、`data/logs/` 和 `.claude/settings.local.json`。
- `config.py` 只从 `JKY_*` 环境变量读取凭据；工作流账号只从 `DS_USER`、`DS_PASSWORD` 读取。
- 示例只能使用占位符。发现源码内嵌真实 cURL 或密码时，先移除并提示轮换，不得直接推送。

## 当前文件

```text
config.py                         # DB_CONFIG、DATA_DIR 等公共配置
common.py                         # 旧公共工具，保留兼容
requirements.txt                  # Python 依赖
scripts/
  销售单查询_web.py       # 销售单网页版导出 -> ods.销售单查询
  销售单明细账_web.py # 销售单明细账网页版导出 -> ods.销售单明细账
  分仓库查询_web.py   # 分仓库存网页版导出 -> ods.分仓库查询
  总库存查询_web.py   # 总库存查询接口同步 -> ods.总库存查询
  渠道列表_web.py       # 渠道列表接口同步 -> ods.渠道列表
  warehouse_list.py               # 仓库编码辅助查询
data/
  .gitkeep                        # 仅保留目录
README.md
agent.md
CLAUDE.md
```

`docs/`、旧 API 版 `sales_order.py`、旧 API 版 `warehouse_stock.py` 已不作为当前项目入口。

## 运行方式

安装依赖：

```bash
pip install -r requirements.txt
```

销售单：

```bash
python scripts/销售单查询_web.py
```

销售单明细账：

```bash
python scripts/销售单明细账_web.py
```

分仓库存：

```bash
python scripts/分仓库查询_web.py
```

总库存查询：

```bash
python scripts/总库存查询_web.py
```

渠道列表：

```bash
python scripts/渠道列表_web.py
```

本地 PyCharm 调试时，直接把最新 cURL 粘贴到脚本顶部 `START_EXPORT_CURL_TEXT`。

DolphinScheduler 上更推荐通过环境变量传入 cURL：

```text
JKY_SALES_ORDER_CURL
JKY_SALES_ORDER_DETAIL_CURL
JKY_WAREHOUSE_STOCK_CURL
```

## 数据写入

- 销售单表：`ods.销售单查询`
- 销售单明细账表：`ods.销售单明细账`
- 库存表：`ods.分仓库查询`
- 总库存表：`ods.总库存查询`
- 渠道列表表：`ods.渠道列表`
- 写入 MySQL 优先使用 `LOAD DATA LOCAL INFILE`
- 库存表使用全量快照替换：临时表写入完成后 `RENAME TABLE` 原子切换

## 库存字段精度

- 数量类字段：`DECIMAL(18,0)`
- 金额/价格类字段：`DECIMAL(18,2)`
- `含税价`、`不含税价`、`当前成本价`、`库存金额` 保留 2 位小数

## 清理规则

项目里只保留脚本、配置和文档。导出文件、缓存、IDE 配置和本地二进制不进项目：

```text
data/**/*.csv
data/**/*.xlsx
data/**/*.log
curl/
manual_inputs/*.xlsx
output/
__pycache__/
.idea/
.claude/settings.local.json
*.jar
```
