# agent.md - 项目上下文速查

## 项目定位

吉客云网页版数据同步脚本。通过 DevTools 复制的 cURL 复用页面筛选条件，脚本重新签名后调用吉客云网页接口或导出任务，并写入 MySQL `ods` 库。

Git 仓库根目录是本 `jike-trade-export` 目录；上层 `anzhuanghaitun` 是 DolphinScheduler 运行目录，包含日志和数据库卷，不应整体初始化或推送为 Git 仓库。

本地 Windows 路径：

```text
D:\code\anzhuanghaitun\ds_resources\default\resources\jike-trade-export
```

DolphinScheduler Worker 容器路径：

```text
/dolphinscheduler/default/resources/jike-trade-export
```

## DolphinScheduler 部署现状

当前日常入口已经统一为一条主工作流：

```text
吉客云_每日数据总同步（工作流在线，09:20计划当前停用，更新cURL后手工执行）
  登录态与文件预检
    ├─ 销售单查询（滚动30天）→ 正向全链路（7天）
    ├─ 销售单明细账（滚动30天）
    ├─ 采购入库及明细（滚动30天）
    └─ 渠道 → 总库存 → 分仓库存 → 批次库存
  正向全链路、销售明细、入库、库存汇合后执行货权采购（30天）
  货权采购 → 金额DWD → 天猫补全 → 品牌映射及最近60天回填
  然后执行质量闸门
  质量闸门通过后短连接触发后台 BI ADS，并由本地节点监控发布批次至 ready
```

专项工作流：

- `吉客云_历史库存月末快照`：每月2日06:00，在线。
- `吉客云_每周180天历史对账`：已创建，首次人工验证前保持离线。
- 原销售查询、销售明细和库存三条入口已下线，历史版本保留，可回滚。

工作流由 `scripts/配置海豚工作流.py` 通过 API 幂等维护，不直接修改海豚元数据库。

Shell 节点统一写法：

```bash
cd /dolphinscheduler/default/resources/jike-trade-export
python3 scripts/脚本名_web.py --curl curl/对应_curl.txt
```

Worker 容器已验证：

```bash
cd /dolphinscheduler/default/resources/jike-trade-export
python3 -m py_compile scripts/*_web.py config.py
```

## cURL 运维方式

DolphinScheduler 不直接在 Shell 节点粘大段 cURL，而是维护 `curl/*.txt` 文件。脚本读取优先级统一为：

```text
环境变量 cURL > --curl 文件 > 脚本顶部内置 cURL
```

当前 cURL 文件：

```text
curl/
  渠道列表_curl.txt
  总库存查询_curl.txt
  分仓库查询_curl.txt
  批次货品库存查询_curl.txt
  销售单查询_curl.txt
  销售单明细账_curl.txt
```

token 或 `commonVerify` 过期时，重新在吉客云页面筛选/导出，复制对应 cURL，替换上述文件即可。

## 当前脚本

```text
scripts/
  渠道列表_web.py
  总库存查询_web.py
  分仓库查询_web.py
  批次货品库存查询_web.py
  销售单查询_web.py
  销售单明细账_web.py
  warehouse_list.py
```

目标表：

```text
ods.渠道列表
ods.总库存查询
ods.分仓库查询
ods.批次货品库存查询
ods.销售单查询
ods.销售单明细账
```

## Web 签名

- appkey：`jackyun_web_browser_2024`
- secret：由运行环境变量 `JKY_WEB_SIGN_SECRET` 提供，禁止写入代码、文档或提交记录。
- 签名逻辑：非空参数按 key 排序，拼接 `key + value`，计算 `MD5(secret + payload + secret)`，结果转大写。

## 库存快照类

### 渠道列表

脚本：

```bash
python3 scripts/渠道列表_web.py --curl curl/渠道列表_curl.txt
```

接口：`getsaleschannelinfoforcols`

写库方式：全量快照替换，先写临时表，再 `RENAME TABLE`。

额外字段：只追加 `updatetime`。

### 总库存查询

脚本：

```bash
python3 scripts/总库存查询_web.py --curl curl/总库存查询_curl.txt
```

接口：`allStockSkuList`

写库方式：全量快照替换。

字段使用中文列名，额外字段只追加 `updatetime`。

### 分仓库查询

脚本：

```bash
python3 scripts/分仓库查询_web.py --curl curl/分仓库查询_curl.txt --mode auto
```

支持：

- `stockSkuList`：直接网页分页拉取。
- `startExcelExport`：走吉客云导出任务，需注意 `commonVerify`。

写库方式：全量快照替换。

字段使用中文列名，额外字段只追加 `updatetime`。

### 批次货品库存查询

脚本：

```bash
python3 scripts/批次货品库存查询_web.py --curl curl/批次货品库存查询_curl.txt
```

接口：`batch.stock.search/pagelist`

写库方式：全量快照替换。

字段顺序：

```text
仓库, 批次, 货品编号, 货品名称, 规格, 条码, 品牌, 总库存量, 库存数量, 可用库存,
生产日期, 到期日期, 保质期, 保质期单位, 剩余有效天数, 剩余有效天数占比(%), 货品分类, updatetime
```

## 销售单查询

脚本：

```bash
python3 scripts/销售单查询_web.py --curl curl/销售单查询_curl.txt
```

建议使用带 `commonVerify` 的 `startExcelExport` cURL。脚本也支持把 `queryList/queryIdList` 查询 cURL 转换为导出参数。

默认同步近 40 天。可手动指定：

```bash
python3 scripts/销售单查询_web.py --curl curl/销售单查询_curl.txt --start 2026-07-01 --end 2026-07-07
```

首次回补 2026 年至当前时间的数据（`--end` 省略时自动取当前时间）：

```bash
python3 scripts/销售单查询_web.py --curl curl/销售单查询_curl.txt --start 2026-01-01
```

主表只保留业务字段和 `updatetime`，不保留同步窗口、任务 ID、源文件、数据日期，也不写中间日志表。

字段顺序：

```text
标记, 订单编号, 订单状态, 结算状态, 销售渠道, 处理时间, 付款时间, 发货仓库,
物流公司, 物流单号, 网店订单号, 发货时间, 订单类型, 应收合计, 货品数量,
货品摘要, 合并备注, 下单时间, 渠道分类, 实付金额, 市, 仓库编码（平台）,
客户编号, 客户账号, 客户名称, updatetime
```

## 进口超市上海仓—正向全链路数据

脚本：

```bash
python3 scripts/进口超市上海仓_正向全链路数据_web.py --curl curl/进口超市上海仓_正向全链路数据_curl.txt
```

海豚日常任务更新近 7 天，初始窗口最大 1 天，并按最新窗口到最旧窗口的顺序写入
`ods.进口超市上海仓_正向全链路数据`。
单个任务超过 50,000 条上限，或导出行数少于数据库现有窗口时，脚本会自动二分时间窗口并继续导出，最小窗口默认为 1 小时。最小窗口仍不完整时保留数据库旧数据并记录降级告警，不阻断后续 DWD 重建。
导出流程为 `exportFulfillOrderList -> queryStatus -> download`。

首次回补 2026 年至当前时间：

```bash
python3 scripts/进口超市上海仓_正向全链路数据_web.py --curl curl/进口超市上海仓_正向全链路数据_curl.txt --start 2026-01-01
```

认证过期时，在页面重新创建一次导出，将 `exportFulfillOrderList` 的完整 cURL 覆盖到上述 cURL 文件。

旧数据替换逻辑：按 `下单时间 >= 窗口开始 AND 下单时间 < 窗口结束` 删除后插入。

## 销售单明细账

脚本：

```bash
python3 scripts/销售单明细账_web.py --curl curl/销售单明细账_curl.txt
```

建议使用带 `commonVerify` 的 `startExcelExport` cURL。脚本也支持把 `tradeOrderDetialList` 查询 cURL 转换为导出参数。

默认同步当月 1 日 `00:00:00` 到当天 `23:59:59`。可手动指定：

```bash
python3 scripts/销售单明细账_web.py --curl curl/销售单明细账_curl.txt --start 2026-07-01 --end 2026-07-07
```

主表只保留业务字段和 `updatetime`，不保留同步窗口、任务 ID、源文件、数据日期，也不写中间日志表。

旧数据替换逻辑：按 `下单时间 >= 窗口开始 AND 下单时间 < 窗口结束` 删除后插入。

## 数据类型约定

库存数量类字段：`DECIMAL(18,0)`。

金额/价格类字段：`DECIMAL(18,2)`。

日期/时间字段：

- 日期字段：`DATE`
- 时间字段：`DATETIME`
- `updatetime`：`DATETIME`

只有数量类字段会 `round(0)`；价格/金额字段保留 2 位小数。

## 常见问题

### DolphinScheduler 日志中文乱码

日志中可能显示 `娓犻亾鍒楄〃` 之类乱码，这是日志显示编码问题。MySQL 表名和脚本内实际字段通常仍是中文。可用 Python `unicode_escape` 查询确认。

### 导出任务验证通过但后台失败

常见原因：

- 缺少 `commonVerify`
- `headersJson` 字段名与页面导出字段不一致
- token/cookie 过期

优先重新复制对应页面的 `startExcelExport` cURL 并替换 `curl/*.txt`。

### 销售类导出达到 500000 行上限

脚本会检测 `rows >= max_rows`，并在窗口长度允许时自动拆分窗口。也可以手动缩小 `--window-hours`。

## 清理约定

GitHub 提交边界：

- 提交：Python 源码、`requirements.txt`、`.env.example`、Markdown 文档。
- 不提交：`curl/`、业务 Excel、导出结果、运行日志、本机 IDE/Agent 配置。
- 所有数据库、开放平台和 DolphinScheduler 凭据通过环境变量注入。

不要保留运行产物：

```text
data/**/*.csv
data/**/*.xlsx
data/**/*_web_exports/
__pycache__/
.idea/
*.jar
```

## 进口超市上海仓金额补全

脚本：

```bash
python3 scripts/进口超市上海仓补全_web.py
```

目标对象：

```text
ods.进口超市上海仓_货权转移采购单
ods.进口超市上海仓_货权转移采购单明细
dwd.进口超市上海仓_销售单金额补全映射
dwd.销售单查询_进口超市上海仓补全
```

日常刷新（自动导出最近 45 天采购单并重建 DWD）可直接执行：

```bash
python3 scripts/进口超市上海仓补全_web.py
```

补全实体表不覆盖 `ods.销售单查询`，且字段与 ODS 销售单完全一致。优先按 `销售单查询.物流单号 = 正向全链路数据.运单号` 补充平台支付 GMV，再按 `正向全链路数据.店铺单号 = 货权转移采购单.业务单号` 聚合采购金额。唯一匹配成功时，只覆盖 DWD 表的 `应收合计`、`实付金额`；关联依据、状态和采购金额放在独立映射表。货权转移采购单使用页面导出接口批量落主表和货品明细表，日常按最新窗口到最旧窗口执行，拆窗后也优先处理较新的半窗；单次超过 10 万行时脚本自动切小时间窗口。首次全量回补使用 `--start 2026-01-01`。

语法检查：

```bash
python -m py_compile scripts/销售单查询_web.py scripts/销售单明细账_web.py scripts/分仓库查询_web.py scripts/总库存查询_web.py scripts/渠道列表_web.py scripts/批次货品库存查询_web.py scripts/warehouse_list.py common.py config.py
```
