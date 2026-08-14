# 吉客云 Web 导出同步脚本

通过吉客云网页版导出任务获取数据，下载 xlsx 后写入 MySQL。当前脚本以 DolphinScheduler 部署为目标，同时保留 PyCharm 右键运行方式。

> 仓库边界：`D:\code\anzhuanghaitun` 是 DolphinScheduler 本地运行目录，不是 Git 仓库。实际 Git 项目根目录是当前 `jike-trade-export` 目录。`ds_logs/`、`postgres_data/`、`zk_data/`、`ds_worker_data/` 等运行卷不属于源码，禁止提交。

## 安全说明

- 数据库密码、吉客云应用密钥、DolphinScheduler 密码必须通过环境变量提供。
- 网页接口签名值通过 `JKY_WEB_SIGN_SECRET` 提供；部署新版资源前必须先在 Worker 环境配置。
- 浏览器复制的 cURL 含 token、cookie 或 `commonVerify`，只能保存在已忽略的 `curl/` 目录或运行环境变量中。
- `manual_inputs/*.xlsx` 是业务补数文件，`output/` 和 `data/logs/` 是运行产物，均不得提交。
- 新增脚本不得在 `START_*_CURL_TEXT` 中保留真实请求。提交前请执行本文末尾的安全检查。

环境变量名称见 `.env.example`。PowerShell 本地运行示例：

```powershell
$env:JKY_DB_HOST = '127.0.0.1'
$env:JKY_DB_PORT = '3306'
$env:JKY_DB_USER = 'your-user'
$env:JKY_DB_PASSWORD = 'your-password'
$env:JKY_DB_NAME = 'ods'
python scripts/吉客云同步预检.py
```

## DolphinScheduler 日常总工作流

以下是截至 2026-08-14 的生产编排说明；GitHub 中的代码更新不会自动发布 ADS，也不会自动修改 DolphinScheduler 工作流。

正式日常入口为 `吉客云_每日数据总同步`。因销售导出依赖人工更新 cURL/commonVerify，当前09:20计划保持停用，只允许更新认证材料后手工执行。销售单查询、销售单明细账、采购入库和货权采购统一滚动刷新最近30天；正向全链路保留最近7天逐日可靠刷新。销售明细、采购入库和库存完成后刷新品牌映射并回填，金额链与品牌链都完成后执行统一质量闸门，只有质量检查通过才刷新 BI ADS。

主要运维脚本：

```bash
python3 scripts/吉客云同步预检.py
python3 scripts/数据质量闸门.py --max-age-hours 36
python3 scripts/销售品牌补全.py --refresh-map --include-sales --backfill
```

工作流定义通过 API 幂等维护；账号密码必须用环境变量传入：

```bash
DS_USER=admin DS_PASSWORD='***' python3 scripts/配置海豚工作流.py
# 确认 cURL 可用后，如需恢复每天09:20计划：追加 --enable-daily
```

`吉客云_每周180天历史对账` 已创建但默认保持离线。首次人工验证长周期运行时间和平台承载能力后，才使用 `--enable-weekly` 启用每周日02:00计划。原三条独立日常入口已下线并保留历史版本。

质量检查结果写入 `ops.dq_check_result`。销售品牌的稳定下游入口为 `dwd.销售单明细账_品牌补全`；当前为兼容视图，避免每日复制四百多万行数据。日常品牌任务只回填最近60天，历史全量仅在离线周对账任务中执行；货权采购、金额DWD、天猫补全、品牌回填、质量闸门和ADS严格串行，避免多个大写任务同时压 MySQL。ADS 使用远端后台短连接触发、Dolphin 本地轮询发布批次，避免 SSH 中途抖动误判长时间构建失败。

## 目录结构

```text
config.py
common.py
requirements.txt
scripts/
  入库查询_web.py
  销售单查询_web.py
  销售单明细账_web.py
  分仓库查询_web.py
  总库存查询_web.py
  历史库存_web.py
  渠道列表_web.py
  warehouse_list.py
data/
  .gitkeep
curl/                             # 运行时登录态，Git 忽略
manual_inputs/
  README.md                       # 只提交说明，不提交业务 Excel
output/                           # 生成结果，Git 忽略
CLAUDE.md
agent.md
README.md
```

完整脚本分类和用途见 [docs/SCRIPTS.md](docs/SCRIPTS.md)。日常同步、运维辅助、历史回补和一次性报表脚本已分组说明，避免把回补脚本误接入每日工作流。

## 安装依赖

```bash
pip install -r requirements.txt
```

安装后先配置 `.env.example` 中列出的环境变量，并把最新登录态放入本机/Worker 的 `curl/` 目录。项目不自动读取 `.env` 文件，生产环境应由 DolphinScheduler 或容器注入变量。

## 采购入库与品牌月度到货

脚本直接调用入库主单和明细 XHR，并固定筛选 `inouttypes=101`（采购入库）：

```bash
python scripts/入库查询_web.py
```

默认刷新最近 30 个自然日。手工指定历史范围时，`--end` 为不包含的结束边界：

```bash
python scripts/入库查询_web.py --start 2025-07-21 --end 2026-07-22
```

目标对象：

```text
ods.入库查询
ods.入库查询明细
ods.品牌月度到货
```

`ods.品牌月度到货` 为实体汇总表；每次同步会根据入库主单和明细全量重算。主单、明细和月度汇总在同一事务内更新，避免接口中断或写入异常造成数据不一致。首次运行新版脚本时，原同名视图会自动转换为实体表。cURL 保存在 `curl/入库查询_curl.txt`，已纳入 `sync_curl_auth.py` 的通用 `*_curl.txt` 登录态同步。

## 销售单同步

脚本：

```bash
python scripts/销售单查询_web.py
```

目标表：

```text
ods.销售单查询
```

本地调试时，把 DevTools 复制的销售单查询 `queryList` 或 `startExcelExport` 完整 cURL 粘到脚本顶部 `START_EXPORT_CURL_TEXT`。

## 分仓库存同步

脚本：

```bash
python scripts/分仓库查询_web.py
```

目标表：

```text
ods.分仓库查询
```

本地调试时，把 DevTools 复制的分仓库存查询 `stockSkuList` 或 `startExcelExport` 完整 cURL 粘到脚本顶部 `START_EXPORT_CURL_TEXT`。

库存表写入是全量快照替换：先写临时表，再用 `RENAME TABLE` 原子切换。

## 数据类型约定

库存数量类字段使用 `DECIMAL(18,0)`；金额/价格类字段使用 `DECIMAL(18,2)`。

`含税价`、`不含税价`、`当前成本价`、`库存金额` 保留 2 位小数。

## 销售单明细账同步

脚本：

```bash
python scripts/销售单明细账_web.py
```

目标表：

```text
ods.销售单明细账
```

本地调试时，把 DevTools 复制的销售单明细账 `tradeOrderDetialList` 或导出 `startExcelExport` 完整 cURL 粘到脚本顶部 `START_EXPORT_CURL_TEXT`。

不传 `--start/--end` 时，默认同步当月 1 日 `00:00:00` 到今天 `23:59:59`。

如果导出触发安全验证，需要重新复制带 `commonVerify` 的 cURL，或设置环境变量 `JKY_SALES_ORDER_DETAIL_COMMON_VERIFY`。

## 渠道列表同步

脚本：

```bash
python scripts/渠道列表_web.py
```

目标表：

```text
ods.渠道列表
```

本地调试时，把 DevTools 复制的 `getsaleschannelinfoforcols` 完整 cURL 粘到脚本顶部 `START_LIST_CURL_TEXT`。

渠道列表是主数据，脚本会直接分页拉接口，并用全量快照替换写入 MySQL。

## 总库存查询同步

脚本：

```bash
python scripts/总库存查询_web.py
```

目标表：

```text
ods.总库存查询
```

本地调试时，把 DevTools 复制的 `allStockSkuList` 完整 cURL 粘到脚本顶部 `START_LIST_CURL_TEXT`。

脚本会直接分页拉接口，并用全量快照替换写入 MySQL。数量类字段使用 `DECIMAL(18,0)`，价格类字段使用 `DECIMAL(18,2)`。

## 历史库存月末快照

`specialExcelExport` 只会把页面当前已有的 `datas` 生成 Excel，不等于完整历史库存。`历史库存_web.py` 会复用该 cURL 的登录态，改调真实分页接口 `/jkyun/birc/stock/history`，并按截止日期（当天 `23:59:59`）拉取全部仓库。

回补 2024 年 12 月至 2025 年 12 月的月末快照：

```bash
python3 scripts/历史库存_web.py \
  --curl curl/历史库存_curl.txt \
  --start-month 2024-12 \
  --end-month 2025-12
```

只重跑单个快照：

```bash
python3 scripts/历史库存_web.py \
  --curl curl/历史库存_curl.txt \
  --snapshot-date 2025-12-31
```

不传日期时，默认刷新最近两个已经结束的自然月月末：

```bash
python3 scripts/历史库存_web.py --curl curl/历史库存_curl.txt
```

例如任务在 2026 年 8 月运行时，会刷新 `2026-06-30` 和 `2026-07-31`，用于覆盖迟到核算或历史调整。

目标表：

```text
ods.历史库存
ods.历史库存快照批次
```

`ods.历史库存` 的粒度为“快照日期 + 仓库ID + SKUID”。同一日期重跑时，脚本先把完整结果装入阶段表并校验行数，再在事务内仅替换该日期；其他日期不会受影响。若同一日期的新结果少于旧结果的 80%，默认拒绝覆盖。

`ods.历史库存快照批次` 保存状态、行数、库存量、核算数量、核算成本金额、库存金额、未核算金额、数据哈希、接口上下文和运行时间，用于发现漏页、缩水或失败快照。

## 一次复制 cURL，同步登录态

普通库存、渠道类接口通常只是网页登录态过期。可以只复制任意一个最新网页请求的 cURL，粘到：

```text
curl/每日更新_curl.txt
```

然后在本 Git 仓库根目录运行：

```bat
py -3 scripts\sync_curl_auth.py
```

脚本会自动把新的 `authorization`、cookie、`ati` 分发到同目录下其他 `*_curl.txt`。

销售单导出如果触发手机验证，必须先在吉客云页面完成手机验证，再复制验证后的 `startExcelExport` cURL。判断标准是复制出来的 cURL 里能看到：

```text
commonVerify=
```

如果没有 `commonVerify`，只能更新登录态，销售单节点仍会在 `startExcelExport` 阶段触发手机验证。

如果不想保存文件，也可以直接从剪贴板读取：

```bat
py -3 scripts\sync_curl_auth.py --clipboard
```

如果已经把 cURL 保存成文件，可以这样运行：

```bash
python3 scripts/sync_curl_auth.py --source curl/最新复制的_curl.txt
```

如果在 Windows 本机运行：

```bat
py -3 scripts\sync_curl_auth.py --source curl\最新复制的_curl.txt
```

这个工具只刷新登录态，不会改筛选条件、导出字段、`commonVerify`。

销售单、销售单明细账如果触发手机验证，仍然需要拿到验证后的 `commonVerify` 或 `startExcelExport` 请求；登录态同步只能减少其他 cURL 的维护成本，不能替代手机验证。

## 清理约定

`curl/`、`manual_inputs/*.xlsx`、`output/`、`data` 下的导出和日志、`__pycache__/`、`.idea/`、`.claude/settings.local.json`、`*.jar` 都不进入项目版本。

提交前检查：

```bash
git status --short
git diff --check
python -m compileall -q config.py common.py scripts
git grep -n -I -E "authorization: Bearer|access_token=Bearer|commonVerify=[A-Za-z0-9]|password[[:space:]]*[:=]"
```

最后一条命令正常情况下不应命中真实凭据；变量名、参数解析和文档占位符需要人工确认。

## 进口超市上海仓金额补全

统一入口：

```bash
python3 scripts/进口超市上海仓补全_web.py
```

默认动作：调用货权转移采购单导出接口，同步近 30 天数据，随后刷新金额映射和 DWD 实体表。平台单个导出任务超过 10 万行时，脚本会自动缩小时间窗口并续跑。DolphinScheduler 日常工作流显式滚动回看 45 天，用于覆盖采购单创建后较晚到货、完结或金额更新的情况。

数据对象：

```text
ods.进口超市上海仓_货权转移采购单
ods.进口超市上海仓_货权转移采购单明细
dwd.进口超市上海仓_销售单金额补全映射
dwd.销售单查询_进口超市上海仓补全
```

`dwd.销售单查询_进口超市上海仓补全` 是实体表，字段名称和顺序与 `ods.销售单查询` 完全一致，不增加字段。唯一匹配成功时，只把表内同名的 `应收合计`、`实付金额` 改为平台支付 GMV；`ods.销售单查询` 不修改。关联依据和采购金额保留在独立映射表中。

只刷新 DWD（不访问抖音平台）：

```bash
python3 scripts/进口超市上海仓补全_web.py --build-only
```

DolphinScheduler Shell 节点：

```bash
cd /dolphinscheduler/default/resources/jike-trade-export
python3 scripts/进口超市上海仓补全_web.py --lookback-days 45 --window-days 7 --timeout 1800 --interval 2
```

到货时效策略：

- 销售单和货权采购单日常滚动刷新最近 45 天；采购单按 7 天一个导出窗口，单窗超量时自动继续拆分。
- 正向全链路滚动刷新最近 30 天，必须逐日顺序导出。平台多日窗口或并发窗口可能生成残缺文件，脚本会拒绝 `--window-days > 1`。
- 正向全链路新文件少于数据库同日已有行数时拒绝覆盖并自动重试，空文件也不会删除旧数据，避免到货数据倒退。
- 不建议把采购单日常回看缩短到 3 天，否则采购单创建后较晚到货或完结时可能无法刷新。
- 建议每周低峰期执行一次 180 天长周期对账，补偿超过 45 天才发生的异常晚到货：

```bash
python3 scripts/进口超市上海仓_正向全链路数据_web.py --lookback-days 180 --window-days 1 --incomplete-retries 3 --timeout 1800 --interval 2
python3 scripts/进口超市上海仓补全_web.py --lookback-days 180 --window-days 14 --timeout 1800 --interval 2 --sync-only
python3 scripts/进口超市上海仓补全_web.py --build-only
python3 scripts/天猫国际自营金额补全.py --apply
```

`--build-only` 会先分别生成全链路、采购单和销售物流的阶段汇总表并建立连接索引，再生成金额映射和 DWD；正式表只在阶段表完整成功后切换。数据库互斥锁会阻止两个 DWD 重建任务同时运行。

推荐依赖顺序：

```text
销售单查询 -> 进口超市上海仓_正向全链路数据 -> 进口超市上海仓金额补全
```

首次历史回补可指定开始时间；日常任务无需参数：

```bash
python3 scripts/进口超市上海仓补全_web.py --start 2026-01-01
```

关联路径为 `销售单查询.物流单号 = 正向全链路数据.运单号`，再以 `正向全链路数据.店铺单号 = 货权转移采购单.业务单号` 补充采购金额。只有平台订单数和销售单数均为 1 时才回填 DWD 的 `应收合计`、`实付金额`；合并物流保留原值。核验字段只存在于独立映射表。

## 天猫国际自营金额补全

固定补数文件：

```text
manual_inputs/天猫国际自营店数据补BI.xlsx
```

后续更新时直接覆盖同名文件。脚本读取“吉客云”列，并按吉客云订单号汇总工作表最后一列的实际金额；只对金额完整且订单号不歧义的数据，同时覆盖 DWD 的 `应收合计` 和 `实付金额`。原始 `ods.销售单查询` 不修改。

DolphinScheduler Shell 节点：

```bash
cd /dolphinscheduler/default/resources/jike-trade-export
python3 scripts/天猫国际自营金额补全.py --apply
```

把该节点放在“进口超市上海仓金额补全”节点之后：

```text
销售单查询 -> 进口超市上海仓_正向全链路数据 -> 进口超市上海仓金额补全 -> 天猫国际自营金额补全
```

脚本会刷新 `dwd.天猫国际自营_销售单金额补全映射`，并修正 `dwd.销售单查询_进口超市上海仓补全`。同时，后续再次运行“进口超市上海仓金额补全”重建 DWD 时，也会自动套用这张天猫映射表。
