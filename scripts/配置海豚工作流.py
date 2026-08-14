#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""通过 DolphinScheduler API 幂等配置吉客云日常与每周工作流。

认证信息只从环境变量读取：DS_USER、DS_PASSWORD。更新已有定义前会先下线，
DolphinScheduler 自身的版本历史作为可回滚备份。不会直接修改海豚元数据库。
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests


BASE_URL = os.getenv("DS_BASE_URL", "http://127.0.0.1:12345/dolphinscheduler").rstrip("/")
PROJECT_NAME = os.getenv("DS_PROJECT_NAME", "吉客云_库存主数据同步")
WORKDIR = "/dolphinscheduler/default/resources/jike-trade-export"

DAILY_NAME = "吉客云_每日数据总同步"
WEEKLY_NAME = "吉客云_每周180天历史对账"
LEGACY_WORKFLOWS = (
    "吉客云_销售单查询同步",
    "吉客云_销售单明细账",
    "渠道列表 -> 总库存查询 -> 分仓库查询 -> 批次货品库存查询",
)


@dataclass(frozen=True)
class TaskSpec:
    key: str
    name: str
    command: str
    description: str
    timeout: int = 120
    retries: int = 1
    task_type: str = "SHELL"


def sh(command: str) -> str:
    return f"set -e\ncd {WORKDIR}\n{command}"


DAILY_TASKS = (
    TaskSpec("preflight", "登录态与文件预检", sh("python3 scripts/吉客云同步预检.py"), "检查全部日常 cURL 的登录态和销售 commonVerify", 10, 0),
    TaskSpec("sales", "销售单查询_滚动30天", sh("python3 scripts/销售单查询_web.py --curl curl/销售单查询_curl.txt --lookback-days 30 --window-hours 720 --timeout 1800 --interval 5"), "滚动刷新最近30天销售主单", 180),
    TaskSpec("detail", "销售单明细账_滚动30天", sh("python3 scripts/销售单明细账_web.py --curl curl/销售单明细账_curl.txt --lookback-days 30 --window-hours 720 --timeout 1800 --interval 5"), "滚动刷新最近30天销售明细", 180),
    TaskSpec("inbound", "采购入库及明细_滚动30天", sh("python3 scripts/入库查询_web.py --curl curl/入库查询_curl.txt --lookback-days 30"), "滚动刷新最近30天采购入库主单、明细和品牌月度到货", 120),
    TaskSpec("channels", "渠道列表", sh("python3 scripts/渠道列表_web.py --curl curl/渠道列表_curl.txt"), "全量刷新渠道主数据", 60),
    TaskSpec("stock_total", "总库存查询", sh("python3 scripts/总库存查询_web.py --curl curl/总库存查询_curl.txt"), "原子替换总库存快照", 60),
    TaskSpec("stock_warehouse", "分仓库查询", sh("python3 scripts/分仓库查询_web.py --curl curl/分仓库查询_curl.txt --mode auto"), "原子替换分仓库存快照", 90),
    TaskSpec("stock_batch", "批次货品库存查询", sh("python3 scripts/批次货品库存查询_web.py --curl curl/批次货品库存查询_curl.txt"), "原子替换批次库存快照", 90),
    TaskSpec("fulfill", "正向全链路_可靠滚动7天", sh("python3 scripts/进口超市上海仓_正向全链路数据_web.py --lookback-days 7 --window-days 1 --incomplete-retries 1 --timeout 1800 --interval 2"), "逐日刷新最近7天正向全链路", 240),
    TaskSpec("purchase", "货权采购单及明细_滚动30天", sh("python3 scripts/进口超市上海仓补全_web.py --lookback-days 30 --window-days 7 --timeout 1800 --interval 2 --sync-only"), "滚动刷新最近30天货权采购单", 240),
    TaskSpec("dwd", "重建销售单金额补全DWD", sh("python3 scripts/进口超市上海仓补全_web.py --build-only"), "重建销售金额映射和DWD实体表", 180),
    TaskSpec("tmall", "天猫国际自营金额补全", sh("python3 scripts/天猫国际自营金额补全.py --apply"), "应用无歧义天猫国际线下金额", 60),
    TaskSpec("brand", "销售明细品牌映射及回填_最近60天", sh("python3 scripts/销售品牌补全.py --refresh-map --include-sales --backfill --lookback-days 60"), "刷新无冲突品牌映射并增量补全最近60天空品牌", 120),
    TaskSpec("dq", "统一数据质量闸门", sh("python3 scripts/数据质量闸门.py --max-age-hours 36"), "核心完整性、新鲜度和DWD一致性检查", 60, 0),
    TaskSpec("ads_trigger", "BI ADS 后台触发", "set -euo pipefail\nlog=/tmp/bi-refresh-ads-dolphin.log\nnohup sh -c 'exec 9>/tmp/bi-refresh-ads.lock; flock -n 9 || exit 0; sudo -n /usr/local/sbin/bi-refresh-ads' >>\"$log\" 2>&1 </dev/null &\necho \"ADS refresh detached pid=$!\"", "短连接触发远端后台发布，文件锁避免重复执行", 5, 2, "REMOTESHELL"),
    TaskSpec("ads", "BI ADS 发布状态监控", sh("python3 scripts/等待ADS发布.py --timeout 5400 --interval 30 --started-within-minutes 120"), "锁定批次ID并通过数据库状态等待销售和库存ADS发布完成", 100, 1),
)

DAILY_EDGES = (
    ("preflight", "sales"), ("preflight", "detail"), ("preflight", "inbound"), ("preflight", "channels"),
    ("channels", "stock_total"), ("stock_total", "stock_warehouse"), ("stock_warehouse", "stock_batch"),
    ("sales", "fulfill"),
    ("fulfill", "purchase"), ("detail", "purchase"), ("inbound", "purchase"), ("stock_batch", "purchase"),
    ("purchase", "dwd"), ("dwd", "tmall"), ("tmall", "brand"),
    ("brand", "dq"), ("dq", "ads_trigger"), ("ads_trigger", "ads"),
)

WEEKLY_TASKS = (
    TaskSpec("preflight", "登录态与文件预检", sh("python3 scripts/吉客云同步预检.py"), "每周长周期同步前置检查", 10, 0),
    TaskSpec("sales", "销售单查询_滚动180天", sh("python3 scripts/销售单查询_web.py --curl curl/销售单查询_curl.txt --lookback-days 180 --window-hours 720 --timeout 1800 --interval 5"), "长周期销售主单对账", 480),
    TaskSpec("detail", "销售单明细账_滚动180天", sh("python3 scripts/销售单明细账_web.py --curl curl/销售单明细账_curl.txt --lookback-days 180 --window-hours 720 --timeout 1800 --interval 5"), "长周期销售明细对账", 480),
    TaskSpec("inbound", "采购入库及明细_滚动180天", sh("python3 scripts/入库查询_web.py --curl curl/入库查询_curl.txt --lookback-days 180"), "长周期采购入库对账", 240),
    TaskSpec("fulfill", "正向全链路_滚动180天", sh("python3 scripts/进口超市上海仓_正向全链路数据_web.py --lookback-days 180 --window-days 1 --incomplete-retries 3 --timeout 1800 --interval 2"), "逐日长周期全链路对账", 720),
    TaskSpec("purchase", "货权采购单及明细_滚动180天", sh("python3 scripts/进口超市上海仓补全_web.py --lookback-days 180 --window-days 14 --timeout 1800 --interval 2 --sync-only"), "长周期货权采购对账", 480),
    TaskSpec("dwd", "重建销售单金额补全DWD", sh("python3 scripts/进口超市上海仓补全_web.py --build-only"), "重建销售金额映射和DWD实体表", 180),
    TaskSpec("tmall", "天猫国际自营金额补全", sh("python3 scripts/天猫国际自营金额补全.py --apply"), "应用无歧义天猫国际线下金额", 60),
    TaskSpec("brand", "销售明细品牌映射及回填", sh("python3 scripts/销售品牌补全.py --refresh-map --include-sales --backfill"), "重建品牌映射并回填", 120),
    TaskSpec("dq", "统一数据质量闸门", sh("python3 scripts/数据质量闸门.py --max-age-hours 36"), "长周期对账后的质量检查", 60, 0),
    TaskSpec("ads_trigger", "BI ADS 后台触发", "set -euo pipefail\nlog=/tmp/bi-refresh-ads-dolphin.log\nnohup sh -c 'exec 9>/tmp/bi-refresh-ads.lock; flock -n 9 || exit 0; sudo -n /usr/local/sbin/bi-refresh-ads' >>\"$log\" 2>&1 </dev/null &\necho \"ADS refresh detached pid=$!\"", "短连接触发远端后台发布，文件锁避免重复执行", 5, 2, "REMOTESHELL"),
    TaskSpec("ads", "BI ADS 发布状态监控", sh("python3 scripts/等待ADS发布.py --timeout 5400 --interval 30 --started-within-minutes 120"), "锁定批次ID并通过数据库状态等待销售和库存ADS发布完成", 100, 1),
)
WEEKLY_EDGES = tuple((WEEKLY_TASKS[i].key, WEEKLY_TASKS[i + 1].key) for i in range(len(WEEKLY_TASKS) - 1))


class DolphinClient:
    def __init__(self, username: str, password: str):
        self.session = requests.Session()
        response = self.session.post(
            f"{BASE_URL}/login",
            params={"userName": username, "userPassword": password},
            timeout=30,
        )
        payload = response.json()
        if not payload.get("success"):
            raise RuntimeError(f"DolphinScheduler 登录失败: {payload.get('msg')}")
        self.session.headers["sessionId"] = payload["data"]["sessionId"]

    def request(self, method: str, path: str, **kwargs):
        response = self.session.request(method, f"{BASE_URL}{path}", timeout=60, **kwargs)
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            raise RuntimeError(f"DolphinScheduler API失败: {payload.get('msg')}")
        return payload.get("data")

    def project_code(self) -> int:
        projects = self.request("GET", "/projects/list")
        matches = [item for item in projects if item["name"] == PROJECT_NAME]
        if len(matches) != 1:
            raise RuntimeError(f"项目匹配数量异常: {PROJECT_NAME} -> {len(matches)}")
        return int(matches[0]["code"])

    def definitions(self, project_code: int) -> list[dict[str, Any]]:
        data = self.request(
            "GET",
            f"/projects/{project_code}/process-definition",
            params={"pageNo": 1, "pageSize": 100},
        )
        return data["totalList"]

    def task_codes(self, project_code: int, count: int) -> list[int]:
        return [int(x) for x in self.request("GET", f"/projects/{project_code}/task-definition/gen-task-codes", params={"genNum": count})]

    def release(self, project_code: int, code: int, state: str) -> None:
        self.request(
            "POST",
            f"/projects/{project_code}/process-definition/{code}/release",
            params={"releaseState": state, "name": ""},
        )


def task_json(spec: TaskSpec, code: int) -> dict[str, Any]:
    params: dict[str, Any] = {"localParams": [], "rawScript": spec.command, "resourceList": []}
    if spec.task_type == "REMOTESHELL":
        params.update({"type": "SSH", "datasource": 1})
    return {
        "code": code,
        "name": spec.name,
        "version": 0,
        "description": spec.description,
        "delayTime": 0,
        "taskType": spec.task_type,
        "taskParams": params,
        "flag": "YES",
        "isCache": "NO",
        "taskPriority": "MEDIUM",
        "workerGroup": "default",
        "environmentCode": -1,
        "failRetryTimes": spec.retries,
        "failRetryInterval": 5,
        "timeoutFlag": "OPEN",
        "timeoutNotifyStrategy": "WARNFAILED",
        "timeout": spec.timeout,
        "taskGroupId": 0,
        "taskGroupPriority": 0,
        "cpuQuota": -1,
        "memoryMax": -1,
        "taskExecuteType": "BATCH",
    }


def workflow_payload(client: DolphinClient, project_code: int, tasks: tuple[TaskSpec, ...], edges: tuple[tuple[str, str], ...]):
    codes = client.task_codes(project_code, len(tasks))
    code_by_key = {task.key: code for task, code in zip(tasks, codes)}
    incoming = {post for _, post in edges}
    relations = [
        {
            "name": "",
            "preTaskCode": 0,
            "preTaskVersion": 0,
            "postTaskCode": code_by_key[task.key],
            "postTaskVersion": 0,
            "conditionType": "NONE",
            "conditionParams": {},
        }
        for task in tasks
        if task.key not in incoming
    ]
    relations.extend(
        {
            "name": "",
            "preTaskCode": code_by_key[pre],
            "preTaskVersion": 0,
            "postTaskCode": code_by_key[post],
            "postTaskVersion": 0,
            "conditionType": "NONE",
            "conditionParams": {},
        }
        for pre, post in edges
    )
    locations = [
        {"taskCode": code_by_key[task.key], "x": 180 + (index % 5) * 260, "y": 160 + (index // 5) * 180}
        for index, task in enumerate(tasks)
    ]
    return (
        [task_json(task, code_by_key[task.key]) for task in tasks],
        relations,
        locations,
    )


def upsert_workflow(
    client: DolphinClient,
    project_code: int,
    name: str,
    description: str,
    tasks: tuple[TaskSpec, ...],
    edges: tuple[tuple[str, str], ...],
    online: bool,
) -> int:
    existing = {item["name"]: item for item in client.definitions(project_code)}.get(name)
    if existing and existing["releaseState"] == "ONLINE":
        client.release(project_code, int(existing["code"]), "OFFLINE")
    task_definitions, relations, locations = workflow_payload(client, project_code, tasks, edges)
    params = {
        "name": name,
        "description": description,
        "globalParams": "[]",
        "locations": json.dumps(locations, ensure_ascii=False, separators=(",", ":")),
        "timeout": 0,
        "taskRelationJson": json.dumps(relations, ensure_ascii=False, separators=(",", ":")),
        "taskDefinitionJson": json.dumps(task_definitions, ensure_ascii=False, separators=(",", ":")),
        "executionType": "SERIAL_WAIT",
    }
    if existing:
        code = int(existing["code"])
        data = client.request("PUT", f"/projects/{project_code}/process-definition/{code}", data=params)
    else:
        data = client.request("POST", f"/projects/{project_code}/process-definition", data=params)
        code = int(data["code"] if isinstance(data, dict) else data)
    if online:
        client.release(project_code, code, "ONLINE")
    print(f"[WORKFLOW] {name}: code={code}; state={'ONLINE' if online else 'OFFLINE'}", flush=True)
    return code


def upsert_schedule(client: DolphinClient, project_code: int, workflow_code: int, crontab: str, online: bool) -> int:
    schedules = client.request(
        "GET",
        f"/projects/{project_code}/schedules",
        params={"pageNo": 1, "pageSize": 100},
    )["totalList"]
    existing = next((item for item in schedules if int(item["processDefinitionCode"]) == workflow_code), None)
    schedule = json.dumps(
        {
            "startTime": datetime.now().strftime("%Y-%m-%d 00:00:00"),
            "endTime": "2099-12-31 23:59:59",
            "crontab": crontab,
            "timezoneId": "Asia/Shanghai",
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    params = {
        "processDefinitionCode": workflow_code,
        "schedule": schedule,
        "warningType": "NONE",
        "warningGroupId": 0,
        "failureStrategy": "END",
        "workerGroup": "default",
        "tenantCode": "default",
        "environmentCode": -1,
        "processInstancePriority": "MEDIUM",
    }
    if existing:
        schedule_id = int(existing["id"])
        client.request("PUT", f"/projects/{project_code}/schedules/{schedule_id}", data=params)
    else:
        created = client.request("POST", f"/projects/{project_code}/schedules", data=params)
        schedule_id = int(created["id"] if isinstance(created, dict) else created)
    if online:
        client.request("POST", f"/projects/{project_code}/schedules/{schedule_id}/online")
    elif existing and existing.get("releaseState") == "ONLINE":
        client.request("POST", f"/projects/{project_code}/schedules/{schedule_id}/offline")
    print(f"[SCHEDULE] workflow={workflow_code}; id={schedule_id}; cron={crontab}; state={'ONLINE' if online else 'OFFLINE'}", flush=True)
    return schedule_id


def main() -> None:
    parser = argparse.ArgumentParser(description="配置吉客云 DolphinScheduler 工作流")
    parser.add_argument("--dry-run", action="store_true", help="只打印目标结构，不调用写接口")
    parser.add_argument("--enable-daily", action="store_true", help="启用每日09:20计划；默认仅保留工作流供手工执行")
    parser.add_argument("--enable-weekly", action="store_true", help="启用每周180天自动对账；默认仅创建离线定义")
    args = parser.parse_args()
    if args.dry_run:
        print(json.dumps({"daily": [t.name for t in DAILY_TASKS], "daily_edges": DAILY_EDGES, "weekly": [t.name for t in WEEKLY_TASKS]}, ensure_ascii=False, indent=2))
        return

    username = os.getenv("DS_USER")
    password = os.getenv("DS_PASSWORD")
    if not username or not password:
        raise RuntimeError("请通过 DS_USER 和 DS_PASSWORD 环境变量提供海豚账号，不要写入脚本")

    client = DolphinClient(username, password)
    project_code = client.project_code()
    daily_code = upsert_workflow(
        client,
        project_code,
        DAILY_NAME,
        "日常源数据完成后，严格串行执行货权采购、金额DWD、天猫补全、最近60天品牌回填、质量闸门和ADS。",
        DAILY_TASKS,
        DAILY_EDGES,
        True,
    )
    upsert_schedule(client, project_code, daily_code, "0 20 9 * * ? *", args.enable_daily)

    weekly_code = upsert_workflow(
        client,
        project_code,
        WEEKLY_NAME,
        "每周低峰执行180天长周期对账。默认不自动启用，首次人工验证通过后使用 --enable-weekly 开启。",
        WEEKLY_TASKS,
        WEEKLY_EDGES,
        args.enable_weekly,
    )
    if args.enable_weekly:
        upsert_schedule(client, project_code, weekly_code, "0 0 2 ? * SUN *", True)

    definitions = {item["name"]: item for item in client.definitions(project_code)}
    for name in LEGACY_WORKFLOWS:
        item = definitions.get(name)
        if item and item["releaseState"] == "ONLINE":
            client.release(project_code, int(item["code"]), "OFFLINE")
            print(f"[LEGACY] 已下线旧入口: {name}; code={item['code']}", flush=True)
    print("[DONE] DolphinScheduler 工作流配置完成", flush=True)


if __name__ == "__main__":
    main()
