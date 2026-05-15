#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""医务科每日上报数据钉钉推送脚本。

默认统计“昨天”的数据，查询 HIS Oracle 后通过钉钉企业内部机器人单聊推送。

直接运行：
  python3 daily_medical_report_push.py

测试只打印不发送：
  python3 daily_medical_report_push.py --dry-run

指定日期/接收人：
  python3 daily_medical_report_push.py --date 2026-05-14 --userid 215944441533346540
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import sys
import traceback
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import requests

from async_process_runner import AsyncProcessRunner

import oracledb
oracledb.init_oracle_client()

# ===================== 写死的运行配置 =====================
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

# 钉钉应用配置（按要求写死）
DINGTALK_APP_ID = "47a12eb3-9cd8-4b6b-937a-36a8c302d669"
DINGTALK_AGENT_ID = 4581485935  # 保留备用；当前脚本不走工作通知
DINGTALK_APP_KEY = "dingltywmtwpagzinyjz"
DINGTALK_APP_SECRET = "WNxBG0FX7eHtyiTrF420QAfzMTlOr2W4Ly1k06QtpAezF0expgW6ucQbK6EhudHY"
DINGTALK_ROBOT_CODE = "dingltywmtwpagzinyjz"

# 钉钉外网访问代理；不需要代理时改成空字符串 ""。
DINGTALK_PROXY_URL = "socks5h://172.16.4.160:1080"

# 默认接收人 userid。参考 morning_meeting 里“发给自己”的 userid；如需换人用 --userid 覆盖。
DEFAULT_RECEIVER_USERIDS = "215944441533346540"

# HIS Oracle 连接配置（参考 /home/cdsw/dev/projects/morning_meeting）
ORACLE_USER = "HMISW2003"
ORACLE_PASSWORD = "lmwzpwxw6287"
ORACLE_DSN = "192.168.6.3:1521/feyy"


SURGERY_SQL = r"""
SELECT
    NVL(SUM(南院手术量), 0) AS 手术量,
    NVL(SUM(昨日分娩量), 0) AS 分娩量,
    NVL(SUM(南院日间手术量), 0) AS 日间手术量
FROM (
    -- 分娩量日度数据
    SELECT TRUNC(csrq) AS day,
           COUNT(*) AS 昨日分娩量,
           0 AS 南院日间手术量,
           0 AS 南院手术量
      FROM zybrxx a join ksmc b on a.ksbh = b.no
     WHERE TRUNC(csrq) = TO_DATE(:report_date, 'YYYY-MM-DD')
       AND zyh LIKE '%B1%' and b.mc not in ('特需产科一体化中心（北）', '特需产科病区(南)')
     GROUP BY TRUNC(csrq)
    UNION ALL
    -- 南院日间手术量
    SELECT TRUNC(a.sssj) AS day,
           0 AS 昨日分娩量,
           COUNT(*) AS 南院日间手术量,
           0 AS 南院手术量
      FROM zybrsssq a, zgxx b, ssczmc c
     WHERE a.ssysbh = b.id
       AND c.dm = a.ssczdm
       AND TRUNC(a.sssj) = TO_DATE(:report_date, 'YYYY-MM-DD')
       AND sfjz = 'R'
     GROUP BY TRUNC(a.sssj)
    UNION ALL
    -- 南院手术量
    SELECT TRUNC(a.sssj) AS day,
           0 AS 昨日分娩量,
           0 AS 南院日间手术量,
           COUNT(*) AS 南院手术量
      FROM zybrsssq a, zgxx b, ssczmc c
     WHERE a.ssysbh = b.id
       AND c.dm = a.ssczdm
       AND TRUNC(a.sssj) = TO_DATE(:report_date, 'YYYY-MM-DD')
       AND sqzt IN ('4', '3')
     GROUP BY TRUNC(a.sssj)
)
"""


# “在院病人统计”改为“床位使用”：复用 morning_meeting 里床位使用 SJCW 口径。
BED_USAGE_SQL = r"""
SELECT
    NVL(SUM(NVL(SJCW, 0)), 0) AS 全院床位使用,
    NVL(SUM(CASE WHEN BQMC LIKE '%儿童重症%' THEN NVL(SJCW, 0) ELSE 0 END), 0) AS PICU床位使用,
    NVL(SUM(CASE WHEN BQMC LIKE '%新生儿重症%' THEN NVL(SJCW, 0) ELSE 0 END), 0) AS NICU床位使用,
    NVL(SUM(CASE WHEN BQMC LIKE '%小儿监护(北)%' THEN NVL(SJCW, 0) ELSE 0 END), 0) AS 北监护床位使用,
    NVL(SUM(CASE WHEN TO_CHAR(KSBH) = '513' OR BQMC LIKE '%外二心胸外科%' THEN NVL(SJCW, 0) ELSE 0 END), 0) AS 外二心胸外科床位使用
FROM (
    SELECT F.FYBH,
           F.BQH,
           F.KSBH,
           (F.BQ || '【' || F.KS || '】') BQMC,
           F.XH,
           F.LC BQLC,
           D.RYRS,
           D.CYRS,
           D.EDCWS DECW,
           (NVL(D.ZYRS, 0) + NVL(G.BRSL, 0)) SJCW,
           NVL(E.BRSL, 0) YES,
           0 SJDECW,
           0 CWLYL,
           'N' BZ,
           (NVL(D.ZYRS, 0) - NVL(D.EDCWS, 0)) JCSL
      FROM (SELECT FDDM,
                   BQH,
                   KSBH,
                   EDCWS,
                   SUM(RYRS) RYRS,
                   SUM(CYRS) CYRS,
                   SUM(ZYRS) ZYRS
              FROM CX.ZYQTXX
             WHERE RQ >= TO_DATE(:report_date, 'YYYY-MM-DD')
               AND RQ <= TO_DATE(:report_date, 'YYYY-MM-DD')
             GROUP BY FDDM, BQH, KSBH, EDCWS) D,
           (SELECT A.BQH, B.KSBH, COUNT(A.ZYH) BRSL
              FROM HMISW2003.ZYCW A, HMISW2003.ZYBRXX B
             WHERE A.ZYH = B.ZYH
               AND B.YEBZ = 'Y'
               AND B.JZBZ = 'N'
               AND B.CYRQ IS NULL
               AND B.ZYH NOT LIKE '-%'
               AND B.ZYRQ <= TO_DATE(:report_date, 'YYYY-MM-DD')
             GROUP BY A.BQH, B.KSBH) E,
           (SELECT Y.FYBH,
                   X.BQH,
                   Y.MC BQ,
                   X.KSBH,
                   Z.MC KS,
                   CWEDS,
                   Y.XH,
                   Y.LC
              FROM ZYBQKSB X, ZYBQ Y, KSMC Z
             WHERE X.BQH = Y.DM
               AND X.KSBH = Z.NO
               AND Y.SYBZ = 'Y') F,
           (SELECT BQH, KSBH, COUNT(*) BRSL
              FROM ZYBRXX
             WHERE ZYRQ >= TO_DATE(:report_date, 'YYYY-MM-DD')
               AND CYRQ <= TO_DATE(:report_date, 'YYYY-MM-DD')
               AND ZYRQ - CYRQ = 0
               AND ZYH NOT LIKE '-%'
               AND ZYH NOT LIKE '%B%'
               AND CWH IS NOT NULL
             GROUP BY BQH, KSBH) G
     WHERE F.BQH = D.BQH(+)
       AND F.KSBH = D.KSBH(+)
       AND F.BQH = E.BQH(+)
       AND F.KSBH = E.KSBH(+)
       AND F.BQH = G.BQH(+)
       AND F.KSBH = G.KSBH(+)
)
"""


OUTPATIENT_SQL = r"""
SELECT YQ AS 院区,
       SUM(CASE WHEN MZLX <> '114' THEN DECODE(MZKB, '0', 0, 1) ELSE 0 END) AS 门急诊量,
       SUM(CASE WHEN MZLB = '2' THEN DECODE(MZKB, '0', 0, 1) ELSE 0 END) AS 急诊量
FROM (
    SELECT '北院' AS YQ,
           MZLX,
           MZKB,
           (SELECT LB FROM MZGHLXB WHERE BH = HMISW2003_BBYQ.MZGHXX.MZLX) AS MZLB
      FROM HMISW2003_BBYQ.MZGHXX
     WHERE mzh >= TO_CHAR(TO_DATE(:report_date, 'YYYY-MM-DD'), 'YYYYMMDD') || '00000'
       AND mzh <= TO_CHAR(TO_DATE(:report_date, 'YYYY-MM-DD'), 'YYYYMMDD') || '99999'
       AND ZFRY IS NULL
    UNION ALL
    SELECT '南院' AS YQ,
           MZLX,
           MZKB,
           (SELECT LB FROM MZGHLXB WHERE BH = HMISW2003.MZGHXX.MZLX) AS MZLB
      FROM HMISW2003.MZGHXX
     WHERE mzh >= TO_CHAR(TO_DATE(:report_date, 'YYYY-MM-DD'), 'YYYYMMDD') || '00000'
       AND mzh <= TO_CHAR(TO_DATE(:report_date, 'YYYY-MM-DD'), 'YYYYMMDD') || '99999'
       AND ZFRY IS NULL
)
GROUP BY YQ
ORDER BY YQ
"""


class PushError(RuntimeError):
    pass


def log(message: str) -> None:
    now = dt.datetime.now(SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{now}] {message}", flush=True)


def default_report_date() -> str:
    return (dt.datetime.now(SHANGHAI_TZ).date() - dt.timedelta(days=1)).isoformat()


def split_csv(*values: Any) -> list[str]:
    result: list[str] = []
    for value in values:
        if not value:
            continue
        raw_items = value if isinstance(value, (list, tuple, set)) else [value]
        for raw in raw_items:
            for item in str(raw).split(','):
                item = item.strip()
                if item:
                    result.append(item)
    return result


def to_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, Decimal):
        return int(value)
    return int(value)


def fetch_one_dict(conn: oracledb.Connection, sql: str, report_date: str) -> dict[str, Any]:
    with conn.cursor() as cursor:
        cursor.execute(sql, report_date=report_date)
        row = cursor.fetchone()
        if row is None:
            return {}
        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, row))


def fetch_all_dicts(conn: oracledb.Connection, sql: str, report_date: str) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(sql, report_date=report_date)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def query_report(report_date: str) -> dict[str, Any]:
    with oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN) as conn:
        log("查询手术量/分娩量/日间手术量")
        surgery = fetch_one_dict(conn, SURGERY_SQL, report_date)

        log("查询床位使用")
        bed_usage = fetch_one_dict(conn, BED_USAGE_SQL, report_date)

        log("查询南北院门急诊量/急诊量")
        outpatient_rows = fetch_all_dicts(conn, OUTPATIENT_SQL, report_date)

    outpatient_total = sum(to_int(row.get("门急诊量")) for row in outpatient_rows)
    emergency_total = sum(to_int(row.get("急诊量")) for row in outpatient_rows)

    return {
        "日期": report_date,
        "手术量": to_int(surgery.get("手术量")),
        "分娩量": to_int(surgery.get("分娩量")),
        "日间手术量": to_int(surgery.get("日间手术量")),
        "全院床位使用": to_int(bed_usage.get("全院床位使用")),
        "PICU床位使用": to_int(bed_usage.get("PICU床位使用")),
        "NICU床位使用": to_int(bed_usage.get("NICU床位使用")),
        "北监护床位使用": to_int(bed_usage.get("北监护床位使用")),
        "外二心胸外科床位使用": to_int(bed_usage.get("外二心胸外科床位使用")),
        "南北院门急诊量": outpatient_total,
        "南北院急诊量": emergency_total,
        "门急诊明细": [
            {
                "院区": row.get("院区"),
                "门急诊量": to_int(row.get("门急诊量")),
                "急诊量": to_int(row.get("急诊量")),
            }
            for row in outpatient_rows
        ],
    }


def build_message(report: dict[str, Any]) -> str:
    lines = [
        f"医务科每日上报数据（{report['日期']}）",
        "",
        f"手术量：{report['手术量']}",
        f"分娩量：{report['分娩量']}",
        f"日间手术量：{report['日间手术量']}",
        f"全院床位使用：{report['全院床位使用']}",
        f"PICU床位使用：{report['PICU床位使用']}",
        f"NICU床位使用：{report['NICU床位使用']}",
        f"北监护床位使用：{report['北监护床位使用']}",
        f"外二心胸外科床位使用：{report['外二心胸外科床位使用']}",
        f"南北院门急诊量：{report['南北院门急诊量']}",
        f"南北院急诊量：{report['南北院急诊量']}",
    ]

    details = report.get("门急诊明细") or []
    if details:
        lines.append("")
        lines.append("门急诊明细：")
        for item in details:
            lines.append(f"{item['院区']}：门急诊 {item['门急诊量']}，急诊 {item['急诊量']}")
    return "\n".join(lines)


def get_dingtalk_proxies() -> dict[str, str] | None:
    if not DINGTALK_PROXY_URL:
        return None
    return {"http": DINGTALK_PROXY_URL, "https": DINGTALK_PROXY_URL}


def response_json(response: requests.Response) -> dict[str, Any]:
    try:
        return response.json()
    except ValueError as exc:
        raise PushError(f"钉钉返回非 JSON：HTTP {response.status_code} {response.text[:500]}") from exc


def get_access_token() -> str:
    response = requests.get(
        "https://oapi.dingtalk.com/gettoken",
        params={"appkey": DINGTALK_APP_KEY, "appsecret": DINGTALK_APP_SECRET},
        timeout=20,
        proxies=get_dingtalk_proxies(),
    )
    data = response_json(response)
    if response.status_code >= 400 or data.get("errcode") != 0:
        raise PushError(f"获取 access_token 失败：HTTP {response.status_code} {data}")
    return data["access_token"]


def send_robot_single_chat_text(userids: list[str], text: str) -> dict[str, Any]:
    """通过钉钉企业内部机器人发送单聊文本消息。"""
    if not userids:
        raise PushError("接收人 userid 为空")
    if len(userids) > 20:
        raise PushError("钉钉机器人单聊 batchSend 单次最多支持 20 个 userId")

    access_token = get_access_token()
    payload = {
        "robotCode": DINGTALK_ROBOT_CODE,
        "userIds": userids,
        "msgKey": "sampleText",
        "msgParam": json.dumps({"content": text}, ensure_ascii=False),
    }
    response = requests.post(
        "https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend",
        headers={
            "x-acs-dingtalk-access-token": access_token,
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
        proxies=get_dingtalk_proxies(),
    )
    data = response_json(response)
    if response.status_code >= 400 or data.get("code") or data.get("errcode"):
        raise PushError(f"发送钉钉机器人单聊消息失败：HTTP {response.status_code} {data}")
    return data


def seconds_until_next_run(hour: int, minute: int) -> tuple[float, dt.datetime]:
    now = dt.datetime.now(SHANGHAI_TZ)
    next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if next_run <= now:
        next_run += dt.timedelta(days=1)
    return (next_run - now).total_seconds(), next_run


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    report_date = args.date or default_report_date()
    userids = split_csv(DEFAULT_RECEIVER_USERIDS, args.userids)
    # 去重保持顺序
    userids = list(dict.fromkeys(userids))

    report = query_report(report_date)
    message = build_message(report)

    print("\n" + message + "\n", flush=True)
    result: dict[str, Any] | None = None
    if args.dry_run:
        log("dry-run：只查询并打印，不发送钉钉")
    else:
        log(f"发送钉钉机器人单聊：robotCode={DINGTALK_ROBOT_CODE}, userids={userids}")
        result = send_robot_single_chat_text(userids, message)
        log(f"钉钉机器人单聊发送成功：{result}")

    return {"report": report, "send_result": result}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="医务科每日上报数据钉钉推送")
    parser.add_argument("--date", default=None, help="统计日期 YYYY-MM-DD；默认昨天")
    parser.add_argument("--userid", dest="userids", action="append", default=[], help="钉钉接收人 userid；可重复，或逗号分隔。默认写死为脚本内 DEFAULT_RECEIVER_USERIDS")
    parser.add_argument("--dry-run", action="store_true", help="只查询并打印，不发送钉钉")
    parser.add_argument("--json", action="store_true", help="结束时输出 JSON 结果")
    parser.add_argument("--schedule", action="store_true", help="常驻进程，每天定时执行")
    parser.add_argument("--hour", type=int, default=15, help="定时小时，默认 8")
    parser.add_argument("--minute", type=int, default=22, help="定时分钟，默认 0")
    parser.add_argument("--timeout", type=int, default=6000, help="子进程超时时间（秒），默认 6000")
    return parser


async def run_scheduler(args: argparse.Namespace) -> None:
    """定时调度；每次执行放到子进程里，避免 Oracle/网络调用卡死主进程。"""
    log(f"进入定时模式：每天 {args.hour:02d}:{args.minute:02d} 推送，timeout={args.timeout}s")
    while True:
        wait_seconds, next_run = seconds_until_next_run(args.hour, args.minute)
        log(f"下一次执行：{next_run.isoformat()}，等待 {wait_seconds:.0f} 秒")
        await asyncio.sleep(wait_seconds)

        log("定时任务开始")
        runner = AsyncProcessRunner(run_once, args, timeout=args.timeout)
        result = await runner.run()
        if isinstance(result, tuple) and result and result[0] is False and len(result) >= 3:
            log(f"定时任务失败：{result[1]}")
            log(str(result[2]))
        elif result is False:
            log("定时任务失败：子进程超时或异常退出")
        else:
            log(f"定时任务完成：{result}")


def main() -> int:
    args = build_parser().parse_args()

    if not args.schedule:
        result = run_once(args)
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        return 0

    asyncio.run(run_scheduler(args))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        log("收到退出信号")
        raise SystemExit(0)
    except Exception as exc:
        log(f"程序异常：{exc}")
        traceback.print_exc()
        raise SystemExit(1)
