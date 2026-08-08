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
import psycopg

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
DEFAULT_RECEIVER_USERIDS = "215944441533346540,19303350101213213"

# HIS Oracle 连接配置（参考 /home/cdsw/dev/projects/morning_meeting）
ORACLE_USER = "HMISW2003"
ORACLE_PASSWORD = "lmwzpwxw6287"
ORACLE_DSN = "192.168.6.3:1521/feyy"
DEFAULT_NEW_HIS_DSN = "WDHIS_KT/kingthis#Fe0726@172.16.99.26:1521/feyy"

NEW_HIS_DAILY_START = dt.date(2026, 7, 26)
NEW_HIS_OUTPATIENT_START = dt.date(2026, 7, 27)
SELF_SNAPSHOT_START = dt.date(2026, 7, 27)

BI_REPORTS_DB = {
    'dbname': 'BI_reports',
    'user': 'postgres',
    'password': 'nbfeyy123',
    'host': '172.16.0.81',
    'port': '5432',
}


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


OLD_DELIVERY_EVENTS_SQL = r"""
SELECT DISTINCT TRIM(a.ZYH) AS 住院号
FROM ZYBRXX a
JOIN KSMC b ON a.KSBH = b.NO
WHERE TRUNC(a.CSRQ) = TO_DATE(:report_date, 'YYYY-MM-DD')
  AND a.ZYH LIKE '%B1%'
  AND b.MC NOT IN ('特需产科一体化中心（北）', '特需产科病区(南)')
"""


NEW_SURGERY_METRICS_SQL = r"""
WITH valid_ops AS (
    SELECT ops.REQUEST_NO,
           ops.IS_AMBULATORY_SURGERY,
           ops.BEGIN_TIME,
           ops.END_TIME,
           ops.FINISH_TIME
    FROM WDHIS.CIS_OPS_REQUEST ops
    JOIN WDHIS.CIS_IN_PAT_REG reg
      ON reg.VISIT_ID = ops.REG_ID
     AND reg.BRANCH_CODE = ops.BRANCH_CODE
     AND NVL(reg.IS_INVALID, 0) = 0
    JOIN WDHIS.PAT_REGISTER patient
      ON patient.REG_ID = reg.VISIT_ID
     AND patient.PAT_ID = reg.PAT_ID
     AND patient.SOURCE_TYPE = 2
     AND patient.BRANCH_CODE = reg.BRANCH_CODE
     AND NVL(patient.IS_INVALID, 0) = 0
    JOIN WDHIS.PUB_WARD ward ON ward.ID = reg.WARD_ID
    JOIN WDHIS.PUB_EMP_INFO surgeon_info
      ON surgeon_info.ID = ops.SURGEON_DOCTOR
    LEFT JOIN WDHIS.PUB_WARD request_ward ON request_ward.ID = ops.REQ_WARD
    LEFT JOIN WDHIS.PUB_EMP reg_record_emp ON reg_record_emp.ID = reg.REG_EMPID
    LEFT JOIN WDHIS.PUB_EMP input_emp ON input_emp.ID = ops.INPUT_EMPID
    LEFT JOIN WDHIS.PUB_EMP surgeon_emp ON surgeon_emp.ID = ops.SURGEON_DOCTOR
    WHERE ops.SOURCE_TYPE = 2
      AND ops.STATE IN (60, 80)
      AND ops.INVALID_TIME IS NULL
      AND ops.CANCEL_TIME IS NULL
      AND ops.SURGEON_DOCTOR IS NOT NULL
      AND ward.BRANCH_CODE IN ('00', '01')
      AND EXISTS (
          SELECT 1
          FROM WDHIS.CIS_OPS_REQUEST_ITEM item
          WHERE item.REQUEST_NO = ops.REQUEST_NO
            AND item.INVALID_TIME IS NULL
      )
      AND INSTR(
          NVL(patient.NAME, '~') || NVL(ward.NAME, '~') ||
          NVL(request_ward.NAME, '~') || NVL(reg_record_emp.NAME, '~') ||
          NVL(input_emp.NAME, '~') || NVL(surgeon_emp.NAME, '~'),
          '测试'
      ) = 0
      AND INSTR(
          NVL(patient.NAME, '~') || NVL(ward.NAME, '~') ||
          NVL(request_ward.NAME, '~') || NVL(reg_record_emp.NAME, '~') ||
          NVL(input_emp.NAME, '~') || NVL(surgeon_emp.NAME, '~'),
          '考核'
      ) = 0
      AND INSTR(
          NVL(patient.NAME, '~') || NVL(ward.NAME, '~') ||
          NVL(request_ward.NAME, '~') || NVL(reg_record_emp.NAME, '~') ||
          NVL(input_emp.NAME, '~') || NVL(surgeon_emp.NAME, '~'),
          '演练'
      ) = 0
      AND INSTR(
          UPPER(
              NVL(patient.NAME, '~') || NVL(ward.NAME, '~') ||
              NVL(request_ward.NAME, '~') || NVL(reg_record_emp.NAME, '~') ||
              NVL(input_emp.NAME, '~') || NVL(surgeon_emp.NAME, '~')
          ),
          'CESHI'
      ) = 0
      AND NVL(reg.REG_EMPID, -1) <> 999
      AND NVL(reg_record_emp.CODE, '~') <> '999'
      AND NVL(input_emp.CODE, '~') <> '999'
      AND NVL(surgeon_emp.CODE, '~') <> '999'
)
SELECT
    COUNT(DISTINCT CASE
        WHEN END_TIME >= TO_DATE(:report_date, 'YYYY-MM-DD')
         AND END_TIME < TO_DATE(:report_date, 'YYYY-MM-DD') + 1
        THEN REQUEST_NO
    END) AS 手术量,
    COUNT(DISTINCT CASE
        WHEN IS_AMBULATORY_SURGERY = 1
         AND COALESCE(BEGIN_TIME, END_TIME, FINISH_TIME)
             >= TO_DATE(:report_date, 'YYYY-MM-DD')
         AND COALESCE(BEGIN_TIME, END_TIME, FINISH_TIME)
             < TO_DATE(:report_date, 'YYYY-MM-DD') + 1
        THEN REQUEST_NO
    END) AS 日间手术量
FROM valid_ops
"""


NEW_DELIVERY_EVENTS_SQL = r"""
SELECT DISTINCT TRIM(baby.VISIT_NO) AS 住院号
FROM WDHIS.CIS_IN_PAT_REG baby
JOIN WDHIS.PAT_REGISTER patient
  ON patient.REG_ID = baby.VISIT_ID
 AND patient.PAT_ID = baby.PAT_ID
 AND patient.SOURCE_TYPE = 2
 AND patient.BRANCH_CODE = baby.BRANCH_CODE
 AND NVL(patient.IS_INVALID, 0) = 0
JOIN WDHIS.PUB_WARD ward ON ward.ID = baby.WARD_ID
JOIN WDHIS.PUB_DEPT dept ON dept.ID = baby.DEPT_ID
LEFT JOIN WDHIS.PUB_EMP reg_record_emp ON reg_record_emp.ID = baby.REG_EMPID
WHERE patient.DATE_OF_BIRTH >= TO_DATE(:report_date, 'YYYY-MM-DD')
  AND patient.DATE_OF_BIRTH < TO_DATE(:report_date, 'YYYY-MM-DD') + 1
  AND NVL(baby.IS_INVALID, 0) = 0
  AND TRIM(baby.VISIT_NO) NOT LIKE '0%'
  AND TRIM(baby.VISIT_NO) NOT LIKE '-%'
  AND TRIM(baby.VISIT_NO) LIKE '%B1%'
  AND ward.BRANCH_CODE IN ('00', '01')
  AND dept.NAME NOT IN ('特需产科一体化中心（北）', '特需产科病区(南)')
  AND INSTR(NVL(patient.NAME, '~') || NVL(ward.NAME, '~') || NVL(dept.NAME, '~') || NVL(reg_record_emp.NAME, '~'), '测试') = 0
  AND INSTR(NVL(patient.NAME, '~') || NVL(ward.NAME, '~') || NVL(dept.NAME, '~') || NVL(reg_record_emp.NAME, '~'), '考核') = 0
  AND INSTR(NVL(patient.NAME, '~') || NVL(ward.NAME, '~') || NVL(dept.NAME, '~') || NVL(reg_record_emp.NAME, '~'), '演练') = 0
  AND INSTR(UPPER(NVL(patient.NAME, '~') || NVL(ward.NAME, '~') || NVL(dept.NAME, '~') || NVL(reg_record_emp.NAME, '~')), 'CESHI') = 0
  AND NVL(dept.CODE, '~') <> '001'
  AND NVL(baby.REG_EMPID, -1) <> 999
  AND NVL(reg_record_emp.CODE, '~') <> '999'
"""


NEW_BED_USAGE_SQL = r"""
WITH statistics_rows AS (
    SELECT ROWIDTOCHAR(stats.ROWID) AS SOURCE_ROWID,
           ward.CODE AS BQH,
           ward.NAME AS BQ,
           dept.CODE AS KSBH,
           dept.NAME AS KS,
           DBMS_LOB.SUBSTR(stats.CURRENT_PAT_DETAIL, 32767, 1) AS CURRENT_PAT_DETAIL,
           DBMS_LOB.SUBSTR(stats.IN_PAT_DETAIL, 32767, 1) AS IN_PAT_DETAIL,
           DBMS_LOB.SUBSTR(stats.OUT_PAT_DETAIL, 32767, 1) AS OUT_PAT_DETAIL,
           stats.TODAY_IN_OUT_NUM
    FROM WDHIS.PUB_IN_PAT_STATISTICS stats
    JOIN WDHIS.PUB_WARD ward ON ward.ID = stats.WARD_ID
    JOIN WDHIS.PUB_DEPT dept ON dept.ID = stats.DEPT_ID
    WHERE stats.STATISTICS_DATE >= TO_DATE(:report_date, 'YYYY-MM-DD')
      AND stats.STATISTICS_DATE < TO_DATE(:report_date, 'YYYY-MM-DD') + 1
      AND ward.BRANCH_CODE IN ('00', '01')
      AND TRIM(ward.CODE) <> '21'
      AND INSTR(NVL(ward.NAME, '~') || NVL(dept.NAME, '~'), '测试') = 0
      AND INSTR(NVL(ward.NAME, '~') || NVL(dept.NAME, '~'), '考核') = 0
      AND INSTR(NVL(ward.NAME, '~') || NVL(dept.NAME, '~'), '演练') = 0
      AND INSTR(UPPER(NVL(ward.NAME, '~') || NVL(dept.NAME, '~')), 'CESHI') = 0
      AND NVL(dept.CODE, '~') <> '001'
),
bed_occurrences AS (
    SELECT source.SOURCE_ROWID,
           detail.VISIT_ID
    FROM statistics_rows source
    CROSS APPLY (
        SELECT TO_NUMBER(
                   REGEXP_SUBSTR(source.CURRENT_PAT_DETAIL, '[^,]+', 1, LEVEL)
               ) AS VISIT_ID
        FROM DUAL
        CONNECT BY LEVEL <= REGEXP_COUNT(source.CURRENT_PAT_DETAIL, '[^,]+')
    ) detail

    UNION ALL

    SELECT source.SOURCE_ROWID,
           detail.VISIT_ID
    FROM statistics_rows source
    CROSS APPLY (
        SELECT TO_NUMBER(
                   REGEXP_SUBSTR(source.IN_PAT_DETAIL, '[^,]+', 1, LEVEL)
               ) AS VISIT_ID
        FROM DUAL
        CONNECT BY LEVEL <= REGEXP_COUNT(source.IN_PAT_DETAIL, '[^,]+')
    ) detail
    WHERE INSTR(
        ',' || NVL(source.OUT_PAT_DETAIL, '') || ',',
        ',' || TO_CHAR(detail.VISIT_ID) || ','
    ) > 0
),
test_visits AS (
    SELECT DISTINCT reg.VISIT_ID
    FROM bed_occurrences occurrence
    JOIN WDHIS.CIS_IN_PAT_REG reg ON reg.VISIT_ID = occurrence.VISIT_ID
    JOIN WDHIS.PAT_REGISTER patient
      ON patient.REG_ID = reg.VISIT_ID
     AND patient.PAT_ID = reg.PAT_ID
     AND patient.SOURCE_TYPE = 2
     AND patient.BRANCH_CODE = reg.BRANCH_CODE
    LEFT JOIN WDHIS.PUB_EMP reg_emp ON reg_emp.ID = reg.REG_EMPID
    LEFT JOIN WDHIS.PUB_WARD current_ward ON current_ward.ID = reg.WARD_ID
    LEFT JOIN WDHIS.PUB_DEPT current_dept ON current_dept.ID = reg.DEPT_ID
    WHERE INSTR(
        NVL(patient.NAME, '~') || NVL(current_ward.NAME, '~') ||
        NVL(current_dept.NAME, '~') || NVL(reg_emp.NAME, '~'),
        '测试'
    ) > 0
       OR INSTR(
        NVL(patient.NAME, '~') || NVL(current_ward.NAME, '~') ||
        NVL(current_dept.NAME, '~') || NVL(reg_emp.NAME, '~'),
        '考核'
    ) > 0
       OR INSTR(
        NVL(patient.NAME, '~') || NVL(current_ward.NAME, '~') ||
        NVL(current_dept.NAME, '~') || NVL(reg_emp.NAME, '~'),
        '演练'
    ) > 0
       OR INSTR(
        UPPER(
            NVL(patient.NAME, '~') || NVL(current_ward.NAME, '~') ||
            NVL(current_dept.NAME, '~') || NVL(reg_emp.NAME, '~')
        ),
        'CESHI'
    ) > 0
       OR NVL(current_dept.CODE, '~') = '001'
       OR NVL(reg_emp.CODE, '~') = '999'
       OR NVL(reg.REG_EMPID, -1) = 999
),
test_adjustments AS (
    SELECT occurrence.SOURCE_ROWID,
           COUNT(*) AS TEST_BED_DAYS
    FROM bed_occurrences occurrence
    JOIN test_visits test_visit ON test_visit.VISIT_ID = occurrence.VISIT_ID
    GROUP BY occurrence.SOURCE_ROWID
),
bed_usage AS (
    SELECT source.BQH,
           source.BQ,
           source.KSBH,
           source.KS,
           NVL(REGEXP_COUNT(source.CURRENT_PAT_DETAIL, '[^,]+'), 0)
             + NVL(source.TODAY_IN_OUT_NUM, 0)
             - NVL(adjustment.TEST_BED_DAYS, 0) AS SJCW
    FROM statistics_rows source
    LEFT JOIN test_adjustments adjustment
      ON adjustment.SOURCE_ROWID = source.SOURCE_ROWID
)
SELECT
    NVL(SUM(SJCW), 0) AS 全院床位使用,
    NVL(SUM(CASE
        WHEN KSBH = '403' OR BQ LIKE '%儿童重症%' OR KS LIKE '%儿童重症%'
        THEN SJCW ELSE 0
    END), 0) AS PICU床位使用,
    NVL(SUM(CASE
        WHEN KSBH = '132' OR BQ LIKE '%新生儿重症%' OR KS LIKE '%新生儿重症%'
        THEN SJCW ELSE 0
    END), 0) AS NICU床位使用,
    NVL(SUM(CASE
        WHEN KSBH = '347'
          OR BQ LIKE '%小儿监护(北)%'
          OR BQ LIKE '%(北)小儿监护%'
          OR KS LIKE '%监护病区(北)%'
        THEN SJCW ELSE 0
    END), 0) AS 北监护床位使用,
    NVL(SUM(CASE
        WHEN KSBH = '513' OR BQ LIKE '%外二心胸外科%' OR KS LIKE '%外二心胸外科%'
        THEN SJCW ELSE 0
    END), 0) AS 外二心胸外科床位使用
FROM bed_usage
"""


NEW_OUTPATIENT_SQL = r"""
SELECT CASE reg.BRANCH_CODE
           WHEN '00' THEN '南院'
           WHEN '01' THEN '北院'
       END AS 院区,
       SUM(CASE WHEN reg_type.CODE <> '114' THEN 1 ELSE 0 END) AS 门急诊量,
       SUM(CASE
           WHEN NVL(reg.IS_EME, 0) = 1
             OR NVL(reg_type.IS_EME, 0) = 1
             OR reg_type.NAME LIKE '%急%'
           THEN 1 ELSE 0
       END) AS 急诊量
FROM WDHIS.OIS_REG_INFO reg
JOIN WDHIS.PAT_REGISTER patient
  ON patient.REG_ID = reg.OPC_ID
 AND patient.SOURCE_TYPE = 1
 AND patient.BRANCH_CODE = reg.BRANCH_CODE
 AND NVL(patient.IS_INVALID, 0) = 0
 AND TRIM(patient.PAT_NO) IS NOT NULL
JOIN WDHIS.PUB_DIC_REG_TYPE reg_type ON reg_type.ID = reg.REG_TYPE
JOIN WDHIS.PUB_REG_DEPT reg_dept ON reg_dept.ID = reg.REG_DEPT
LEFT JOIN WDHIS.PUB_DEPT org_dept ON org_dept.ID = reg_dept.DEPT_ID
LEFT JOIN WDHIS.PUB_EMP reg_input_emp ON reg_input_emp.ID = reg.REG_INPUT_EMPID
LEFT JOIN WDHIS.PUB_EMP reg_emp ON reg_emp.ID = reg.REG_EMPID
LEFT JOIN WDHIS.PUB_EMP doctor ON doctor.ID = reg.DOC_EMPID
WHERE reg.REG_DATE >= TO_DATE(:report_date, 'YYYY-MM-DD')
  AND reg.REG_DATE < TO_DATE(:report_date, 'YYYY-MM-DD') + 1
  AND reg.BRANCH_CODE IN ('00', '01')
  AND reg.INVALID_EMPID IS NULL
  AND reg.INVALID_TIME IS NULL
  AND NVL(reg.IS_BACK, 0) = 0
  AND reg.REG_DEPT <> 0
  AND INSTR(
      NVL(patient.NAME, '~') || NVL(reg_type.NAME, '~') ||
      NVL(reg_dept.NAME, '~') || NVL(org_dept.NAME, '~') ||
      NVL(reg_input_emp.NAME, '~') || NVL(reg_emp.NAME, '~') ||
      NVL(doctor.NAME, '~'),
      '测试'
  ) = 0
  AND INSTR(
      NVL(patient.NAME, '~') || NVL(reg_type.NAME, '~') ||
      NVL(reg_dept.NAME, '~') || NVL(org_dept.NAME, '~') ||
      NVL(reg_input_emp.NAME, '~') || NVL(reg_emp.NAME, '~') ||
      NVL(doctor.NAME, '~'),
      '考核'
  ) = 0
  AND INSTR(
      NVL(patient.NAME, '~') || NVL(reg_type.NAME, '~') ||
      NVL(reg_dept.NAME, '~') || NVL(org_dept.NAME, '~') ||
      NVL(reg_input_emp.NAME, '~') || NVL(reg_emp.NAME, '~') ||
      NVL(doctor.NAME, '~'),
      '演练'
  ) = 0
  AND INSTR(
      UPPER(
          NVL(patient.NAME, '~') || NVL(reg_type.NAME, '~') ||
          NVL(reg_dept.NAME, '~') || NVL(org_dept.NAME, '~') ||
          NVL(reg_input_emp.NAME, '~') || NVL(reg_emp.NAME, '~') ||
          NVL(doctor.NAME, '~')
      ),
      'CESHI'
  ) = 0
  AND NVL(reg_type.CODE, '~') <> '99'
  AND NVL(org_dept.CODE, '~') <> '001'
  AND NVL(reg_input_emp.CODE, '~') <> '999'
  AND NVL(reg_emp.CODE, '~') <> '999'
  AND NVL(doctor.CODE, '~') <> '999'
GROUP BY reg.BRANCH_CODE
ORDER BY CASE reg.BRANCH_CODE WHEN '01' THEN 0 ELSE 1 END
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


def fetch_visit_no_set(
    conn: oracledb.Connection,
    sql: str,
    report_date: str,
) -> set[str]:
    rows = fetch_all_dicts(conn, sql, report_date)
    return {
        str(row["住院号"]).strip()
        for row in rows
        if row.get("住院号") is not None and str(row["住院号"]).strip()
    }


def fetch_authoritative_new_his_visit_nos(
    conn: oracledb.Connection,
    visit_nos: set[str],
) -> set[str]:
    normalized = sorted({str(value).strip() for value in visit_nos if str(value).strip()})
    result: set[str] = set()
    with conn.cursor() as cursor:
        for offset in range(0, len(normalized), 800):
            chunk = normalized[offset:offset + 800]
            binds = {f"visit_no_{index}": value for index, value in enumerate(chunk)}
            placeholders = ", ".join(f":{name}" for name in binds)
            cursor.execute(
                f"""
SELECT TRIM(reg.VISIT_NO)
FROM WDHIS.CIS_IN_PAT_REG reg
JOIN WDHIS.PAT_REGISTER patient
  ON patient.REG_ID = reg.VISIT_ID
 AND patient.PAT_ID = reg.PAT_ID
 AND patient.SOURCE_TYPE = 2
 AND patient.BRANCH_CODE = reg.BRANCH_CODE
 AND NVL(patient.IS_INVALID, 0) = 0
JOIN WDHIS.PUB_WARD ward ON ward.ID = reg.WARD_ID
JOIN WDHIS.PUB_DEPT dept ON dept.ID = reg.DEPT_ID
LEFT JOIN WDHIS.PUB_EMP reg_record_emp ON reg_record_emp.ID = reg.REG_EMPID
WHERE NVL(reg.IS_INVALID, 0) = 0
  AND reg.BRANCH_CODE IN ('00', '01')
  AND TRIM(reg.VISIT_NO) IN ({placeholders})
  AND INSTR(NVL(patient.NAME, '~') || NVL(ward.NAME, '~') || NVL(dept.NAME, '~') || NVL(reg_record_emp.NAME, '~'), '测试') = 0
  AND INSTR(NVL(patient.NAME, '~') || NVL(ward.NAME, '~') || NVL(dept.NAME, '~') || NVL(reg_record_emp.NAME, '~'), '考核') = 0
  AND INSTR(NVL(patient.NAME, '~') || NVL(ward.NAME, '~') || NVL(dept.NAME, '~') || NVL(reg_record_emp.NAME, '~'), '演练') = 0
  AND INSTR(UPPER(NVL(patient.NAME, '~') || NVL(ward.NAME, '~') || NVL(dept.NAME, '~') || NVL(reg_record_emp.NAME, '~')), 'CESHI') = 0
  AND NVL(dept.CODE, '~') <> '001'
  AND NVL(reg.REG_EMPID, -1) <> 999
  AND NVL(reg_record_emp.CODE, '~') <> '999'
""",
                binds,
            )
            result.update(
                str(row[0]).strip()
                for row in cursor
                if row[0] is not None and str(row[0]).strip()
            )
    return result


def fetch_self_snapshot_bed_usage(report_date: str) -> dict[str, Any]:
    sql = """
SELECT COALESCE(SUM(snapshot.bed_usage), 0) AS "全院床位使用",
       COALESCE(SUM(CASE
           WHEN dept_code = '403'
             OR ward_name LIKE '%%儿童重症%%'
             OR dept_name LIKE '%%儿童重症%%'
           THEN snapshot.bed_usage ELSE 0
       END), 0) AS "PICU床位使用",
       COALESCE(SUM(CASE
           WHEN dept_code = '132'
             OR ward_name LIKE '%%新生儿重症%%'
             OR dept_name LIKE '%%新生儿重症%%'
           THEN snapshot.bed_usage ELSE 0
       END), 0) AS "NICU床位使用",
       COALESCE(SUM(CASE
           WHEN dept_code = '347'
             OR ward_name LIKE '%%小儿监护(北)%%'
             OR ward_name LIKE '%%(北)小儿监护%%'
             OR dept_name LIKE '%%监护病区(北)%%'
           THEN snapshot.bed_usage ELSE 0
       END), 0) AS "北监护床位使用",
       COALESCE(SUM(CASE
           WHEN dept_code = '513'
             OR ward_name LIKE '%%外二心胸外科%%'
             OR dept_name LIKE '%%外二心胸外科%%'
           THEN snapshot.bed_usage ELSE 0
       END), 0) AS "外二心胸外科床位使用"
FROM public.new_his_daily_inpatient_snapshot snapshot
JOIN public.new_his_daily_inpatient_snapshot_run run
  ON run.report_date = snapshot.report_date
WHERE snapshot.report_date = %s
"""
    with psycopg.connect(**BI_REPORTS_DB) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, (report_date,))
            columns = [description.name for description in cursor.description]
            row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f'{report_date} 自建住院结转尚未生成')
    result = dict(zip(columns, row))
    if to_int(result.get('全院床位使用')) <= 0:
        raise RuntimeError(f'{report_date} 自建住院结转为空')
    return result


def query_report(report_date: str, new_his_dsn: str = DEFAULT_NEW_HIS_DSN) -> dict[str, Any]:
    report_day = dt.datetime.strptime(report_date, "%Y-%m-%d").date()
    old_surgery: dict[str, Any] = {}
    new_surgery: dict[str, Any] = {}
    old_delivery_visits: set[str] = set()
    new_delivery_visits: set[str] = set()
    authoritative_new_visits: set[str] = set()
    bed_usage: dict[str, Any] = {}
    outpatient_rows: list[dict[str, Any]] = []

    if report_day < NEW_HIS_OUTPATIENT_START:
        log("查询老 HIS 医务科每日上报数据")
        with oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN) as conn:
            old_surgery = fetch_one_dict(conn, SURGERY_SQL, report_date)
            if report_day == NEW_HIS_DAILY_START:
                old_delivery_visits = fetch_visit_no_set(
                    conn,
                    OLD_DELIVERY_EVENTS_SQL,
                    report_date,
                )
            if report_day < NEW_HIS_DAILY_START:
                bed_usage = fetch_one_dict(conn, BED_USAGE_SQL, report_date)
            outpatient_rows = fetch_all_dicts(conn, OUTPATIENT_SQL, report_date)

    if report_day >= NEW_HIS_DAILY_START:
        log("查询新 HIS 医务科每日上报数据")
        with oracledb.connect(new_his_dsn, disable_oob=True) as conn:
            new_surgery = fetch_one_dict(conn, NEW_SURGERY_METRICS_SQL, report_date)
            new_delivery_visits = fetch_visit_no_set(
                conn,
                NEW_DELIVERY_EVENTS_SQL,
                report_date,
            )
            bed_usage = (
                fetch_self_snapshot_bed_usage(report_date)
                if report_day >= SELF_SNAPSHOT_START
                else fetch_one_dict(conn, NEW_BED_USAGE_SQL, report_date)
            )
            if report_day == NEW_HIS_DAILY_START:
                authoritative_new_visits = fetch_authoritative_new_his_visit_nos(
                    conn,
                    old_delivery_visits,
                )
            if report_day >= NEW_HIS_OUTPATIENT_START:
                outpatient_rows = fetch_all_dicts(conn, NEW_OUTPATIENT_SQL, report_date)

    if report_day < NEW_HIS_DAILY_START:
        surgery_total = to_int(old_surgery.get("手术量"))
        delivery_total = to_int(old_surgery.get("分娩量"))
        day_surgery_total = to_int(old_surgery.get("日间手术量"))
    elif report_day == NEW_HIS_DAILY_START:
        surgery_total = (
            to_int(old_surgery.get("手术量"))
            + to_int(new_surgery.get("手术量"))
        )
        delivery_total = len(
            (old_delivery_visits - authoritative_new_visits)
            | new_delivery_visits
        )
        day_surgery_total = (
            to_int(old_surgery.get("日间手术量"))
            + to_int(new_surgery.get("日间手术量"))
        )
    else:
        surgery_total = to_int(new_surgery.get("手术量"))
        delivery_total = len(new_delivery_visits)
        day_surgery_total = to_int(new_surgery.get("日间手术量"))

    outpatient_total = sum(to_int(row.get("门急诊量")) for row in outpatient_rows)
    emergency_total = sum(to_int(row.get("急诊量")) for row in outpatient_rows)

    return {
        "日期": report_date,
        "手术量": surgery_total,
        "分娩量": delivery_total,
        "日间手术量": day_surgery_total,
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

    report = query_report(report_date, args.new_his_dsn)
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
    parser.add_argument(
        "--new-his-dsn",
        default=os.environ.get("NEW_HIS_DSN") or DEFAULT_NEW_HIS_DSN,
        help="新 HIS 只读 Oracle DSN",
    )
    parser.add_argument("--userid", dest="userids", action="append", default=[], help="钉钉接收人 userid；可重复，或逗号分隔。默认写死为脚本内 DEFAULT_RECEIVER_USERIDS")
    parser.add_argument("--dry-run", action="store_true", help="只查询并打印，不发送钉钉")
    parser.add_argument("--json", action="store_true", help="结束时输出 JSON 结果")
    parser.add_argument("--schedule", action="store_true", help="常驻进程，每天定时执行")
    parser.add_argument("--hour", type=int, default=8, help="定时小时，默认 8")
    parser.add_argument("--minute", type=int, default=0, help="定时分钟，默认 0")
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
