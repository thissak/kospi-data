#!/usr/bin/env python3
"""코스피 전종목 일별 데이터 수집기 (맥미니 Dagu 작업 kospi-collector).

출처 우선순위
  1) 한국투자증권 Open API (KIS, 읽기 전용 키)
  2) pykrx  - .env에 KRX_ID/KRX_PW가 있을 때만 (2025년 말부터 KRX 로그인 필수)
  3) FinanceDataReader - 일봉만 (거래대금·시가총액·투자자 없음)

코스피 수집이 끝나면 watch.json의 관심 종목(코스닥)과 코스닥 시장 수급을 이어서 받는다.

사용법
  python collector.py --mode initial          # 2025-01-01부터 오늘까지
  python collector.py --mode daily            # 마지막 수집일부터 오늘까지 (빠진 날 자동 보충)
  python collector.py --mode daily --push     # 수집 후 git add/commit/push
  python collector.py --mode detail --codes 352820,041510 --from 2026-09-01 --push
                                              # 기관 세부 순매수만 과거까지 다시 받기
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
import zipfile
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
DAILY = DATA / "daily"
DB_PATH = DATA / "kospi.sqlite"
META_PATH = DATA / "meta.json"
MARKET_CSV = DATA / "market.csv"
LOG_PATH = ROOT / "logs" / "collector.log"
TOKEN_PATH = ROOT / "run" / "kis_token.json"
WATCH_CONFIG = ROOT / "watch.json"
WATCH_DIR = DATA / "watch"
WATCH_DB = DATA / "watch.sqlite"
MARKET_KOSDAQ_CSV = DATA / "market_kosdaq.csv"
DAILY_DETAIL = DATA / "daily_detail"
WATCH_DETAIL = DATA / "watch_detail"

INITIAL_START = date(2025, 1, 1)
CUTOFF = (15, 40)    # 이 시각 전에는 오늘을 빼고 어제까지만 받는다 (장중 미완성 + KIS 투자자 API 시간 제한)
REQUEST_GAP = 0.1    # 요청 사이 쉬는 시간(초). KIS 한도는 초당 20건
STOCK_GROUPS = {"ST", "RT", "IF", "MF", "DR", "FS"}  # 주권·리츠·인프라·뮤추얼·DR·외국주권 (ETF/ETN/펀드 제외)
KIS_BASE = "https://openapi.koreainvestment.com:9443"
MASTER_URL = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"

DAILY_COLUMNS = [
    "date", "code", "name", "open", "high", "low", "close", "volume", "value",
    "marcap", "shares", "marcap_approx",
    "foreign_net_value", "inst_net_value", "indiv_net_value",
    "foreign_net_qty", "inst_net_qty", "indiv_net_qty", "source",
]
MARKET_COLUMNS = [
    "date", "open", "high", "low", "close", "volume", "value",
    "foreign_net_value", "inst_net_value", "indiv_net_value",
    "foreign_net_qty", "inst_net_qty", "indiv_net_qty",
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS prices(date TEXT, code TEXT, name TEXT, open INTEGER, high INTEGER, low INTEGER,
    close INTEGER, volume INTEGER, value INTEGER, source TEXT, PRIMARY KEY(date, code));
CREATE TABLE IF NOT EXISTS marketcap(date TEXT, code TEXT, marcap INTEGER, shares INTEGER, approx INTEGER,
    PRIMARY KEY(date, code));
CREATE TABLE IF NOT EXISTS investor(date TEXT, code TEXT, foreign_net_qty INTEGER, inst_net_qty INTEGER,
    indiv_net_qty INTEGER, foreign_net_value INTEGER, inst_net_value INTEGER, indiv_net_value INTEGER,
    PRIMARY KEY(date, code));
CREATE TABLE IF NOT EXISTS investor_detail(date TEXT, code TEXT, key TEXT, value INTEGER,
    PRIMARY KEY(date, code, key));
CREATE TABLE IF NOT EXISTS "index"(date TEXT PRIMARY KEY, open REAL, high REAL, low REAL, close REAL,
    volume INTEGER, value INTEGER);
"""
# 확정값(approx=0)은 추정값(approx=1)으로 덮지 않는다.
MARKETCAP_UPSERT = """
INSERT INTO marketcap VALUES(?,?,?,?,?) ON CONFLICT(date, code) DO UPDATE SET
    marcap=excluded.marcap, shares=excluded.shares, approx=excluded.approx
WHERE marketcap.approx=1 OR excluded.approx=0
"""

log = logging.getLogger("collector")


# ---------- 공통 ----------

def ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def pdate(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def iso(s: str) -> str:
    return pdate(s).isoformat()


def num(s) -> int | None:
    return None if s in (None, "") else int(float(s))


def make_session() -> requests.Session:
    """연결 오류·429·5xx는 urllib3 Retry가 지수 백오프(1,2,4,8,16초)로 재시도한다.
    KIS의 초당 한도 초과(EGW00201)도 HTTP 500으로 오므로 여기서 함께 처리된다."""
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=None, raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def collect_back(fetch, start: date, end: date) -> list[dict]:
    """최신순으로 몇십 건씩 주는 KIS 과거 조회를 start까지 거슬러 올라가며 모은다."""
    rows, cur = {}, end
    while cur >= start:
        page = [r for r in fetch(cur) if r.get("stck_bsop_date")]
        if not page:
            break
        for r in page:
            rows.setdefault(r["stck_bsop_date"], r)
        oldest = min(pdate(r["stck_bsop_date"]) for r in page)
        if oldest > cur:  # 기준일을 무시하는 응답이면 무한 반복을 막는다
            break
        cur = oldest - timedelta(days=1)
    return [r for k, r in sorted(rows.items()) if start <= pdate(k) <= end]


# ---------- KIS ----------

class KIS:
    def __init__(self, session: requests.Session):
        self.s = session
        self.key = os.environ["KIS_APP_KEY"]
        self.secret = os.environ["KIS_APP_SECRET"]
        self.token = self._token()

    def _token(self) -> str:
        if TOKEN_PATH.exists():
            c = json.loads(TOKEN_PATH.read_text())
            if datetime.fromisoformat(c["expires_at"]) > datetime.now() + timedelta(minutes=30):
                return c["access_token"]
        r = self.s.post(f"{KIS_BASE}/oauth2/tokenP", timeout=15, json={
            "grant_type": "client_credentials", "appkey": self.key, "appsecret": self.secret}).json()
        if "access_token" not in r:
            raise RuntimeError(f"KIS 토큰 발급 실패: {r.get('error_description') or r}")
        expires = datetime.strptime(r["access_token_token_expired"], "%Y-%m-%d %H:%M:%S")
        TOKEN_PATH.parent.mkdir(exist_ok=True)
        TOKEN_PATH.write_text(json.dumps({"access_token": r["access_token"], "expires_at": expires.isoformat()}))
        TOKEN_PATH.chmod(0o600)
        return r["access_token"]

    def get(self, path: str, tr_id: str, params: dict) -> dict:
        time.sleep(REQUEST_GAP)
        headers = {"authorization": f"Bearer {self.token}", "appkey": self.key, "appsecret": self.secret,
                   "tr_id": tr_id, "custtype": "P"}
        d = self.s.get(f"{KIS_BASE}/uapi/domestic-stock/v1/quotations/{path}",
                       headers=headers, params=params, timeout=15).json()
        if d.get("rt_cd") != "0":
            raise RuntimeError(f"KIS {tr_id}: {d.get('msg_cd')} {d.get('msg1')}")
        return d


def investor_row(r: dict, code: str, qty_unit: int = 1) -> tuple:
    """순매수 금액은 백만원 → 원. 시장 전체 수량은 천주 단위라 qty_unit=1000."""
    q = lambda k: num(r[k]) * qty_unit
    v = lambda k: num(r[k]) * 1_000_000
    return (iso(r["stck_bsop_date"]), code, q("frgn_ntby_qty"), q("orgn_ntby_qty"), q("prsn_ntby_qty"),
            v("frgn_ntby_tr_pbmn"), v("orgn_ntby_tr_pbmn"), v("prsn_ntby_tr_pbmn"))


DETAIL_SUFFIX = "_ntby_tr_pbmn"
BASIC_INVESTORS = {"frgn", "orgn", "prsn"}  # 외국인·기관 합계·개인은 investor 테이블에 이미 있다


def detail_rows(r: dict, code: str) -> list[tuple]:
    """기관 세부(금융투자·투신·사모·은행·보험·연기금 등) 순매수 금액. 응답의 '<투자자>_ntby_tr_pbmn'
    항목을 KIS 이름 그대로 key로 둔다(백만원 → 원). 이름을 미리 정해 두지 않아 KIS가 항목을 바꿔도 깨지지 않는다."""
    out = []
    for k, v in r.items():
        who = k[:-len(DETAIL_SUFFIX)]
        if not k.endswith(DETAIL_SUFFIX) or who in BASIC_INVESTORS:
            continue
        try:
            n = num(v)
        except (TypeError, ValueError):
            continue
        if n is not None:
            out.append((iso(r["stck_bsop_date"]), code, who, n * 1_000_000))
    return out


def kis_index(kis: KIS, start: date, end: date, iscd: str = "0001") -> list[tuple]:
    """iscd: 0001 코스피, 1001 코스닥."""
    rows = collect_back(lambda cur: kis.get("inquire-daily-indexchartprice", "FHKUP03500100", {
        "FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": iscd, "FID_INPUT_DATE_1": ymd(start),
        "FID_INPUT_DATE_2": ymd(cur), "FID_PERIOD_DIV_CODE": "D"})["output2"], start, end)
    # 지수 거래량은 천주, 거래대금은 백만원 단위로 온다.
    return [(iso(r["stck_bsop_date"]), float(r["bstp_nmix_oprc"]), float(r["bstp_nmix_hgpr"]),
             float(r["bstp_nmix_lwpr"]), float(r["bstp_nmix_prpr"]),
             num(r["acml_vol"]) * 1000, num(r["acml_tr_pbmn"]) * 1_000_000) for r in rows]


MARKETS = {"KOSPI": ("0001", "KSP"), "KOSDAQ": ("1001", "KSQ")}  # 지수 업종코드, 시장 구분


def kis_market_investor(kis: KIS, start: date, end: date, market: str = "KOSPI") -> list[tuple]:
    iscd, mkt = MARKETS[market]
    rows = collect_back(lambda cur: kis.get("inquire-investor-daily-by-market", "FHPTJ04040000", {
        "FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": iscd, "FID_INPUT_DATE_1": ymd(cur),
        "FID_INPUT_ISCD_1": mkt, "FID_INPUT_DATE_2": ymd(cur), "FID_INPUT_ISCD_2": iscd})["output"],
        start, end)
    return [investor_row(r, market, qty_unit=1000) for r in rows]


def kis_stock_investor(kis: KIS, code: str, start: date, end: date) -> tuple[list[tuple], list[tuple]]:
    """(investor 행, investor_detail 행)을 돌려준다. 같은 응답에서 둘 다 뽑아 요청 수는 그대로다."""
    rows = collect_back(lambda cur: kis.get("investor-trade-by-stock-daily", "FHPTJ04160001", {
        "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": ymd(cur),
        "FID_ORG_ADJ_PRC": "", "FID_ETC_CLS_CODE": ""})["output2"], start, end)
    return [investor_row(r, code) for r in rows], [d for r in rows for d in detail_rows(r, code)]


def kis_prices(kis: KIS, code: str, start: date, end: date):
    """원주가(수정주가 아님) 일봉. 상장주식수는 조회 시점 값이다."""
    shares = None

    def fetch(cur):
        nonlocal shares
        d = kis.get("inquire-daily-itemchartprice", "FHKST03010100", {
            "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": ymd(start),
            "FID_INPUT_DATE_2": ymd(cur), "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "1"})
        shares = shares or num(d["output1"].get("lstn_stcn"))
        return d["output2"]

    rows = collect_back(fetch, start, end)
    return [(iso(r["stck_bsop_date"]), num(r["stck_oprc"]), num(r["stck_hgpr"]), num(r["stck_lwpr"]),
             num(r["stck_clpr"]), num(r["acml_vol"]), num(r["acml_tr_pbmn"])) for r in rows], shares


# ---------- 대체 출처 ----------

def pykrx_prices(code: str, start: date, end: date):
    from pykrx import stock  # KRX_ID/KRX_PW 환경 변수로 pykrx가 직접 로그인한다
    df = stock.get_market_ohlcv_by_date(ymd(start), ymd(end), code)
    if df.empty:
        raise RuntimeError("pykrx 빈 응답")
    return [(d.date().isoformat(), int(r["시가"]), int(r["고가"]), int(r["저가"]), int(r["종가"]),
             int(r["거래량"]), num(r.get("거래대금"))) for d, r in df.iterrows()], None


def fdr_prices(code: str, start: date, end: date):
    import FinanceDataReader as fdr
    df = fdr.DataReader(code, start.isoformat(), end.isoformat())
    if df.empty:
        raise RuntimeError("FDR 빈 응답")
    return [(d.date().isoformat(), int(r.Open), int(r.High), int(r.Low), int(r.Close), int(r.Volume), None)
            for d, r in df.iterrows()], None


def fetch_prices(kis: KIS | None, code: str, start: date, end: date):
    """KIS → pykrx(KRX 계정 있을 때) → FDR 순서로 시도한다. (출처, 행, 상장주식수)를 돌려준다."""
    chain = []
    if kis:
        chain.append(("KIS", lambda: kis_prices(kis, code, start, end)))
    if os.getenv("KRX_ID") and os.getenv("KRX_PW"):
        chain.append(("pykrx", lambda: pykrx_prices(code, start, end)))
    chain.append(("FDR", lambda: fdr_prices(code, start, end)))
    errors = []
    for name, fn in chain:
        try:
            rows, shares = fn()
            if errors:
                log.warning("%s 대체 출처 %s 사용 (%s)", code, name, " / ".join(errors))
            return name, rows, shares
        except Exception as e:
            errors.append(f"{name}: {e}")
    raise RuntimeError(" / ".join(errors))


def fdr_trading_days(start: date, end: date) -> list[str]:
    """KIS 지수 조회가 실패했을 때만 쓴다. 삼성전자 거래일 = 코스피 거래일."""
    import FinanceDataReader as fdr
    df = fdr.DataReader("005930", start.isoformat(), end.isoformat())
    return [d.date().isoformat() for d in df.index]


def load_tickers(session: requests.Session) -> list[tuple[str, str]]:
    """KIS 종목 마스터(공개 파일)에서 코스피 주식만 고른다. 끝 227자가 고정 폭 필드다."""
    raw = zipfile.ZipFile(io.BytesIO(session.get(MASTER_URL, timeout=30).content)).read("kospi_code.mst")
    out = []
    for line in raw.decode("cp949").splitlines():
        if line.strip() and line[-227:][:2] in STOCK_GROUPS:
            out.append((line[:9].strip(), line[21:-227].strip()))
    if len(out) < 500:
        raise RuntimeError(f"종목 마스터 이상: {len(out)}종목")
    return out


# ---------- 저장 ----------

def open_db(path: Path = DB_PATH) -> sqlite3.Connection:
    DATA.mkdir(exist_ok=True)
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    return con


def write_csv(path: Path, header: list[str], rows) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return len(rows)


def export_day(con: sqlite3.Connection, day: str, folder: Path = DAILY) -> int:
    rows = con.execute("""
        SELECT p.date, p.code, p.name, p.open, p.high, p.low, p.close, p.volume, p.value,
               m.marcap, m.shares, m.approx,
               i.foreign_net_value, i.inst_net_value, i.indiv_net_value,
               i.foreign_net_qty, i.inst_net_qty, i.indiv_net_qty, p.source
        FROM prices p
        LEFT JOIN marketcap m ON m.date = p.date AND m.code = p.code
        LEFT JOIN investor i ON i.date = p.date AND i.code = p.code
        WHERE p.date = ? ORDER BY p.code""", (day,))
    return write_csv(folder / f"{day}.csv", DAILY_COLUMNS, rows)


def export_detail(con: sqlite3.Connection, day: str, folder: Path = DAILY_DETAIL) -> int:
    """그날의 기관 세부 순매수(원)를 종목당 한 줄로 낸다. 열은 그날 응답에 있던 투자자 이름이다."""
    keys = [k for (k,) in con.execute("SELECT DISTINCT key FROM investor_detail WHERE date = ? ORDER BY key", (day,))]
    if not keys:
        return 0
    by_code: dict[str, dict] = {}
    for code, name, key, value in con.execute("""
            SELECT d.code, p.name, d.key, d.value FROM investor_detail d
            LEFT JOIN prices p ON p.date = d.date AND p.code = d.code
            WHERE d.date = ? ORDER BY d.code""", (day,)):
        by_code.setdefault(code, {"name": name})[key] = value
    return write_csv(folder / f"{day}.csv", ["date", "code", "name", *[f"{k}_net_value" for k in keys]],
                     [[day, code, v["name"], *[v.get(k) for k in keys]] for code, v in by_code.items()])


def export_market(con: sqlite3.Connection, path: Path = MARKET_CSV, market: str = "KOSPI") -> None:
    rows = con.execute("""
        SELECT x.date, x.open, x.high, x.low, x.close, x.volume, x.value,
               i.foreign_net_value, i.inst_net_value, i.indiv_net_value,
               i.foreign_net_qty, i.inst_net_qty, i.indiv_net_qty
        FROM "index" x LEFT JOIN investor i ON i.date = x.date AND i.code = ?
        ORDER BY x.date""", (market,))
    write_csv(path, MARKET_COLUMNS, rows)


def load_meta() -> dict:
    return json.loads(META_PATH.read_text(encoding="utf-8")) if META_PATH.exists() else {}


def save_meta(meta: dict) -> None:
    DATA.mkdir(exist_ok=True)
    META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------- 수집 ----------

def save_stock(con, kis: KIS | None, code: str, name: str, start: date, end: date,
               dayset: set[str], last_day: str, fail, stage: str = "") -> str | None:
    """종목 하나의 일봉·시가총액·투자자를 DB에 넣는다. 일봉 출처를 돌려준다(일봉 실패면 None)."""
    src = None
    try:
        src, rows, shares = fetch_prices(kis, code, start, end)
        rows = [r for r in rows if r[0] in dayset]
        con.executemany("INSERT OR REPLACE INTO prices VALUES(?,?,?,?,?,?,?,?,?,?)",
                        [(d, code, name, o, h, l, c, v, val, src) for d, o, h, l, c, v, val in rows])
        if shares:
            # 상장주식수는 조회 시점 값이라, 마지막 거래일만 확정(approx=0)이고 과거는 추정(approx=1)이다.
            con.executemany(MARKETCAP_UPSERT, [(d, code, c * shares, shares, int(d != last_day))
                                               for d, _, _, _, c, _, _ in rows])
    except Exception as e:
        src = None
        fail(f"{stage}prices", e, code)
    if kis:
        try:
            inv, detail = kis_stock_investor(kis, code, start, end)
            con.executemany("INSERT OR REPLACE INTO investor VALUES(?,?,?,?,?,?,?,?)",
                            [r for r in inv if r[0] in dayset])
            con.executemany("INSERT OR REPLACE INTO investor_detail VALUES(?,?,?,?)",
                            [r for r in detail if r[0] in dayset])
        except Exception as e:
            fail(f"{stage}investor", e, code)
    return src


def collect_watch(kis: KIS, end: date, prev: str | None, fail) -> str | None:
    """watch.json의 관심 종목(코스닥)과 코스닥 지수·시장 수급을 받아 data/watch/와 market_kosdaq.csv로 낸다.
    prev(마지막 수집일)도 다시 받는다. 새 watch_last_date를 돌려준다. 하나라도 실패하면 prev를 그대로 돌려줘
    다음 실행이 같은 구간을 다시 받는다."""
    failed: list[str] = []

    def wfail(stage, e, code=None):
        failed.append(stage)
        fail(stage, e, code)

    cfg = json.loads(WATCH_CONFIG.read_text(encoding="utf-8"))
    start = date.fromisoformat(prev or cfg["start"])
    con = open_db(WATCH_DB)
    try:
        idx = kis_index(kis, start, end, MARKETS["KOSDAQ"][0])
        con.executemany('INSERT OR REPLACE INTO "index" VALUES(?,?,?,?,?,?,?)', idx)
        days = [r[0] for r in idx]
        if not days:
            return prev
        dayset, last_day = set(days), days[-1]
        start = date.fromisoformat(days[0])
        try:
            con.executemany("INSERT OR REPLACE INTO investor VALUES(?,?,?,?,?,?,?,?)",
                            kis_market_investor(kis, start, end, "KOSDAQ"))
        except Exception as e:
            wfail("watch_market_investor", e)
        for code, name in cfg["stocks"].items():
            save_stock(con, kis, code, name, start, end, dayset, last_day, wfail, stage="watch_")
        con.commit()
        for d in days:
            export_day(con, d, WATCH_DIR)
            export_detail(con, d, WATCH_DETAIL)
        export_market(con, MARKET_KOSDAQ_CSV, "KOSDAQ")
        log.info("관심 종목 %d개 수집: %s ~ %s, 오류 %d", len(cfg["stocks"]), days[0], last_day, len(failed))
        return prev if failed else last_day
    except Exception as e:
        wfail("watch", e)
        return prev
    finally:
        con.close()


def collect(mode: str) -> dict:
    meta = load_meta()
    now = datetime.now()
    end = now.date() if (now.hour, now.minute) >= CUTOFF else now.date() - timedelta(days=1)
    if mode == "initial" or not meta.get("last_date"):
        start = INITIAL_START
    else:
        # 마지막 수집일도 다시 받는다. 잠정치였다면 확정치로 덮어쓴다.
        start = date.fromisoformat(meta["last_date"])
    log.info("수집 시작: mode=%s, %s ~ %s", mode, start, end)

    errors: list[dict] = []

    def fail(stage: str, e: Exception, code: str | None = None):
        errors.append({"stage": stage, "code": code, "error": str(e)[:300]})
        log.error("%s %s 실패: %s", stage, code or "", e)

    session = make_session()
    con = open_db()
    kis = None
    try:
        kis = KIS(session)
    except Exception as e:
        fail("kis_token", e)

    # 1) 코스피 지수 = 거래일 달력 (지수에 데이터가 없는 날은 휴장일)
    days: list[str] | None = None
    if kis:
        try:
            idx = kis_index(kis, start, end)
            con.executemany('INSERT OR REPLACE INTO "index" VALUES(?,?,?,?,?,?,?)', idx)
            days = [r[0] for r in idx]
        except Exception as e:
            fail("index", e)
    if days is None:  # KIS 지수 조회 실패 시에만 FDR로 거래일을 구한다
        days = fdr_trading_days(start, end)
    if not days:
        log.info("수집할 거래일이 없습니다 (휴장).")
        meta.update(updated_at=now.isoformat(timespec="seconds"), errors=errors)
        save_meta(meta)
        return meta
    dayset, last_day = set(days), days[-1]
    start = date.fromisoformat(days[0])

    # 2) 시장 전체 투자자별 순매수
    if kis:
        try:
            con.executemany("INSERT OR REPLACE INTO investor VALUES(?,?,?,?,?,?,?,?)",
                            kis_market_investor(kis, start, end))
        except Exception as e:
            fail("market_investor", e)
    con.commit()

    # 3) 종목별 일봉·시가총액·투자자
    try:
        tickers = load_tickers(session)
    except Exception as e:
        fail("tickers", e)
        tickers = con.execute("SELECT code, name FROM prices WHERE date = (SELECT MAX(date) FROM prices)").fetchall()
        if not tickers:
            raise
    log.info("거래일 %d일 (%s ~ %s), 종목 %d개", len(days), days[0], last_day, len(tickers))

    sources: Counter = Counter()
    for n, (code, name) in enumerate(tickers, 1):
        src = save_stock(con, kis, code, name, start, end, dayset, last_day, fail)
        if src:
            sources[src] += 1
        if n % 50 == 0:
            con.commit()
            log.info("진행 %d/%d", n, len(tickers))
    con.commit()

    # 4) CSV 내보내기
    for d in days:
        export_day(con, d)
        export_detail(con, d)
    export_market(con)

    # 5) 관심 종목(코스닥) — 코스피가 끝난 뒤 이어서 받는다
    watch_last = collect_watch(kis, end, meta.get("watch_last_date"), fail) if kis else meta.get("watch_last_date")

    # 6) meta.json — 일봉 실패가 5%를 넘으면 마지막 수집일을 올리지 않아 다음 실행이 다시 받는다.
    price_fail = sum(1 for e in errors if e["stage"] == "prices")
    ok = price_fail <= len(tickers) * 0.05
    q = lambda sql, *a: con.execute(sql, a).fetchone()[0]
    meta.update(
        last_date=last_day if ok else meta.get("last_date"),
        watch_last_date=watch_last,
        first_date=q("SELECT MIN(date) FROM prices"),
        ticker_count=q("SELECT COUNT(*) FROM prices WHERE date = ?", last_day),
        investor_count=q("SELECT COUNT(*) FROM investor WHERE date = ? AND code != 'KOSPI'", last_day),
        universe_count=len(tickers),
        trading_days=q("SELECT COUNT(DISTINCT date) FROM prices"),
        last_run={"mode": mode, "range": [days[0], last_day], "sources": dict(sources),
                  "price_failures": price_fail, "ok": ok},
        updated_at=now.isoformat(timespec="seconds"),
        errors=errors[:300],
        notes=[
            "가격은 원주가(수정주가 아님). 금액 단위는 원.",
            "marcap_approx=1: 조회 시점 상장주식수 x 그날 종가로 계산한 추정값.",
            "종목 목록은 수집 시점 상장 종목 기준이라 기간 중 상장폐지 종목은 빠진다.",
        ],
    )
    save_meta(meta)
    con.close()
    log.info("수집 완료: 마지막 거래일 %s, 종목 %d, 오류 %d", last_day, meta["ticker_count"], len(errors))
    return meta


def backfill_detail(codes: list[str], since: date) -> int:
    """이미 수집한 날짜의 기관 세부 순매수만 다시 받는다. 일봉·meta.json은 건드리지 않는다.
    watch.json 종목은 watch.sqlite·data/watch_detail, 나머지는 kospi.sqlite·data/daily_detail로 간다."""
    now = datetime.now()
    end = now.date() if (now.hour, now.minute) >= CUTOFF else now.date() - timedelta(days=1)
    kis = KIS(make_session())
    watch = set(json.loads(WATCH_CONFIG.read_text(encoding="utf-8"))["stocks"]) if WATCH_CONFIG.exists() else set()
    failed = 0
    for db, folder, group in ((WATCH_DB, WATCH_DETAIL, [c for c in codes if c in watch]),
                              (DB_PATH, DAILY_DETAIL, [c for c in codes if c not in watch])):
        if not group:
            continue
        con = open_db(db)
        known = {d for (d,) in con.execute("SELECT DISTINCT date FROM prices WHERE date >= ?", (since.isoformat(),))}
        for code in group:
            try:
                _, detail = kis_stock_investor(kis, code, since, end)
                con.executemany("INSERT OR REPLACE INTO investor_detail VALUES(?,?,?,?)",
                                [r for r in detail if r[0] in known])
                log.info("기관 세부 %s: %d일", code, len({r[0] for r in detail if r[0] in known}))
            except Exception as e:
                failed += 1
                log.error("기관 세부 %s 실패: %s", code, e)
        con.commit()
        for d in sorted(known):
            export_detail(con, d, folder)
        con.close()
    return failed


def git_push(meta: dict) -> bool:
    def git(*args):
        return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True)

    paths = ["data/daily", "data/market.csv", "data/meta.json", "data/watch", "data/market_kosdaq.csv",
             "data/daily_detail", "data/watch_detail"]
    git("add", *[p for p in paths if (ROOT / p).exists()])  # 없는 경로가 하나라도 있으면 git add 전체가 실패한다
    if git("diff", "--cached", "--quiet").returncode == 0:
        log.info("커밋할 변경이 없습니다.")
        return True
    for args in (("commit", "-m", f"data: {meta.get('last_date') or date.today().isoformat()}"),
                 ("push", "origin", "HEAD")):
        r = git(*args)
        if r.returncode != 0:
            log.error("git %s 실패: %s", args[0], (r.stderr or r.stdout).strip())
            return False
    log.info("GitHub push 완료")
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description="코스피 전종목 일별 데이터 수집기")
    ap.add_argument("--mode", choices=["initial", "daily", "detail"], required=True)
    ap.add_argument("--codes", default="", help="detail 모드: 쉼표로 나눈 종목코드")
    ap.add_argument("--from", dest="since", default="", help="detail 모드: 시작일 YYYY-MM-DD")
    ap.add_argument("--push", action="store_true", help="수집 후 git add/commit/push")
    args = ap.parse_args()

    LOG_PATH.parent.mkdir(exist_ok=True)
    handlers: list[logging.Handler] = [logging.FileHandler(LOG_PATH, encoding="utf-8")]
    if sys.stdout.isatty():
        handlers.append(logging.StreamHandler(sys.stdout))
    logging.basicConfig(level=logging.INFO, handlers=handlers,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    load_dotenv(ROOT / ".env")

    status = 0
    if args.mode == "detail":
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
        if not codes or not args.since:
            ap.error("detail 모드는 --codes와 --from이 필요합니다")
        meta = load_meta()
        try:
            status = 1 if backfill_detail(codes, date.fromisoformat(args.since)) else 0
        except Exception:
            log.exception("기관 세부 수집 실패")
            status = 1
        if args.push and not git_push(meta):
            status = 1
        return status
    try:
        meta = collect(args.mode)
    except Exception as e:
        log.exception("수집 실패")
        meta = load_meta()
        meta.update(updated_at=datetime.now().isoformat(timespec="seconds"),
                    errors=[{"stage": "run", "code": None, "error": str(e)[:300]}])
        save_meta(meta)
        status = 1
    if args.push and not git_push(meta):
        status = 1
    return status


if __name__ == "__main__":
    sys.exit(main())
