"""
JY Wealth Cockpit — FastAPI Backend
====================================
기존 Streamlit app.py의 스크래핑 로직을 API 서버로 전환.
실행: uvicorn main:app --reload --port 8000
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import requests, re, time, threading
from datetime import datetime, timedelta, timezone
import pandas as pd

# 🔒 구글 시트 ID 고정
SHEET_ID = "1YubXMkoyA3Fa63Juf98rmicN6XEbUE9QGw2sQHF30Vo"

# ── 앱 초기화 ──────────────────────────────────────────
app = FastAPI(title="JY Wealth Cockpit API", version="2.0")

# CORS: 브라우저에서 직접 fetch 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# static 폴더(HTML/CSS/JS) 서빙
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── 캐시 (10분) ────────────────────────────────────────
_cache: dict = {}
CACHE_TTL = 600  # seconds

def get_cache(key):
    entry = _cache.get(key)
    if entry and time.time() - entry["ts"] < CACHE_TTL:
        return entry["data"]
    return None

def set_cache(key, data):
    _cache[key] = {"data": data, "ts": time.time()}


# ══════════════════════════════════════════════════════
#  CORE: 주가 스크래핑 (기존 app.py 로직 그대로)
# ══════════════════════════════════════════════════════
def fetch_stock_info(ticker: str) -> dict:
    """네이버 → 다음 순서로 주가 조회. 기존 app.py get_stock_info() 동일 로직."""
    cached = get_cache(f"stock_{ticker}")
    if cached:
        return cached

    clean = str(ticker).strip().upper().zfill(6)
    price, change_amt, change_rate, high52w = 0, 0, 0.0, 0

    # 1차: 네이버 모바일 API
    try:
        url = f"https://m.stock.naver.com/api/stock/{clean}/basic"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        data = res.json()
        if data.get("closePrice"):
            price       = int(data["closePrice"].replace(",", ""))
            change_amt  = int(data.get("compareToPreviousClosePrice", "0").replace(",", ""))
            change_rate = float(data.get("fluctuationsRatio", "0"))
    except Exception:
        pass

    # 2차: 다음 금융 API (네이버 실패 시)
    if price == 0:
        try:
            url = f"https://finance.daum.net/api/quotes/A{clean}?summary=false"
            res = requests.get(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Referer":    "https://finance.daum.net/"
            }, timeout=5)
            data = res.json()
            if data.get("tradePrice"):
                price       = int(data["tradePrice"])
                change_amt  = int(data.get("changePrice", 0))
                change_rate = float(data.get("changeRate", 0)) * 100
                if data.get("change") == "FALL":
                    change_amt  = -change_amt
                    change_rate = -change_rate
        except Exception:
            pass

    result = {
        "ticker":      clean,
        "price":       price,
        "change_amt":  change_amt,
        "change_rate": round(change_rate, 2),
        "high52w":     0,  # 별도 API로 조회
        "updated_at":  datetime.now(timezone(timedelta(hours=9))).strftime("%H:%M:%S"),
    }
    set_cache(f"stock_{ticker}", result)
    return result


def fetch_high52w(ticker: str) -> int:
    """52주 최고가 별도 조회 (느림 — 백그라운드용)"""
    cached = get_cache(f"high52_{ticker}")
    if cached:
        return cached["value"]

    clean = str(ticker).strip().upper().zfill(6)
    high52w = 0
    try:
        url = f"https://finance.naver.com/item/main.naver?code={clean}"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        match = re.search(r"52주최고.*?<em>([\d,]+)</em>", res.text, re.DOTALL)
        if match:
            high52w = int(match.group(1).replace(",", ""))
    except Exception:
        pass

    set_cache(f"high52_{ticker}", {"value": high52w})
    return high52w


# ══════════════════════════════════════════════════════
#  CORE: Google Sheets 로드 (기존 app.py 로직 그대로)
# ══════════════════════════════════════════════════════
def load_sheet(sheet_id: str) -> dict:
    """구글 시트에서 계좌별 포트폴리오 데이터 로드."""
    cached = get_cache(f"sheet_{sheet_id}")
    if cached:
        return cached

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    try:
        all_sheets = pd.read_excel(url, sheet_name=None, engine="openpyxl")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"구글 시트 로드 실패: {e}")

    accounts = {}
    for sheet_name, df in all_sheets.items():
        df.columns = df.columns.astype(str).str.strip()
        if df.empty or "종목코드" not in df.columns:
            continue

        account_val  = df.iloc[0, 1]
        account_name = str(account_val) if pd.notna(account_val) else sheet_name
        df = df.dropna(subset=["종목코드"]).copy()

        df["보유수량"] = (
            df["보유수량"].astype(str).str.replace(",", "").replace("nan", "0").astype(float)
        )
        df["목표비중"] = (
            df["목표비중"].astype(str).str.replace("%", "").replace("nan", "0").astype(float)
        )
        df["목표비중"] = df["목표비중"].apply(lambda x: x / 100 if x > 1 else x)

        holdings = []
        for _, row in df.iterrows():
            holdings.append({
                "name":       str(row.get("종목명", "")),
                "ticker":     str(row["종목코드"]).strip().zfill(6),
                "qty":        float(row["보유수량"]),
                "target_pct": float(row["목표비중"]) * 100,  # % 단위로 저장
                "avg_price":  float(row.get("매입가", 0)) if "매입가" in df.columns else 0,
            })
        accounts[account_name] = holdings

    result = {"accounts": accounts, "sheet_id": sheet_id}
    set_cache(f"sheet_{sheet_id}", result)
    return result


# ══════════════════════════════════════════════════════
#  API ROUTES
# ══════════════════════════════════════════════════════

@app.get("/")
def root():
    """메인 HTML 서빙"""
    return FileResponse("static/index.html")


@app.get("/api/stock/{ticker}")
def api_stock(ticker: str):
    """
    단일 종목 실시간 주가 조회
    GET /api/stock/005930
    """
    return fetch_stock_info(ticker)


@app.get("/api/stocks")
def api_stocks(tickers: str):
    """
    복수 종목 일괄 조회 (쉼표 구분)
    GET /api/stocks?tickers=005930,015760,034020
    """
    ticker_list = [t.strip() for t in tickers.split(",") if t.strip()]
    results = {}

    # 병렬 조회를 위한 스레드 사용
    threads = []
    lock = threading.Lock()

    def fetch_and_store(t):
        data = fetch_stock_info(t)
        with lock:
            results[t.zfill(6)] = data

    for t in ticker_list:
        th = threading.Thread(target=fetch_and_store, args=(t,))
        threads.append(th)
        th.start()

    for th in threads:
        th.join(timeout=10)

    return results


class SheetRequest(BaseModel):
    sheet_id: str

@app.get("/api/portfolio")
def api_portfolio():
    """포트폴리오 로드 + 실시간 주가 결합 (시트ID 고정)"""
    sheet_data = load_sheet(SHEET_ID)
    accounts   = sheet_data["accounts"]

    result = {}
    for acct_name, holdings in accounts.items():
        tickers    = [h["ticker"] for h in holdings]
        ticker_str = ",".join(tickers)

        # 주가 일괄 조회
        prices = {}
        if ticker_str:
            threads = []
            lock = threading.Lock()

            def fetch_price(t):
                data = fetch_stock_info(t)
                with lock:
                    prices[t] = data

            for t in tickers:
                th = threading.Thread(target=fetch_price, args=(t,))
                threads.append(th)
                th.start()
            for th in threads:
                th.join(timeout=10)

        # 종목별 계산
        enriched = []
        total_value = 0
        for h in holdings:
            t    = h["ticker"]
            info = prices.get(t, {})
            cur  = info.get("price", 0)
            avg  = h.get("avg_price", 0)
            qty  = h["qty"]

            value   = cur * qty
            cost    = avg * qty
            pnl     = value - cost
            ret_pct = (pnl / cost * 100) if cost > 0 else 0
            total_value += value

            enriched.append({
                **h,
                "cur_price":   cur,
                "change_amt":  info.get("change_amt", 0),
                "change_rate": info.get("change_rate", 0.0),
                "high52w":     info.get("high52w", 0),
                "value":       value,
                "cost":        cost,
                "pnl":         pnl,
                "ret_pct":     round(ret_pct, 2),
                "updated_at":  info.get("updated_at", ""),
            })

        # 현재비중 & 괴리율 계산 (기존 app.py 로직)
        for h in enriched:
            cur_pct  = (h["value"] / total_value * 100) if total_value > 0 else 0
            dev      = cur_pct - h["target_pct"]
            is_action = abs(dev) >= 5

            # 리밸런싱 수량 계산 (기존 app.py 로직)
            rebal_qty = 0
            if h["cur_price"] > 0 and total_value > 0:
                target_val = total_value * (h["target_pct"] / 100)
                rebal_qty  = int(target_val / h["cur_price"]) - int(h["qty"])

            # MDD 계산
            mdd = 0.0
            if h["high52w"] > 0 and h["cur_price"] > 0:
                mdd = (h["cur_price"] - h["high52w"]) / h["high52w"] * 100

            h["cur_pct"]   = round(cur_pct, 2)
            h["deviation"] = round(dev, 2)
            h["is_action"] = is_action
            h["rebal_qty"] = rebal_qty
            h["mdd"]       = round(mdd, 2)

        result[acct_name] = {
            "holdings":    enriched,
            "total_value": total_value,
        }

    return {
        "accounts":   result,
        "updated_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S"),
    }


@app.delete("/api/cache")
def clear_cache():
    """캐시 초기화 (강제 새로고침)"""
    _cache.clear()
    return {"message": "캐시가 초기화되었습니다."}



@app.get("/api/high52")
def api_high52():
    """
    전체 종목 52주 최고가 일괄 조회 (백그라운드용 — 느림)
    GET /api/high52
    """
    sheet_data = load_sheet(SHEET_ID)
    accounts   = sheet_data["accounts"]

    result = {}
    lock = threading.Lock()

    def fetch_one(ticker):
        val = fetch_high52w(ticker)
        with lock:
            result[ticker] = val

    threads = []
    for holdings in accounts.values():
        for h in holdings:
            t = h["ticker"]
            if t not in result:
                th = threading.Thread(target=fetch_one, args=(t,))
                threads.append(th)
                th.start()

    for th in threads:
        th.join(timeout=15)

    return {
        "high52w":    result,
        "updated_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S"),
    }


@app.get("/api/health")
def health():
    kst = datetime.now(timezone(timedelta(hours=9)))
    return {
        "status":     "ok",
        "time_kst":   kst.strftime("%Y-%m-%d %H:%M:%S"),
        "cache_keys": len(_cache),
    }
