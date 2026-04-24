"""
JY Wealth Cockpit — FastAPI Backend v3.0
=========================================
- 한국 주식: 네이버 → 다음 금융 API
- 미국 주식: Yahoo Finance (yfinance)
- 매입단가: 구글 시트 '매입단가' 컬럼 지원 → 수익률 계산
- 티커 자동 감지: 숫자 6자리 → 한국 / 영문자 → 미국
실행: uvicorn main:app --reload --port 8000
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import requests, re, time, threading, json, os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
import pandas as pd

# ── 설정 ───────────────────────────────────────────────
SHEET_ID  = "1YubXMkoyA3Fa63Juf98rmicN6XEbUE9QGw2sQHF30Vo"
CACHE_TTL = 600  # 10분

# ── 앱 초기화 ──────────────────────────────────────────
app = FastAPI(title="JY Wealth Cockpit API", version="3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── 캐시 ───────────────────────────────────────────────
_cache: dict = {}

# ── 구글 시트 쓰기 클라이언트 ────────────────────────
_gs_client = None

def get_gs_client():
    global _gs_client
    if _gs_client:
        return _gs_client
    try:
        creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
        if not creds_json:
            raise ValueError("GOOGLE_CREDENTIALS 환경변수가 없습니다")
        creds_dict = json.loads(creds_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        _gs_client = gspread.authorize(creds)
        return _gs_client
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"구글 인증 실패: {e}")


def get_gs_sheet(sheet_id: str, sheet_name: str):
    """특정 시트 탭 반환"""
    gc = get_gs_client()
    wb = gc.open_by_key(sheet_id)
    try:
        return wb.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        raise HTTPException(status_code=404, detail=f"시트 탭 '{sheet_name}'을 찾을 수 없습니다")


def cache_key(key):
    """key"""
    return key


def get_cache(key):
    e = _cache.get(key)
    return e["data"] if e and time.time() - e["ts"] < CACHE_TTL else None

def set_cache(key, data):
    _cache[key] = {"data": data, "ts": time.time()}


# ══════════════════════════════════════════════════════
#  헬퍼: 한국/미국 티커 자동 감지
# ══════════════════════════════════════════════════════
def is_korean(ticker: str) -> bool:
    return str(ticker).strip().replace("0","").isdigit() or str(ticker).strip().isdigit()


# ══════════════════════════════════════════════════════
#  한국 주식 조회 (네이버 → 다음)
# ══════════════════════════════════════════════════════
def fetch_kr_stock(ticker: str) -> dict:
    cached = get_cache(f"kr_{ticker}")
    if cached:
        return cached

    clean = str(ticker).strip().zfill(6)
    price, change_amt, change_rate = 0, 0, 0.0

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

    if price == 0:
        try:
            url = f"https://finance.daum.net/api/quotes/A{clean}?summary=false"
            res = requests.get(url, headers={
                "User-Agent": "Mozilla/5.0", "Referer": "https://finance.daum.net/"
            }, timeout=5)
            data = res.json()
            if data.get("tradePrice"):
                price       = int(data["tradePrice"])
                change_amt  = int(data.get("changePrice", 0))
                change_rate = float(data.get("changeRate", 0)) * 100
                if data.get("change") == "FALL":
                    change_amt = -change_amt; change_rate = -change_rate
        except Exception:
            pass

    result = {
        "ticker": clean, "market": "KR",
        "price": price, "price_usd": None,
        "change_amt": change_amt, "change_amt_usd": None,
        "change_rate": round(change_rate, 2),
        "high52w": 0, "high52w_usd": None,
        "currency": "KRW", "usd_krw": None,
        "updated_at": datetime.now(timezone(timedelta(hours=9))).strftime("%H:%M:%S"),
    }
    set_cache(f"kr_{ticker}", result)
    return result


def fetch_kr_high52w(ticker: str) -> int:
    cached = get_cache(f"kr_h52_{ticker}")
    if cached:
        return cached["value"]

    clean = str(ticker).strip().zfill(6)
    high52w = 0
    try:
        url = f"https://finance.naver.com/item/main.naver?code={clean}"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        match = re.search(r"52주최고.*?<em>([\d,]+)</em>", res.text, re.DOTALL)
        if match:
            high52w = int(match.group(1).replace(",", ""))
    except Exception:
        pass

    set_cache(f"kr_h52_{ticker}", {"value": high52w})
    return high52w


# ══════════════════════════════════════════════════════
#  미국 주식 조회 (Yahoo Finance)
# ══════════════════════════════════════════════════════
# Yahoo Finance HTTP 헤더 (브라우저 위장)
_YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

def _yf_quote(ticker: str) -> dict:
    """Yahoo Finance v8 API 직접 호출 (yfinance 라이브러리 없이)"""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"
    try:
        res = requests.get(url, headers=_YF_HEADERS, timeout=8)
        data = res.json()
        meta = data["chart"]["result"][0]["meta"]
        return {
            "price":       meta.get("regularMarketPrice", 0),
            "prev_close":  meta.get("chartPreviousClose", 0),
            "year_high":   meta.get("fiftyTwoWeekHigh", 0),
            "year_low":    meta.get("fiftyTwoWeekLow", 0),
        }
    except Exception:
        pass

    # 2차: v7 API 시도
    try:
        url2 = f"https://query2.finance.yahoo.com/v7/finance/quote?symbols={ticker}"
        res2 = requests.get(url2, headers=_YF_HEADERS, timeout=8)
        q = res2.json()["quoteResponse"]["result"][0]
        return {
            "price":      q.get("regularMarketPrice", 0),
            "prev_close": q.get("regularMarketPreviousClose", 0),
            "year_high":  q.get("fiftyTwoWeekHigh", 0),
            "year_low":   q.get("fiftyTwoWeekLow", 0),
        }
    except Exception:
        return {}


def get_usd_krw() -> float:
    cached = get_cache("usd_krw")
    if cached:
        return cached["rate"]
    try:
        q = _yf_quote("USDKRW=X")
        rate = float(q.get("price", 0) or 0)
        if rate > 100:
            set_cache("usd_krw", {"rate": rate})
            return rate
    except Exception:
        pass
    return 1350.0


def fetch_us_stock(ticker: str) -> dict:
    cached = get_cache(f"us_{ticker}")
    if cached:
        return cached

    clean = str(ticker).strip().upper()
    price_usd = change_amt_usd = change_rate = high52w_usd = 0.0

    q = _yf_quote(clean)
    if q:
        price_usd   = float(q.get("price", 0) or 0)
        prev_close  = float(q.get("prev_close", 0) or 0)
        high52w_usd = float(q.get("year_high", 0) or 0)
        if prev_close > 0 and price_usd > 0:
            change_amt_usd = price_usd - prev_close
            change_rate    = (change_amt_usd / prev_close) * 100

    rate      = get_usd_krw()
    price_krw = int(price_usd * rate)

    result = {
        "ticker": clean, "market": "US",
        "price":          price_krw,
        "price_usd":      round(price_usd, 2),
        "change_amt":     round(change_amt_usd * rate),
        "change_amt_usd": round(change_amt_usd, 2),
        "change_rate":    round(change_rate, 2),
        "high52w":        int(high52w_usd * rate),
        "high52w_usd":    round(high52w_usd, 2),
        "currency":       "USD",
        "usd_krw":        rate,
        "updated_at":     datetime.now(timezone(timedelta(hours=9))).strftime("%H:%M:%S"),
    }
    set_cache(f"us_{ticker}", result)
    return result


def fetch_stock(ticker: str) -> dict:
    return fetch_kr_stock(ticker) if is_korean(ticker) else fetch_us_stock(ticker)


# ══════════════════════════════════════════════════════
#  구글 시트 로드
# ══════════════════════════════════════════════════════
def load_sheet(sheet_id: str) -> dict:
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

        if "목표비중" in df.columns:
            df["목표비중"] = (
                df["목표비중"].astype(str).str.replace("%", "").replace("nan", "0").astype(float)
            )
            df["목표비중"] = df["목표비중"].apply(lambda x: x / 100 if x > 1 else x)
        else:
            df["목표비중"] = 0.0

        # 매입단가 컬럼 자동 탐색
        avg_col = next((c for c in ["매입단가", "매입가", "평균단가", "취득단가"] if c in df.columns), None)

        holdings = []
        for _, row in df.iterrows():
            raw_ticker = str(row["종목코드"]).strip()
            ticker     = raw_ticker.zfill(6) if is_korean(raw_ticker) else raw_ticker.upper()
            market     = "KR" if is_korean(raw_ticker) else "US"

            avg_price = 0.0
            if avg_col:
                try:
                    avg_price = float(str(row[avg_col]).replace(",", "").replace("nan", "0"))
                except Exception:
                    avg_price = 0.0

            holdings.append({
                "name":       str(row.get("종목명", ticker)),
                "ticker":     ticker,
                "market":     market,
                "qty":        float(row["보유수량"]),
                "target_pct": float(row["목표비중"]) * 100,
                "avg_price":  avg_price,
            })

        accounts[account_name] = holdings

    result = {"accounts": accounts}
    set_cache(f"sheet_{sheet_id}", result)
    return result


# ══════════════════════════════════════════════════════
#  포트폴리오 계산
# ══════════════════════════════════════════════════════
def build_portfolio(accounts: dict) -> dict:
    result = {}

    for acct_name, holdings in accounts.items():
        # 병렬 주가 조회
        prices = {}
        lock   = threading.Lock()

        def fetch_one(t, lock=lock, prices=prices):
            data = fetch_stock(t)
            with lock:
                prices[t] = data

        threads = [threading.Thread(target=fetch_one, args=(h["ticker"],)) for h in holdings]
        for th in threads: th.start()
        for th in threads: th.join(timeout=12)

        markets    = set(h["market"] for h in holdings)
        is_us_acct = "US" in markets and "KR" not in markets

        enriched    = []
        total_value = 0.0

        for h in holdings:
            info    = prices.get(h["ticker"], {})
            cur     = info.get("price", 0)
            cur_usd = info.get("price_usd")
            avg     = h["avg_price"]
            qty     = h["qty"]
            market  = h["market"]
            usd_krw = info.get("usd_krw", 1350.0)

            value        = cur * qty
            total_value += value

            # 수익률: 매입단가가 있을 때만
            cost = pnl = ret_pct = 0.0
            avg_is_usd = False
            if avg > 0:
                if market == "US":
                    if avg < 5000:
                        # USD 단위로 입력된 경우 (5000 미만이면 USD로 판단)
                        cost      = avg * usd_krw * qty
                        avg_is_usd = True
                    else:
                        # 원화로 입력된 경우
                        cost = avg * qty
                    pnl     = value - cost
                    ret_pct = (pnl / cost * 100) if cost > 0 else 0
                else:
                    cost    = avg * qty
                    pnl     = value - cost
                    ret_pct = (pnl / cost * 100) if cost > 0 else 0

            enriched.append({
                **h,
                "avg_is_usd":     avg_is_usd,
                "cur_price":      cur,
                "cur_price_usd":  cur_usd,
                "change_amt":     info.get("change_amt", 0),
                "change_amt_usd": info.get("change_amt_usd"),
                "change_rate":    info.get("change_rate", 0.0),
                "high52w":        info.get("high52w", 0),
                "high52w_usd":    info.get("high52w_usd"),
                "usd_krw":        usd_krw,
                "currency":       info.get("currency", "KRW"),
                "value":          value,
                "cost":           cost,
                "pnl":            pnl,
                "ret_pct":        round(ret_pct, 2),
                "has_avg":        avg > 0,
                "updated_at":     info.get("updated_at", ""),
            })

        for h in enriched:
            cur_pct   = (h["value"] / total_value * 100) if total_value > 0 else 0
            dev       = cur_pct - h["target_pct"]
            is_action = abs(dev) >= 5 and h["target_pct"] > 0

            rebal_qty = 0
            if h["cur_price"] > 0 and total_value > 0 and h["target_pct"] > 0:
                rebal_qty = int(total_value * h["target_pct"] / 100 / h["cur_price"]) - int(h["qty"])

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
            "is_us_acct":  is_us_acct,
        }

    return result


# ══════════════════════════════════════════════════════
#  API ROUTES
# ══════════════════════════════════════════════════════
@app.get("/")
def root():
    return FileResponse("static/index.html")

@app.get("/api/portfolio")
def api_portfolio():
    sheet_data = load_sheet(SHEET_ID)
    result     = build_portfolio(sheet_data["accounts"])
    return {
        "accounts":   result,
        "updated_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S"),
    }

@app.get("/api/high52")
def api_high52():
    """한국 종목만 52주 최고가 백그라운드 조회 (미국은 portfolio에 포함)"""
    sheet_data = load_sheet(SHEET_ID)
    result = {}
    lock   = threading.Lock()

    def fetch_one(ticker, market):
        val = fetch_kr_high52w(ticker) if market == "KR" else (get_cache(f"us_{ticker}") or {}).get("high52w", 0)
        with lock:
            result[ticker] = val

    threads = []
    for holdings in sheet_data["accounts"].values():
        for h in holdings:
            if h["ticker"] not in result:
                th = threading.Thread(target=fetch_one, args=(h["ticker"], h["market"]))
                threads.append(th); th.start()
    for th in threads: th.join(timeout=15)

    return {"high52w": result, "updated_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")}

@app.get("/api/rate/usdkrw")
def api_usdkrw():
    return {"rate": get_usd_krw()}

@app.delete("/api/cache")
def clear_cache():
    _cache.clear()
    return {"message": "캐시 초기화 완료"}


# ── 매수/매도 요청 모델 ──────────────────────────────
from pydantic import BaseModel

class TradeRequest(BaseModel):
    sheet_name: str        # 구글 시트 탭 이름 (= 계좌명)
    ticker:     str        # 종목코드 또는 티커
    trade_type: str        # "buy" 또는 "sell"
    qty:        float      # 거래 수량
    price:      float      # 거래 단가


@app.post("/api/trade")
def api_trade(req: TradeRequest):
    """
    매수/매도 처리 → 구글 시트 보유수량 & 매입단가 자동 업데이트
    POST /api/trade
    """
    try:
        ws = get_gs_sheet(SHEET_ID, req.sheet_name)
        rows = ws.get_all_values()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"시트 읽기 실패: {e}")

    # 헤더 행 찾기
    header = None
    header_row_idx = None
    for i, row in enumerate(rows):
        if "종목코드" in row:
            header = row
            header_row_idx = i
            break

    if header is None:
        raise HTTPException(status_code=400, detail="종목코드 헤더를 찾을 수 없습니다")

    # 컬럼 인덱스 매핑
    def col_idx(name):
        for alt in [name]:
            if alt in header:
                return header.index(alt)
        return None

    code_col = col_idx("종목코드")
    qty_col  = col_idx("보유수량")

    # 매입단가 컬럼
    avg_col = None
    for name in ["매입단가", "매입가", "평균단가", "취득단가"]:
        if name in header:
            avg_col = header.index(name)
            break

    if code_col is None or qty_col is None:
        raise HTTPException(status_code=400, detail="종목코드 또는 보유수량 컬럼이 없습니다")

    # 종목 행 찾기
    target_row_idx = None
    for i, row in enumerate(rows):
        if i <= header_row_idx:
            continue
        cell_val = str(row[code_col]).strip() if len(row) > code_col else ""
        # 한국: 앞 0 제거 후 비교 / 미국: 대문자 비교
        if cell_val.lstrip("0") == req.ticker.lstrip("0") or cell_val.upper() == req.ticker.upper():
            target_row_idx = i
            break

    if target_row_idx is None:
        raise HTTPException(status_code=404, detail=f"종목 '{req.ticker}'을 시트에서 찾을 수 없습니다")

    target_row = rows[target_row_idx]

    # 현재 보유수량, 매입단가 읽기
    try:
        cur_qty = float(str(target_row[qty_col]).replace(",", "") or "0")
    except Exception:
        cur_qty = 0.0

    cur_avg = 0.0
    if avg_col is not None and len(target_row) > avg_col:
        try:
            cur_avg = float(str(target_row[avg_col]).replace(",", "") or "0")
        except Exception:
            cur_avg = 0.0

    # 매수/매도 계산
    if req.trade_type == "buy":
        new_qty = cur_qty + req.qty
        # 평균단가 재계산: (기존금액 + 신규금액) / 신규수량
        if cur_avg > 0 and cur_qty > 0:
            new_avg = round((cur_avg * cur_qty + req.price * req.qty) / new_qty, 2)
        else:
            new_avg = req.price

    elif req.trade_type == "sell":
        new_qty = cur_qty - req.qty
        if new_qty < 0:
            raise HTTPException(status_code=400, detail=f"매도 수량({req.qty})이 보유수량({cur_qty})을 초과합니다")
        new_avg = cur_avg  # 매도 시 평균단가 유지

    else:
        raise HTTPException(status_code=400, detail="trade_type은 'buy' 또는 'sell'이어야 합니다")

    # 구글 시트 업데이트 (1-indexed, +1 for header offset)
    sheet_row = target_row_idx + 1  # gspread는 1부터 시작

    try:
        # 보유수량 업데이트
        qty_cell = gspread.utils.rowcol_to_a1(sheet_row, qty_col + 1)
        ws.update(qty_cell, [[new_qty]])

        # 매입단가 업데이트 (컬럼이 있을 때만)
        if avg_col is not None and req.trade_type == "buy":
            avg_cell = gspread.utils.rowcol_to_a1(sheet_row, avg_col + 1)
            ws.update(avg_cell, [[new_avg]])

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"시트 업데이트 실패: {e}")

    # 캐시 초기화 (변경사항 즉시 반영)
    _cache.clear()

    return {
        "success":   True,
        "ticker":    req.ticker,
        "trade":     req.trade_type,
        "prev_qty":  cur_qty,
        "new_qty":   new_qty,
        "prev_avg":  cur_avg,
        "new_avg":   new_avg if req.trade_type == "buy" else cur_avg,
        "message":   f"{'매수' if req.trade_type == 'buy' else '매도'} 완료: {req.ticker} {req.qty}주"
    }


@app.get("/api/health")
def health():
    kst = datetime.now(timezone(timedelta(hours=9)))
    return {"status": "ok", "time_kst": kst.strftime("%Y-%m-%d %H:%M:%S"), "cache_keys": len(_cache)}
