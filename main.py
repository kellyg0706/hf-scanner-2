import os
import asyncio
import httpx
from datetime import datetime
import csv
import pandas as pd
from zoneinfo import ZoneInfo
from fastapi import FastAPI
import uvicorn
import threading

app = FastAPI()

# Load environment variables from .beast.env
from dotenv import load_dotenv
load_dotenv('.beast.env')

UW_API_KEY = os.getenv('UW_API_KEY')
MASSIVE_API_KEY = os.getenv('MASSIVE_API_KEY')
DISCORD_WEBHOOK = os.getenv('DISCORD_WEBHOOK')

if not UW_API_KEY or not DISCORD_WEBHOOK:
    raise RuntimeError("Please set UW_API_KEY and DISCORD_WEBHOOK in .beast.env")

BASE_URL = "https://api.unusualwhales.com/api"
HEADERS = {"Authorization": f"Bearer {UW_API_KEY}"}

# Full Universe
SP500 = "NVDA,AAPL,GOOG,GOOGL,MSFT,AMZN,META,AVGO,TSLA,BRK.B,LLY,WMT,JPM,V,ORCL,MA,JNJ,XOM,PLTR,NFLX,BAC,ABBV,COST,AMD,HD,PG,GE,CSCO,KO,MU,CVX,UNH,WFC,IBM,MS,GS,CAT,AXP,MRK,PM,RTX,APP,CRM,MCD,TMUS,ABT,LRCX,TMO,C,PEP,AMAT,ISRG,DIS,LIN,INTU,BX,QCOM,GEV,AMGN,INTC,SCHW,BKNG,BLK,TJX,T,VZ,ACN,NEE,APH,ANET,UBER,KLAC,BA,NOW,TXN,DHR,SPGI,COF,GILD,ADBE,PFE,BSX,UNP,SYK,LOW,ADI,PGR,PANW,WELL,DE,MDT,HON,ETN,CB,CRWD,PLD,KKR,VRTX,COP,CEG,PH,NEM,BMY,LMT,HOOD,HCA,CMCSA,ADP,MCK,DASH,SBUX,CVS,MO,CME,SO,GD,ICE,MMC,DUK,MCO,SNPS,WM,NKE,UPS,TT,MMM,CDNS,APO,USB,DELL,MAR,PNC,ABNB,HWM,NOC,BK,AMT,RCL,SHW,REGN,GM,ORLY,ELV,GLW,AON,CTAS,EMR,ECL,MNST,EQIX,JCI,CI,ITW,TDG,WMB,FCX,MDLZ,SPG,WBD,CSX,HLT,FDX,TEL,COR,COIN,RSG,NSC,TRV,AJG,STX,TFC,PWR,ADSK,CL,WDC,MSI,AEP,FTNT,ROST,KMI,PCAR,AFL,WDAY,NXPI,SRE,AZO,PYPL,IDXX,BDX,EOG,VST,NDAQ,ARES,ZTS,LHX,MET,F,ALL,APD,DLR,O,PSX,URI,EA,D,MPC,CMG,EW,VLO,DDOG,GWW,FAST,CAH,ROP,CBRE,AXON,AME,AIG,DAL,TTWO,PSA,AMP,CARR,LVS,OKE,MPWR,CTVA,TGT,BKR,EXC,XEL,DHI,MSCI,YUM,FANG,TKO,FICO,ETR,CTSH,CCL,PAYX,PRU,PEG,KR,OXY,EL,A,GRMN,HIG,VMC,TRGP,HSY,EBAY,MLM,KDP,CPRT,GEHC,IQV,CCI,VTR,WAB,UAL,NUE,RMD,SYY,EXPE,ED,MCHP,ACGL,KEYS,PCG,FIS,OTIS,WEC,EQT,XYL,KMB,ODFL,KVUE,HPE,RJF,FOXA,WTW,MTB,FITB,IR,HUM,TER,VRSK,DG,FOX,NRG,CHTR,VICI,KHC,ROL,EXR,MTD,FSLR,IBKR,ADM,HBAN,CSGP,BRO,EME,TSCO,ATO,DOV,EFX,LEN,AEE,ULTA,DTE,BR,NTRS,WRB,CINF,CBOE,DXCM,TPR,BIIB,FE,GIS,STLD,DLTR,CFG,AWK,PPL,OMC,AVB,ES,STE,LULU,CNP,RF,JBL,TDY,EQR,IRM,LDOS,HUBB,STZ,PHM,HAL,EIX,PPG,KEY,WSM,VRSN,TROW,WAT,DVN,ON,NTAP,DRI,L,RL,CPAY,HPQ,LUV,PTC,CMS,NVR,LH,TPL,TSN,EXPD,CHD,PODD,SBAC,IP,INCY,SW,TYL,WST,DGX,NI,PFG,CTRA,TRMB,CNC,GPN,AMCR,JBHT,SMCI,MKC,CDW,PKG,IT,TTD,SNA,BG,ZBH,GPC,FTV,LII,DD,GDDY,ALB,ESS,GEN,PNR,WY,APTV,IFF,HOLX,Q,EVRG,INVH,LNT,DOW,COO,MAA,J,TXT,NWS,BBY,FFIV,ERIE,DPZ,NWSA,DECK,UHS,AVY,BALL,EG,LYB,ALLE,VTRS,KIM,NDSN,JKHY,MAS,IEX,HII,MRNA,WYNN,HRL,UDR,HST,AKAM,REG,ZBRA,BEN,CF,BXP,IVZ,CLX,AIZ,CPT,EPAM,HAS,BLDR,DOC,ALGN,SWK,GL,DAY,RVTY,FDS,SJM,NCLH,PNW,MGM,BAX,CRL,AES,SWKS,AOS,TAP,HSIC,TECH,PAYC,FRT,POOL,APA,CPB,MOH,CAG,ARE,GNRC,DVA,MTCH,LKQ,LW,MOS,MHK".split(',')
DOW = "MMM,AXP,AMGN,AMZN,AAPL,BA,CAT,CVX,CSCO,KO,DIS,GS,HD,HON,IBM,JNJ,JPM,MCD,MRK,MSFT,NKE,NVDA,PG,CRM,SHW,TRV,UNH,VZ,V,WMT".split(',')
NASDAQ100 = "ADBE,AMD,ABNB,GOOGL,GOOG,AMZN,AEP,AMGN,ADI,AAPL,AMAT,APP,ARM,ASML,AZN,TEAM,ADSK,ADP,AXON,BKR,BIIB,BKNG,AVGO,CDNS,CDW,CHTR,CTAS,CSCO,CCEP,CTSH,CMCSA,CEG,CPRT,CSGP,COST,CRWD,CSX,DDOG,DXCM,FANG,DASH,EA,EXC,FAST,FTNT,GEHC,GILD,GFS,HON,IDXX,INTC,INTU,ISRG,KDP,KLAC,KHC,LRCX,LIN,LULU,MAR,MRVL,MELI,META,MCHP,MU,MSFT,MSTR,MDLZ,MNST,NFLX,NVDA,NXPI,ORLY,ODFL,ON,PCAR,PLTR,PANW,PAYX,PYPL,PDD,PEP,QCOM,REGN,ROP,ROST,SHOP,SBUX,SNPS,TMUS,TTWO,TSLA,TXN,TRI,TTD,VRSK,VRTX,WBD,WDAY,XEL,ZS".split(',')
RUSSELL_TOP100 = "MSTR,CVNA,SMCI,FIX,INSM,SMMT,SFM,ASTS,APG,CRS,ITCI,ATI,FTAI,AVAV,HIMS,FN,MTSI,COKE,SSB,COOP,ALTR,AIT,MLI,SATS,HQY,ENSG,WTS,APPF,FLR".split(',')
ETF_LIST = "SPY,QQQ,DIA,IWM,XLK,XLV,XLF,XLE,XLY,XLP,XLI,XLU,XLB,XLC,XOP".split(',')

UNIVERSE = list(set(SP500 + DOW + NASDAQ100 + RUSSELL_TOP100 + ETF_LIST + ['COIN']))
OWNED_STOCKS = ['ALAB', 'AMD', 'AVGO', 'COIN', 'CRDO', 'CRM', 'CRWD', 'GOOGL', 'IWM', 'LLY', 'META', 'MRVL', 'MU', 'NVDA', 'QCOM', 'SMCI', 'TSLA', 'TSM']

# Your Personal Sniper List — scanned first + bold in alerts
SNIPER_LIST = OWNED_STOCKS + ['SPY', 'QQQ', 'MSTR', 'HOOD', 'PLTR', 'APP']

CSV_FILE = 'signals.csv'
CSV_HEADERS = ['timestamp','date','symbol','score','gap_pct','whale_premium','fvg_timeframe','whale_type','boosts','status','reason',
               'hypo_entry_price','stop_price','target_10','target_20','risk_pct','actual_entry_price','actual_entry_time',
               'exit_price','exit_time','pnl_percent','pnl_dollars','notes']

if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)

async def get_ohlc(symbol: str, interval: str = '1h', limit: int = 250):
    async with httpx.AsyncClient() as client:
        try:
            url = f"{BASE_URL}/stock/{symbol.upper()}/ohlc/{interval}"
            params = {"limit": limit}
            resp = await client.get(url, params=params, headers=HEADERS, timeout=20.0)
            resp.raise_for_status()
            data = resp.json()
            return {'bars': data.get('data', [])}
        except Exception:
            return None

async def get_flow(symbol: str):
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{BASE_URL}/flow-per-strike-intraday", params={"symbol": symbol}, headers=HEADERS, timeout=20.0)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

async def get_darkpool(symbol: str):
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{BASE_URL}/darkpool/{symbol.upper()}", headers=HEADERS, timeout=20.0)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

async def get_insider_activity():
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{BASE_URL}/insider-trading/recent", params={"limit": 100}, headers=HEADERS, timeout=20.0)
            resp.raise_for_status()
            data = resp.json().get('data', [])
            alerts = []
            for trade in data:
                sym = trade.get('ticker')
                if sym not in UNIVERSE: continue
                value = trade.get('value_usd', 0)
                if abs(value) < 500000: continue
                filer = trade.get('filer_name', 'Insider')
                trans_type = "BUY" if value > 0 else "SELL"
                amount = f"${abs(value):,.0f}"
                alerts.append(f"{sym} — {filer} {trans_type} {amount}")
            if alerts:
                msg = "**Daily Insider Edge**\n\n" + "\n".join(alerts[:12])
                await send_discord(msg)
        except Exception:
            pass

def detect_fvg(bars, min_gap_pct: float, positive: bool = True):
    if len(bars) < 3: return 0, 0.0, 0.0
    count = 0
    max_gap = 0.0
    entry = 0.0
    for i in range(2, len(bars)):
        prev_high = bars[i-2]['high']
        prev_low = bars[i-2]['low']
        curr_low = bars[i]['low']
        curr_high = bars[i]['high']
        if positive and curr_low > prev_high:
            gap = (curr_low - prev_high) / prev_high * 100
            if gap > min_gap_pct:
                count += 1
                max_gap = max(max_gap, gap)
                entry = (prev_high + curr_low) / 2
        elif not positive and curr_high < prev_low:
            gap = (prev_low - curr_high) / prev_low * 100
            if gap > min_gap_pct:
                count += 1
                max_gap = max(max_gap, gap)
                entry = (curr_high + prev_low) / 2
    return count, max_gap, entry

def aggregate_to_4h(bars):
    agg = []
    for i in range(0, len(bars), 4):
        chunk = bars[i:i+4]
        if len(chunk) < 4: break
        agg.append({
            'open': chunk[0]['open'],
            'close': chunk[-1]['close'],
            'high': max(b['high'] for b in chunk),
            'low': min(b['low'] for b in chunk),
            'volume': sum(b['volume'] for b in chunk)
        })
    return agg

def has_volume_boost(bars):
    if len(bars) < 11: return False
    avg = sum(b['volume'] for b in bars[-11:-1]) / 10
    return bars[-1]['volume'] > 1.5 * avg

def has_pre_market_surge(bars):
    pre_bars = [b for b in bars if b.get('market_time') in ['pr', 'po']]
    if len(pre_bars) < 5: return False
    recent_vol = sum(b['volume'] for b in pre_bars[-5:])
    avg_vol = sum(b['volume'] for b in pre_bars[:-5]) / max(1, len(pre_bars[:-5]))
    return recent_vol > 3 * avg_vol

def get_whale_premium(flow_data):
    if not flow_data or 'data' not in flow_data:
        return 0, "No flow", ""

    premium_call = 0
    premium_put = 0
    otm_call_prem = 0
    otm_put_prem = 0
    otm_sweep_count = 0
    above_ask_count = 0
    multi_ex_count = 0
    spot = flow_data.get('spot_price', 0)

    for trade in flow_data['data']:
        prem = trade.get('premium', 0)
        strike = trade.get('strike', 0)
        is_call = trade.get('is_call')

        if is_call:
            premium_call += prem
            if strike > spot * 1.02:
                otm_call_prem += prem
                if trade.get('type') in ['sweep', 'block']:
                    otm_sweep_count += 1
        else:
            premium_put += prem
            if strike < spot * 0.98:
                otm_put_prem += prem

        if trade.get('sent_at_ask') or trade.get('above_ask'):
            above_ask_count += 1

        ex = trade.get('exchange', '')
        if isinstance(ex, str) and ',' in ex:
            multi_ex_count += 1

    total_premium = premium_call

    if total_premium > 100000 and otm_sweep_count >= 2 and above_ask_count >= 3:
        confirm = "CONFIRMED BEAST — Cheddar-level aggression"
    elif total_premium > 50000 and otm_sweep_count >= 1:
        confirm = "Strong conviction — sweeps above ask"
    elif total_premium > 25000:
        confirm = "Flow building — monitor"
    else:
        confirm = "Light flow"

    details = ""
    if otm_sweep_count: details += f" | {otm_sweep_count} OTM sweeps"
    if above_ask_count: details += f" | {above_ask_count} above ask"
    if multi_ex_count: details += f" | {multi_ex_count} multi-ex"

    gex_tag = ""
    if otm_call_prem > 100000:
        gex_tag = " | GAMMA WALL BUILDING ABOVE (positive GEX)"
    elif otm_put_prem > 100000:
        gex_tag = " | NEGATIVE GEX RISK BELOW (dealers short gamma)"

    return total_premium, confirm + details + gex_tag

async def get_macro_context():
    context = "Macro Context: "
    symbols = {
        '^VIX': 'VIX',
        'CL=F': 'Oil',
        '^TNX': '10yr',
        'GC=F': 'Gold',
        'SI=F': 'Silver',
        'DX-Y.NYB': 'DXY'
    }

    async with httpx.AsyncClient() as client:
        for sym, name in symbols.items():
            try:
                url = f"https://api.polygon.io/v2/snapshot/locale/global/markets/indices/tickers/{sym}"
                params = {"apiKey": MASSIVE_API_KEY}
                resp = await client.get(url, params=params, timeout=10.0)
                resp.raise_for_status()
                data = resp.json()
                ticker_data = data.get('ticker', {})
                day = ticker_data.get('day', {})
                close = day.get('c') or ticker_data.get('lastQuote', {}).get('p') or ticker_data.get('prevDayClose') or "N/A"
                if isinstance(close, (int, float)):
                    context += f"{name} ${close:.2f} | "
                else:
                    context += f"{name} N/A | "
            except Exception:
                context += f"{name} N/A | "

    return context.rstrip(" | ")

async def send_discord(message: str):
    async with httpx.AsyncClient() as client:
        try:
            await client.post(DISCORD_WEBHOOK, json={"content": message[:1995]})
        except Exception:
            pass

sector_daily_flow = {}

async def darkpool_scan():
    majors = ['SPY', 'QQQ', 'IWM', 'NVDA', 'TSLA', 'AAPL', 'MSFT', 'GOOGL', 'META', 'AMZN']
    alerts = []
    for sym in majors:
        data = await get_darkpool(sym)
        if data and 'data' in data:
            notional = sum(t.get('price', 0) * t.get('size', 0) for t in data['data'])
            if notional > 50_000_000:
                alerts.append(f"Dark Pool Accumulation: {sym} — ${notional:,.0f}")
    if alerts:
        await send_discord("\n".join(alerts))

async def sector_rotation():
    global sector_daily_flow
    alerts = []
    for etf in ETF_LIST:
        flow = await get_flow(etf)
        if not flow: continue
        call = sum(t.get('premium', 0) for t in flow.get('data', []) if t.get('is_call'))
        put = sum(t.get('premium', 0) for t in flow.get('data', []) if not t.get('is_call'))
        net = call - put
        sector_daily_flow[etf] = sector_daily_flow.get(etf, 0) + net
        if abs(net) > 200_000:
            dir = "→ CALL HEAVY" if net > 0 else "← PUT HEAVY"
            alerts.append(f"{etf} {dir} ${abs(net):,.0f}")
    if alerts:
        await send_discord("Sector Rotation Flow:\n" + "\n".join(alerts))

    # EOD recap
    now = datetime.now(ZoneInfo("America/Chicago"))
    if now.hour == 15 and now.minute < 5:
        if sector_daily_flow:
            sorted_flow = sorted(sector_daily_flow.items(), key=lambda x: x[1], reverse=True)
            msg = "**Daily Sector Flow Recap**\n\nTop Inflows:\n"
            for etf, net in sorted_flow[:3]:
                if net > 0: msg += f"{etf} +${net:,.0f}\n"
            msg += "\nTop Outflows:\n"
            for etf, net in sorted_flow[-3:]:
                if net < 0: msg += f"{etf} ${net:,.0f}\n"
            await send_discord(msg)
        sector_daily_flow = {}

async def get_top_pre_market_movers():
    movers = []
    scan_list = SNIPER_LIST + [s for s in UNIVERSE if s not in SNIPER_LIST][:80]
    for symbol in scan_list:
        ohlc = await get_ohlc(symbol, '30m', 50)
        if not ohlc or len(ohlc['bars']) < 10: continue
        bars = ohlc['bars']
        pre_bars = [b for b in bars if b.get('market_time') in ['pr', 'po']]
        if len(pre_bars) < 3: continue
        change = (pre_bars[-1]['close'] - pre_bars[0]['open']) / pre_bars[0]['open'] * 100
        if abs(change) > 1.0:
            movers.append((symbol, change))
        await asyncio.sleep(0.2)
    movers.sort(key=lambda x: abs(x[1]), reverse=True)
    return movers[:5]

async def daily_pre_market_summary():
    now = datetime.now(ZoneInfo("America/Chicago"))
    if now.hour == 8 and 30 <= now.minute < 35:
        movers = await get_top_pre_market_movers()
        if movers:
            msg = "**Pre-Market Battlefield — Top Movers**\n\n"
            for sym, chg in movers:
                dir_emoji = "🚀" if chg > 0 else "🔻"
                msg += f"{dir_emoji} **{sym}** {chg:+.2f}%\n"
            await send_discord(msg)

async def check_rollovers():
    if not os.path.exists(CSV_FILE): return
    df = pd.read_csv(CSV_FILE)
    active = df[df['status'].isin(['new', 'open'])]
    if active.empty: return
    updated = False
    ts = datetime.now(ZoneInfo("America/Chicago")).isoformat()
    for idx, row in active.iterrows():
        sym = row['symbol']
        ohlc = await get_ohlc(sym)
        if not ohlc or not ohlc['bars']: continue
        close = ohlc['bars'][-1]['close']
        stop = float(row['stop_price']) if pd.notna(row['stop_price']) else None
        t10 = float(row['target_10']) if pd.notna(row['target_10']) else None
        t20 = float(row['target_20']) if pd.notna(row['target_20']) else None
        entry = float(row['hypo_entry_price']) if pd.notna(row['hypo_entry_price']) else None
        if t10 and close >= t10: 
            await send_discord(f"🎯 TP 10% HIT: {sym} @ ${close:.2f}")
            if entry: df.at[idx, 'pnl_percent'] = ((close - entry) / entry) * 100
        if t20 and close >= t20: 
            await send_discord(f"🎯 TP 20% HIT: {sym} @ ${close:.2f}")
            if entry: df.at[idx, 'pnl_percent'] = ((close - entry) / entry) * 100
        if stop and close < stop:
            await send_discord(f"🛑 ROLLOVER EXIT: {sym} broke stop @ ${close:.2f}")
            df.at[idx, 'status'] = 'rolled_over'
            df.at[idx, 'exit_price'] = close
            df.at[idx, 'exit_time'] = ts
            if entry: df.at[idx, 'pnl_percent'] = ((close - entry) / entry) * 100
            updated = True
    if updated:
        df.to_csv(CSV_FILE, index=False)

async def log_new_signal(symbol, gap, premium, entry, timeframe):
    new_row = {
        'timestamp': datetime.now(ZoneInfo("America/Chicago")).isoformat(),
        'date': datetime.now().date(),
        'symbol': symbol,
        'score': 'HIGH',
        'gap_pct': round(gap, 2),
        'whale_premium': premium,
        'fvg_timeframe': timeframe,
        'whale_type': 'call flow',
        'boosts': 'volume+whale',
        'status': 'new',
        'reason': 'FVG + Whale + Volume',
        'hypo_entry_price': round(entry, 2),
        'stop_price': round(entry * 0.93, 2),
        'target_10': round(entry * 1.10, 2),
        'target_20': round(entry * 1.20, 2),
        'risk_pct': 7.0
    }
    df = pd.read_csv(CSV_FILE)
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df.to_csv(CSV_FILE, index=False)

async def fvg_whale_scan():
    now = datetime.now(ZoneInfo("America/Chicago"))
    holiday_mode = now.month == 12 and now.day >= 20
    gap_thr = 0.12 if holiday_mode else 0.18
    min_prem = 10000 if holiday_mode else 30000

    macro = await get_macro_context()
    message = f"{macro}\n\n**FVG + Whale Scan — {now.strftime('%H:%M CT')}**\n\n"

    positives = []
    negatives = []

    scan_order = SNIPER_LIST + [s for s in UNIVERSE if s not in SNIPER_LIST]

    for symbol in scan_order:
        try:
            ohlc = await get_ohlc(symbol)
            if not ohlc or len(ohlc['bars']) < 30: continue
            bars = ohlc['bars']

            pre_surge = has_pre_market_surge(bars)

            p1h, p_gap, p_entry = detect_fvg(bars, gap_thr, True)
            bars4h = aggregate_to_4h(bars)
            p4h, _, _ = detect_fvg(bars4h, gap_thr, True)
            if p1h > 0 or p4h > 0:
                boost = has_volume_boost(bars)
                flow = await get_flow(symbol)
                premium, confirm = get_whale_premium(flow)
                sniper_tag = "**SNIPER** " if symbol in SNIPER_LIST else ""
                owned = " ★OWNED★" if symbol in OWNED_STOCKS else ""
                extra = ""
                if pre_surge: extra += " | PRE-MARKET SURGE 3x+"
                positives.append({
                    'sym': symbol, 'sniper': sniper_tag, 'owned': owned, 'gap': p_gap, 'premium': premium,
                    'boost': boost, 'confirm': confirm, 'entry': p_entry, 'extra': extra,
                    'tf': f"1H:{p1h} 4H:{p4h}"
                })
                if premium > min_prem and (boost or pre_surge):
                    await log_new_signal(symbol, p_gap, premium, p_entry, f"1H:{p1h} 4H:{p4h}")

            n1h, n_gap, _ = detect_fvg(bars, gap_thr, False)
            n4h, _, _ = detect_fvg(bars4h, gap_thr, False)
            if n1h > 0 or n4h > 0:
                owned = " ★OWNED★" if symbol in OWNED_STOCKS else ""
                negatives.append({'sym': symbol, 'owned': owned, 'gap': n_gap, 'tf': f"1H:{n1h} 4H:{n4h}"})

            await asyncio.sleep(0.3)
        except Exception:
            continue

    if positives:
        message += "**Bullish FVG Setups**\n\n"
        for p in sorted(positives, key=lambda x: x['premium'], reverse=True)[:20]:
            message += f"{p['sniper']}{p['sym']}{p['owned']} — {p['tf']} Gap {p['gap']:.2f}%\n"
            message += f"${p['premium']:,} call flow{p['extra']}\n{p['confirm']}\n"
            if p['boost']: message += "Volume boost: YES\n"
            if p['entry'] > 0:
                message += f"Entry ~${p['entry']:.2f} | Stop ${round(p['entry']*0.93,2):.2f} | T10 ${round(p['entry']*1.10,2):.2f} | T20 ${round(p['entry']*1.20,2):.2f}\n\n"

    if negatives:
        message += "**Bearish FVG — Rollover Risk**\n\n"
        for n in sorted(negatives, key=lambda x: x['gap'], reverse=True)[:15]:
            message += f"{n['sym']}{n['owned']} — {n['tf']} Gap {n['gap']:.2f}%\nConsider profit taking\n\n"

    if not positives and not negatives:
        message += "Market quiet — no strong FVGs detected.\n"

    await send_discord(message)
    await darkpool_scan()
    await sector_rotation()

async def macro_pulse():
    now = datetime.now(ZoneInfo("America/Chicago"))
    macro = await get_macro_context()
    vix_change = ""
    vix_data = await get_ohlc('^VIX')
    if vix_data and len(vix_data['bars']) > 1:
        today = vix_data['bars'][-1].get('close')
        yesterday = vix_data['bars'][-2].get('close')
        if today and yesterday:
            change = (today - yesterday) / yesterday * 100
            vix_change = f" ({change:+.1f}%)"
            if change < -3:
                vix_change += " — RISK ON CRUSH"
            elif change > 3:
                vix_change += " — RISK OFF"
    msg = f"**Live Macro Pulse — {now.strftime('%H:%M CT')}**\n\n"
    msg += macro + vix_change + "\n\n"
    msg += "Market bias: Low VIX = premium decay / grind plays"
    await send_discord(msg)

async def scheduler():
    await send_discord("BEAST ONLINE — Massive API macro live | Live macro pulse every hour | Full scans on schedule | All upgrades active. 🦁🔥")
    while True:
        try:
            now = datetime.now(ZoneInfo("America/Chicago"))
            hour = now.hour
            minute = now.minute

            # Macro pulse every hour
            if minute == 0:
                await macro_pulse()

            # Pre-market: 3:00–8:30 CT → every 30 minutes
            if 3 <= hour < 8 or (hour == 8 and minute < 30):
                if minute in [0, 30]:
                    await fvg_whale_scan()
                    await daily_pre_market_summary()
                    await check_rollovers()

            # Regular hours: 8:30–15:00 CT → every 10 minutes
            elif 8 <= hour < 15:
                if minute % 10 == 0:
                    await fvg_whale_scan()
                    await daily_pre_market_summary()
                    await check_rollovers()
                    if hour == 9 and minute == 0:
                        await get_insider_activity()

        except Exception as e:
            await send_discord(f"Scanner error: {str(e)}")

        await asyncio.sleep(60)

@app.get("/backtest")
async def backtest(days: int = 7):
    await send_discord(f"Manual backtest trigger for {days} days.")
    return {"status": "triggered"}

def start_scheduler():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(scheduler())

if __name__ == "__main__":
    threading.Thread(target=start_scheduler, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
