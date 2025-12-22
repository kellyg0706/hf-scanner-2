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

# Environment variables
UW_API_KEY = os.environ.get('UW_API_KEY')
DISCORD_WEBHOOK = os.environ.get('DISCORD_WEBHOOK')

if not UW_API_KEY or not DISCORD_WEBHOOK:
    raise RuntimeError("Please set UW_API_KEY and DISCORD_WEBHOOK environment variables.")

BASE_URL = "https://api.unusualwhales.com/api"
HEADERS = {"Authorization": f"Bearer {UW_API_KEY}"}

# Universe
SP500 = "NVDA,AAPL,GOOG,GOOGL,MSFT,AMZN,META,AVGO,TSLA,BRK.B,LLY,WMT,JPM,V,ORCL,MA,JNJ,XOM,PLTR,NFLX,BAC,ABBV,COST,AMD,HD,PG,GE,CSCO,KO,MU,CVX,UNH,WFC,IBM,MS,GS,CAT,AXP,MRK,PM,RTX,APP,CRM,MCD,TMUS,ABT,LRCX,TMO,C,PEP,AMAT,ISRG,DIS,LIN,INTU,BX,QCOM,GEV,AMGN,INTC,SCHW,BKNG,BLK,TJX,T,VZ,ACN,NEE,APH,ANET,UBER,KLAC,BA,NOW,TXN,DHR,SPGI,COF,GILD,ADBE,PFE,BSX,UNP,SYK,LOW,ADI,PGR,PANW,WELL,DE,MDT,HON,ETN,CB,CRWD,PLD,KKR,VRTX,COP,CEG,PH,NEM,BMY,LMT,HOOD,HCA,CMCSA,ADP,MCK,DASH,SBUX,CVS,MO,CME,SO,GD,ICE,MMC,DUK,MCO,SNPS,WM,NKE,UPS,TT,MMM,CDNS,APO,USB,DELL,MAR,PNC,ABNB,HWM,NOC,BK,AMT,RCL,SHW,REGN,GM,ORLY,ELV,GLW,AON,CTAS,EMR,ECL,MNST,EQIX,JCI,CI,ITW,TDG,WMB,FCX,MDLZ,SPG,WBD,CSX,HLT,FDX,TEL,COR,COIN,RSG,NSC,TRV,AJG,STX,TFC,PWR,ADSK,CL,WDC,MSI,AEP,FTNT,ROST,KMI,PCAR,AFL,WDAY,NXPI,SRE,AZO,PYPL,IDXX,BDX,EOG,VST,NDAQ,ARES,ZTS,LHX,MET,F,ALL,APD,DLR,O,PSX,URI,EA,D,MPC,CMG,EW,VLO,DDOG,GWW,FAST,CAH,ROP,CBRE,AXON,AME,AIG,DAL,TTWO,PSA,AMP,CARR,LVS,OKE,MPWR,CTVA,TGT,BKR,EXC,XEL,DHI,MSCI,YUM,FANG,TKO,FICO,ETR,CTSH,CCL,PAYX,PRU,PEG,KR,OXY,EL,A,GRMN,HIG,VMC,TRGP,HSY,EBAY,MLM,KDP,CPRT,GEHC,IQV,CCI,VTR,WAB,UAL,NUE,RMD,SYY,EXPE,ED,MCHP,ACGL,KEYS,PCG,FIS,OTIS,WEC,EQT,XYL,KMB,ODFL,KVUE,HPE,RJF,FOXA,WTW,MTB,FITB,IR,HUM,TER,VRSK,DG,FOX,NRG,CHTR,VICI,KHC,ROL,EXR,MTD,FSLR,IBKR,ADM,HBAN,CSGP,BRO,EME,TSCO,ATO,DOV,EFX,LEN,AEE,ULTA,DTE,BR,NTRS,WRB,CINF,CBOE,DXCM,TPR,BIIB,FE,GIS,STLD,DLTR,CFG,AWK,PPL,OMC,AVB,ES,STE,LULU,CNP,RF,JBL,TDY,EQR,IRM,LDOS,HUBB,STZ,PHM,HAL,EIX,PPG,KEY,WSM,VRSN,TROW,WAT,DVN,ON,NTAP,DRI,L,RL,CPAY,HPQ,LUV,PTC,CMS,NVR,LH,TPL,TSN,EXPD,CHD,PODD,SBAC,IP,INCY,SW,TYL,WST,DGX,NI,PFG,CTRA,TRMB,CNC,GPN,AMCR,JBHT,SMCI,MKC,CDW,PKG,IT,TTD,SNA,BG,ZBH,GPC,FTV,LII,DD,GDDY,ALB,ESS,GEN,PNR,WY,APTV,IFF,HOLX,Q,EVRG,INVH,LNT,DOW,COO,MAA,J,TXT,NWS,BBY,FFIV,ERIE,DPZ,NWSA,DECK,UHS,AVY,BALL,EG,LYB,ALLE,VTRS,KIM,NDSN,JKHY,MAS,IEX,HII,MRNA,WYNN,HRL,UDR,HST,AKAM,REG,ZBRA,BEN,CF,BXP,IVZ,CLX,AIZ,CPT,EPAM,HAS,BLDR,DOC,ALGN,SWK,GL,DAY,RVTY,FDS,SJM,NCLH,PNW,MGM,BAX,CRL,AES,SWKS,AOS,TAP,HSIC,TECH,PAYC,FRT,POOL,APA,CPB,MOH,CAG,ARE,GNRC,DVA,MTCH,LKQ,LW,MOS,MHK".split(',')
DOW = "MMM,AXP,AMGN,AMZN,AAPL,BA,CAT,CVX,CSCO,KO,DIS,GS,HD,HON,IBM,JNJ,JPM,MCD,MRK,MSFT,NKE,NVDA,PG,CRM,SHW,TRV,UNH,VZ,V,WMT".split(',')
NASDAQ100 = "ADBE,AMD,ABNB,GOOGL,GOOG,AMZN,AEP,AMGN,ADI,AAPL,AMAT,APP,ARM,ASML,AZN,TEAM,ADSK,ADP,AXON,BKR,BIIB,BKNG,AVGO,CDNS,CDW,CHTR,CTAS,CSCO,CCEP,CTSH,CMCSA,CEG,CPRT,CSGP,COST,CRWD,CSX,DDOG,DXCM,FANG,DASH,EA,EXC,FAST,FTNT,GEHC,GILD,GFS,HON,IDXX,INTC,INTU,ISRG,KDP,KLAC,KHC,LRCX,LIN,LULU,MAR,MRVL,MELI,META,MCHP,MU,MSFT,MSTR,MDLZ,MNST,NFLX,NVDA,NXPI,ORLY,ODFL,ON,PCAR,PLTR,PANW,PAYX,PYPL,PDD,PEP,QCOM,REGN,ROP,ROST,SHOP,SBUX,SNPS,TMUS,TTWO,TSLA,TXN,TRI,TTD,VRSK,VRTX,WBD,WDAY,XEL,ZS".split(',')
RUSSELL_TOP100 = "MSTR,CVNA,SMCI,FIX,INSM,SMMT,SFM,ASTS,APG,CRS,ITCI,ATI,FTAI,AVAV,HIMS,FN,MTSI,COKE,SSB,COOP,ALTR,AIT,MLI,SATS,HQY,ENSG,WTS,APPF,FLR".split(',')
ETF_LIST = "SPY,QQQ,DIA,IWM,XLK,XLV,XLF,XLE,XLY,XLP,XLI,XLU,XLB,XLC,XOP".split(',')

UNIVERSE = list(set(SP500 + DOW + NASDAQ100 + RUSSELL_TOP100 + ETF_LIST + ['COIN']))

OWNED_STOCKS = ['ALAB', 'AMD', 'AVGO', 'COIN', 'CRDO', 'CRM', 'CRWD', 'GOOGL', 'IWM', 'LLY', 'META', 'MRVL', 'MU', 'NVDA', 'QCOM', 'SMCI', 'TSLA', 'TSM']

CSV_FILE = 'signals.csv'
CSV_HEADERS = ['timestamp', 'date', 'symbol', 'score', 'gap_pct', 'whale_premium', 'fvg_timeframe', 'whale_type', 'boosts', 'status', 'reason',
               'hypo_entry_price', 'stop_price', 'target_10', 'target_20', 'risk_pct', 'actual_entry_price', 'actual_entry_time',
               'exit_price', 'exit_time', 'pnl_percent', 'pnl_dollars', 'max_return_1w', 'max_return_2w', 'notes']

if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)

async def get_ohlc(symbol: str, interval: str = '1h', limit: int = 200):
    async with httpx.AsyncClient() as client:
        try:
            url = f"{BASE_URL}/stock/{symbol.upper()}/ohlc/{interval}"
            params = {"limit": limit}
            resp = await client.get(url, params=params, headers=HEADERS, timeout=20.0)
            resp.raise_for_status()
            json_data = resp.json()
            return {'bars': json_data.get('data', [])}
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
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            return None
        except Exception:
            return None

def detect_fvg(bars, min_gap_pct: float, positive: bool = True):
    if len(bars) < 3:
        return 0, 0.0, 0.0
    fvg_count = 0
    latest_gap_pct = 0.0
    hypo_entry = 0.0
    for i in range(2, len(bars)):
        prev_high = bars[i-2]['high']
        prev_low = bars[i-2]['low']
        curr_low = bars[i]['low']
        curr_high = bars[i]['high']
        if positive and curr_low > prev_high:
            gap_pct = (curr_low - prev_high) / prev_high * 100
            if gap_pct > min_gap_pct:
                fvg_count += 1
                latest_gap_pct = max(latest_gap_pct, gap_pct)
                hypo_entry = (prev_high + curr_low) / 2
        elif not positive and curr_high < prev_low:
            gap_pct = (prev_low - curr_high) / prev_low * 100
            if gap_pct > min_gap_pct:
                fvg_count += 1
                latest_gap_pct = max(latest_gap_pct, gap_pct)
                hypo_entry = (curr_high + prev_low) / 2
    return fvg_count, latest_gap_pct, hypo_entry

def aggregate_to_4h(bars):
    aggregated = []
    for i in range(0, len(bars), 4):
        chunk = bars[i:i+4]
        if len(chunk) < 4:
            break
        aggregated.append({
            'open': chunk[0]['open'],
            'close': chunk[-1]['close'],
            'high': max(b['high'] for b in chunk),
            'low': min(b['low'] for b in chunk),
            'volume': sum(b['volume'] for b in chunk)
        })
    return aggregated

def has_volume_boost(bars):
    if len(bars) < 11:
        return False
    avg_vol = sum(b['volume'] for b in bars[-11:-1]) / 10
    return bars[-1]['volume'] > 1.5 * avg_vol

def get_whale_premium(flow_data):
    call_premium = 0
    has_otm_sweep = False
    spot_price = flow_data.get('spot_price', 0) if flow_data else 0
    for trade in flow_data.get('data', []) if flow_data else []:
        if trade.get('is_call'):
            premium = trade.get('premium', 0)
            call_premium += premium
            if trade.get('type') in ['sweep', 'block'] and trade.get('strike', 0) > spot_price:
                has_otm_sweep = True
    whale_type = "backed by UW sweeps on OTM calls" if has_otm_sweep else ""
    return call_premium, whale_type

async def get_macro_context():
    symbols = {'^VIX': 'VIX', 'CL=F': 'Crude Oil', '^TNX': '10yr Yield', 'GC=F': 'Gold', 'SI=F': 'Silver', 'DX-Y.NYB': 'DXY'}
    context = "Macro Context: "
    for sym, name in symbols.items():
        data = await get_ohlc(sym)
        if data and data['bars']:
            close = data['bars'][-1]['close']
            context += f"{name} ${close:.2f} | "
        else:
            context += f"{name} N/A | "
    return context.rstrip(" | ")

async def send_discord(message: str):
    async with httpx.AsyncClient() as client:
        try:
            await client.post(DISCORD_WEBHOOK, json={"content": message[:1995]})
        except Exception:
            pass

async def darkpool_scan():
    major = ['SPY', 'QQQ', 'IWM', 'NVDA', 'TSLA', 'AAPL', 'MSFT', 'GOOGL', 'META', 'AMZN']
    alerts = []
    for symbol in major:
        data = await get_darkpool(symbol)
        if not data or 'data' not in data:
            continue
        notional = sum(t.get('price', 0) * t.get('size', 0) for t in data['data'])
        if notional > 50_000_000:
            alerts.append(f"Dark Pool Alert: {symbol} — ${notional:,.0f} notional (bullish accumulation)")
    if alerts:
        await send_discord("\n".join(alerts))

async def sector_rotation():
    alerts = []
    for etf in ETF_LIST:
        flow = await get_flow(etf)
        if not flow:
            continue
        call = sum(t.get('premium', 0) for t in flow.get('data', []) if t.get('is_call'))
        put = sum(t.get('premium', 0) for t in flow.get('data', []) if not t.get('is_call'))
        net = call - put
        if abs(net) > 300_000:
            direction = "Inflow" if net > 0 else "Outflow"
            alerts.append(f"{etf} {direction} ${abs(net):,.0f}")
    if alerts:
        await send_discord("Sector Rotation:\n" + "\n".join(alerts))

async def check_rollovers():
    if not os.path.exists(CSV_FILE):
        return
    try:
        df = pd.read_csv(CSV_FILE)
    except:
        return
    active = df[df['status'].isin(['new', 'open'])]
    if active.empty:
        return
    updated = False
    timestamp = datetime.now(ZoneInfo("America/Chicago")).isoformat()
    for idx, row in active.iterrows():
        symbol = row['symbol']
        ohlc = await get_ohlc(symbol)
        if not ohlc or not ohlc['bars']:
            continue
        current = ohlc['bars'][-1]['close']
        stop = float(row['stop_price']) if pd.notna(row['stop_price']) else None
        t10 = float(row['target_10']) if pd.notna(row['target_10']) else None
        t20 = float(row['target_20']) if pd.notna(row['target_20']) else None
        if t10 and current >= t10:
            await send_discord(f"TP 10%: {symbol} hit ${current:.2f}")
        if t20 and current >= t20:
            await send_discord(f"TP 20%: {symbol} hit ${current:.2f}")
        if stop and current < stop:
            await send_discord(f"ROLLOVER EXIT: {symbol} broke stop at ${current:.2f}")
            df.at[idx, 'status'] = 'rolled_over'
            df.at[idx, 'exit_price'] = current
            df.at[idx, 'exit_time'] = timestamp
            updated = True
    if updated:
        df.to_csv(CSV_FILE, index=False)

async def fvg_whale_scan():
    now = datetime.now(ZoneInfo("America/Chicago"))
    is_holiday_mode = now.month == 12  # December low-volume mode
    gap_threshold = 0.12 if is_holiday_mode else 0.18

    macro = await get_macro_context()
    message = f"{macro}\n\n**Scanner Live — Test Message 🚀**\n\n"

    positive_list = []
    negative_list = []

    for symbol in UNIVERSE:
        try:
            ohlc_data = await get_ohlc(symbol)
            if not ohlc_data or len(ohlc_data['bars']) < 20:
                continue
            bars = ohlc_data['bars']

            pos1h, pos_gap, pos_entry = detect_fvg(bars, gap_threshold, positive=True)
            bars4h = aggregate_to_4h(bars)
            pos4h, _, _ = detect_fvg(bars4h, gap_threshold, positive=True)

            if pos1h > 0 or pos4h > 0:
                volume_boost = has_volume_boost(bars)
                flow = await get_flow(symbol)
                call_premium, whale_type = get_whale_premium(flow)
                if call_premium > 100000:
                    whale_status = "Whales ALL IN — JAY BE ALERT"
                elif call_premium > 25000:
                    whale_status = "Whales building — early conviction"
                else:
                    whale_status = "Whales not committed yet — monitor"
                positive_list.append({
                    'symbol': symbol, 'pos1h': pos1h, 'pos4h': pos4h, 'gap': pos_gap,
                    'boost': volume_boost, 'premium': call_premium, 'status': whale_status,
                    'type': whale_type, 'entry': pos_entry
                })

            neg1h, neg_gap, _ = detect_fvg(bars, gap_threshold, positive=False)
            neg4h, _, _ = detect_fvg(bars4h, gap_threshold, positive=False)
            if neg1h > 0 or neg4h > 0:
                negative_list.append({'symbol': symbol, 'neg1h': neg1h, 'neg4h': neg4h, 'gap': neg_gap})

            await asyncio.sleep(0.3)
        except Exception:
            continue

    if positive_list:
        message += "**Positive FVG Setups**\n\n"
        for item in sorted(positive_list, key=lambda x: x['premium'], reverse=True)[:20]:
            owned = " (OWNED)" if item['symbol'] in OWNED_STOCKS else ""
            boost = "Yes — conviction" if item['boost'] else "No"
            message += f"{item['symbol']}{owned} — FVG (1H:{item['pos1h']} 4H:{item['pos4h']} gap {item['gap']:.2f}%)\n"
            message += f"Volume Boost: {boost}\n"
            message += f"Whale: {item['status']} (${item['premium']:,} {item['type']})\n\n"

    if negative_list:
        message += "**Negative FVG — Rollover Risk**\n\n"
        for item in sorted(negative_list, key=lambda x: x['gap'], reverse=True)[:20]:
            owned = " (OWNED)" if item['symbol'] in OWNED_STOCKS else ""
            message += f"{item['symbol']}{owned} — Bearish FVG (1H:{item['neg1h']} 4H:{item['neg4h']} gap {item['gap']:.2f}%)\n\n"

    if not positive_list and not negative_list:
        message += "No significant FVGs detected this scan.\n"

    await send_discord(message)
    await darkpool_scan()

async def scheduler():
    await send_discord("Scanner started successfully! Running every 10 minutes during market hours. 🚀")
    while True:
        try:
            now = datetime.now(ZoneInfo("America/Chicago"))
            if 3 <= now.hour < 15 and now.weekday() < 5:
                await fvg_whale_scan()
                await check_rollovers()
                await sector_rotation()
        except Exception as e:
            await send_discord(f"Error in scanner: {str(e)}")
        await asyncio.sleep(600)

@app.get("/backtest")
async def backtest(days: int = 7):
    await send_discord(f"Backtest triggered for {days} days.")
    return {"status": "triggered"}

def start_scheduler():
    asyncio.run(scheduler())

if __name__ == "__main__":
    threading.Thread(target=start_scheduler, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
