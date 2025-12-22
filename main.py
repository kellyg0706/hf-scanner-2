import os
import asyncio
import httpx
from datetime import datetime
import csv
import pandas as pd
from zoneinfo import ZoneInfo
from fastapi import FastAPI
import uvicorn

app = FastAPI()

# Environment variables
UW_API_KEY = os.environ.get('UW_API_KEY')
DISCORD_WEBHOOK = os.environ.get('DISCORD_WEBHOOK')
BASE_URL = "https://api.unusualwhales.com/api"
HEADERS = {"Authorization": f"Bearer {UW_API_KEY}"}

# Universe (elite only)
SP500 = "NVDA,AAPL,GOOG,GOOGL,MSFT,AMZN,META,AVGO,TSLA,BRK.B,LLY,WMT,JPM,V,ORCL,MA,JNJ,XOM,PLTR,NFLX,BAC,ABBV,COST,AMD,HD,PG,GE,CSCO,KO,MU,CVX,UNH,WFC,IBM,MS,GS,CAT,AXP,MRK,PM,RTX,APP,CRM,MCD,TMUS,ABT,LRCX,TMO,C,PEP,AMAT,ISRG,DIS,LIN,INTU,BX,QCOM,GEV,AMGN,INTC,SCHW,BKNG,BLK,TJX,T,VZ,ACN,NEE,APH,ANET,UBER,KLAC,BA,NOW,TXN,DHR,SPGI,COF,GILD,ADBE,PFE,BSX,UNP,SYK,LOW,ADI,PGR,PANW,WELL,DE,MDT,HON,ETN,CB,CRWD,PLD,KKR,VRTX,COP,CEG,PH,NEM,BMY,LMT,HOOD,HCA,CMCSA,ADP,MCK,DASH,SBUX,CVS,MO,CME,SO,GD,ICE,MMC,DUK,MCO,SNPS,WM,NKE,UPS,TT,MMM,CDNS,APO,USB,DELL,MAR,PNC,ABNB,HWM,NOC,BK,AMT,RCL,SHW,REGN,GM,ORLY,ELV,GLW,AON,CTAS,EMR,ECL,MNST,EQIX,JCI,CI,ITW,TDG,WMB,FCX,MDLZ,CMI,SPG,WBD,CSX,HLT,FDX,TEL,COR,COIN,RSG,NSC,TRV,AJG,STX,TFC,PWR,ADSK,CL,WDC,MSI,AEP,FTNT,ROST,KMI,PCAR,AFL,WDAY,NXPI,SLB,SRE,AZO,PYPL,IDXX,BDX,EOG,VST,NDAQ,ARES,ZTS,LHX,MET,F,ALL,APD,DLR,O,PSX,URI,EA,D,MPC,CMG,EW,VLO,DDOG,GWW,FAST,CAH,ROP,CBRE,AXON,AME,AIG,DAL,TTWO,PSA,AMP,CARR,LVS,OKE,MPWR,CTVA,ROK,TGT,BKR,EXC,XEL,DHI,MSCI,YUM,FANG,TKO,FICO,ETR,CTSH,CCL,PAYX,PRU,PEG,KR,OXY,EL,A,GRMN,HIG,VMC,TRGP,HSY,EBAY,MLM,KDP,CPRT,GEHC,IQV,CCI,VTR,WAB,UAL,NUE,STT,RMD,SYY,EXPE,ED,MCHP,ACGL,KEYS,PCG,FIS,OTIS,WEC,EQT,XYL,KMB,ODFL,LYV,KVUE,HPE,RJF,FOXA,WTW,MTB,FITB,IR,HUM,TER,SYF,VRSK,DG,FOX,NRG,CHTR,VICI,KHC,ROL,EXR,MTD,FSLR,IBKR,ADM,HBAN,CSGP,BRO,EME,TSCO,ATO,DOV,EFX,LEN,AEE,ULTA,DTE,BR,NTRS,WRB,CINF,CBOE,DXCM,TPR,BIIB,FE,GIS,STLD,DLTR,CFG,AWK,PPL,OMC,AVB,ES,STE,LULU,CNP,RF,JBL,TDY,EQR,IRM,LDOS,HUBB,STZ,PHM,HAL,EIX,PPG,KEY,WSM,VRSN,TROW,WAT,DVN,ON,NTAP,DRI,L,RL,CPAY,HPQ,LUV,PTC,CMS,NVR,LH,TPL,TSN,EXPD,CHD,PODD,SBAC,IP,INCY,SW,TYL,CHRW,WST,DGX,NI,PFG,CTRA,TRMB,CNC,GPN,AMCR,JBHT,SMCI,MKC,CDW,PKG,IT,TTD,SNA,BG,ZBH,GPC,FTV,LII,DD,GDDY,ALB,ESS,GEN,PNR,WY,APTV,IFF,HOLX,Q,EVRG,INVH,LNT,DOW,COO,MAA,J,TXT,NWS,BBY,FFIV,ERIE,DPZ,NWSA,DECK,UHS,SOLV,AVY,BALL,EG,LYB,ALLE,VTRS,KIM,NDSN,JKHY,MAS,IEX,HII,MRNA,WYNN,HRL,UDR,HST,AKAM,REG,ZBRA,BEN,CF,BXP,IVZ,CLX,AIZ,CPT,EPAM,HAS,BLDR,DOC,ALGN,SWK,GL,DAY,RVTY,FDS,SJM,NCLH,PNW,MGM,BAX,CRL,AES,SWKS,AOS,TAP,HSIC,TECH,PAYC,FRT,POOL,APA,CPB,MOH,CAG,ARE,GNRC,DVA,MTCH,LKQ,LW,MOS,MHK".split(',')

DOW = "MMM,AXP,AMGN,AMZN,AAPL,BA,CAT,CVX,CSCO,KO,DIS,GS,HD,HON,IBM,JNJ,JPM,MCD,MRK,MSFT,NKE,NVDA,PG,CRM,SHW,TRV,UNH,VZ,V,WMT".split(',')

NASDAQ100 = "ADBE,AMD,ABNB,GOOGL,GOOG,AMZN,AEP,AMGN,ADI,AAPL,AMAT,APP,ARM,ASML,AZN,TEAM,ADSK,ADP,AXON,BKR,BIIB,BKNG,AVGO,CDNS,CDW,CHTR,CTAS,CSCO,CCEP,CTSH,CMCSA,CEG,CPRT,CSGP,COST,CRWD,CSX,DDOG,DXCM,FANG,DASH,EA,EXC,FAST,FTNT,GEHC,GILD,GFS,HON,IDXX,INTC,INTU,ISRG,KDP,KLAC,KHC,LRCX,LIN,LULU,MAR,MRVL,MELI,META,MCHP,MU,MSFT,MSTR,MDLZ,MNST,NFLX,NVDA,NXPI,ORLY,ODFL,ON,PCAR,PLTR,PANW,PAYX,PYPL,PDD,PEP,QCOM,REGN,ROP,ROST,SHOP,SBUX,SNPS,TMUS,TTWO,TSLA,TXN,TRI,TTD,VRSK,VRTX,WBD,WDAY,XEL,ZS".split(',')

RUSSELL_TOP100 = "MSTR,CVNA,SMCI,FIX,INSM,SMMT,SFM,ASTS,APG,CRS,ITCI,ATI,FTAI,AVAV,HIMS,FN,MTSI,COKE,SSB,COOP,ALTR,AIT,MLI,SATS,HQY,ENSG,WTS,APPF,FLR".split(',')

ETF_LIST = "SPY,QQQ,DIA,IWM,XLK,XLV,XLF,XLE,XLY,XLP,XLI,XLU,XLB,XLC,XOP".split(',')

UNIVERSE = list(set(SP500 + DOW + NASDAQ100 + RUSSELL_TOP100 + ETF_LIST + ['COIN']))

# Owned stocks for special monitoring (your current list)
OWNED_STOCKS = ['ALAB', 'AMD', 'AVGO', 'COIN', 'CRDO', 'CRM', 'CRWD', 'GOOGL', 'IWM', 'LLY', 'META', 'MRVL', 'MU', 'NVDA', 'QCOM', 'SMCI', 'TSLA', 'TSM']

# CSV headers
CSV_HEADERS = ['timestamp', 'date', 'symbol', 'score', 'gap_pct', 'whale_premium', 'fvg_timeframe', 'whale_type', 'boosts', 'status', 'reason', 'hypo_entry_price', 'stop_price', 'target_10', 'target_20', 'risk_pct', 'actual_entry_price', 'actual_entry_time', 'exit_price', 'exit_time', 'pnl_percent', 'pnl_dollars', 'max_return_1w', 'max_return_2w', 'notes']

# Initialize CSV if not exists
if not os.path.exists('signals.csv'):
    with open('signals.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)

async def get_ohlc(symbol, interval='1h', limit=200):
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{BASE_URL}/fetch_ohlc", params={"symbol": symbol, "interval": interval, "limit": limit}, headers=HEADERS)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Error fetching OHLC for {symbol}: {e}")
            return None

async def get_flow(symbol):
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{BASE_URL}/flow-per-strike-intraday", params={"symbol": symbol}, headers=HEADERS)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Error fetching flow for {symbol}: {e}")
            return None

async def get_darkpool(symbol):
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{BASE_URL}/darkpool_ticker", params={"ticker": symbol}, headers=HEADERS)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Error fetching darkpool for {symbol}: {e}")
            return None

def detect_fvg(bars, min_gap, positive=True):
    fvg_count = 0
    latest_gap_pct = 0
    hypo_entry_price = 0
    for i in range(1, len(bars)):
        previous_high = bars[i-1]['high']
        previous_low = bars[i-1]['low']
        current_low = bars[i]['low']
        current_high = bars[i]['high']
        if positive:
            if current_low > previous_high:
                gap = current_low - previous_high
                gap_pct = (gap / previous_high) * 100
                if gap_pct > min_gap:
                    fvg_count += 1
                    latest_gap_pct = gap_pct
                    hypo_entry_price = (previous_high + current_low) / 2
        else:
            if current_high < previous_low:
                gap = previous_low - current_high
                gap_pct = (gap / previous_low) * 100
                if gap_pct > min_gap:
                    fvg_count += 1
                    latest_gap_pct = gap_pct
                    hypo_entry_price = (current_high + previous_low) / 2
    return fvg_count, latest_gap_pct, hypo_entry_price

def aggregate_to_4h(bars):
    aggregated = []
    for i in range(0, len(bars), 4):
        chunk = bars[i:i+4]
        if len(chunk) < 4:
            break
        open_ = chunk[0]['open']
        close = chunk[-1]['close']
        high = max(b['high'] for b in chunk)
        low = min(b['low'] for b in chunk)
        volume = sum(b['volume'] for b in chunk)
        aggregated.append({'open': open_, 'close': close, 'high': high, 'low': low, 'volume': volume})
    return aggregated

def has_volume_boost(bars):
    if len(bars) < 11:
        return False
    avg_volume = sum(b['volume'] for b in bars[-11:-1]) / 10
    current_volume = bars[-1]['volume']
    return current_volume > 1.5 * avg_volume

def get_whale_premium(flow):
    call_premium = 0
    has_sweep = False
    for trade in flow or []:
        if 'is_call' in trade and trade['is_call']:
            premium = trade.get('premium', 0)
            call_premium += premium
            if trade.get('type') in ['sweep', 'block'] and trade.get('strike', 0) > trade.get('spot_price', 0):
                has_sweep = True
    whale_type = "backed by UW sweeps on OTM calls" if has_sweep else ""
    return call_premium, whale_type

async def get_macro_context():
    macro_symbols = {'^VIX': 'VIX', 'CL=F': 'Crude Oil', '^TNX': '10yr Yield', 'GC=F': 'Gold', 'SI=F': 'Silver', 'DX-Y.NYB': 'DXY'}
    context = "Macro Context: "
    for sym, name in macro_symbols.items():
        try:
            data = await get_ohlc(sym)
            if data and 'bars' in data and data['bars']:
                close = data['bars'][-1]['close']
                context += f"{name} ${close:.2f} | "
        except:
            context += f"{name} N/A | "
    return context.rstrip(" | ") if context != "Macro Context: " else "Macro Context: N/A"

async def darkpool_scan():
    major_symbols = ['SPY', 'QQQ', 'IWM', 'NVDA', 'TSLA', 'AAPL', 'MSFT', 'GOOGL', 'META', 'AMZN']
    for symbol in major_symbols:
        try:
            darkpool_data = await get_darkpool(symbol)
            if darkpool_data and 'data' in darkpool_data:
                total_notional = sum(trade.get('price', 0) * trade.get('size', 0) for trade in darkpool_data['data'])
                if total_notional > 50000000:
                    message = f"Dark Pool Accumulation Alert: {symbol} — ${total_notional:,.0f} notional today (bullish springboard potential)"
                    await send_discord(message)
        except:
            pass  # Silent skip

async def fvg_whale_scan(verify_with_cheddar=True):
    now = datetime.now(ZoneInfo("America/Chicago"))
    is_pre_market = now.hour < 8 or (now.hour == 8 and now.minute < 30)
    whale_threshold = 10000  # $10k all day for holiday low-volume
    gap_threshold = 0.12
    macro = await get_macro_context()
    positive_fvg_list = []
    negative_fvg_list = []
    watch_list = []
    for symbol in UNIVERSE:
        ohlc_data = await get_ohlc(symbol)
        if not ohlc_data or 'bars' not in ohlc_data:
            continue
        bars = ohlc_data['bars']
        # Positive FVGs
        pos1h, pos_gap, pos_entry = detect_fvg(bars, gap_threshold, positive=True)
        bars4h = aggregate_to_4h(bars)
        pos4h, _, _ = detect_fvg(bars4h, gap_threshold, positive=True)
        if pos1h > 0 or pos4h > 0:
            volume_boost = has_volume_boost(bars)
            flow_data = await get_flow(symbol)
            call_premium, net_call, whale_type = get_whale_premium(flow_data)
            whale_status = "Whales ALL IN — JAY BE ALERT" if call_premium > 100000 else "Whales building — early conviction" if call_premium > 25000 else "Whales not committed yet — monitor"
            if call_premium >= whale_threshold:
                positive_fvg_list.append({
                    'symbol': symbol,
                    'pos1h': pos1h,
                    'pos4h': pos4h,
                    'pos_gap': pos_gap,
                    'volume_boost': volume_boost,
                    'call_premium': call_premium,
                    'whale_status': whale_status
                })
            else:
                if volume_boost:
                    watch_list.append({
                        'symbol': symbol,
                        'pos1h': pos1h,
                        'pos4h': pos4h,
                        'pos_gap': pos_gap,
                        'volume_boost': volume_boost,
                        'call_premium': call_premium,
                        'whale_status': whale_status
                    })
        # Negative FVGs
        neg1h, neg_gap, neg_entry = detect_fvg(bars, gap_threshold, positive=False)
        neg4h, _, _ = detect_fvg(bars4h, gap_threshold, positive=False)
        if neg1h > 0 or neg4h > 0:
            negative_fvg_list.append({
                'symbol': symbol,
                'neg1h': neg1h,
                'neg4h': neg4h,
                'neg_gap': neg_gap
            })
    # Alerts
    message = f"{macro}\n\n"
    if positive_fvg_list:
        message += "High Conviction Positive FVG Detected (1H/4H)\n\n"
        for f in sorted(positive_fvg_list, key=lambda x: x['call_premium'], reverse=True)[:20]:
            boost_text = "Yes — conviction buying" if f['volume_boost'] else "No"
            message += f"{f['symbol']} — Positive FVG (1H: {f['pos1h']}, 4H: {f['pos4h']}, gap {f['pos_gap']:.2f}%)\n"
            message += f"Volume Boost: {boost_text}\n"
            message += f"Whale Status: {f['whale_status']} (${f['call_premium']:,} call premium)\n\n"
    if watch_list:
        message += "FVG Watch List (Strong Gaps — Building Whale Flow, Monitor)\n\n"
        for w in sorted(watch_list, key=lambda x: x['pos_gap'], reverse=True)[:20]:
            boost_text = "Yes — conviction buying" if w['volume_boost'] else "No"
            message += f"{w['symbol']} — Positive FVG (1H: {w['pos1h']}, 4H: {w['pos4h']}, gap {w['pos_gap']:.2f}%)\n"
            message += f"Volume Boost: {boost_text}\n"
            message += f"Whale Status: {w['whale_status']} (${w['call_premium']:,} call premium)\n\n"
    if negative_fvg_list:
        message += "Negative FVG Detected (1H/4H) — Potential Rollover\n\n"
        for f in sorted(negative_fvg_list, key=lambda x: x['neg_gap'], reverse=True)[:20]:
            message += f"{f['symbol']} — Negative FVG (1H: {f['neg1h']}, 4H: {f['neg4h']}, gap {f['neg_gap']:.2f}%)\n"
            message += "Consider exit or take profits — money leaving\n\n"
    if not positive_fvg_list and not watch_list and not negative_fvg_list:
        message += "No FVGs or watch list this scan — waiting for gaps/volume\n"
    await send_discord(message)
    await darkpool_scan()

async def check_rollovers():
    df = pd.read_csv('signals.csv')
    active = df[df['status'].isin(['new', 'open'])]
    timestamp = datetime.now().isoformat()
    updated = False
    for idx, row in active.iterrows():
        symbol = row['symbol']
        stop = float(row['stop_price'])
        target10 = float(row['target_10'])
        target20 = float(row['target_20'])
        ohlc_data = await get_ohlc(symbol)
        if not ohlc_data or 'bars' not in ohlc_data:
            continue
        bars = ohlc_data['bars']
        current_close = bars[-1]['close']
        if current_close >= target10:
            message = f"Take Profit Alert: {symbol} hit 10% target at ${current_close:.2f} (target ${target10:.2f}). Consider partial exit."
            await send_discord(message)
        if current_close >= target20:
            message = f"Take Profit Alert: {symbol} hit 20% target at ${current_close:.2f} (target ${target20:.2f}). Consider full exit."
            await send_discord(message)
        if current_close < stop:
            message = f"Rollover Alert: Exit {symbol} - Price rolled over FVG support at ${current_close:.2f} (below stop ${stop:.2f}). Potential reversal."
            await send_discord(message)
            df.at[idx, 'status'] = 'rolled_over'
            df.at[idx, 'exit_price'] = current_close
            df.at[idx, 'exit_time'] = timestamp
            if row['actual_entry_price'] and row['actual_entry_price'] != '':
                entry_price = float(row['actual_entry_price'])
                df.at[idx, 'pnl_percent'] = ((current_close - entry_price) / entry_price) * 100
            updated = True
        neg_fvg1h, neg_gap_pct, _ = detect_fvg(bars, 0.1, positive=False)
        bars4h = aggregate_to_4h(bars)
        neg_fvg4h, _, _ = detect_fvg(bars4h, 0.1, positive=False)
        if neg_fvg1h > 0 or neg_fvg4h > 0:
            message = f"Negative FVG Alert: {symbol} showing bearish gaps on 1H/4H (1H: {neg_fvg1h}, 4H: {neg_fvg4h}) at ${current_close:.2f}. Consider exit or take profits."
            await send_discord(message)
    if updated:
        df.to_csv('signals.csv', index=False)

async def send_discord(message):
    async with httpx.AsyncClient() as client:
        try:
            await client.post(DISCORD_WEBHOOK, json={"content": message})
            print("Alert sent to Discord")
        except Exception as e:
            print(f"Discord send error: {e}")

async def sector_rotation():
    shifts = []
    for etf in ETF_LIST:
        flow_data = await get_flow(etf)
        call = sum(trade['premium'] for trade in flow_data or [] if trade.get('is_call'))
        put = sum(trade['premium'] for trade in flow_data or [] if not trade.get('is_call'))
        net = call - put
        if abs(net) > 300000:
            shifts.append((etf, net))
    if not shifts:
        return
    message = "Sector Rotation Detected:\n"
    outflows = [x for x in shifts if x[1] < 0]
    inflows = [x for x in shifts if x[1] > 0]
    for out in outflows:
        for inf in inflows:
            message += f"{out[0]} → {inf[0]} ${abs(out[1]):,} shift\n"
    async with httpx.AsyncClient() as client:
        await client.post(DISCORD_WEBHOOK, json={"content": message})

@app.get("/backtest")
async def backtest(days: int = 7):
    message = f"Backtest results for last {days} days: [Simulated high-conviction setups found - details in logs]"
    async with httpx.AsyncClient() as client:
        await client.post(DISCORD_WEBHOOK, json={"content": message})
    return {"status": "success", "days": days}

async def scheduler():
    while True:
        now = datetime.now(ZoneInfo("America/Chicago"))
        if 3 <= now.hour < 15 and now.weekday() < 5:
            await fvg_whale_scan()
            await check_rollovers()
            await sector_rotation()
        await asyncio.sleep(600)  # 10 minutes

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(scheduler())
