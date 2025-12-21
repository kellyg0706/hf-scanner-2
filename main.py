import os
import asyncio
import httpx
from datetime import datetime, timedelta
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
SP500 = "NVDA,AAPL,GOOG,GOOGL,MSFT,AMZN,META,AVGO,TSLA,BRK.B,LLY,WMT,JPM,V,ORCL,MA,JNJ,XOM,PLTR,NFLX,BAC,ABBV,COST,AMD,HD,PG,GE,CSCO,KO,MU,CVX,UNH,WFC,IBM,MS,GS,CAT,AXP,MRK,PM,RTX,APP,CRM,MCD,TMUS,ABT,LRCX,TMO,C,PEP,AMAT,ISRG,DIS,LIN,INTU,BX,QCOM,GEV,AMGN,INTC,SCHW,BKNG,BLK,TJX,T,VZ,ACN,NEE,APH,ANET,UBER,KLAC,BA,NOW,TXN,DHR,SPGI,COF,GILD,ADBE,PFE,BSX,UNP,SYK,LOW,ADI,PGR,PANW,WELL,DE,MDT,HON,ETN,CB,CRWD,PLD,KKR,VRTX,COP,CEG,PH,NEM,BMY,LMT,HOOD,HCA,CMCSA,ADP,MCK,DASH,SBUX,CVS,MO,CME,SO,GD,ICE,MMC,DUK,MCO,SNPS,WM,NKE,UPS,TT,MMM,CDNS,APO,USB,DELL,MAR,PNC,ABNB,HWM,NOC,BK,AMT,RCL,SHW,REGN,GM,ORLY,ELV,GLW,AON,CTAS,EMR,ECL,MNST,EQIX,JCI,CI,ITW,TDG,WMB,FCX,MDLZ,CMI,SPG,WBD,CSX,HLT,FDX,TEL,COR,COIN,RSG,NSC,TRV,AJG,STX,TFC,PWR,ADSK,CL,WDC,MSI,AEP,FTNT,ROST,KMI,PCAR,AFL,WDAY,NXPI,SLB,SRE,AZO,PYPL,IDXX,BDX,EOG,VST,NDAQ,ARES,ZTS,LHX,MET,F,ALL,APD,DLR,O,PSX,URI,EA,D,MPC,CMG,EW,VLO,DDOG,GWW,FAST,CAH,ROP,CBRE,AXON,AME,AIG,DAL,TTWO,PSA,AMP,CARR,LVS,OKE,MPWR,CTVA,ROK,TGT,BKR,EXC,XEL,DHI,MSCI,YUM,FANG,TKO,FICO,ETR,CTSH,CCL,PAYX,PRU,PEG,KR,OXY,EL,A,GRMN,HIG,VMC,TRGP,HSY,EBAY,MLM,KDP,CPRT,GEHC,IQV,CCI,VTR,WAB,UAL,NUE,STT,RMD,SYY,EXPE,ED,MCHP,ACGL,KEYS,PCG,FIS,OTIS,WEC,EQT,XYL,KMB,ODFL,LYV,KVUE,HPE,RJF,FOXA,WTW,MTB,FITB,IR,HUM,TER,SYF,VRSK,DG,FOX,NRG,CHTR,VICI,KHC,ROL,EXR,MTD,FSLR,IBKR,ADM,HBAN,CSGP,BRO,EME,TSCO,ATO,DOV,EFX,LEN,AEE,ULTA,DTE,BR,NTRS,WRB,CINF,CBOE,DXCM,TPR,BIIB,FE,GIS,STLD,DLTR,CFG,AWK,PPL,OMC,AVB,ES,STE,LULU,CNP,RF,JBL,TDY,EQR,IRM,LDOS,HUBb,STZ,PHM,HAL,EIX,PPG,KEY,WSM,VRSN,TROW,WAT,DVN,ON,NTAP,DRI,L,RL,CPAY,HPQ,LUV,PTC,CMS,NVR,LH,TPL,TSN,EXPD,CHD,PODD,SBAC,IP,INCY,SW,TYL,CHRW,WST,DGX,NI,PFG,CTRA,TRMB,CNC,GPN,AMCR,JBHT,SMCI,MKC,CDW,PKG,IT,TTD,SNA,BG,ZBH,GPC,FTV,LII,DD,GDDY,ALB,ESS,GEN,PNR,WY,APTV,IFF,HOLX,Q,EVRG,INVH,LNT,DOW,COO,MAA,J,TXT,NWS,BBY,FFIV,ERIE,DPZ,NWSA,DECK,UHS,SOLV,AVY,BALL,EG,LYb,ALLE,VTRS,KIM,NDSN,JKHY,MAS,IEX,HII,MRNA,WYNN,HRL,UDR,HST,AKAM,REG,ZBRA,BEN,CF,BXP,IVZ,CLX,AIZ,CPT,EPAM,HAS,BLDR,DOC,ALGN,SWK,GL,DAY,RVTY,FDS,Sjm,NCLH,PNW,MGM,BAX,CRL,AES,SWKS,AOS,TAP,HSIC,TECH,PAYc,FRT,POOL,APA,CPB,MOH,CAG,ARE,GNRC,DVA,MTCH,LKQ,LW,MOS,MHK".split(',')

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

def detect_fvg(bars, min_gap, positive=True):
    count = 0
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
                    count += 1
                    latest_gap_pct = gap_pct
                    hypo_entry_price = (previous_high + current_low) / 2
        else:
            if current_high < previous_low:
                gap = previous_low - current_high
                gap_pct = (gap / previous_low) * 100
                if gap_pct > min_gap:
                    count += 1
                    latest_gap_pct = gap_pct
                    hypo_entry_price = (current_high + previous_low) / 2
    return count, latest_gap_pct, hypo_entry_price

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

async def fvg_whale_scan(verify_with_cheddar=True):
    setups = []
    for symbol in UNIVERSE:
        ohlc_data = await get_ohlc(symbol)
        if not ohlc_data or 'bars' not in ohlc_data:
            continue
        bars = ohlc_data['bars']
        min_gap = 0.05 if symbol in ETF_LIST else 0.1
        fvg1h, gap_pct, hypo_entry = detect_fvg(bars, min_gap, positive=True)
        if gap_pct < 0.2:
            continue
        bars4h = aggregate_to_4h(bars)
        fvg4h, _, _ = detect_fvg(bars4h, min_gap, positive=True)
        flow_data = await get_flow(symbol)
        whale, whale_type = get_whale_premium(flow_data)
        if whale < 200000:
            continue
        volume_boost = has_volume_boost(bars)
        score = gap_pct * 1500 + whale / 10000 + (20 if volume_boost else 0)
        if score < 60:
            continue
        reason = "Strong gap + whale confirmation — bounce potential 10-20%+" if score > 80 else "Decent setup with whale flow"
        setups.append({
            'symbol': symbol,
            'score': score,
            'gap_pct': gap_pct,
            'whale_premium': whale,
            'fvg1h': fvg1h,
            'fvg4h': fvg4h,
            'whale_type': whale_type,
            'volume_boost': volume_boost,
            'hypo_entry_price': hypo_entry,
            'reason': reason
        })
    setups = sorted(setups, key=lambda x: x['score'], reverse=True)[:20]
    if not setups:
        message = "No high-conviction FVGs this scan — waiting for whale/volume"
    else:
        message = "Top 20 FVG Setups (Detailed Analysis)\n\n"
        for i, s in enumerate(setups, 1):
            boost_text = "Yes — conviction buying" if s['volume_boost'] else "No"
            entry = s['hypo_entry_price']
            stop = entry * 0.985
            target10 = entry * 1.1
            target20 = entry * 1.2
            message += f"{i}. {s['symbol']} - Score {s['score']:.1f}\n"
            message += f"FVG Structure: {s['fvg1h']} positive on 1H, {s['fvg4h']} on 4H — bullish gaps holding as support\n"
            message += f"Whale Premium: ${s['whale_premium']:,} in call flow ({s['whale_type']})\n"
            message += f"Volume Boost: {boost_text}\n"
            message += f"Why #{i}: {s['reason']}\n"
            message += f"Entry ~${entry:,.0f} | Stop ${stop:,.0f} (1.5%) | Target 10% ${target10:,.0f} | 20% ${target20:,.0f}\n"
            if verify_with_cheddar:
                message += f"Verification: Cross-check Cheddar Flow for similar call sweeps on {s['symbol']} – log in or search 'unusual options activity {s['symbol']} Cheddar Flow'\n\n"
            else:
                message += "\n"
    async with httpx.AsyncClient() as client:
        try:
            await client.post(DISCORD_WEBHOOK, json={"content": message})
            print("Alert sent to Discord")
        except Exception as e:
            print(f"Discord send error: {e}")
    timestamp = datetime.now().isoformat()
    date = datetime.now().date()
    with open('signals.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        for s in setups:
            entry = s['hypo_entry_price']
            stop = entry * 0.985
            target10 = entry * 1.1
            target20 = entry * 1.2
            boosts = 'volume' if s['volume_boost'] else ''
            writer.writerow([timestamp, date, s['symbol'], s['score'], s['gap_pct'], s['whale_premium'], '1H/4H', s['whale_type'], boosts, 'new', s['reason'], entry, stop, target10, target20, 1.5, '', '', '', '', '', '', '', '', ''])

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
        # Take profits
        if current_close >= target10:
            message = f"Take Profit Alert: {symbol} hit 10% target at ${current_close:.2f} (target ${target10:.2f}). Consider partial exit."
            await send_discord(message)
        if current_close >= target20:
            message = f"Take Profit Alert: {symbol} hit 20% target at ${current_close:.2f} (target ${target20:.2f}). Consider full exit."
            await send_discord(message)
        # Rollover
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
        # Negative FVG detection
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

# (rest of the script continues as is — sector_rotation, backtest, scheduler)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(scheduler())
