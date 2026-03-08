from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from analysis import analyze_stock
from scanner import scan_market
from backtest import run_backtest
from scanner import scan_volume_spikes
import os
from datetime import datetime

app = FastAPI(title="Shunbao Trading Platform")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Endpoints
@app.get("/api/volume_monitor")
async def api_volume_monitor(threshold: int = 400):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] API VOLUME MONITOR REQUEST: threshold={threshold}")
    results = scan_volume_spikes(threshold)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] API VOLUME MONITOR RESULT: Found {len(results)} items")
    return {"results": results, "count": len(results)}

@app.get("/api/scan")
async def api_scan_market(min_price: float = 0, max_price: float = 10000, strategy: str = "ALL"):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] API SCAN REQUEST: min={min_price}, max={max_price}, strategy={strategy}")
    results = scan_market(min_price, max_price, strategy)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] API SCAN RESULT: Found {len(results)} items")
    return {"results": results, "count": len(results)}

@app.get("/api/analyze/{stock_code}")
async def api_analyze_stock(stock_code: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] API ANALYZE REQUEST: code={stock_code}")
    result = analyze_stock(stock_code)
    if "error" in result:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] API ANALYZE ERROR: {result['error']}")
        raise HTTPException(status_code=404, detail=result["error"])
        
    # Run backtest on the data
    backtest_result = run_backtest(result['data'])
    
    # Prepare response (remove full dataframe to save bandwidth, keep indicators and backtest)
    response = {
        "stock_code": result['stock_code'],
        "current_price": result['current_price'],
        "indicators": result['indicators'],
        "backtest": backtest_result,
        "strategy_summary": "Comprehensive Strategy: " + ("BUY" if result['data']['Strategy_Signal'].iloc[-1] == 1 else "SELL" if result['data']['Strategy_Signal'].iloc[-1] == -1 else "HOLD")
    }
    
    return response

# Serve Frontend
# We will serve the index.html for root and static files
# Ensure frontend directory exists
frontend_path = os.path.join(os.getcwd(), "frontend")
if not os.path.exists(frontend_path):
    os.makedirs(frontend_path)

app.mount("/static", StaticFiles(directory=frontend_path), name="static")

@app.get("/")
async def read_index():
    return FileResponse(os.path.join(frontend_path, "index.html"))
