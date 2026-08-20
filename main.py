"""
Companies House Real-Time Monitor - Single Service Version
"""

import os
import json
import asyncio
import httpx
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import aiosqlite

# Configuration
API_KEY = os.environ.get("API_KEY", "")
SSE_URL = os.environ.get("SSE_URL", "https://stream.companieshouse.gov.uk/companies")
DATABASE_FILE = os.environ.get("DATABASE_FILE", "/data/companies.db")
PORT = int(os.environ.get("PORT", "8000"))

# Target SIC codes
TARGET_SIC_CODES = {
    "62011", "62012", "62020", "62030", "62090",
    "63110", "63120", "63910", "63990",
    "64999", "66190", "66220", "66300",
    "70229", "72110", "72190", "72200",
    "73110", "73120", "73200",
    "74100", "74200", "74300", "74900",
    "82990", "85590", "86900", "87900",
    "90030", "91010", "91020", "93290",
}

BUZZWORDS = [" AI"]

# Global state
sse_clients: List[asyncio.Queue] = []
db_conn: Optional[aiosqlite.Connection] = None
companies_seen = 0
companies_matched = 0


def matches_criteria(data: Dict[str, Any]) -> Optional[str]:
    company_name = data.get("company_name", "")
    sic_codes = data.get("sic_codes", [])
    sic_codes_str = [str(sic) for sic in sic_codes]
    
    for sic in sic_codes_str:
        if sic in TARGET_SIC_CODES:
            return "target_sic"
    
    for buzzword in BUZZWORDS:
        if buzzword in company_name:
            return "buzzword"
    
    return None


async def get_db_connection():
    db_path = Path(DATABASE_FILE)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = await aiosqlite.connect(str(db_path))
    conn.row_factory = aiosqlite.Row
    
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS screened_companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_number TEXT UNIQUE NOT NULL,
            company_name TEXT NOT NULL,
            incorporation_date TEXT NOT NULL,
            sic_codes TEXT,
            source_type TEXT NOT NULL,
            published_at TEXT NOT NULL
        )
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_published_at 
        ON screened_companies(published_at DESC)
    """)
    
    return conn


async def process_stream():
    global db_conn, companies_seen, companies_matched
    
    timepoint_file = Path("/data/timepoint.txt")
    timepoint_file.parent.mkdir(parents=True, exist_ok=True)
    
    last_timepoint = None
    if timepoint_file.exists():
        last_timepoint = timepoint_file.read_text().strip()
        print(f"Loaded timepoint: {last_timepoint}", flush=True)
    
    print("Starting stream processor...", flush=True)
    
    while True:
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                auth = (API_KEY, "")
                headers = {"Accept": "application/json"}
                
                if last_timepoint:
                    headers["Last-Event-ID"] = last_timepoint
                
                print(f"Connecting to stream: {SSE_URL}", flush=True)
                
                async with client.stream(
                    "GET",
                    SSE_URL,
                    auth=auth,
                    headers=headers,
                ) as response:
                    if response.status_code != 200:
                        print(f"Stream connection failed: {response.status_code}", flush=True)
                        await asyncio.sleep(5)
                        continue
                    
                    print("Stream connected successfully", flush=True)
                    
                    async for line in response.aiter_lines():
                        if not line or line.startswith("id: "):
                            continue
                        
                        # Parse JSON directly (no "data: " prefix)
                        try:
                            data = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        
                        companies_seen += 1
                        
                        # Extract from nested "data" key
                        company_data = {
                            "company_number": data.get("data", {}).get("company_number", ""),
                            "company_name": data.get("data", {}).get("company_name", ""),
                            "incorporation_date": data.get("data", {}).get("date_of_creation", ""),
                            "sic_codes": data.get("data", {}).get("sic_codes", []),
                            "type": data.get("data", {}).get("type", ""),
                        }
                        
                        if companies_seen <= 20 or companies_seen % 20 == 0:
                            print(f"DEBUG #{companies_seen}: {company_data['company_number']} - {company_data['company_name']}", flush=True)
                            print(f"  SIC: {company_data['sic_codes']}", flush=True)
                        
                        source_type = matches_criteria(company_data)
                        
                        if source_type:
                            companies_matched += 1
                            print(f"MATCH #{companies_matched}: {company_data['company_number']} - {company_data['company_name']} ({source_type})", flush=True)
                            
                            published_at = datetime.now(timezone.utc).isoformat()
                            
                            try:
                                await db_conn.execute(
                                    """
                                    INSERT OR REPLACE INTO screened_companies 
                                    (company_number, company_name, incorporation_date, sic_codes, source_type, published_at)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        company_data["company_number"],
                                        company_data["company_name"],
                                        company_data["incorporation_date"],
                                        json.dumps(company_data["sic_codes"]),
                                        source_type,
                                        published_at,
                                    ),
                                )
                                print(f"Stored: {company_data['company_number']} - {company_data['company_name']}", flush=True)
                                
                                notification = {
                                    "type": "new_company",
                                    "data": {
                                        "company_number": company_data["company_number"],
                                        "company_name": company_data["company_name"],
                                        "sic_codes": company_data["sic_codes"],
                                        "source_type": source_type,
                                        "published_at": published_at,
                                    },
                                }
                                
                                for queue in sse_clients:
                                    await queue.put(notification)
                            
                            except Exception as e:
                                print(f"Database error: {e}", flush=True)
                        
                        # Update timepoint
                        event = data.get("event", {})
                        if "timepoint" in event:
                            last_timepoint = str(event["timepoint"])
                            timepoint_file.write_text(last_timepoint)
        
        except Exception as e:
            print(f"Stream error: {e}", flush=True)
            print("Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)


# FastAPI App
app = FastAPI(title="Companies House Monitor")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def startup():
    global db_conn
    
    print("Starting Companies House Monitor...", flush=True)
    db_conn = await get_db_connection()
    print(f"Database initialized: {DATABASE_FILE}", flush=True)
    
    asyncio.create_task(process_stream())


@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/metrics")
async def metrics():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cursor = await db_conn.execute(
        "SELECT COUNT(*) FROM screened_companies WHERE DATE(published_at) = ?",
        (today,)
    )
    row = await cursor.fetchone()
    return {"target_sic_buzzword_count": row[0] if row else 0}


@app.get("/api/companies")
async def list_companies(limit: int = 100):
    cursor = await db_conn.execute(
        """
        SELECT company_number, company_name, sic_codes, source_type, published_at
        FROM screened_companies
        ORDER BY published_at DESC
        LIMIT ?
        """,
        (limit,)
    )
    rows = await cursor.fetchall()
    
    companies = []
    for row in rows:
        companies.append({
            "company_number": row[0],
            "company_name": row[1],
            "sic_codes": json.loads(row[2]) if row[2] else [],
            "source_type": row[3],
            "published_at": row[4],
        })
    
    return {"companies": companies, "count": len(companies)}


@app.get("/stream")
async def sse_stream():
    queue = asyncio.Queue()
    sse_clients.append(queue)
    
    async def generate():
        try:
            while True:
                notification = await queue.get()
                yield f"data: {json.dumps(notification)}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            sse_clients.remove(queue)
    
    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Companies House Monitor</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; padding: 20px; }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 20px; }
        .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .metric-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .metric-label { color: #666; font-size: 14px; margin-bottom: 8px; }
        .metric-value { font-size: 32px; font-weight: bold; color: #22c55e; }
        .connection-status { display: inline-flex; align-items: center; gap: 8px; padding: 8px 16px; background: #f0fdf4; border-radius: 20px; font-size: 14px; color: #16a34a; }
        .status-dot { width: 8px; height: 8px; border-radius: 50%; background: #22c55e; animation: pulse 2s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .companies-table { background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow: hidden; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px 16px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f9fafb; font-weight: 600; color: #374151; }
        tr.new-row { background: #dcfce7; }
        .sic-badge { display: inline-block; padding: 4px 8px; background: #dbeafe; color: #1e40af; border-radius: 4px; font-size: 12px; margin-right: 4px; }
        .type-badge.target_sic { background: #dcfce7; color: #166534; padding: 4px 8px; border-radius: 4px; font-size: 12px; }
        .type-badge.buzzword { background: #fef3c7; color: #92400e; padding: 4px 8px; border-radius: 4px; font-size: 12px; }
        .links a { color: #3b82f6; text-decoration: none; margin-right: 12px; }
        .copy-btn { background: #f3f4f6; border: none; padding: 4px 8px; border-radius: 4px; cursor: pointer; margin-left: 8px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏢 Companies House Monitor</h1>
        <div class="metrics">
            <div class="metric-card">
                <div class="metric-label">Status</div>
                <div class="connection-status"><span class="status-dot" id="statusDot"></span><span id="statusText">Connecting...</span></div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Target SIC + Buzzword (Today)</div>
                <div class="metric-value" id="targetCount">-</div>
            </div>
        </div>
        <div class="companies-table">
            <table>
                <thead><tr><th>Number</th><th>Name</th><th>SIC</th><th>Type</th><th>Time</th><th>Links</th></tr></thead>
                <tbody id="companiesTable"><tr><td colspan="6" style="text-align:center;padding:40px;">Loading...</td></tr></tbody>
            </table>
        </div>
    </div>
    <script>
        let es = null;
        function timeAgo(ts) {
            const d = Math.floor((new Date() - new Date(ts)) / 1000);
            if (d < 60) return d + 's';
            if (d < 3600) return Math.floor(d / 60) + 'm';
            return Math.floor(d / 3600) + 'h';
        }
        function setStatus(ok) {
            document.getElementById('statusDot').style.opacity = ok ? '1' : '0.5';
            document.getElementById('statusText').textContent = ok ? 'Live' : 'Disconnected';
        }
        async function loadMetrics() {
            try {
                const r = await fetch('/api/metrics');
                const d = await r.json();
                document.getElementById('targetCount').textContent = d.target_sic_buzzword_count;
            } catch(e) {}
        }
        async function loadCompanies() {
            try {
                const r = await fetch('/api/companies?limit=100');
                const d = await r.json();
                const tb = document.getElementById('companiesTable');
                if (!d.companies.length) { tb.innerHTML = '<tr><td colspan="6" style="text-align:center;padding:40px;">No companies yet</td></tr>'; return; }
                tb.innerHTML = d.companies.map(c => {
                    const sics = c.sic_codes.map(s => `<span class="sic-badge">${s}</span>`).join('');
                    const tc = c.source_type === 'target_sic' ? 'target_sic' : 'buzzword';
                    const tl = c.source_type === 'target_sic' ? 'Target SIC' : 'Buzzword';
                    return `<tr><td style="font-family:monospace">${c.company_number}<button class="copy-btn" onclick="navigator.clipboard.writeText('${c.company_name.replace(/'/g, "\\'")}')">Copy</button></td><td>${c.company_name}</td><td>${sics}</td><td><span class="type-badge ${tc}">${tl}</span></td><td>${timeAgo(c.published_at)}</td><td><a href="https://find-and-update.company-information.service.gov.uk/company/${c.company_number}">CH</a> <a href="https://google.com/search?q=${encodeURIComponent(c.company_name)}">Google</a></td></tr>`;
                }).join('');
            } catch(e) {}
        }
        function connect() {
            es = new EventSource('/stream');
            es.onopen = () => setStatus(true);
            es.onmessage = e => { const n = JSON.parse(e.data); if (n.type === 'new_company') { loadCompanies(); loadMetrics(); } };
            es.onerror = () => { setStatus(false); es.close(); setTimeout(connect, 5000); };
        }
        loadMetrics(); loadCompanies();
        setInterval(loadMetrics, 5000);
        connect();
    </script>
</body>
</html>
    """


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
