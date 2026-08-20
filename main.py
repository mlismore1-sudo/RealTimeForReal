"""
Companies House Real-Time Monitor - Production Version
Filters by incorporation date (today's date)
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

# Target SIC codes with grouping
SIC_GROUPS = {
    "Prime Tech": {"62012", "58290", "72110"},
    "Tech": {"58210", "61100", "61200", "61300", "61900", "62011", "62030", "62090", "63110", "63120", "71200", "72190", "72200", "71129"},
    "Holdings": {"64201", "64202", "64203", "64204", "64205", "64209", "66300"},
    "Financial Services": {"66300", "64304", "64303"},
    "Space & Aviation": {"30300", "72190", "33160"},
}

ALL_TARGET_SICS = set()
for group_sics in SIC_GROUPS.values():
    ALL_TARGET_SICS.update(group_sics)

SIC_TO_GROUP = {}
for group_name, group_sics in SIC_GROUPS.items():
    for sic in group_sics:
        SIC_TO_GROUP[sic] = group_name

# Buzzwords - case insensitive matching
BUZZWORDS = [
    "UK", "Group", "Labs", "Technologies", "Tech", "Capital",
    " AI", "Europe", "EMEA", "Inc", "Asset", "Assets",
    "Partners", "Ventures", "Investments", "Equity", "Marine", "Yacht"
]

sse_clients: List[asyncio.Queue] = []
db_conn: Optional[aiosqlite.Connection] = None
companies_seen = 0
companies_matched = 0
buzzword_matches = 0
target_sic_matches = 0


def get_sic_group(sic_code: str) -> Optional[str]:
    return SIC_TO_GROUP.get(str(sic_code))


def matches_criteria(data: Dict[str, Any]) -> Optional[str]:
    """Check if company matches target SIC or buzzwords"""
    global buzzword_matches, target_sic_matches
    
    company_name = data.get("company_name", "")
    sic_codes = data.get("sic_codes", [])
    sic_codes_str = [str(sic) for sic in sic_codes]
    
    # Priority 1: Check target SIC codes
    for sic in sic_codes_str:
        if sic in ALL_TARGET_SICS:
            target_sic_matches += 1
            return "target_sic"
    
    # Priority 2: Check buzzwords (case insensitive)
    company_name_upper = company_name.upper()
    for buzzword in BUZZWORDS:
        if buzzword.upper() in company_name_upper:
            buzzword_matches += 1
            print(f"  BUZZWORD MATCH: '{buzzword}' in '{company_name}'", flush=True)
            return "buzzword"
    
    return None


async def get_db_connection():
    db_path = Path(DATABASE_FILE)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = await aiosqlite.connect(str(db_path))
    conn.row_factory = aiosqlite.Row
    
    # Create table if not exists
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS screened_companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_number TEXT NOT NULL,
            company_name TEXT NOT NULL,
            incorporation_date TEXT NOT NULL,
            sic_codes TEXT,
            source_type TEXT NOT NULL,
            published_at TEXT NOT NULL,
            UNIQUE(company_number, incorporation_date)
        )
    """)
    
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_incorporation_date 
        ON screened_companies(incorporation_date DESC, published_at DESC)
    """)
    
    return conn


async def process_stream():
    global db_conn, companies_seen, companies_matched, buzzword_matches, target_sic_matches
    
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
                        
                        try:
                            data = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        
                        companies_seen += 1
                        
                        company_data = {
                            "company_number": data.get("data", {}).get("company_number", ""),
                            "company_name": data.get("data", {}).get("company_name", ""),
                            "incorporation_date": data.get("data", {}).get("date_of_creation", ""),
                            "sic_codes": data.get("data", {}).get("sic_codes", []),
                            "type": data.get("data", {}).get("type", ""),
                        }
                        
                        if companies_seen <= 50 or companies_seen % 100 == 0:
                            print(f"DEBUG #{companies_seen}: {company_data['company_number']} - {company_data['company_name']}", flush=True)
                            print(f"  SIC: {company_data['sic_codes']}", flush=True)
                            print(f"  Incorp Date: {company_data['incorporation_date']}", flush=True)
                        
                        source_type = matches_criteria(company_data)
                        
                        if source_type:
                            companies_matched += 1
                            print(f"MATCH #{companies_matched}: {company_data['company_number']} - {company_data['company_name']} ({source_type})", flush=True)
                            
                            published_at = datetime.now(timezone.utc).isoformat()
                            
                            try:
                                await db_conn.execute(
                                    """
                                    INSERT OR IGNORE INTO screened_companies 
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
                                
                                if db_conn.total_changes > 0:
                                    print(f"Stored: {company_data['company_number']} - {company_data['company_name']}", flush=True)
                                    
                                    notification = {
                                        "type": "new_company",
                                        "data": {
                                            "company_number": company_data["company_number"],
                                            "company_name": company_data["company_name"],
                                            "sic_codes": company_data["sic_codes"],
                                            "source_type": source_type,
                                            "published_at": published_at,
                                            "incorporation_date": company_data["incorporation_date"],
                                        },
                                    }
                                    
                                    for queue in sse_clients:
                                        await queue.put(notification)
                            
                            except Exception as e:
                                print(f"Database error: {e}", flush=True)
                        
                        event = data.get("event", {})
                        if "timepoint" in event:
                            last_timepoint = str(event["timepoint"])
                            timepoint_file.write_text(last_timepoint)
        
        except Exception as e:
            print(f"Stream error: {e}", flush=True)
            print("Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)


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
        "SELECT COUNT(*) FROM screened_companies WHERE incorporation_date = ?",
        (today,)
    )
    row = await cursor.fetchone()
    return {
        "target_sic_buzzword_count": row[0] if row else 0,
        "date": today,
        "buzzword_matches": buzzword_matches,
        "target_sic_matches": target_sic_matches,
    }


@app.get("/api/companies")
async def list_companies(limit: int = 100):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    cursor = await db_conn.execute(
        """
        SELECT company_number, company_name, sic_codes, source_type, published_at, incorporation_date
        FROM screened_companies
        WHERE incorporation_date = ?
        ORDER BY published_at DESC
        LIMIT ?
        """,
        (today, limit)
    )
    rows = await cursor.fetchall()
    
    companies = []
    for row in rows:
        sic_codes = json.loads(row[2]) if row[2] else []
        sic_groups = []
        for sic in sic_codes:
            group = get_sic_group(str(sic))
            if group:
                sic_groups.append({"sic": str(sic), "group": group})
        
        companies.append({
            "company_number": row[0],
            "company_name": row[1],
            "sic_codes": sic_codes,
            "sic_groups": sic_groups,
            "source_type": row[3],
            "published_at": row[4],
            "incorporation_date": row[5],
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
        .container { max-width: 1600px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 10px; }
        .date-display { color: #666; font-size: 14px; margin-bottom: 20px; }
        .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .metric-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .metric-label { color: #666; font-size: 14px; margin-bottom: 8px; }
        .metric-value { font-size: 32px; font-weight: bold; color: #22c55e; }
        .connection-status { display: inline-flex; align-items: center; gap: 8px; padding: 8px 16px; background: #f0fdf4; border-radius: 20px; font-size: 14px; color: #16a34a; }
        .status-dot { width: 8px; height: 8px; border-radius: 50%; background: #22c55e; animation: pulse 2s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .companies-table { background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow-x: auto; }
        table { width: 100%; border-collapse: collapse; min-width: 1000px; }
        th, td { padding: 12px 16px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f9fafb; font-weight: 600; color: #374151; white-space: nowrap; }
        tr.new-row { background: #dcfce7; animation: fadeGreen 3s forwards; }
        @keyframes fadeGreen { 0% { background: #dcfce7; } 100% { background: transparent; } }
        .company-number { font-family: 'Courier New', monospace; font-size: 13px; }
        .sic-badge { display: inline-block; padding: 4px 8px; background: #dbeafe; color: #1e40af; border-radius: 4px; font-size: 12px; margin-right: 4px; margin-bottom: 4px; }
        .sic-group { display: inline-block; padding: 4px 8px; background: #fef3c7; color: #92400e; border-radius: 4px; font-size: 11px; margin-right: 4px; margin-bottom: 4px; font-weight: 500; }
        .type-badge { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: 500; }
        .type-badge.target_sic { background: #dcfce7; color: #166534; }
        .type-badge.buzzword { background: #fef3c7; color: #92400e; }
        .time-ago { color: #6b7280; font-size: 13px; white-space: nowrap; }
        .links a { color: #3b82f6; text-decoration: none; margin-right: 12px; }
        .copy-btn { background: #f3f4f6; border: none; padding: 4px 8px; border-radius: 4px; cursor: pointer; margin-left: 8px; font-size: 12px; }
        .enable-sound-btn { background: #3b82f6; color: white; border: none; padding: 8px 16px; border-radius: 6px; cursor: pointer; font-size: 13px; margin-left: 12px; }
        .enable-sound-btn:hover { background: #2563eb; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏢 Companies House Monitor</h1>
        <div class="date-display" id="currentDate">Loading date...</div>
        <div class="metrics">
            <div class="metric-card">
                <div class="metric-label">Status</div>
                <div class="connection-status"><span class="status-dot" id="statusDot"></span><span id="statusText">Connecting...</span><button class="enable-sound-btn" id="enableSoundBtn" onclick="enableSound()">Enable Sound</button></div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Target Companies Incorporated Today</div>
                <div class="metric-value" id="targetCount">-</div>
            </div>
        </div>
        <div class="companies-table">
            <table>
                <thead>
                    <tr>
                        <th>Number</th>
                        <th>Name</th>
                        <th>SIC Codes</th>
                        <th>Groups</th>
                        <th>Type</th>
                        <th>Incorporated</th>
                        <th>Published</th>
                        <th>Links</th>
                    </tr>
                </thead>
                <tbody id="companiesTable"><tr><td colspan="8" style="text-align:center;padding:40px;">Loading...</td></tr></tbody>
            </table>
        </div>
    </div>
    <script>
        let es = null;
        let soundEnabled = false;
        
        function timeAgo(ts) {
            const d = Math.floor((new Date() - new Date(ts)) / 1000);
            if (d < 60) return d + 's';
            if (d < 3600) return Math.floor(d / 60) + 'm';
            if (d < 86400) return Math.floor(d / 3600) + 'h';
            return Math.floor(d / 86400) + 'd';
        }
        function formatDate(isoString) {
            const d = new Date(isoString);
            return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' });
        }
        function setStatus(ok) {
            document.getElementById('statusDot').style.opacity = ok ? '1' : '0.5';
            document.getElementById('statusText').textContent = ok ? 'Live' : 'Disconnected';
        }
        function updateDate() {
            const now = new Date();
            document.getElementById('currentDate').textContent = '📅 Showing companies incorporated on: ' + now.toLocaleDateString('en-GB', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });
        }
        function enableSound() {
            soundEnabled = true;
            playNotificationSound();
            document.getElementById('enableSoundBtn').textContent = 'Sound Enabled ✓';
        }
        function playNotificationSound() {
            if (!soundEnabled) return;
            try {
                const AudioContext = window.AudioContext || window.webkitAudioContext;
                const audioContext = new AudioContext();
                const oscillator = audioContext.createOscillator();
                const gain = audioContext.createGain();
                oscillator.type = 'sine';
                oscillator.frequency.setValueAtTime(880, audioContext.currentTime);
                oscillator.frequency.setValueAtTime(1175, audioContext.currentTime + 0.12);
                gain.gain.setValueAtTime(0.08, audioContext.currentTime);
                gain.gain.exponentialRampToValueAtTime(0.001, audioContext.currentTime + 0.32);
                oscillator.connect(gain);
                gain.connect(audioContext.destination);
                oscillator.start();
                oscillator.stop(audioContext.currentTime + 0.32);
            } catch (error) {
                console.warn('Sound unavailable:', error);
            }
        }
        async function loadMetrics() {
            try {
                const r = await fetch('/api/metrics');
                const d = await r.json();
                document.getElementById('targetCount').textContent = d.target_sic_buzzword_count;
                if (d.date) updateDate();
            } catch(e) {}
        }
        async function loadCompanies() {
            try {
                const r = await fetch('/api/companies?limit=200');
                const d = await r.json();
                const tb = document.getElementById('companiesTable');
                if (!d.companies.length) {
                    tb.innerHTML = '<tr><td colspan="8" style="text-align:center;padding:40px;">No companies incorporated today yet</td></tr>';
                    return;
                }
                tb.innerHTML = d.companies.map(c => {
                    const sics = c.sic_codes.map(s => `<span class="sic-badge">${s}</span>`).join('');
                    const groups = c.sic_groups && c.sic_groups.length ? c.sic_groups.map(g => `<span class="sic-group">${g.group}</span>`).join('') : '<span style="color:#999;font-size:12px;">-</span>';
                    const tc = c.source_type === 'target_sic' ? 'target_sic' : 'buzzword';
                    const tl = c.source_type === 'target_sic' ? 'Target SIC' : 'Buzzword';
                    return `<tr class="company-row" data-number="${c.company_number}">
                        <td class="company-number">${c.company_number}<button class="copy-btn" onclick="navigator.clipboard.writeText('${c.company_name.replace(/'/g, "\\'")}')">Copy</button></td>
                        <td>${c.company_name}</td>
                        <td>${sics}</td>
                        <td>${groups}</td>
                        <td><span class="type-badge ${tc}">${tl}</span></td>
                        <td class="time-ago">${formatDate(c.incorporation_date)}</td>
                        <td class="time-ago">${timeAgo(c.published_at)}</td>
                        <td class="links"><a href="https://find-and-update.company-information.service.gov.uk/company/${c.company_number}" target="_blank">CH</a> <a href="https://google.com/search?q=${encodeURIComponent(c.company_name)}" target="_blank">Google</a></td>
                    </tr>`;
                }).join('');
            } catch(e) { console.error('Error:', e); }
        }
        function addCompany(c) {
            const tb = document.getElementById('companiesTable');
            const existing = tb.querySelector(`[data-number="${c.company_number}"]`);
            if (existing) return;
            const sics = c.sic_codes.map(s => `<span class="sic-badge">${s}</span>`).join('');
            const tc = c.source_type === 'target_sic' ? 'target_sic' : 'buzzword';
            const tl = c.source_type === 'target_sic' ? 'Target SIC' : 'Buzzword';
            const row = document.createElement('tr');
            row.className = 'company-row new-row';
            row.dataset.number = c.company_number;
            row.innerHTML = `<td class="company-number">${c.company_number}<button class="copy-btn" onclick="navigator.clipboard.writeText('${c.company_name.replace(/'/g, "\\'")}')">Copy</button></td><td>${c.company_name}</td><td>${sics}</td><td><span style="color:#999;font-size:12px;">-</span></td><td><span class="type-badge ${tc}">${tl}</span></td><td class="time-ago">-</td><td class="time-ago">Just now</td><td class="links"><a href="https://find-and-update.company-information.service.gov.uk/company/${c.company_number}" target="_blank">CH</a> <a href="https://google.com/search?q=${encodeURIComponent(c.company_name)}" target="_blank">Google</a></td>`;
            tb.insertBefore(row, tb.firstChild);
            while (tb.children.length > 200) tb.removeChild(tb.lastChild);
            playNotificationSound();
        }
        function connect() {
            es = new EventSource('/stream');
            es.onopen = () => setStatus(true);
            es.onmessage = e => {
                const n = JSON.parse(e.data);
                if (n.type === 'new_company') {
                    console.log('New company:', n.data.company_name, 'Type:', n.data.source_type);
                    addCompany(n.data);
                    loadCompanies();
                    loadMetrics();
                }
            };
            es.onerror = () => { setStatus(false); es.close(); setTimeout(connect, 5000); };
        }
        updateDate(); loadMetrics(); loadCompanies();
        setInterval(loadMetrics, 5000);
        connect();
    </script>
</body>
</html>
    """


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
