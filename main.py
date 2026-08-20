"""
Companies House Real-Time Monitor - Single Service Version
Combined SSE streaming + FastAPI dashboard with SQLite storage
"""

import os
import json
import asyncio
import httpx
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import aiosqlite

# Configuration
API_KEY = os.environ.get("API_KEY", "")
SSE_URL = os.environ.get("SSE_URL", "https://stream.companieshouse.gov.uk/companies")
DATABASE_FILE = os.environ.get("DATABASE_FILE", "/data/companies.db")
PORT = int(os.environ.get("PORT", "8000"))

# Target SIC codes (software/tech/finance)
TARGET_SIC_CODES = {
    "62011", "62012", "62020", "62030", "62090",  # Software development
    "63110", "63120", "63910", "63990",  # Web portals/IT
    "64999", "66190", "66220", "66300",  # Finance/Investment
    "70229", "72110", "72190", "72200",  # Consulting/R&D
    "73110", "73120", "73200",  # Advertising/Market research
    "74100", "74200", "74300", "74900",  # Design/Professional
    "82990", "85590", "86900", "87900",  # Other business services
    "90030", "91010", "91020", "93290",  # Arts/Recreation
}

# Buzzwords to match in company names
BUZZWORDS = [" AI"]  # Space before to avoid false positives

# In-memory store for SSE clients
sse_clients: List[asyncio.Queue] = []

# Global database connection
db_conn: Optional[aiosqlite.Connection] = None

# Counter for debugging
companies_seen = 0
companies_matched = 0


def matches_criteria(data: Dict[str, Any]) -> Optional[str]:
    """Check if company matches target SIC or buzzwords"""
    company_name = data.get("company_name", "")
    sic_codes = data.get("sic_codes", [])
    
    # Convert SIC codes to strings (handle both int and str)
    sic_codes_str = [str(sic) for sic in sic_codes]
    
    # Priority 1: Check target SIC codes
    for sic in sic_codes_str:
        if sic in TARGET_SIC_CODES:
            return "target_sic"
    
    # Priority 2: Check buzzwords
    for buzzword in BUZZWORDS:
        if buzzword in company_name:
            return "buzzword"
    
    return None


async def get_db_connection():
    """Get SQLite connection"""
    db_path = Path(DATABASE_FILE)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = await aiosqlite.connect(str(db_path))
    conn.row_factory = aiosqlite.Row
    
    # Initialize schema
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
    """Process Companies House SSE stream"""
    global db_conn, companies_seen, companies_matched
    
    timepoint_file = Path("/data/timepoint.txt")
    timepoint_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Load last timepoint
    last_timepoint = None
    if timepoint_file.exists():
        last_timepoint = timepoint_file.read_text().strip()
        print(f"Loaded timepoint: {last_timepoint}")
    
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
                        try:
                            error_body = await response.text()
                            print(f"Error response: {error_body[:200]}", flush=True)
                        except:
                            pass
                        await asyncio.sleep(5)
                        continue
                    
                    print("Stream connected successfully", flush=True)
                    
                    async for line in response.aiter_lines():
                        if not line:
                            continue
                        
                        if line.startswith("data: "):
                            data_str = line[6:]
                            try:
                                data = json.loads(data_str)
                                
                                # Extract company data - FIXED: nested inside "data" key
                                company_data = {
                                    "company_number": data.get("data", {}).get("company_number", ""),
                                    "company_name": data.get("data", {}).get("company_name", ""),
                                    "incorporation_date": data.get("data", {}).get("date_of_creation", ""),
                                    "sic_codes": data.get("data", {}).get("sic_codes", []),
                                    "type": data.get("data", {}).get("type", ""),
                                }
                                
                                # Debug: Log every 10th company
                                companies_seen += 1
                                if companies_seen % 10 == 0:
                                    print(f"DEBUG: Seen {companies_seen} companies, {companies_matched} matched", flush=True)
                                    print(f"  Latest: {company_data['company_number']} - {company_data['company_name']}", flush=True)
                                    print(f"  SIC codes: {company_data['sic_codes']}", flush=True)
                                
                                # Check if matches criteria
                                source_type = matches_criteria(company_data)
                                
                                if source_type:
                                    companies_matched += 1
                                    print(f"MATCH #{companies_matched}: {company_data['company_number']} - {company_data['company_name']} (type: {source_type})", flush=True)
                                    
                                    # Store in database
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
                                        
                                        # Notify SSE clients
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
                            
                            except json.JSONDecodeError as e:
                                print(f"JSON decode error: {e}", flush=True)
                                continue
                        
                        elif line.startswith("id: "):
                            last_timepoint = line[4:]
                            timepoint_file.write_text(last_timepoint)
        
        except Exception as e:
            print(f"Stream error: {e}", flush=True)
            print("Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)


# Global state
db_conn_global: Optional[aiosqlite.Connection] = None


async def get_today_count():
    """Get count of companies matched today"""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cursor = await db_conn_global.execute(
        "SELECT COUNT(*) FROM screened_companies WHERE DATE(published_at) = ?",
        (today,)
    )
    row = await cursor.fetchone()
    return row[0] if row else 0


async def get_recent_companies(limit: int = 100):
    """Get most recent companies"""
    cursor = await db_conn_global.execute(
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
    
    return companies


# FastAPI App
app = FastAPI(title="Companies House Monitor")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def startup():
    """Initialize database and start stream processor"""
    global db_conn, db_conn_global
    
    print("Starting Companies House Monitor...", flush=True)
    db_conn = await get_db_connection()
    db_conn_global = db_conn
    print(f"Database initialized: {DATABASE_FILE}", flush=True)
    
    # Start stream processor in background
    asyncio.create_task(process_stream())


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/metrics")
async def metrics():
    """Get current metrics"""
    today_count = await get_today_count()
    return {
        "target_sic_buzzword_count": today_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/companies")
async def list_companies(limit: int = 100):
    """Get recent companies"""
    companies = await get_recent_companies(limit)
    return {"companies": companies, "count": len(companies)}


@app.get("/stream")
async def sse_stream():
    """SSE endpoint for real-time updates"""
    
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
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Main dashboard HTML"""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Companies House Real-Time Monitor</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f5f5;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 20px; }
        
        .metrics { 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
            gap: 20px; 
            margin-bottom: 30px;
        }
        .metric-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .metric-label { color: #666; font-size: 14px; margin-bottom: 8px; }
        .metric-value { font-size: 32px; font-weight: bold; color: #22c55e; }
        .metric-value.blue { color: #3b82f6; }
        .metric-value.red { color: #ef4444; }
        
        .connection-status {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 16px;
            background: #f0fdf4;
            border-radius: 20px;
            font-size: 14px;
            color: #16a34a;
        }
        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #22c55e;
            animation: pulse 2s infinite;
        }
        .status-dot.disconnected { background: #ef4444; animation: none; }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        .companies-table {
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px 16px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f9fafb; font-weight: 600; color: #374151; }
        tr:hover { background: #f9fafb; }
        tr.new-row { background: #dcfce7; transition: background 0.3s; }
        
        .company-number { font-family: 'Courier New', monospace; font-size: 13px; }
        .sic-badge {
            display: inline-block;
            padding: 4px 8px;
            background: #dbeafe;
            color: #1e40af;
            border-radius: 4px;
            font-size: 12px;
            margin-right: 4px;
        }
        .type-badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 500;
        }
        .type-badge.target_sic { background: #dcfce7; color: #166534; }
        .type-badge.buzzword { background: #fef3c7; color: #92400e; }
        
        .time-ago { color: #6b7280; font-size: 13px; }
        
        .links a {
            color: #3b82f6;
            text-decoration: none;
            margin-right: 12px;
            font-size: 13px;
        }
        .links a:hover { text-decoration: underline; }
        
        .copy-btn {
            background: #f3f4f6;
            border: none;
            padding: 4px 8px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
            margin-left: 8px;
        }
        .copy-btn:hover { background: #e5e7eb; }
        
        .loading { text-align: center; padding: 40px; color: #6b7280; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏢 Companies House Real-Time Monitor</h1>
        
        <div class="metrics">
            <div class="metric-card">
                <div class="metric-label">Connection Status</div>
                <div class="connection-status">
                    <span class="status-dot" id="statusDot"></span>
                    <span id="statusText">Connecting...</span>
                </div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Target SIC + Buzzword (Today)</div>
                <div class="metric-value" id="targetCount">-</div>
            </div>
        </div>
        
        <div class="companies-table">
            <table>
                <thead>
                    <tr>
                        <th>Company Number</th>
                        <th>Company Name</th>
                        <th>SIC Codes</th>
                        <th>Type</th>
                        <th>Time</th>
                        <th>Links</th>
                    </tr>
                </thead>
                <tbody id="companiesTable">
                    <tr><td colspan="6" class="loading">Loading companies...</td></tr>
                </tbody>
            </table>
        </div>
    </div>
    
    <script>
        let eventSource = null;
        
        function formatTimeAgo(timestamp) {
            const now = new Date();
            const then = new Date(timestamp);
            const diff = Math.floor((now - then) / 1000);
            
            if (diff < 60) return diff + 's';
            if (diff < 3600) return Math.floor(diff / 60) + 'm';
            if (diff < 86400) return Math.floor(diff / 3600) + 'h';
            return Math.floor(diff / 86400) + 'd';
        }
        
        function updateStatus(connected) {
            const dot = document.getElementById('statusDot');
            const text = document.getElementById('statusText');
            
            if (connected) {
                dot.classList.remove('disconnected');
                text.textContent = 'Live';
            } else {
                dot.classList.add('disconnected');
                text.textContent = 'Disconnected';
            }
        }
        
        async function loadMetrics() {
            try {
                const response = await fetch('/api/metrics');
                const data = await response.json();
                document.getElementById('targetCount').textContent = data.target_sic_buzzword_count;
            } catch (error) {
                console.error('Error loading metrics:', error);
            }
        }
        
        async function loadCompanies() {
            try {
                const response = await fetch('/api/companies?limit=100');
                const data = await response.json();
                renderCompanies(data.companies);
            } catch (error) {
                console.error('Error loading companies:', error);
            }
        }
        
        function renderCompanies(companies) {
            const tbody = document.getElementById('companiesTable');
            
            if (companies.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" class="loading">No companies found yet</td></tr>';
                return;
            }
            
            tbody.innerHTML = companies.map(company => {
                const sicBadges = company.sic_codes.map(sic => 
                    `<span class="sic-badge">${sic}</span>`
                ).join('');
                
                const typeClass = company.source_type === 'target_sic' ? 'target_sic' : 'buzzword';
                const typeLabel = company.source_type === 'target_sic' ? 'Target SIC' : 'Buzzword';
                
                return `
                    <tr class="company-row" data-number="${company.company_number}">
                        <td class="company-number">
                            ${company.company_number}
                            <button class="copy-btn" onclick="copyName('${company.company_name.replace(/'/g, "\\'")}')">Copy</button>
                        </td>
                        <td>${company.company_name}</td>
                        <td>${sicBadges}</td>
                        <td><span class="type-badge ${typeClass}">${typeLabel}</span></td>
                        <td class="time-ago">${formatTimeAgo(company.published_at)}</td>
                        <td class="links">
                            <a href="https://find-and-update.company-information.service.gov.uk/company/${company.company_number}" target="_blank">CH</a>
                            <a href="https://www.google.com/search?q=${encodeURIComponent(company.company_name)}" target="_blank">Google</a>
                        </td>
                    </tr>
                `;
            }).join('');
        }
        
        function addCompany(company) {
            const tbody = document.getElementById('companiesTable');
            const existingRow = tbody.querySelector(`[data-number="${company.company_number}"]`);
            
            if (existingRow) return;
            
            const sicBadges = company.sic_codes.map(sic => 
                `<span class="sic-badge">${sic}</span>`
            ).join('');
            
            const typeClass = company.source_type === 'target_sic' ? 'target_sic' : 'buzzword';
            const typeLabel = company.source_type === 'target_sic' ? 'Target SIC' : 'Buzzword';
            
            const row = document.createElement('tr');
            row.className = 'company-row new-row';
            row.dataset.number = company.company_number;
            row.innerHTML = `
                <td class="company-number">
                    ${company.company_number}
                    <button class="copy-btn" onclick="copyName('${company.company_name.replace(/'/g, "\\'")}')">Copy</button>
                </td>
                <td>${company.company_name}</td>
                <td>${sicBadges}</td>
                <td><span class="type-badge ${typeClass}">${typeLabel}</span></td>
                <td class="time-ago">Just now</td>
                <td class="links">
                    <a href="https://find-and-update.company-information.service.gov.uk/company/${company.company_number}" target="_blank">CH</a>
                    <a href="https://www.google.com/search?q=${encodeURIComponent(company.company_name)}" target="_blank">Google</a>
                </td>
            `;
            
            tbody.insertBefore(row, tbody.firstChild);
            
            // Remove new-row highlight after 3 seconds
            setTimeout(() => {
                row.classList.remove('new-row');
            }, 3000);
            
            // Keep only 100 rows
            while (tbody.children.length > 100) {
                tbody.removeChild(tbody.lastChild);
            }
        }
        
        function copyName(name) {
            navigator.clipboard.writeText(name);
        }
        
        function connectSSE() {
            eventSource = new EventSource('/stream');
            
            eventSource.onopen = () => {
                updateStatus(true);
            };
            
            eventSource.onmessage = (event) => {
                const notification = JSON.parse(event.data);
                if (notification.type === 'new_company') {
                    addCompany(notification.data);
                    loadMetrics();
                }
            };
            
            eventSource.onerror = () => {
                updateStatus(false);
                eventSource.close();
                setTimeout(connectSSE, 5000);
            };
        }
        
        // Initial load
        loadMetrics();
        loadCompanies();
        
        // Refresh metrics every 5 seconds
        setInterval(loadMetrics, 5000);
        
        // Connect SSE
        connectSSE();
    </script>
</body>
</html>
    """


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
