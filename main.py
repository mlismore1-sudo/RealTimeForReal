"""
Companies House Real-Time Monitor - Main FastAPI Application
Dashboard + API endpoints for viewing screened companies
"""
import os
import json
import asyncio
from datetime import datetime
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import asyncpg
import uvicorn

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/companies")
API_KEY = os.getenv("API_KEY", "")

# Target SIC codes
TARGET_SIC_CODES = {
    "62011", "62012", "62020", "62030", "62090",  # Software
    "63110", "63120", "63910", "63990",  # Web/IT
    "64999", "66190", "66220", "66300",  # Finance
    "70229", "72110", "72190", "72200",  # Consulting/R&D
    "73110", "73120", "73200",  # Advertising
    "74100", "74200", "74300", "74900",  # Design
    "82990", "85590", "86900", "87900",  # Business services
    "90030", "91010", "91020", "93290",  # Arts
}

# Buzzwords
BUZZWORDS = [" AI"]  # Space before to avoid false positives


async def get_db_pool():
    """Create database connection pool"""
    return await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)


# Global database pool
db_pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage database pool lifecycle"""
    global db_pool
    db_pool = await get_db_pool()
    yield
    await db_pool.close()


app = FastAPI(lifespan=lifespan)


def matches_criteria(sic_codes: List[str], company_name: str) -> tuple[bool, str]:
    """Check if company matches target SIC codes or buzzwords"""
    # Priority 1: Target SIC code
    for sic in sic_codes:
        if sic in TARGET_SIC_CODES:
            return True, "target_sic"
    
    # Priority 2: Buzzword in name
    for buzzword in BUZZWORDS:
        if buzzword in company_name:
            return True, "buzzword"
    
    return False, ""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the main dashboard"""
    return HTMLResponse(content=HTML_TEMPLATE)


@app.get("/api/companies")
async def get_companies(limit: int = 100):
    """Get recent screened companies"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT company_number, company_name, incorporation_date, sic_codes, 
                   source_type, published_at, created_at
            FROM screened_companies
            ORDER BY published_at DESC
            LIMIT $1
            """,
            limit
        )
    
    companies = []
    for row in rows:
        companies.append({
            "company_number": row["company_number"],
            "company_name": row["company_name"],
            "incorporation_date": row["incorporation_date"],
            "sic_codes": json.loads(row["sic_codes"]) if row["sic_codes"] else [],
            "source_type": row["source_type"],
            "published_at": row["published_at"],
            "created_at": row["created_at"].isoformat(),
        })
    
    return JSONResponse(content={"companies": companies})


@app.get("/api/metrics")
async def get_metrics():
    """Get real-time metrics"""
    async with db_pool.acquire() as conn:
        # Count companies incorporated today
        today = datetime.utcnow().date().isoformat()
        row = await conn.fetchrow(
            """
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE source_type = 'target_sic') as target_sic,
                COUNT(*) FILTER (WHERE source_type = 'buzzword') as buzzword
            FROM screened_companies
            WHERE incorporation_date = $1
            """,
            today
        )
    
    return JSONResponse(content={
        "total": row["total"] or 0,
        "target_sic": row["target_sic"] or 0,
        "buzzword": row["buzzword"] or 0,
        "timestamp": datetime.utcnow().isoformat(),
    })


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


# HTML Dashboard Template
HTML_TEMPLATE = """
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
        
        /* Metrics */
        .metrics {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }
        .metric-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .metric-label { color: #666; font-size: 14px; margin-bottom: 8px; }
        .metric-value { font-size: 32px; font-weight: bold; }
        .metric-value.green { color: #10b981; }
        .metric-value.blue { color: #3b82f6; }
        .status-indicator {
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            margin-right: 8px;
        }
        .status-indicator.live { background: #10b981; }
        .status-indicator.disconnected { background: #ef4444; }
        
        /* Table */
        .table-container {
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px 16px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f9fafb; font-weight: 600; color: #374151; }
        tr:hover { background: #f9fafb; }
        tr.new-row { background: #d1fae5; animation: fadeOut 3s forwards; }
        @keyframes fadeOut {
            to { background: white; }
        }
        .company-number { font-family: 'Courier New', monospace; font-size: 13px; }
        .sic-badge {
            display: inline-block;
            background: #dbeafe;
            color: #1e40af;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 12px;
            margin: 2px;
        }
        .type-badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 500;
        }
        .type-badge.target_sic { background: #10b981; color: white; }
        .type-badge.buzzword { background: #f59e0b; color: white; }
        .copy-btn {
            background: #e5e7eb;
            border: none;
            padding: 4px 8px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
            margin-left: 8px;
        }
        .copy-btn:hover { background: #d1d5db; }
        .time-ago { color: #6b7280; font-size: 13px; }
        .links a {
            color: #3b82f6;
            text-decoration: none;
            margin-right: 12px;
        }
        .links a:hover { text-decoration: underline; }
        
        /* Responsive */
        @media (max-width: 768px) {
            .metrics { grid-template-columns: 1fr; }
            table { font-size: 13px; }
            th, td { padding: 8px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏢 Companies House Real-Time Monitor</h1>
        
        <div class="metrics">
            <div class="metric-card">
                <div class="metric-label">
                    <span class="status-indicator live" id="status"></span>
                    Connection Status
                </div>
                <div class="metric-value" id="status-text">Connecting...</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Target SIC + Buzzword (Today)</div>
                <div class="metric-value green" id="target-count">-</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Total Companies (Today)</div>
                <div class="metric-value blue" id="total-count">-</div>
            </div>
        </div>
        
        <div class="table-container">
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
                <tbody id="companies-table">
                </tbody>
            </table>
        </div>
    </div>

    <script>
        let lastCompanyNumber = null;
        
        // Fetch companies
        async function fetchCompanies() {
            try {
                const response = await fetch('/api/companies?limit=50');
                const data = await response.json();
                renderTable(data.companies);
                updateStatus(true);
            } catch (error) {
                console.error('Error fetching companies:', error);
                updateStatus(false);
            }
        }
        
        // Fetch metrics
        async function fetchMetrics() {
            try {
                const response = await fetch('/api/metrics');
                const data = await response.json();
                document.getElementById('target-count').textContent = data.target_sic + data.buzzword;
                document.getElementById('total-count').textContent = data.total;
            } catch (error) {
                console.error('Error fetching metrics:', error);
            }
        }
        
        // Render table
        function renderTable(companies) {
            const tbody = document.getElementById('companies-table');
            tbody.innerHTML = '';
            
            companies.forEach((company, index) => {
                const tr = document.createElement('tr');
                if (company.company_number === lastCompanyNumber && index === 0) {
                    tr.classList.add('new-row');
                }
                
                const sicBadges = company.sic_codes.map(sic => 
                    `<span class="sic-badge">${sic}</span>`
                ).join('');
                
                const timeAgo = getTimeAgo(new Date(company.published_at));
                
                tr.innerHTML = `
                    <td class="company-number">${company.company_number}</td>
                    <td>
                        ${company.company_name}
                        <button class="copy-btn" onclick="copyName('${company.company_name.replace(/'/g, "\\'")}')">Copy</button>
                    </td>
                    <td>${sicBadges}</td>
                    <td><span class="type-badge ${company.source_type}">${company.source_type}</span></td>
                    <td class="time-ago">${timeAgo}</td>
                    <td class="links">
                        <a href="https://find-and-update.company-information.service.gov.uk/company/${company.company_number}" target="_blank">CH</a>
                        <a href="https://www.google.com/search?q=${encodeURIComponent(company.company_name)}" target="_blank">Google</a>
                    </td>
                `;
                tbody.appendChild(tr);
            });
            
            if (companies.length > 0) {
                lastCompanyNumber = companies[0].company_number;
            }
        }
        
        // Update connection status
        function updateStatus(isLive) {
            const indicator = document.getElementById('status');
            const text = document.getElementById('status-text');
            if (isLive) {
                indicator.classList.remove('disconnected');
                indicator.classList.add('live');
                text.textContent = 'Live';
            } else {
                indicator.classList.remove('live');
                indicator.classList.add('disconnected');
                text.textContent = 'Disconnected';
            }
        }
        
        // Time ago helper
        function getTimeAgo(date) {
            const seconds = Math.floor((new Date() - date) / 1000);
            if (seconds < 60) return `${seconds}s`;
            const minutes = Math.floor(seconds / 60);
            if (minutes < 60) return `${minutes}m`;
            const hours = Math.floor(minutes / 60);
            if (hours < 24) return `${hours}h`;
            const days = Math.floor(hours / 24);
            return `${days}d`;
        }
        
        // Copy name
        function copyName(name) {
            navigator.clipboard.writeText(name);
        }
        
        // Initial fetch
        fetchCompanies();
        fetchMetrics();
        
        // Refresh intervals
        setInterval(fetchCompanies, 5000);
        setInterval(fetchMetrics, 5000);
    </script>
</body>
</html>
"""


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
