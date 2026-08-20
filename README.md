Companies House Real-Time Monitor
A real-time dashboard that monitors UK company incorporations and displays companies matching specific criteria (target SIC codes or buzzwords) with minimal delay.

Features
✅ Real-time SSE streaming from Companies House

✅ Filter by target SIC codes (software/tech/finance)

✅ Filter by buzzwords in company names (e.g., " AI")

✅ SQLite database on persistent storage

✅ Live dashboard with auto-refresh

✅ Single service deployment (~$5.25/mo on Render)

Architecture
text
┌─────────────────────────────────────┐
│   Single Web Service (Render)       │
│                                     │
│  ┌───────────────────────────────┐  │
│  │  FastAPI Dashboard            │  │
│  │  - HTML/JS frontend           │  │
│  │  - REST API endpoints         │  │
│  │  - SSE streaming endpoint     │  │
│  └───────────────────────────────┘  │
│                                     │
│  ┌───────────────────────────────┐  │
│  │  SSE Stream Processor         │  │
│  │  - Connects to CH stream      │  │
│  │  - Filters by SIC/buzzwords   │  │
│  │  - Stores in SQLite           │  │
│  └───────────────────────────────┘  │
│                                     │
│  ┌───────────────────────────────┐  │
│  │  SQLite Database              │  │
│  │  - /data/companies.db         │  │
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  Persistent Disk (1GB)              │
│  - companies.db                     │
│  - timepoint.txt                    │
└─────────────────────────────────────┘
Target SIC Codes
text
62011, 62012, 62020, 62030, 62090  (Software development)
63110, 63120, 63910, 63990         (Web portals/IT)
64999, 66190, 66220, 66300         (Finance/Investment)
70229, 72110, 72190, 72200         (Consulting/R&D)
73110, 73120, 73200                (Advertising/Market research)
74100, 74200, 74300, 74900         (Design/Professional)
82990, 85590, 86900, 87900         (Other business services)
90030, 91010, 91020, 93290         (Arts/Recreation)
Buzzwords
AI (with space to avoid false positives)

Deployment
1. Get Companies House Streaming API Key

Go to https://developer.company-information.service.gov.uk/signin

Create a NEW application with Streaming API access

Copy the API key

2. Deploy to Render

bash
# Push code to GitHub
git add .
git commit -m "Deploy Companies House Monitor"
git push

# In Render Dashboard:
# 1. Click "New +" → "Blueprint"
# 2. Select your repo
# 3. Click "Deploy Blueprint"
3. Add Environment Variable

In Render Dashboard:

Go to service → Environment tab

Add: API_KEY = your streaming API key

Save and wait for redeploy

4. Access Dashboard

Open your Render service URL (e.g., https://your-app.onrender.com)

API Endpoints
GET / - Main dashboard

GET /health - Health check

GET /api/metrics - Current metrics

GET /api/companies?limit=100 - Recent companies

GET /stream - SSE endpoint for real-time updates

Cost
Component	Cost
Web Service (Standard-256mb)	$5/mo
Persistent Disk (1GB)	$0.25/mo
Total	$5.25/mo
Troubleshooting
No companies appearing?

Check service logs for "Stream connected successfully"

Verify API_KEY is set correctly (must be streaming key, not REST)

Check Companies House API status

Database errors?

Verify disk is mounted at /data

Check logs for specific error messages

Service keeps restarting?

Check disk is properly attached

Verify all environment variables are set

License
MIT
