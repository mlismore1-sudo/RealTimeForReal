Companies House Real-Time Monitor
A real-time dashboard that monitors UK company incorporations and displays companies matching specific criteria (target SIC codes or buzzwords) with minimal delay.

Architecture
text
┌─────────────────────┐      ┌─────────────────────┐
│  Stream Worker      │      │  Dashboard (Web)    │
│  (Background)       │      │                     │
│                     │      │  FastAPI + HTML     │
│  - SSE Connection   │      │                     │
│  - Filter Logic     │─────▶│  - Metrics API      │
│  - Store to DB      │      │  - Company Table    │
└─────────────────────┘      └─────────────────────┘
           │                            │
           ▼                            ▼
    ┌─────────────────────────────────────┐
    │      PostgreSQL Database            │
    │      (Render Managed)               │
    └─────────────────────────────────────┘
Why This Works (vs Previous Attempt)
Issue	Previous	New Solution
Database	SQLite on ephemeral disk	PostgreSQL (managed, persistent)
Stream Processing	Web service (sleeps)	Background worker (always on)
Timepoint Tracking	Not implemented	File-based persistent storage
API Key	Possibly REST key	Requires Streaming API key
Error Handling	Basic	Auto-reconnect, retry logic
Prerequisites
Companies House Streaming API Key (NOT the REST API key)

Go to: https://developer.company-information.service.gov.uk/

Create a new application specifically for streaming

Copy the streaming API key

Render Account

Sign up at: https://render.com/

Free tier available, but database requires paid plan after 30 days

Setup Steps
1. Create GitHub Repository

bash
# Create new repo on GitHub
git init
git add .
git commit -m "Initial commit"
git push origin main
2. Connect to Render

Go to https://render.com/dashboard

Click "New +" → "Blueprint"

Connect your GitHub repository

Render will auto-detect render.yaml

3. Configure Environment Variables

In Render dashboard:

Dashboard Service:

Add API_KEY environment variable (your streaming API key)

Stream Worker Service:

Add API_KEY environment variable (same key)

4. Deploy

Render will automatically:

Create PostgreSQL database

Deploy dashboard web service (free tier)

Deploy stream worker (standard-256mb, $7/mo)

Attach persistent disk to worker

5. Verify

Dashboard URL: https://[your-app].onrender.com

Check worker logs in Render dashboard

Verify companies appearing in table

Target SIC Codes
text
62011, 62012, 62020, 62030, 62090  (Software)
63110, 63120, 63910, 63990         (Web/IT)
64999, 66190, 66220, 66300         (Finance)
70229, 72110, 72190, 72200         (Consulting/R&D)
73110, 73120, 73200                (Advertising)
74100, 74200, 74300, 74900         (Design)
82990, 85590, 86900, 87900         (Business services)
90030, 91010, 91020, 93290         (Arts/Recreation)
Buzzwords
" AI" (with leading space to avoid false positives)

Cost Breakdown
Service	Plan	Cost
Dashboard	Free	$0/mo
Stream Worker	Standard-256mb	$7/mo
PostgreSQL	Basic-256mb	$6/mo (after 30-day trial)
Disk (1GB)	Persistent	$0.25/mo
Total		~$13.25/mo
Troubleshooting
No Companies Appearing

Check worker logs for "Stream connected successfully"

Verify API_KEY is set correctly

Check if timepoint is being saved

Verify database connection in logs

Worker Crashes

Check logs for error messages

Verify API key has streaming access (not REST)

Check disk is mounted at /data

Database Connection Errors

Verify DATABASE_URL is auto-populated from database

Check database is in same region (London)

Restart services if needed

High Latency

Check worker logs for processing time

Verify REST API rate limits (600/5min)

Consider caching company details

API Endpoints
GET / - Dashboard HTML

GET /api/companies?limit=100 - Recent companies

GET /api/metrics - Real-time metrics

GET /health - Health check

Local Development
bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export DATABASE_URL="postgresql://..."
export API_KEY="your-key"

# Run dashboard
python main.py

# Run worker (separate terminal)
python stream_worker.py
Monitoring
Render Dashboard: View logs, metrics, uptime

Database: Query screened_companies table

Timepoint: Check /data/timepoint.txt on worker

Support
For issues:

Check Render service logs

Verify Companies House API status

Test SSE connection locally first
