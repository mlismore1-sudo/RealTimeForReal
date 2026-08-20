"""
Companies House SSE Stream Worker
Maintains persistent connection to Companies House streaming API
Filters and stores matching companies to database
"""
import os
import json
import asyncio
import time
from datetime import datetime
from typing import Optional, List

import httpx
import asyncpg
import base64

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/companies")
API_KEY = os.getenv("API_KEY", "")
SSE_URL = os.getenv("SSE_URL", "https://stream.companieshouse.gov.uk/")
TIMEPOINT_FILE = os.getenv("TIMEPOINT_FILE", "/data/timepoint.txt")  # Persistent storage

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

# Buzzwords
BUZZWORDS = [" AI"]


def matches_criteria(sic_codes: List[str], company_name: str) -> tuple[bool, str]:
    """Check if company matches target SIC codes or buzzwords"""
    for sic in sic_codes:
        if sic in TARGET_SIC_CODES:
            return True, "target_sic"
    
    for buzzword in BUZZWORDS:
        if buzzword in company_name:
            return True, "buzzword"
    
    return False, ""


def load_timepoint() -> Optional[int]:
    """Load last processed timepoint from persistent storage"""
    try:
        if os.path.exists(TIMEPOINT_FILE):
            with open(TIMEPOINT_FILE, 'r') as f:
                return int(f.read().strip())
    except Exception as e:
        print(f"Error loading timepoint: {e}")
    return None


def save_timepoint(timepoint: int):
    """Save timepoint to persistent storage"""
    try:
        os.makedirs(os.path.dirname(TIMEPOINT_FILE), exist_ok=True)
        with open(TIMEPOINT_FILE, 'w') as f:
            f.write(str(timepoint))
    except Exception as e:
        print(f"Error saving timepoint: {e}")


async def init_database(pool: asyncpg.Pool):
    """Initialize database schema"""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS screened_companies (
                id SERIAL PRIMARY KEY,
                company_number TEXT UNIQUE NOT NULL,
                company_name TEXT NOT NULL,
                incorporation_date TEXT NOT NULL,
                sic_codes JSONB,
                source_type TEXT NOT NULL,
                published_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_published_at 
            ON screened_companies(published_at DESC)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_incorporation_date 
            ON screened_companies(incorporation_date)
        """)
        print("Database initialized")


async def store_company(pool: asyncpg.Pool, company_data: dict):
    """Store a matched company in the database"""
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                """
                INSERT INTO screened_companies 
                (company_number, company_name, incorporation_date, sic_codes, source_type, published_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (company_number) DO NOTHING
                """,
                company_data["company_number"],
                company_data["company_name"],
                company_data["incorporation_date"],
                json.dumps(company_data["sic_codes"]),
                company_data["source_type"],
                company_data["published_at"],
            )
            print(f"Stored: {company_data['company_number']} - {company_data['company_name']}")
        except Exception as e:
            print(f"Error storing company: {e}")


async def process_event(pool: asyncpg.Pool, event: dict) -> Optional[int]:
    """Process a single SSE event"""
    try:
        event_type = event.get("type", "")
        event_data = event.get("data", {})
        timepoint = event.get("event", {}).get("timepoint")
        
        # Only process company profiles (new incorporations and updates)
        if event_type != "company-profile":
            return timepoint
        
        company_number = event.get("resource_id", "")
        if not company_number:
            return timepoint
        
        # Fetch full company details from REST API
        company_details = await fetch_company_details(company_number)
        if not company_details:
            return timepoint
        
        # Extract SIC codes
        sic_codes = company_details.get("sic_codes", [])
        company_name = company_details.get("company_name", "")
        incorporation_date = company_details.get("date_of_creation", "")
        
        # Check if matches criteria
        matches, source_type = matches_criteria(sic_codes, company_name)
        if matches:
            company_data = {
                "company_number": company_number,
                "company_name": company_name,
                "incorporation_date": incorporation_date,
                "sic_codes": sic_codes,
                "source_type": source_type,
                "published_at": datetime.utcnow(),
            }
            await store_company(pool, company_data)
        
        return timepoint
    
    except Exception as e:
        print(f"Error processing event: {e}")
        return None


async def fetch_company_details(company_number: str) -> Optional[dict]:
    """Fetch company details from Companies House REST API"""
    url = f"https://api.company-information.service.gov.uk/company/{company_number}"
    
    auth_string = f"{API_KEY}:"
    auth_header = base64.b64encode(auth_string.encode()).decode()
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                url,
                headers={"Authorization": f"Basic {auth_header}"}
            )
            if response.status_code == 200:
                return response.json()
            else:
                print(f"API error for {company_number}: {response.status_code}")
                return None
    except Exception as e:
        print(f"Error fetching company {company_number}: {e}")
        return None


async def stream_processor(pool: asyncpg.Pool):
    """Main SSE stream processing loop"""
    auth_string = f"{API_KEY}:"
    auth_header = base64.b64encode(auth_string.encode()).decode()
    
    while True:
        try:
            # Load last timepoint
            last_timepoint = load_timepoint()
            
            # Build stream URL with timepoint if available
            stream_url = SSE_URL
            if last_timepoint:
                stream_url = f"{SSE_URL}?timepoint={last_timepoint}"
            
            print(f"Connecting to stream: {stream_url}")
            
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream(
                    "GET",
                    stream_url,
                    headers={"Authorization": f"Basic {auth_header}"}
                ) as response:
                    if response.status_code != 200:
                        print(f"Stream connection failed: {response.status_code}")
                        await asyncio.sleep(5)
                        continue
                    
                    print("Stream connected successfully")
                    
                    async for line in response.aiter_lines():
                        try:
                            if not line.strip():
                                continue
                            
                            # Parse SSE event
                            if line.startswith("data:"):
                                event_data = line[5:].strip()
                                event = json.loads(event_data)
                                
                                # Process event
                                timepoint = await process_event(pool, event)
                                
                                # Save timepoint after successful processing
                                if timepoint:
                                    save_timepoint(timepoint)
                        
                        except json.JSONDecodeError as e:
                            print(f"JSON decode error: {e}")
                            continue
                        except Exception as e:
                            print(f"Event processing error: {e}")
                            continue
        
        except Exception as e:
            print(f"Stream connection error: {e}")
            print("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


async def main():
    """Main entry point"""
    print("Starting Companies House Stream Worker...")
    
    # Validate API key
    if not API_KEY:
        print("ERROR: API_KEY environment variable not set")
        return
    
    # Create database pool
    print(f"Connecting to database: {DATABASE_URL[:50]}...")
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    
    # Initialize database
    await init_database(pool)
    
    # Start stream processing
    print("Starting stream processor...")
    await stream_processor(pool)


if __name__ == "__main__":
    asyncio.run(main())
