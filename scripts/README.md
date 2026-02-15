# Standalone Scripts

This folder contains standalone Python scripts designed to be run as cron jobs or scheduled tasks.

## Scripts

### 1. sync_instruments.py
**Purpose**: Fetches all instruments from Kite API and stores them in the database.
...

### 2. stream_option_chain.py

**Purpose**: Stream real-time Option Chain data for NIFTY (Next 4 expiries) and BANKNIFTY (Next 2 expiries) using WebSocket.

**Key Features**:
- Connects to Kite Ticker (WebSocket).
- automatically selects relevant option contracts (NIFTY & BANKNIFTY).
- Aggregates CE and PE ticks.
- Stores data in `option_chain_data` table.
- Runs continuously.

**Usage**:
```bash
python scripts/stream_option_chain.py
```
*Note: This script requires an active internet connection and valid Admin credentials in the database.*

**Features**:
- Fetches Kite credentials from admin user in database (no hardcoded credentials)
- Downloads all available instruments from Kite
- Processes and stores data with proper timestamps
- Handles duplicates by deleting old data for the same date
- Comprehensive error handling and logging

**Usage**:
```bash
# Run manually
python scripts/sync_instruments.py

# Or from project root
cd c:\apps\power_app
.\venv\Scripts\python.exe scripts\sync_instruments.py
```

**Requirements**:
- Admin user must have valid Kite API credentials set
- Admin user must be connected to Kite (has access_token)
- Database must be accessible
- pandas package installed

**Cron Schedule** (Recommended):
Run once daily after market hours (e.g., 6:00 PM IST)

**Windows Task Scheduler**:
```
Program: C:\apps\power_app\venv\Scripts\python.exe
Arguments: C:\apps\power_app\scripts\sync_instruments.py
Start in: C:\apps\power_app
Schedule: Daily at 18:00
```

**Linux Cron**:
```bash
# Edit crontab
crontab -e

# Add this line (runs daily at 6 PM)
0 18 * * * cd /path/to/power_app && ./venv/bin/python scripts/sync_instruments.py >> logs/instruments_sync.log 2>&1
```

**Output**:
- Success: Exit code 0
- Failure: Exit code 1
- Detailed progress messages printed to stdout

**Database Table**: `instruments`

**Columns Stored**:
- instrument_token
- exchange_token
- tradingsymbol
- name
- last_price
- expiry
- strike
- tick_size
- lot_size
- instrument_type
- segment
- exchange
- fetch_date
- expiry_weekday
- created_at
- updated_at

## Adding New Scripts

When creating new standalone scripts:

1. Add the script to this folder
2. Include proper error handling
3. Use admin credentials from database
4. Add documentation to this README
5. Test manually before scheduling
6. Add logging for debugging

## Notes

- All scripts use the Flask app's database configuration
- Credentials are fetched from the `users` table (admin user)
- Scripts are designed to be idempotent (safe to run multiple times)
- Each script should have clear success/failure exit codes
