import subprocess
import sys
import os
import time

def run_script(script_name, wait=False, new_window=True):
    print(f"Running {script_name}...")
    cmd = [sys.executable, script_name]
    
    if wait:
        subprocess.run(cmd)
        return None
    
    kwargs = {}
    if new_window and sys.platform == 'win32':
        kwargs['creationflags'] = subprocess.CREATE_NEW_CONSOLE
        
    return subprocess.Popen(cmd, **kwargs)

if __name__ == "__main__":
    print("--- NorthStar Automation Helper ---")
    
    # 1. Sync Instruments first (Blocking)
    if os.path.exists("scripts/sync_instruments.py"):
        print("Step 1: Syncing Instruments (This may take a moment)...")
        run_script("scripts/sync_instruments.py", wait=True)
    
    # 2. Start Web Server
    print("Step 2: Starting Web Server...")
    # run.py usually runs in debug mode with reloader.
    run_script("run.py", new_window=True)
    
    # 3. Start Data Streamers
    print("Step 3: Starting Data Streamers...")
    time.sleep(3) # Wait for web server to initialize DB
    
    if os.path.exists("scripts/stream_index.py"):
        run_script("scripts/stream_index.py", new_window=True)
        
    if os.path.exists("scripts/stream_option_chain.py"):
        run_script("scripts/stream_option_chain.py", new_window=True)
        
    print("\nAll services launched in separate windows.")
    print("You can close this window, the services will continue running.")
