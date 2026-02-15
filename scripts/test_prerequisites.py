"""
Quick test script to verify the instruments sync script setup.
This checks if all prerequisites are met before running the actual sync.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from app.config import config


def test_prerequisites():
    """Test if all prerequisites for sync_instruments.py are met"""
    print("=" * 60)
    print("🧪 Testing Instruments Sync Prerequisites")
    print("=" * 60)
    
    all_passed = True
    
    # Test 1: Database Connection
    print("\n1️⃣  Testing database connection...")
    try:
        db_config = config['development']
        engine = create_engine(db_config.SQLALCHEMY_DATABASE_URI)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("   ✅ Database connection successful")
    except Exception as e:
        print(f"   ❌ Database connection failed: {e}")
        all_passed = False
    
    # Test 2: Instruments Table Exists
    print("\n2️⃣  Checking if instruments table exists...")
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'instruments'
                )
            """))
            exists = result.fetchone()[0]
            if exists:
                print("   ✅ Instruments table exists")
            else:
                print("   ❌ Instruments table does not exist")
                print("      Run: flask db upgrade")
                all_passed = False
    except Exception as e:
        print(f"   ❌ Error checking table: {e}")
        all_passed = False
    
    # Test 3: Required Packages
    print("\n3️⃣  Checking required packages...")
    try:
        import pandas
        print("   ✅ pandas installed")
    except ImportError:
        print("   ❌ pandas not installed")
        print("      Run: pip install pandas")
        all_passed = False
    
    # Test 4: Internet Connection
    print("\n4️⃣  Checking internet connection to Kite...")
    try:
        import urllib.request
        code = urllib.request.urlopen("https://api.kite.trade/instruments").getcode()
        if code == 200:
            print("   ✅ Kite public endpoint accessible")
        else:
            print(f"   ❌ Kite endpoint returned status {code}")
            all_passed = False
    except Exception as e:
        print(f"   ❌ Could not connect to Kite endpoint: {e}")
        all_passed = False
    
    # Summary
    print("\n" + "=" * 60)
    if all_passed:
        print("✅ All prerequisites met! Ready to run sync_instruments.py")
        print("\nRun the script:")
        print("  python scripts/sync_instruments.py")
    else:
        print("❌ Some prerequisites not met. Please fix the issues above.")
    print("=" * 60)
    
    return all_passed


if __name__ == "__main__":
    success = test_prerequisites()
    sys.exit(0 if success else 1)
