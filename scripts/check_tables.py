import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
from sqlalchemy import create_engine, text
eng = create_engine(os.getenv('DATABASE_URL'))
with eng.connect() as c:
    for tbl in ('options_data', 'option_chain_data'):
        rows = c.execute(text(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            f"WHERE table_name='{tbl}' "
            "ORDER BY ordinal_position"
        )).fetchall()
        if rows:
            print(f"\n=== {tbl} ===")
            for r in rows:
                print(f"  {r[0]:35} {r[1]}")
            # Sample row
            s = c.execute(text(f"SELECT * FROM {tbl} LIMIT 1")).fetchone()
            if s:
                keys = c.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    f"WHERE table_name='{tbl}' ORDER BY ordinal_position"
                )).fetchall()
                print(f"\n  -- sample row --")
                for k, v in zip([x[0] for x in keys], s):
                    print(f"  {k:35} {v}")
        else:
            print(f"\n{tbl}: NOT FOUND")
