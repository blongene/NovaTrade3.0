# scripts/init_db_backbone.py
import os, psycopg2

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

DDL = open("db_schema.sql", "r", encoding="utf-8").read().split("URL:")[0]  # strip first line if present

print("Connecting to", DB_URL)
cx = psycopg2.connect(DB_URL)
cur = cx.cursor()
cur.execute(DDL)
cx.commit()
cx.close()
print("✅ DB backbone schema ensured.")
