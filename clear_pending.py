import sqlite3

DB = "incident_manager/incidents.db"

conn = sqlite3.connect(DB)

cursor = conn.execute(
    "DELETE FROM incidents WHERE status = ?",
    ("pending",)
)

conn.commit()

print(f"Deleted {cursor.rowcount} pending incidents")

conn.close()