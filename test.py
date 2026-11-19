import sqlite3

databases = {
    "profiles.db": "anomaly_detector/profiles.db",
    "incidents.db": "incident_manager/incidents.db",
}

for name, path in databases.items():
    print(f"\n===== {name} =====")

    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT name, sql
        FROM sqlite_master
        WHERE type = 'table'
        AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)

    for table_name, schema in cursor.fetchall():
        print(f"\nTable: {table_name}")
        print(schema)

    conn.close()