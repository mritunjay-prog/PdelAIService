"""
db_service.py
-------------
Reads database configuration from config.properties and lists
all tables in the configured MySQL database (matchi).
"""

import mysql.connector
from mysql.connector import Error


# ─────────────────────────────────────────────
# Config loader  (parses key=value .properties)
# ─────────────────────────────────────────────

def load_config(path: str = "config.properties") -> dict:
    """Parse a Java-style .properties file into a plain dict."""
    config = {}
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            # skip blank lines and comments
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                config[key.strip()] = value.strip()
    return config


# ─────────────────────────────────────────────
# Database connection helper
# ─────────────────────────────────────────────

def get_connection(config: dict):
    """Create and return a MySQL connection using config values."""
    connection = mysql.connector.connect(
        host=config.get("db.host", "localhost"),
        port=int(config.get("db.port", 3306)),
        database=config.get("db.name"),
        user=config.get("db.username"),
        password=config.get("db.password"),
        connection_timeout=int(config.get("db.pool.connectionTimeout", 30000)) // 1000,
    )
    return connection


# ─────────────────────────────────────────────
# Service: list all tables
# ─────────────────────────────────────────────

def list_tables(connection) -> list[str]:
    """Return a list of all table names in the connected database."""
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES;")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables


# ─────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────

def main():
    print("=" * 50)
    print("  PdelAIService — DB Table Listing Service")
    print("=" * 50)

    # 1. Load config
    config = load_config("config.properties")
    db_name = config.get("db.name", "unknown")
    print(f"\n📦  Database : {db_name}")
    print(f"🌐  Host     : {config.get('db.host')}:{config.get('db.port')}")
    print(f"👤  User     : {config.get('db.username')}")

    # 2. Connect
    print("\n🔌  Connecting to database…")
    try:
        conn = get_connection(config)
        if conn.is_connected():
            print("✅  Connection successful!\n")
    except Error as e:
        print(f"❌  Connection failed: {e}")
        return

    # 3. List tables
    try:
        tables = list_tables(conn)
        print(f"📋  Tables in `{db_name}` ({len(tables)} found):")
        print("-" * 40)
        for i, table in enumerate(tables, start=1):
            print(f"  {i:>3}.  {table}")
        print("-" * 40)
    except Error as e:
        print(f"❌  Failed to fetch tables: {e}")
    finally:
        conn.close()
        print("\n🔒  Connection closed.")


if __name__ == "__main__":
    main()
