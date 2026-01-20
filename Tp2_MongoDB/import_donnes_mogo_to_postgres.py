#!/usr/bin/env python3
from __future__ import annotations
import os
import json
import argparse
from pathlib import Path
from typing import Any, Dict, Iterable
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values


def load_env(env_path: str | None = None) -> None:
    if env_path:
        load_dotenv(env_path)
    else:
        env_file = Path(__file__).parent.joinpath(".env")
        if env_file.exists():
            load_dotenv(env_file)


def get_mongo_collection(mongo_url: str, db_name: str, collection_name: str):
    client = MongoClient(mongo_url)
    db = client[db_name]
    return db[collection_name]


def iter_documents(
    collection, query: Dict[str, Any] | None = None, limit: int | None = None
) -> Iterable[Dict[str, Any]]:
    cursor = collection.find(query or {})
    if limit:
        cursor = cursor.limit(limit)
    for doc in cursor:
        yield doc


def prepare_row(doc: Dict[str, Any]) -> tuple:
    # Convert Mongo _id to string and store the rest as JSON
    _id = doc.get("_id")
    id_str = str(_id)
    message = doc.get("message")
    timestamp = doc.get("timestamp")
    # normalize timestamp: support seconds or milliseconds epochs, or ISO strings
    ts_val = None
    if isinstance(timestamp, (int, float)):
        # heuristics: if > 1e12 assume milliseconds
        if timestamp > 1e12:
            ts_val = datetime.fromtimestamp(timestamp / 1000.0)
        else:
            ts_val = datetime.fromtimestamp(timestamp)
    elif isinstance(timestamp, str):
        try:
            iv = int(timestamp)
            if iv > 1e12:
                ts_val = datetime.fromtimestamp(iv / 1000.0)
            else:
                ts_val = datetime.fromtimestamp(iv)
        except Exception:
            try:
                ts_val = datetime.fromisoformat(timestamp)
            except Exception:
                ts_val = None

    lattitude = doc.get("iss_position", {}).get("latitude", 0.0)
    longitude = doc.get("iss_position", {}).get("longitude", 0.0)
    return (id_str, message, ts_val, float(lattitude), float(longitude))


def ensure_table(conn, table_name: str):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id TEXT PRIMARY KEY,
        message VARCHAR(255) NOT NULL,
        timestamp TIMESTAMP,
        lattitude FLOAT NOT NULL,
        longitude FLOAT NOT NULL
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


def batch_insert(conn, table_name: str, rows: list[tuple]):
    if not rows:
        return 0
    sql = f"INSERT INTO {table_name} (id, message, timestamp, lattitude, longitude) VALUES %s ON CONFLICT (id) DO NOTHING"
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, template=None, page_size=1000)
        conn.commit()
    return len(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Export MongoDB -> Postgres (Supabase)"
    )
    parser.add_argument("--env", default=None, help="Path to .env file")
    parser.add_argument(
        "--mongo-url",
        default=None,
        help="Mongo connection string (overrides .env MONGO_URL)",
    )
    parser.add_argument("--mongo-db", default=None, help="Mongo DB name")
    parser.add_argument(
        "--mongo-collection", default=None, help="Mongo collection name"
    )
    parser.add_argument(
        "--postgres-url",
        default=None,
        help="Postgres connection URL (overrides .env POSTGRES_URL)",
    )
    parser.add_argument(
        "--table", default="mongo_import", help="Target Postgres table name"
    )
    parser.add_argument(
        "--batch-size", type=int, default=500, help="Number of rows per INSERT batch"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max number of documents to transfer (0 = all)",
    )
    parser.add_argument(
        "--drop-table", action="store_true", help="Drop table before creating it"
    )
    parser.add_argument(
        "--query", default=None, help="Mongo query as JSON string to filter documents"
    )
    args = parser.parse_args()

    load_env(args.env)

    mongo_url = args.mongo_url or os.getenv("MONGO_URL")
    mongo_db = args.mongo_db or os.getenv("MONGO_DB")
    mongo_collection = args.mongo_collection or os.getenv("MONGO_COLLECTION")

    pg_url = (
        args.postgres_url
        or os.getenv("POSTGRES_URL")
        or os.getenv("SUPABASE_DB_URL")
        or os.getenv("SUPABASE_URL")
    )

    if not (mongo_url and mongo_db and mongo_collection and pg_url):
        raise SystemExit(
            "Missing configuration. Ensure MONGO_URL, MONGO_DB, MONGO_COLLECTION and POSTGRES_URL are set (or pass via args)."
        )

    if args.query:
        try:
            query = json.loads(args.query)
        except Exception as e:
            raise SystemExit(f"Invalid JSON for --query: {e}")
    else:
        query = None

    coll = get_mongo_collection(mongo_url, mongo_db, mongo_collection)

    conn = psycopg2.connect(pg_url)

    if args.drop_table:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {args.table}")
            conn.commit()

    ensure_table(conn, args.table)

    batch = []
    total = 0
    limit = args.limit or None
    for doc in iter_documents(coll, query=query, limit=limit):
        batch.append(prepare_row(doc))
        if len(batch) >= args.batch_size:
            inserted = batch_insert(conn, args.table, batch)
            total += inserted
            print(f"Inserted batch {total} rows (last batch {inserted})")
            batch = []

    # final flush
    if batch:
        inserted = batch_insert(conn, args.table, batch)
        total += inserted
        print(f"Final insert: {inserted} rows. Total inserted: {total}")
    else:
        print(f"Total inserted: {total}")

    conn.close()


if __name__ == "__main__":
    main()
