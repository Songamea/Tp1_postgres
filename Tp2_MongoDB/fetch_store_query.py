#!/usr/bin/env python3
"""Fetch data from an API, store it into MongoDB and demonstrate queries.

Usage examples:
  python fetch_store_query.py --insert
  python fetch_store_query.py --insert --show
  python fetch_store_query.py --api "https://api.example.com/data" --insert

Configure `Tp2_MongoDB/.env` with `MONGO_URL`, `MONGO_DB`, `MONGO_COLLECTION` (and optional `API_URL`).
"""
import os
import argparse
import requests
from connexion import load_env, get_collection


def fetch(api_url: str):
    resp = requests.get(api_url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def store(collection, data):
    # If the API returns a list, insert_many; else insert_one.
    if isinstance(data, list):
        if not data:
            return []
        res = collection.insert_many(data)
        return res.inserted_ids
    else:
        res = collection.insert_one(data)
        return res.inserted_id


def demo_queries(collection, limit: int = 5):
    print("\n--- Exemple de requêtes ---")
    one = collection.find_one()
    print("\nfind_one() ->", one)

    total = collection.count_documents({})
    print("\ncount_documents({}) ->", total)

    print(f"\nDerniers {limit} documents (sans _id):")
    for doc in collection.find({}, {"_id": 0}).sort([("_id", -1)]).limit(limit):
        print(doc)

    # Aggregation example: group by a key if present
    print('\nAggregation sample (group by field "type" if exists):')
    pipeline = [
        {"$group": {"_id": "$type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    try:
        for row in collection.aggregate(pipeline):
            print(row)
    except Exception:
        print('Aggregation failed (no "type" field maybe)')


def main():
    parser = argparse.ArgumentParser(description="Fetch API and store into MongoDB")
    parser.add_argument("--env", help="Path to .env file", default=None)
    parser.add_argument(
        "--api", help="API URL to fetch (overrides API_URL in .env)", default=None
    )
    parser.add_argument(
        "--insert", action="store_true", help="Insert fetched data into MongoDB"
    )
    parser.add_argument(
        "--show", action="store_true", help="Run example queries after insert"
    )
    parser.add_argument("--limit", type=int, default=5, help="Limit for demo queries")
    args = parser.parse_args()

    load_env(args.env)

    api_url = args.api or os.getenv("API_URL")
    if not api_url:
        raise SystemExit("API URL not provided. Set API_URL in .env or pass --api")

    try:
        data = fetch(api_url)
    except Exception as e:
        raise SystemExit(f"Failed to fetch API: {e}")

    print("Fetched data type:", type(data))

    if args.insert:
        coll = get_collection()
        try:
            res = store(coll, data)
            print("Inserted:", res)
        except Exception as e:
            raise SystemExit(f"Insert failed: {e}")

        if args.show:
            demo_queries(coll, limit=args.limit)


if __name__ == "__main__":
    main()
