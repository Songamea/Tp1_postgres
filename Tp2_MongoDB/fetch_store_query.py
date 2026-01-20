#!/usr/bin/env python3

import os
import argparse
import time
import requests
from connexion import load_env, get_collection


def fetch(api_url: str):
    resp = requests.get(api_url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def store(collection, data):
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
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Number of times to call the API and insert results",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0,
        help="Seconds to wait between repeated requests",
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Collect fetched items and do a single insert_many (or batched flushes)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="If >0, flush the batch every N items (uses insert_many)",
    )
    args = parser.parse_args()

    load_env(args.env)

    api_url = args.api or os.getenv("API_URL")
    if not api_url:
        raise SystemExit("API URL not provided. Set API_URL in .env or pass --api")

    coll = None
    if args.insert:
        coll = get_collection()

    batch_items = []

    for i in range(args.repeat):
        try:
            data = fetch(api_url)
        except Exception as e:
            print(f"Failed to fetch API on iteration {i+1}: {e}")
            continue

        print(f"Iteration {i+1}/{args.repeat} - fetched type:", type(data))

        if args.insert and coll is not None:
            if args.batch:
                if isinstance(data, list):
                    batch_items.extend(data)
                else:
                    batch_items.append(data)

                if args.batch_size > 0 and len(batch_items) >= args.batch_size:
                    try:
                        res = coll.insert_many(batch_items)
                        print(f"Flushed batch insert ({len(res.inserted_ids)} docs)")
                    except Exception as e:
                        print(f"Batch insert failed on iteration {i+1}: {e}")
                    batch_items = []
            else:
                try:
                    res = store(coll, data)
                    print("Inserted:", res)
                except Exception as e:
                    print(f"Insert failed on iteration {i+1}: {e}")

        if i < args.repeat - 1 and args.interval > 0:
            time.sleep(args.interval)

    if args.insert and args.batch and coll is not None and batch_items:
        try:
            res = coll.insert_many(batch_items)
            print(f"Final batch inserted ({len(res.inserted_ids)} docs)")
        except Exception as e:
            print(f"Final batch insert failed: {e}")

    if args.show and coll is not None:
        demo_queries(coll, limit=args.limit)


if __name__ == "__main__":
    main()
