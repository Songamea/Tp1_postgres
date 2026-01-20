#!/usr/bin/env python3
from connexion import get_mongo_client, load_env, get_collection
import sys
import argparse


def main():
    parser = argparse.ArgumentParser(description="Test MongoDB connection")
    parser.add_argument("--env", help="Path to .env file", default=None)
    parser.add_argument("--insert", action="store_true", help="Insert test document")
    args = parser.parse_args()

    # load env (from provided path or default .env next to connexion.py)
    load_env(args.env)

    try:
        client = get_mongo_client()
        # quick server check
        client.admin.command("ping")
        print("SUCCESS: Connected to MongoDB")

        if args.insert:
            coll = get_collection(client=client)
            res = coll.insert_one({"test": "ok"})
            print("Inserted test document id:", res.inserted_id)

    except Exception as e:
        print("ERROR: Connection failed:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
