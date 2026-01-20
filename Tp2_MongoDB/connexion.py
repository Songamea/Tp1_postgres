import os
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient


def load_env(env_path: str | None = None) -> dict:
	"""Load environment variables from a `.env` file (if present) and return keys.

	Looks for a `.env` next to this file by default.
	"""
	if env_path:
		load_dotenv(env_path)
	else:
		env_file = Path(__file__).parent.joinpath('.env')
		if env_file.exists():
			load_dotenv(env_file)

	return {
		'MONGO_URL': os.getenv('MONGO_URL', '').strip(),
		'MONGO_DB': os.getenv('MONGO_DB', '').strip(),
		'MONGO_COLLECTION': os.getenv('MONGO_COLLECTION', '').strip(),
	}


def get_mongo_client(mongo_url: str | None = None, **kwargs) -> MongoClient:
	"""Return a configured `pymongo.MongoClient`.

	If `mongo_url` is not provided, reads `MONGO_URL` from the environment or `.env`.
	"""
	if not mongo_url:
		env = load_env()
		mongo_url = env.get('MONGO_URL')

	if not mongo_url:
		raise ValueError('MONGO_URL is not set. Put it in the .env file or set the environment variable.')

	return MongoClient(mongo_url, **kwargs)


def get_database(client: MongoClient | None = None, db_name: str | None = None):
	"""Return a `Database` instance using `MONGO_DB` if not provided."""
	if client is None:
		client = get_mongo_client()

	if not db_name:
		env = load_env()
		db_name = env.get('MONGO_DB')

	if not db_name:
		raise ValueError('MONGO_DB is not set. Put it in the .env file or set the environment variable.')

	return client[db_name]


def get_collection(client: MongoClient | None = None, db_name: str | None = None, collection_name: str | None = None):
	if collection_name is None:
		env = load_env()
		collection_name = env.get('MONGO_COLLECTION') or 'test_connection'

	db = get_database(client=client, db_name=db_name)
	return db[collection_name]

