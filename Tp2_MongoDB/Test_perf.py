import time
import json
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import psycopg2
from urllib.parse import urlparse
import pandas as pd

# Charger les variables d'environnement
load_dotenv()
Mongo_url = os.getenv("MONGO_URL")
Supabase_url = os.getenv("SUPABASE_URL")

# Constantes
DB_NAME = os.getenv("MONGO_DB")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION")

# ========== FONCTIONS MONGODB ==========


def connect_mongodb():
    """Établir la connexion à MongoDB"""
    try:
        client = MongoClient(Mongo_url, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        return client
    except Exception as e:
        print(f"Erreur connexion MongoDB: {e}")
        raise


def query_mongodb_with_explain(client, query_filter=None):
    """Exécuter une requête MongoDB avec explain() et mesurer le temps"""
    try:
        if query_filter is None:
            query_filter = {}  # Sélectionner tous les documents

        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Mesurer le temps d'exécution
        start_time = time.time()

        # Exécuter la requête
        results = list(collection.find(query_filter))

        end_time = time.time()
        execution_time = (end_time - start_time) * 1000  # Convertir en ms

        # Obtenir les stats d'explain
        explain_stats = collection.aggregate(
            [{"$match": query_filter}, {"$group": {"_id": None, "count": {"$sum": 1}}}]
        )
        explain_results = list(explain_stats)

        return {
            "execution_time_ms": execution_time,
            "rows_returned": len(results),
            "explain_info": f"Documents scannés: {len(results)}",
            "database": "MongoDB",
        }

    except Exception as e:
        print(f"Erreur requête MongoDB: {e}")
        raise


def get_mongodb_collection_stats(client):
    """Obtenir les statistiques de la collection MongoDB"""
    try:
        db = client[DB_NAME]
        stats = db.command("collstats", COLLECTION_NAME)

        return {
            "collection_name": COLLECTION_NAME,
            "document_count": stats.get("count", 0),
            "avg_size": stats.get("avgObjSize", 0),
            "storage_size": stats.get("size", 0),
        }
    except Exception as e:
        print(f"Erreur stats MongoDB: {e}")
        raise


# ========== FONCTIONS SUPABASE (PostgreSQL) ==========


def connect_supabase():
    """Établir la connexion à Supabase"""
    try:
        parsed = urlparse(Supabase_url)
        connection = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port,
            database=parsed.path.lstrip("/"),
            user=parsed.username,
            password=parsed.password,
        )
        return connection
    except Exception as e:
        print(f"Erreur connexion Supabase: {e}")
        raise


def query_supabase_with_explain(connection, query_sql, params=None):
    """Exécuter une requête Supabase avec EXPLAIN ANALYZE et mesurer le temps"""
    try:
        cursor = connection.cursor()

        # Mesurer le temps d'exécution de la requête normale
        start_time = time.time()

        if params:
            cursor.execute(query_sql, params)
        else:
            cursor.execute(query_sql)

        results = cursor.fetchall()
        end_time = time.time()
        execution_time = (end_time - start_time) * 1000  # Convertir en ms

        # Exécuter EXPLAIN ANALYZE
        explain_query = f"EXPLAIN ANALYZE {query_sql}"
        if params:
            cursor.execute(explain_query, params)
        else:
            cursor.execute(explain_query)

        explain_results = cursor.fetchall()

        cursor.close()

        # Parser les résultats d'EXPLAIN
        explain_text = "\n".join([row[0] for row in explain_results])

        return {
            "execution_time_ms": execution_time,
            "rows_returned": len(results),
            "explain_info": explain_text,
            "database": "Supabase (PostgreSQL)",
        }

    except Exception as e:
        print(f"Erreur requête Supabase: {e}")
        raise


def get_supabase_table_stats(connection):
    """Obtenir les statistiques de la table Supabase"""
    try:
        cursor = connection.cursor()

        # Nombre de lignes
        cursor.execute("SELECT COUNT(*) FROM mongo_import")
        row_count = cursor.fetchone()[0]

        # Taille de la table
        cursor.execute(
            """
            SELECT pg_size_pretty(pg_total_relation_size('mongo_import')) as size
        """
        )
        table_size = cursor.fetchone()[0]

        cursor.close()

        return {
            "table_name": "mongo_import",
            "row_count": row_count,
            "table_size": table_size,
        }
    except Exception as e:
        print(f"Erreur stats Supabase: {e}")
        raise


# ========== FONCTIONS DE COMPARAISON ==========


def compare_simple_select():
    """Comparer les performances d'un SELECT simple sur les deux bases"""
    try:
        print("\n" + "=" * 70)
        print("TEST 1: SELECT SIMPLE (récupérer tous les documents/lignes)")
        print("=" * 70 + "\n")

        # MongoDB
        print("MONGODB - Exécution du SELECT...")
        mongo_client = connect_mongodb()
        mongo_result = query_mongodb_with_explain(mongo_client, {})
        mongo_client.close()

        print(f"Temps d'exécution: {mongo_result['execution_time_ms']:.2f} ms")
        print(f"Lignes retournées: {mongo_result['rows_returned']}")

        # Supabase
        print("\nSUPABASE - Exécution du SELECT...")
        supabase_conn = connect_supabase()
        supabase_result = query_supabase_with_explain(
            supabase_conn, "SELECT lattitude, longitude, timestamp FROM mongo_import"
        )
        supabase_conn.close()

        print(f"Temps d'exécution: {supabase_result['execution_time_ms']:.2f} ms")
        print(f"Lignes retournées: {supabase_result['rows_returned']}")

        # Comparaison
        return {
            "test": "SELECT Simple",
            "mongodb": mongo_result,
            "supabase": supabase_result,
        }

    except Exception as e:
        print(f"Erreur lors du test: {e}")
        raise


def compare_filtered_query():
    """Comparer les performances d'une requête filtrée"""
    try:
        print("\n" + "=" * 70)
        print("TEST 2: SELECT AVEC FILTRE (lattitude > 0)")
        print("=" * 70 + "\n")

        # MongoDB
        print("MONGODB - Exécution du SELECT avec filtre...")
        mongo_client = connect_mongodb()
        mongo_result = query_mongodb_with_explain(
            mongo_client, {"iss_position.lattitude": {"$gt": 0}}
        )
        mongo_client.close()

        print(f"Temps d'exécution: {mongo_result['execution_time_ms']:.2f} ms")
        print(f"Lignes retournées: {mongo_result['rows_returned']}")

        # Supabase
        print("\nSUPABASE - Exécution du SELECT avec filtre...")
        supabase_conn = connect_supabase()
        supabase_result = query_supabase_with_explain(
            supabase_conn,
            "SELECT lattitude, longitude, timestamp FROM mongo_import WHERE lattitude > 0",
        )
        supabase_conn.close()

        print(f"Temps d'exécution: {supabase_result['execution_time_ms']:.2f} ms")
        print(f"Lignes retournées: {supabase_result['rows_returned']}")

        # Comparaison
        return {
            "test": "SELECT avec Filtre",
            "mongodb": mongo_result,
            "supabase": supabase_result,
        }

    except Exception as e:
        print(f"Erreur lors du test: {e}")
        raise


def compare_aggregation():
    """Comparer les performances d'une agrégation (COUNT, AVG)"""
    try:
        print("\n" + "=" * 70)
        print("TEST 3: AGRÉGATION (COUNT et moyenne de lattitude)")
        print("=" * 70 + "\n")

        # MongoDB
        print("MONGODB - Exécution de l'agrégation...")
        mongo_client = connect_mongodb()

        db = mongo_client[DB_NAME]
        collection = db[COLLECTION_NAME]

        start_time = time.time()
        agg_results = list(
            collection.aggregate(
                [
                    {
                        "$group": {
                            "_id": None,
                            "count": {"$sum": 1},
                            "avg_lattitude": {"$avg": "$iss_position.lattitude"},
                        }
                    }
                ]
            )
        )
        end_time = time.time()

        mongo_time = (end_time - start_time) * 1000
        mongo_client.close()

        print(f"Temps d'exécution: {mongo_time:.2f} ms")
        if agg_results:
            count = agg_results[0].get("count", 0)
            avg_lat = agg_results[0].get("avg_lattitude", 0)
            if avg_lat is not None:
                print(f"Résultat: Count={count}, Avg Lat={avg_lat:.4f}")
            else:
                print(f"Résultat: Count={count}, Avg Lat=N/A")

        # Supabase
        print("\nSUPABASE - Exécution de l'agrégation...")
        supabase_conn = connect_supabase()
        supabase_result = query_supabase_with_explain(
            supabase_conn,
            "SELECT COUNT(*) as count, AVG(lattitude) as avg_lattitude FROM mongo_import",
        )
        supabase_conn.close()

        print(f"Temps d'exécution: {supabase_result['execution_time_ms']:.2f} ms")

        # Comparaison
        return {
            "test": "Agrégation",
            "mongodb": {"execution_time_ms": mongo_time, "database": "MongoDB"},
            "supabase": supabase_result,
        }

    except Exception as e:
        print(f"Erreur lors du test: {e}")
        raise


def display_summary(results_list):
    """Afficher un résumé comparatif de tous les tests"""
    print("\n" + "=" * 70)
    print("RÉSUMÉ COMPARATIF")
    print("=" * 70 + "\n")

    # Créer un DataFrame pour afficher
    summary_data = []

    for result in results_list:
        test_name = result["test"]
        mongo_time = result["mongodb"]["execution_time_ms"]
        supabase_time = result["supabase"]["execution_time_ms"]
        ratio = mongo_time / supabase_time if supabase_time > 0 else 0

        winner = "SUPABASE" if supabase_time < mongo_time else "MONGODB"

        summary_data.append(
            {
                "Test": test_name,
                "MongoDB (ms)": f"{mongo_time:.2f}",
                "Supabase (ms)": f"{supabase_time:.2f}",
                "Ratio (M/S)": f"{ratio:.2f}x",
                "Gagnant": winner,
            }
        )

    df = pd.DataFrame(summary_data)
    print(df.to_string(index=False))

    print("\nNote: Un ratio > 1 = MongoDB plus lent | Ratio < 1 = Supabase plus lent")


def display_stats():
    """Afficher les statistiques des bases"""
    print("\n" + "=" * 70)
    print("STATISTIQUES DES BASES")
    print("=" * 70 + "\n")

    # MongoDB Stats
    print("MONGODB:")
    try:
        mongo_client = connect_mongodb()
        mongo_stats = get_mongodb_collection_stats(mongo_client)
        mongo_client.close()

        print(f"  Collection: {mongo_stats['collection_name']}")
        print(f"  Nombre de documents: {mongo_stats['document_count']}")
        print(f"  Taille moyenne d'un doc: {mongo_stats['avg_size']} bytes")
        print(f"  Taille totale: {mongo_stats['storage_size'] / 1024 / 1024:.2f} MB")
    except Exception as e:
        print(f"Erreur: {e}")

    # Supabase Stats
    print("\nSUPABASE:")
    try:
        supabase_conn = connect_supabase()
        supabase_stats = get_supabase_table_stats(supabase_conn)
        supabase_conn.close()

        print(f"  Table: {supabase_stats['table_name']}")
        print(f"  Nombre de lignes: {supabase_stats['row_count']}")
        print(f"  Taille de la table: {supabase_stats['table_size']}")
    except Exception as e:
        print(f"Erreur: {e}")


# ========== INDEX HELPERS ==========


def create_mongo_index(client, field_name: str, index_name: str | None = None):
    db = client[DB_NAME]
    coll = db[COLLECTION_NAME]
    idx = coll.create_index([(field_name, 1)], name=index_name)
    return idx


def drop_mongo_index(client, index_name: str):
    db = client[DB_NAME]
    coll = db[COLLECTION_NAME]
    try:
        coll.drop_index(index_name)
    except Exception:
        pass


def create_postgres_index(conn, table: str, column: str, index_name: str):
    cur = conn.cursor()
    cur.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({column})")
    conn.commit()
    cur.close()


def drop_postgres_index(conn, index_name: str):
    cur = conn.cursor()
    try:
        cur.execute(f"DROP INDEX IF EXISTS {index_name}")
        conn.commit()
    except Exception:
        pass
    cur.close()


def compare_index_effect(field_mongo: str, column_pg: str, filter_value, runs: int = 3):
    """Compare filtered query times without index then with index.

    - `field_mongo` example: 'iss_position.lattitude'
    - `column_pg` example: 'lattitude'
    - `filter_value` is used as threshold (we use > filter_value)
    """
    print("\n" + "=" * 70)
    print(f"TEST INDEX: champ Mongo '{field_mongo}' vs Postgres '{column_pg}'")
    print("=" * 70 + "\n")

    # Prepare filters
    mongo_filter = (
        {f"{field_mongo}": {"$gt": filter_value}}
        if isinstance(filter_value, (int, float))
        else {field_mongo: filter_value}
    )
    pg_query = f"SELECT lattitude, longitude, timestamp FROM mongo_import WHERE {column_pg} > %s"

    # Connect
    mclient = connect_mongodb()
    pconn = connect_supabase()

    # Ensure no indexes
    try:
        # drop default index name if exists
        drop_mongo_index(mclient, field_mongo + "_1")
    except Exception:
        pass
    drop_postgres_index(pconn, f"idx_mongo_import_{column_pg}")

    # Run tests without index
    mongo_times = []
    pg_times = []
    print("-- Sans index --")
    for i in range(runs):
        mr = query_mongodb_with_explain(mclient, mongo_filter)
        mongo_times.append(mr["execution_time_ms"])

        pr = query_supabase_with_explain(pconn, pg_query, params=(filter_value,))
        pg_times.append(pr["execution_time_ms"])

    avg_mongo_noidx = sum(mongo_times) / len(mongo_times) if mongo_times else 0
    avg_pg_noidx = sum(pg_times) / len(pg_times) if pg_times else 0
    print(f"Mongo sans index (ms): {avg_mongo_noidx:.2f} (avg of {runs})")
    print(f"Postgres sans index (ms): {avg_pg_noidx:.2f} (avg of {runs})")

    # Create indexes
    print("\n-- Création des index --")
    idx_name_m = create_mongo_index(mclient, field_mongo, index_name=field_mongo + "_1")
    create_postgres_index(
        pconn, "mongo_import", column_pg, f"idx_mongo_import_{column_pg}"
    )
    print(f"Mongo index created: {idx_name_m}")

    # Run tests with index
    mongo_times = []
    pg_times = []
    print("-- Avec index --")
    for i in range(runs):
        mr = query_mongodb_with_explain(mclient, mongo_filter)
        mongo_times.append(mr["execution_time_ms"])

        pr = query_supabase_with_explain(pconn, pg_query, params=(filter_value,))
        pg_times.append(pr["execution_time_ms"])

    avg_mongo_idx = sum(mongo_times) / len(mongo_times) if mongo_times else 0
    avg_pg_idx = sum(pg_times) / len(pg_times) if pg_times else 0
    print(f"Mongo avec index (ms): {avg_mongo_idx:.2f} (avg of {runs})")
    print(f"Postgres avec index (ms): {avg_pg_idx:.2f} (avg of {runs})")

    # Cleanup: drop indexes
    drop_mongo_index(mclient, idx_name_m)
    drop_postgres_index(pconn, f"idx_mongo_import_{column_pg}")

    mclient.close()
    pconn.close()

    # Return two results so they can be displayed in the comparative summary
    return [
        {
            "test": f"Index effect {field_mongo}/{column_pg} (sans index)",
            "mongodb": {"execution_time_ms": avg_mongo_noidx, "database": "MongoDB"},
            "supabase": {
                "execution_time_ms": avg_pg_noidx,
                "database": "Supabase (PostgreSQL)",
            },
        },
        {
            "test": f"Index effect {field_mongo}/{column_pg} (avec index)",
            "mongodb": {"execution_time_ms": avg_mongo_idx, "database": "MongoDB"},
            "supabase": {
                "execution_time_ms": avg_pg_idx,
                "database": "Supabase (PostgreSQL)",
            },
        },
    ]


# ========== MAIN ==========

if __name__ == "__main__":
    try:
        print("\nBENCHMARK - MONGODB vs SUPABASE")
        print("=" * 70)

        # Afficher les stats
        display_stats()

        # Exécuter les tests
        results = []

        # Test 1: SELECT simple
        results.append(compare_simple_select())

        # Test 2: SELECT avec filtre
        results.append(compare_filtered_query())

        # Test 3: Agrégation
        results.append(compare_aggregation())

        # Test 4: Index effect (lattitude > 0)
        idx_results = compare_index_effect(
            "iss_position.lattitude", "lattitude", 0, runs=3
        )
        if isinstance(idx_results, list):
            results.extend(idx_results)
        else:
            results.append(idx_results)

        # Afficher le résumé
        display_summary(results)

        print("\nTests terminés!")

    except Exception as e:
        print(f"\nErreur générale: {e}")
