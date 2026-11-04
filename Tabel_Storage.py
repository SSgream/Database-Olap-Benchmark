import os
import sys
import csv
import json
import argparse
import urllib.request
import urllib.parse
from datetime import datetime

# Optional database driver imports
try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

try:
    from pymongo import MongoClient
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False


# =========================================================
# 1. POSTGRESQL TABLE & INDEX STORAGE EXPORTER
# =========================================================

def export_postgres_table_storage(config, output_csv):
    """
    Mengambil rincian ukuran fisik per tabel di PostgreSQL:
    - Data Heap Size
    - Index Size
    - Total Size (Heap + Index)
    - Row Count
    - List of Index Names
    """
    if not HAS_PSYCOPG2:
        print("[PostgreSQL] psycopg2 tidak terinstall. Melewati pencatatan PostgreSQL.")
        return False

    print("\n" + "="*70)
    print(" 1. MEMPROSES POSTGRESQL TABLE & INDEX STORAGE DETAILS")
    print("="*70)

    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        # Query detail tabel di schema public
        query = """
            SELECT 
                c.relname AS table_name,
                c.reltuples::bigint AS estimated_rows,
                pg_relation_size(c.oid) AS table_size_bytes,
                pg_indexes_size(c.oid) AS index_size_bytes,
                pg_total_relation_size(c.oid) AS total_size_bytes
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relkind = 'r'
            ORDER BY pg_total_relation_size(c.oid) DESC;
        """
        cur.execute(query)
        tables = cur.fetchall()

        rows_data = []
        for table_name, est_rows, table_bytes, index_bytes, total_bytes in tables:
            # Dapatkan exact count
            try:
                cur.execute(f'SELECT COUNT(*) FROM "{table_name}";')
                exact_rows = cur.fetchone()[0]
            except Exception:
                exact_rows = est_rows

            # Dapatkan daftar indeks
            idx_query = """
                SELECT indexname, pg_relation_size(quote_ident(indexname)::regclass) AS idx_size
                FROM pg_indexes
                WHERE schemaname = 'public' AND tablename = %s;
            """
            cur.execute(idx_query, (table_name,))
            idx_list = cur.fetchall()
            idx_names = [f"{idx[0]} ({idx[1]/1024/1024:.2f} MB)" for idx in idx_list]

            table_mb = table_bytes / (1024 * 1024)
            index_mb = index_bytes / (1024 * 1024)
            total_mb = total_bytes / (1024 * 1024)

            rows_data.append({
                "database": "PostgreSQL",
                "table_name": table_name,
                "row_count": exact_rows,
                "table_heap_size_bytes": table_bytes,
                "table_heap_size_mb": round(table_mb, 4),
                "index_size_bytes": index_bytes,
                "index_size_mb": round(index_mb, 4),
                "total_size_bytes": total_bytes,
                "total_size_mb": round(total_mb, 4),
                "index_count": len(idx_list),
                "index_details": " | ".join(idx_names)
            })

            print(f"  - Table '{table_name}': {exact_rows:,} baris | Heap: {table_mb:.2f} MB | Index: {index_mb:.2f} MB | Total: {total_mb:.2f} MB")

        # Write to CSV
        fieldnames = [
            "database", "table_name", "row_count", 
            "table_heap_size_bytes", "table_heap_size_mb", 
            "index_size_bytes", "index_size_mb", 
            "total_size_bytes", "total_size_mb", 
            "index_count", "index_details"
        ]
        
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_data)

        cur.close()
        conn.close()
        print(f"[Sukses] Data storage PostgreSQL berhasil disimpan ke: {output_csv}")
        return True

    except Exception as e:
        print(f"[Error PostgreSQL] {e}")
        return False


# =========================================================
# 2. CLICKHOUSE TABLE & COLUMN STORAGE EXPORTER
# =========================================================

def execute_clickhouse_http(ch_url, query, user="default", password=""):
    """
    Helper untuk mengeksekusi query SQL ke ClickHouse via HTTP REST Interface
    """
    params = {'query': query.strip().rstrip(";") + " FORMAT JSON"}
    url = f"{ch_url.rstrip('/')}/?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url)
    
    if user or password:
        auth_str = f"{user}:{password}"
        import base64
        encoded_auth = base64.b64encode(auth_str.encode('ascii')).decode('ascii')
        req.add_header("Authorization", f"Basic {encoded_auth}")

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            res_body = response.read().decode('utf-8')
            return json.loads(res_body)
    except Exception as e:
        print(f"[ClickHouse HTTP Error] Query failed: {e}")
        return None


def export_clickhouse_table_storage(ch_url, ch_db, output_table_csv, output_column_csv, user="default", password=""):
    """
    Mengambil rincian ukuran fisik per tabel dan per kolom di ClickHouse:
    - system.tables & system.parts: rows, bytes_compressed, bytes_uncompressed, marks_bytes
    - system.columns: data_compressed_bytes, data_uncompressed_bytes per kolom
    """
    print("\n" + "="*70)
    print(" 2. MEMPROSES CLICKHOUSE TABLE & COLUMN STORAGE DETAILS")
    print("="*70)

    # 1. Query per tabel via system.tables & system.parts
    table_query = f"""
        SELECT 
            table,
            engine,
            sum(rows) AS total_rows,
            sum(bytes_uncompressed) AS uncompressed_bytes,
            sum(data_compressed_bytes) AS compressed_bytes,
            sum(marks_bytes) AS marks_bytes,
            count() AS active_parts
        FROM system.parts
        WHERE database = '{ch_db}' AND active = 1
        GROUP BY table, engine
        ORDER BY compressed_bytes DESC
    """
    
    res_tables = execute_clickhouse_http(ch_url, table_query, user, password)
    
    if not res_tables or 'data' not in res_tables or not res_tables['data']:
        print(f"[ClickHouse] Tidak ada data ditemukan pada database '{ch_db}' via system.parts. Mencoba system.tables...")
        fallback_query = f"""
            SELECT 
                name AS table,
                engine,
                total_rows,
                total_bytes AS compressed_bytes
            FROM system.tables
            WHERE database = '{ch_db}'
        """
        res_tables = execute_clickhouse_http(ch_url, fallback_query, user, password)

    table_rows = []
    if res_tables and 'data' in res_tables:
        for item in res_tables['data']:
            tbl_name = item.get('table') or item.get('name')
            engine = item.get('engine', 'MergeTree')
            rows = int(item.get('total_rows', 0) or 0)
            comp_bytes = int(item.get('compressed_bytes', 0) or 0)
            uncomp_bytes = int(item.get('uncompressed_bytes', 0) or comp_bytes)
            marks_bytes = int(item.get('marks_bytes', 0) or 0)
            parts = int(item.get('active_parts', 0) or 0)

            comp_mb = comp_bytes / (1024 * 1024)
            uncomp_mb = uncomp_bytes / (1024 * 1024)
            marks_mb = marks_bytes / (1024 * 1024)
            ratio = round(uncomp_bytes / comp_bytes, 2) if comp_bytes > 0 else 1.0

            table_rows.append({
                "database": f"ClickHouse ({ch_db})",
                "table_name": tbl_name,
                "engine": engine,
                "row_count": rows,
                "compressed_size_bytes": comp_bytes,
                "compressed_size_mb": round(comp_mb, 4),
                "uncompressed_size_bytes": uncomp_bytes,
                "uncompressed_size_mb": round(uncomp_mb, 4),
                "compression_ratio": f"{ratio}x",
                "sparse_index_marks_bytes": marks_bytes,
                "sparse_index_marks_mb": round(marks_mb, 4),
                "active_parts_count": parts
            })

            print(f"  - Table '{tbl_name}': {rows:,} baris | Compressed: {comp_mb:.2f} MB | Uncompressed: {uncomp_mb:.2f} MB | Ratio: {ratio}x")

        # Write Table CSV
        fieldnames_tbl = [
            "database", "table_name", "engine", "row_count", 
            "compressed_size_bytes", "compressed_size_mb", 
            "uncompressed_size_bytes", "uncompressed_size_mb", 
            "compression_ratio", "sparse_index_marks_bytes", "sparse_index_marks_mb", "active_parts_count"
        ]
        with open(output_table_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames_tbl)
            writer.writeheader()
            writer.writerows(table_rows)
        print(f"[Sukses] Data tabel ClickHouse disimpan ke: {output_table_csv}")

    # 2. Query per kolom via system.columns
    col_query = f"""
        SELECT 
            table,
            name AS column_name,
            type AS column_type,
            data_compressed_bytes,
            data_uncompressed_bytes,
            marks_bytes
        FROM system.columns
        WHERE database = '{ch_db}'
        ORDER BY table, data_compressed_bytes DESC
    """
    res_cols = execute_clickhouse_http(ch_url, col_query, user, password)

    col_rows = []
    if res_cols and 'data' in res_cols:
        for item in res_cols['data']:
            tbl_name = item.get('table')
            col_name = item.get('column_name')
            col_type = item.get('column_type')
            c_bytes = int(item.get('data_compressed_bytes', 0) or 0)
            u_bytes = int(item.get('data_uncompressed_bytes', 0) or 0)
            m_bytes = int(item.get('marks_bytes', 0) or 0)

            c_mb = c_bytes / (1024 * 1024)
            u_mb = u_bytes / (1024 * 1024)
            ratio = round(u_bytes / c_bytes, 2) if c_bytes > 0 else 1.0

            col_rows.append({
                "database": f"ClickHouse ({ch_db})",
                "table_name": tbl_name,
                "column_name": col_name,
                "column_type": col_type,
                "compressed_size_bytes": c_bytes,
                "compressed_size_mb": round(c_mb, 4),
                "uncompressed_size_bytes": u_bytes,
                "uncompressed_size_mb": round(u_mb, 4),
                "compression_ratio": f"{ratio}x",
                "marks_bytes": m_bytes
            })

        fieldnames_col = [
            "database", "table_name", "column_name", "column_type",
            "compressed_size_bytes", "compressed_size_mb",
            "uncompressed_size_bytes", "uncompressed_size_mb",
            "compression_ratio", "marks_bytes"
        ]
        with open(output_column_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames_col)
            writer.writeheader()
            writer.writerows(col_rows)
        print(f"[Sukses] Data rincian kolom ClickHouse disimpan ke: {output_column_csv}")

    return True


# =========================================================
# 3. MONGODB COLLECTION & INDEX STORAGE EXPORTER
# =========================================================

def export_mongodb_collection_storage(mongo_uri, db_name, output_csv):
    """
    Mengambil rincian ukuran fisik per koleksi di MongoDB via collStats:
    - BSON Data Size (Uncompressed)
    - WiredTiger Storage Size (Compressed)
    - Total Index Size
    - Document Count & Avg Object Size
    - Index Names & Sizes
    """
    if not HAS_PYMONGO:
        print("[MongoDB] pymongo tidak terinstall. Melewati pencatatan MongoDB.")
        return False

    print("\n" + "="*70)
    print(" 3. MEMPROSES MONGODB COLLECTION & INDEX STORAGE DETAILS")
    print("="*70)

    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]

        collections = db.list_collection_names()
        rows_data = []

        for coll_name in collections:
            if coll_name.startswith("system."):
                continue

            try:
                stats = db.command("collStats", coll_name)
            except Exception as e:
                print(f"  [Warning] Gagal mengambil collStats untuk '{coll_name}': {e}")
                continue

            doc_count = stats.get("count", 0)
            data_bytes = stats.get("size", 0)  # BSON uncompressed data
            storage_bytes = stats.get("storageSize", 0)  # WiredTiger compressed storage
            avg_obj_bytes = stats.get("avgObjSize", 0)
            n_indexes = stats.get("nindexes", 0)
            total_index_bytes = stats.get("totalIndexSize", 0)
            index_sizes = stats.get("indexSizes", {})

            data_mb = data_bytes / (1024 * 1024)
            storage_mb = storage_bytes / (1024 * 1024)
            index_mb = total_index_bytes / (1024 * 1024)
            total_mb = storage_mb + index_mb
            ratio = round(data_bytes / storage_bytes, 2) if storage_bytes > 0 else 1.0

            idx_details = [f"{k}: {v/1024/1024:.2f} MB" for k, v in index_sizes.items()]

            rows_data.append({
                "database": "MongoDB",
                "collection_name": coll_name,
                "document_count": doc_count,
                "avg_document_size_bytes": round(avg_obj_bytes, 2),
                "bson_data_size_bytes": data_bytes,
                "bson_data_size_mb": round(data_mb, 4),
                "wiredtiger_storage_bytes": storage_bytes,
                "wiredtiger_storage_mb": round(storage_mb, 4),
                "compression_ratio": f"{ratio}x",
                "total_index_size_bytes": total_index_bytes,
                "total_index_size_mb": round(index_mb, 4),
                "total_collection_size_bytes": storage_bytes + total_index_bytes,
                "total_collection_size_mb": round(total_mb, 4),
                "index_count": n_indexes,
                "index_details": " | ".join(idx_details)
            })

            print(f"  - Collection '{coll_name}': {doc_count:,} docs | BSON Data: {data_mb:.2f} MB | Storage: {storage_mb:.2f} MB | Index: {index_mb:.2f} MB | Total: {total_mb:.2f} MB")

        fieldnames = [
            "database", "collection_name", "document_count", "avg_document_size_bytes",
            "bson_data_size_bytes", "bson_data_size_mb",
            "wiredtiger_storage_bytes", "wiredtiger_storage_mb",
            "compression_ratio", "total_index_size_bytes", "total_index_size_mb",
            "total_collection_size_bytes", "total_collection_size_mb",
            "index_count", "index_details"
        ]

        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_data)

        client.close()
        print(f"[Sukses] Data storage MongoDB berhasil disimpan ke: {output_csv}")
        return True

    except Exception as e:
        print(f"[Error MongoDB] {e}")
        return False


# =========================================================
# MAIN EXECUTION
# =========================================================

def main():
    parser = argparse.ArgumentParser(description="Database Table & Collection Storage Exporter")
    
    # PostgreSQL args
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", default="5434")
    parser.add_argument("--pg-db", default="skripsi_db")
    parser.add_argument("--pg-user", default="user")
    parser.add_argument("--pg-pass", default="password")

    # ClickHouse args
    parser.add_argument("--ch-url", default="http://localhost:8124")
    parser.add_argument("--ch-user", default="default")
    parser.add_argument("--ch-pass", default="clickhouse123")
    parser.add_argument("--ch-db", default="default")

    # MongoDB args
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27018/")
    parser.add_argument("--mongo-db", default="skripsi_db")

    # Output CSV names
    parser.add_argument("--pg-csv", default="postgres_table_storage_details.csv")
    parser.add_argument("--ch-table-csv", default="clickhouse_table_storage_details.csv")
    parser.add_argument("--ch-col-csv", default="clickhouse_column_storage_details.csv")
    parser.add_argument("--mongo-csv", default="mongodb_collection_storage_details.csv")

    args = parser.parse_args()

    pg_config = {
        'dbname': args.pg_db,
        'user': args.pg_user,
        'password': args.pg_pass,
        'host': args.pg_host,
        'port': args.pg_port
    }

    print("\n" + "="*70)
    print(" EXPORTING DATABASE STORAGE DETAILS TO INDIVIDUAL CSV FILES")
    print("="*70)

    # 1. Export PostgreSQL
    export_postgres_table_storage(pg_config, args.pg_csv)

    # 2. Export ClickHouse
    export_clickhouse_table_storage(args.ch_url, args.ch_db, args.ch_table_csv, args.ch_col_csv, args.ch_user, args.ch_pass)

    # 3. Export MongoDB
    export_mongodb_collection_storage(args.mongo_uri, args.mongo_db, args.mongo_csv)

    print("\n" + "="*70)
    print(" PROCESS COMPLETED SUCCESSFULLY")
    print(" All CSV files are generated and ready for Bab III thesis documentation.")
    print("="*70)

if __name__ == "__main__":
    main()
