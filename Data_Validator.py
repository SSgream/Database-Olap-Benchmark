import os
import sys
import csv
from datetime import datetime

import psycopg2
import requests
from pymongo import MongoClient

# ==============================================================================
# 1. KONFIGURASI KONEKSI
# Sesuaikan konfigurasi ini jika berbeda dari lingkungan database Anda.
# ==============================================================================
PG_CONFIG = {
    "dbname": "skripsi_db",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": "5434",
}

CH_CONFIG = {
    "host": "localhost",
    "port": "8124",
    "user": "default",
    "password": "clickhouse123",
    "database": "default",
}

MONGO_CONFIG = {
    "uri": "mongodb://localhost:27018",
    "db_name": "skripsi_db",
}

# Pilih skala saat menjalankan skrip:
# python data_validator.py SF0.1
# python data_validator.py SF0.5
# python data_validator.py SF1.0
VALID_SCALE_FACTORS = {"SF0.1", "SF0.5", "SF1.0"}
SCALE_FACTOR = sys.argv[1].upper() if len(sys.argv) > 1 else "SF1.0"
if SCALE_FACTOR == "SF1":
    SCALE_FACTOR = "SF1.0"
if SCALE_FACTOR not in VALID_SCALE_FACTORS:
    raise SystemExit(
        "Scale factor tidak valid. Gunakan: SF0.1, SF0.5, atau SF1.0."
    )

OUTPUT_CSV = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    f"data_validation_{SCALE_FACTOR.lower().replace('.', '')}.csv",
)

# Kolom kedua adalah nama koleksi MongoDB.
# None berarti entitas tersebut di-embed ke dalam dokumen MongoDB,
# sehingga jumlahnya tidak bisa dibandingkan langsung sebagai koleksi terpisah.
TABLE_MAPPING = [
    ("dim_prodi", None),
    ("dim_dosen", None),
    ("dim_mahasiswa", None),
    ("dim_mata_kuliah", None),
    ("dim_kelas_kuliah", None),
    ("dim_semester", None),
    ("dim_tipe_kehadiran", None),
    ("dim_pegawai", None),
    ("dim_golongan", None),
    ("dim_jabatan_fungsional", None),
    ("dim_jabatan_struktural", None),
    ("dim_unit_kerja", None),
    ("dim_jenis_pegawai", None),
    ("dim_status_pegawai", None),
    ("dim_pendidikan", None),
    ("fact_krs", "krs"),
    ("fact_presensi", "presensi"),
    ("fact_penasihat_akademik", "penasihat_akademik"),
    ("fact_riwayat_golongan", "riwayat_golongan"),
    ("fact_riwayat_jabatan_fungsional", "riwayat_jabatan_fungsional"),
    ("fact_riwayat_unit_kerja", "riwayat_unit_kerja"),
    ("fact_riwayat_jenis_pegawai", "riwayat_jenis_pegawai"),
    ("fact_riwayat_status_pegawai", "riwayat_status_pegawai"),
    ("fact_riwayat_program_studi", "riwayat_prodi"),
    ("fact_riwayat_mutasi_struktural", "riwayat_jabatan_struktural"),
    ("fact_riwayat_pendidikan", "riwayat_pendidikan"),
]


def log(message):
    print(f"[VALIDATOR {SCALE_FACTOR}] {message}")


def get_postgres_counts():
    counts = {}
    try:
        with psycopg2.connect(**PG_CONFIG) as conn:
            with conn.cursor() as cursor:
                for table, _ in TABLE_MAPPING:
                    try:
                        cursor.execute(f'SELECT COUNT(*) FROM "{table}";')
                        counts[table] = cursor.fetchone()[0]
                    except Exception as exc:
                        conn.rollback()
                        counts[table] = None
                        log(f"PostgreSQL: gagal memeriksa {table}: {exc}")
    except Exception as exc:
        log(f"Koneksi PostgreSQL gagal: {exc}")
        for table, _ in TABLE_MAPPING:
            counts.setdefault(table, None)
    return counts


def get_clickhouse_counts():
    counts = {}
    url = f"http://{CH_CONFIG['host']}:{CH_CONFIG['port']}/"
    for table, _ in TABLE_MAPPING:
        params = {
            "database": CH_CONFIG["database"],
            "user": CH_CONFIG["user"],
            "password": CH_CONFIG["password"],
            "query": f"SELECT count() FROM `{CH_CONFIG['database']}`.`{table}`",
        }
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                counts[table] = int(response.text.strip())
            else:
                counts[table] = None
                log(f"ClickHouse: gagal memeriksa {table}: {response.text.strip()}")
        except Exception as exc:
            counts[table] = None
            log(f"ClickHouse: gagal memeriksa {table}: {exc}")
    return counts


def get_mongodb_counts():
    counts = {}
    try:
        with MongoClient(
            MONGO_CONFIG["uri"], serverSelectionTimeoutMS=3000
        ) as client:
            client.admin.command("ping")
            db = client[MONGO_CONFIG["db_name"]]
            for _, mongo_collection in TABLE_MAPPING:
                if mongo_collection is None:
                    continue
                try:
                    counts[mongo_collection] = db[mongo_collection].count_documents({})
                except Exception as exc:
                    counts[mongo_collection] = None
                    log(f"MongoDB: gagal memeriksa {mongo_collection}: {exc}")
    except Exception as exc:
        log(f"Koneksi MongoDB gagal: {exc}")
        for _, mongo_collection in TABLE_MAPPING:
            if mongo_collection is not None:
                counts.setdefault(mongo_collection, None)
    return counts


def format_count(value):
    return f"{value:,}" if isinstance(value, int) else "ERROR/NA"


def validate():
    log("Memulai validasi jumlah record.")
    log("Catatan: validator ini memeriksa jumlah record, bukan kesamaan seluruh isi data.")

    pg_counts = get_postgres_counts()
    ch_counts = get_clickhouse_counts()
    mongo_counts = get_mongodb_counts()
    results = []

    print("\n" + "=" * 112)
    print(f"{'TABEL':34} | {'POSTGRESQL':12} | {'CLICKHOUSE':12} | {'MONGODB':12} | STATUS")
    print("=" * 112)

    for table, mongo_collection in TABLE_MAPPING:
        pg_count = pg_counts.get(table)
        ch_count = ch_counts.get(table)

        if mongo_collection is None:
            mongo_count = "EMBEDDED"
            if pg_count is None or ch_count is None:
                status = "CHECK_FAILED"
            elif pg_count == ch_count:
                status = "PG_CH_MATCH; MONGO_EMBEDDED_UNCHECKED"
            else:
                status = "PG_CH_MISMATCH"
        else:
            mongo_count = mongo_counts.get(mongo_collection)
            if pg_count is None or ch_count is None or mongo_count is None:
                status = "CHECK_FAILED"
            elif pg_count == ch_count == mongo_count:
                status = "MATCH_COUNT"
            else:
                status = "MISMATCH_COUNT"

        print(
            f"{table:34} | {format_count(pg_count):12} | "
            f"{format_count(ch_count):12} | {str(mongo_count):12} | {status}"
        )
        results.append({
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "scale_factor": SCALE_FACTOR,
            "table": table,
            "postgresql_count": pg_count if pg_count is not None else "ERROR",
            "clickhouse_count": ch_count if ch_count is not None else "ERROR",
            "mongodb_collection": mongo_collection or "embedded_in_fact_documents",
            "mongodb_count": mongo_count if isinstance(mongo_count, int) else mongo_count,
            "status": status,
        })

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as file:
        fieldnames = [
            "timestamp", "scale_factor", "table", "postgresql_count",
            "clickhouse_count", "mongodb_collection", "mongodb_count", "status"
        ]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    count_matches = sum(row["status"] == "MATCH_COUNT" for row in results)
    pg_ch_matches = sum(
        row["status"] == "PG_CH_MATCH; MONGO_EMBEDDED_UNCHECKED" for row in results
    )
    mismatches = sum("MISMATCH" in row["status"] for row in results)
    failures = sum(row["status"] == "CHECK_FAILED" for row in results)

    print("=" * 112)
    print(f"Ringkasan {SCALE_FACTOR}:")
    print(f"  Jumlah tabel/entitas yang count-nya cocok pada ketiga DBMS: {count_matches}")
    print(f"  Dimensi: jumlah PostgreSQL dan ClickHouse cocok, MongoDB belum diverifikasi: {pg_ch_matches}")
    print(f"  Ketidaksesuaian jumlah record: {mismatches}")
    print(f"  Pemeriksaan gagal/koneksi/tabel bermasalah: {failures}")
    print(f"CSV disimpan ke: {OUTPUT_CSV}")

    if mismatches == 0 and failures == 0:
        print("KESIMPULAN: Pemeriksaan jumlah record yang dapat dibandingkan tidak menemukan mismatch.")
        print("Ini belum membuktikan kesamaan seluruh isi data atau validitas embedded document MongoDB.")
    else:
        print("KESIMPULAN: Periksa baris MISMATCH_COUNT atau CHECK_FAILED sebelum benchmark.")


if __name__ == "__main__":
    validate()
