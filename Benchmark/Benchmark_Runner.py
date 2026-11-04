import os
import sys
import time
import json
import csv
import argparse
import subprocess
from datetime import datetime
import statistics
import math
import json
import hashlib
from decimal import Decimal

import re
import psycopg2
import requests
from pymongo import MongoClient
from bson.decimal128 import Decimal128

# Database Configurations
POSTGRES_CONFIG = {
    'dbname': 'skripsi_db',
    'user': 'user',
    'password': 'password',
    'host': 'localhost',
    'port': '5434'
}

CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': '8124',
    'user': 'default',
    'password': 'clickhouse123',
    'database': 'default'
}

MONGODB_CONFIG = {
    'uri': 'mongodb://localhost:27018/',
    'db_name': 'skripsi_db'
}

CONTAINER_NAMES = {
    'postgres': ['postgres', 'skripsi-postgres-1', 'postgres_db', 'postgres_skripsi', 'skripsi_postgres_1', 'skripsi-postgres'],
    'clickhouse': ['clickhouse', 'skripsi-clickhouse-1', 'clickhouse_db', 'clickhouse_skripsi', 'skripsi_clickhouse_1', 'skripsi-clickhouse'],
    'mongodb': ['mongodb', 'skripsi-mongodb-1', 'mongo_db', 'mongodb_skripsi', 'mongo_skripsi', 'skripsi_mongodb_1', 'skripsi-mongodb']
}


# ------------------------------------------------------------------------------
# HELPER PEMBULATAN SETARA (half-up, aritmetika Decimal eksak) UNTUK MONGODB
# PostgreSQL : ROUND(numeric)        -> half-up, eksak
# ClickHouse : round(Decimal)        -> half-up, eksak (asal TIDAK lewat Float64)
# MongoDB    : $round bawaan         -> half-EVEN (bankir), sehingga diganti manual
# ------------------------------------------------------------------------------
_HALF = Decimal128("0.5")

def mg_dec_avg(field):
    """Rata-rata dalam Decimal128 (dipakai di $group)."""
    return {"$avg": {"$toDecimal": field}}

def mg_round_half_up(dec_expr, places=2):
    """floor(x * 10^p + 0.5) / 10^p dalam Decimal128, hasil akhir dijadikan double."""
    scale = 10 ** places
    return {"$toDouble": {"$divide": [
        {"$floor": {"$add": [{"$multiply": [dec_expr, scale]}, _HALF]}},
        scale
    ]}}

def mg_pct(num_field, den_field, places=2):
    """round_half_up(num * 100 / den) dalam Decimal128."""
    return mg_round_half_up(
        {"$divide": [{"$multiply": [{"$toDecimal": num_field}, 100]}, den_field]},
        places
    )

# 6 Kategori Kueri OLAP Kompleks (Q1 - Q6)

# CATATAN PEMBULATAN stddev: sd = sqrt(variansi) hanya bisa persis di titik tengah (x.xx5) pada grup kecil
# (mis. n=2 dengan selisih ganjil sen). PostgreSQL menghitungnya eksak (numeric); ClickHouse/MongoDB memakai
# Float64 sehingga noise ~1e-16 menentukan arah pembulatan. Ditambah 1e-9 (>> noise, << 0.005) agar titik
# tengah dibulatkan ke atas seperti PostgreSQL. Uji 30.000 grup n=2..4: salah 4.507 -> 0.
QUERIES = {
    "Q1": {
        "name": "Agregasi Kompleks, Conditional CASE WHEN & Pembobotan Formula Nilai",
        "postgres": """
            SELECT 
                COUNT(f.krs_key) AS total_krs,
                ROUND(AVG(f.nilai_akhir)::numeric, 2) AS rata_nilai_akhir,
                ROUND(STDDEV_POP(f.nilai_akhir)::numeric, 2) AS stddev_nilai_akhir,
                COUNT(CASE WHEN f.nilai_akhir >= 60.0 THEN 1 END) AS total_lulus,
                COUNT(CASE WHEN f.nilai_akhir < 60.0 THEN 1 END) AS total_tidak_lulus,
                ROUND((COUNT(CASE WHEN f.nilai_akhir >= 60.0 THEN 1 END) * 100.0 / NULLIF(COUNT(f.krs_key), 0))::numeric, 2) AS persentase_kelulusan,
                ROUND(AVG(CASE WHEN f.nilai_akhir >= 85.0 THEN f.nilai_akhir END)::numeric, 2) AS rata_nilai_kategori_a,
                ROUND(AVG(0.20 * f.nilai_tugas + 0.30 * f.nilai_uts + 0.50 * f.nilai_uas)::numeric, 2) AS rata_komposisi_nilai
            FROM fact_krs f
            WHERE f.is_approved = TRUE;
        """,
        "clickhouse": """
            SELECT 
                count(f.krs_key) AS total_krs,
                round(avg(f.nilai_akhir), 2) AS rata_nilai_akhir,
                round(stddevPop(f.nilai_akhir), 2) AS stddev_nilai_akhir,
                countIf(f.nilai_akhir >= 60.0) AS total_lulus,
                countIf(f.nilai_akhir < 60.0) AS total_tidak_lulus,
                round(countIf(f.nilai_akhir >= 60.0) * 100.0 / nullIf(count(f.krs_key), 0), 2) AS persentase_kelulusan,
                if(countIf(f.nilai_akhir >= 85.0) = 0, CAST(NULL, 'Nullable(Float64)'), round(avgIf(f.nilai_akhir, f.nilai_akhir >= 85.0), 2)) AS rata_nilai_kategori_a,
                round(avg(0.20 * f.nilai_tugas + 0.30 * f.nilai_uts + 0.50 * f.nilai_uas), 2) AS rata_komposisi_nilai
            FROM fact_krs f
            WHERE f.is_approved = 1
        """,
        "mongodb": [
            { "$match": { "is_approved": True } },
            {
                "$group": {
                    "_id": None,
                    "total_krs": { "$sum": 1 },
                    "rata_nilai_akhir": { "$avg": "$nilai_akhir" },
                    "stddev_nilai_akhir": { "$stdDevPop": "$nilai_akhir" },
                    "total_lulus": {
                        "$sum": { "$cond": [{ "$gte": ["$nilai_akhir", 60.0] }, 1, 0] }
                    },
                    "total_tidak_lulus": {
                        "$sum": { "$cond": [{ "$lt": ["$nilai_akhir", 60.0] }, 1, 0] }
                    },
                    "kategori_a_sum": {
                        "$sum": { "$cond": [{ "$gte": ["$nilai_akhir", 85.0] }, "$nilai_akhir", 0] }
                    },
                    "kategori_a_count": {
                        "$sum": { "$cond": [{ "$gte": ["$nilai_akhir", 85.0] }, 1, 0] }
                    },
                    "rata_komposisi_nilai": {
                        "$avg": {
                            "$add": [
                                { "$multiply": [0.20, "$nilai_tugas"] },
                                { "$multiply": [0.30, "$nilai_uts"] },
                                { "$multiply": [0.50, "$nilai_uas"] }
                            ]
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "total_krs": 1,
                    "rata_nilai_akhir": { "$round": ["$rata_nilai_akhir", 2] },
                    "stddev_nilai_akhir": { "$round": ["$stddev_nilai_akhir", 2] },
                    "total_lulus": 1,
                    "total_tidak_lulus": 1,
                    "persentase_kelulusan": {
                        "$round": [
                            { "$multiply": [{ "$divide": ["$total_lulus", "$total_krs"] }, 100] },
                            2
                        ]
                    },
                    "rata_nilai_kategori_a": {
                        "$round": [
                            { "$cond": [{ "$gt": ["$kategori_a_count", 0] }, { "$divide": ["$kategori_a_sum", "$kategori_a_count"] }, None] },
                            2
                        ]
                    },
                    "rata_komposisi_nilai": { "$round": ["$rata_komposisi_nilai", 2] }
                }
            }
        ]
    },
    "Q2": {
        "name": "GROUP BY Multi-Dimensi, Distinct Count Mahasiswa Unik & Statistik Nilai",
        "postgres": """
            SELECT
                p.fakultas_nama,
                p.nama_resmi AS nama_prodi,
                m.angkatan,
                s.tahun AS tahun_semester,
                COUNT(f.krs_key) AS total_krs,
                COUNT(DISTINCT f.mahasiswa_key) AS total_mahasiswa_unik,
                ROUND(AVG(f.nilai_akhir)::numeric, 2) AS rata_nilai_akhir,
                ROUND(STDDEV_POP(f.nilai_akhir)::numeric, 2) AS stddev_nilai,
                COUNT(CASE WHEN f.nilai_akhir >= 80.0 THEN 1 END) AS total_sangat_baik,
                ROUND(
                    (COUNT(CASE WHEN f.nilai_akhir >= 80.0 THEN 1 END) * 100.0
                     / NULLIF(COUNT(f.krs_key), 0))::numeric, 2
                ) AS rasio_sangat_baik
            FROM fact_krs f
            LEFT JOIN dim_prodi p ON f.prodi_key = p.prodi_key
            LEFT JOIN dim_mahasiswa m ON f.mahasiswa_key = m.mahasiswa_key
            LEFT JOIN dim_semester s ON f.semester_key = s.semester_key
            WHERE f.is_approved = TRUE
            GROUP BY p.fakultas_nama, p.nama_resmi, m.angkatan, s.tahun
            HAVING COUNT(f.krs_key) >= 20
               AND AVG(f.nilai_akhir) >= 65.0
            ORDER BY p.fakultas_nama ASC, rata_nilai_akhir DESC, total_krs DESC;
        """,
        "clickhouse": """
            SELECT
                p.fakultas_nama,
                p.nama_resmi AS nama_prodi,
                m.angkatan,
                s.tahun AS tahun_semester,
                count(f.krs_key) AS total_krs,
                uniqExact(f.mahasiswa_key) AS total_mahasiswa_unik,
                round(toDecimal128(sum(f.nilai_akhir), 10) / nullIf(count(f.nilai_akhir), 0), 2) AS rata_nilai_akhir,
                round(stddevPop(f.nilai_akhir) + 0.000000001, 2) AS stddev_nilai,
                countIf(f.nilai_akhir >= 80.0) AS total_sangat_baik,
                round(toDecimal128(countIf(f.nilai_akhir >= 80.0), 10) * 100
                      / nullIf(count(f.krs_key), 0), 2) AS rasio_sangat_baik
            FROM fact_krs f
            LEFT JOIN dim_prodi p ON f.prodi_key = p.prodi_key
            LEFT JOIN dim_mahasiswa m ON f.mahasiswa_key = m.mahasiswa_key
            LEFT JOIN dim_semester s ON f.semester_key = s.semester_key
            WHERE f.is_approved = 1
            GROUP BY p.fakultas_nama, p.nama_resmi, m.angkatan, s.tahun
            HAVING count(f.krs_key) >= 20
               AND sum(f.nilai_akhir) >= 65 * count(f.nilai_akhir)
            ORDER BY p.fakultas_nama ASC, rata_nilai_akhir DESC, total_krs DESC;
        """,
        "mongodb": [
            {"$match": {"is_approved": True}},
            {
                "$group": {
                    "_id": {
                        "fakultas_nama": "$prodi.fakultas_nama",
                        "nama_prodi": "$prodi.nama_resmi",
                        "angkatan": "$mahasiswa.angkatan",
                        "tahun_semester": "$semester.tahun"
                    },
                    "total_krs": {"$sum": 1},
                    "mahasiswa_set": {"$addToSet": "$mahasiswa.id_mahasiswa"},
                    "rata_dec": mg_dec_avg("$nilai_akhir"),
                    "stddev_raw": {"$stdDevPop": "$nilai_akhir"},
                    "total_sangat_baik": {
                        "$sum": {"$cond": [{"$gte": ["$nilai_akhir", 80.0]}, 1, 0]}
                    }
                }
            },
            {
                "$match": {
                    "total_krs": {"$gte": 20},
                    "$expr": {"$gte": ["$rata_dec", 65]}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "fakultas_nama": "$_id.fakultas_nama",
                    "nama_prodi": "$_id.nama_prodi",
                    "angkatan": "$_id.angkatan",
                    "tahun_semester": "$_id.tahun_semester",
                    "total_krs": 1,
                    "total_mahasiswa_unik": {"$size": "$mahasiswa_set"},
                    "rata_nilai_akhir": mg_round_half_up("$rata_dec"),
                    "stddev_nilai": mg_round_half_up({"$toDecimal": {"$add": ["$stddev_raw", 1e-9]}}),
                    "total_sangat_baik": 1,
                    "rasio_sangat_baik": mg_pct("$total_sangat_baik", "$total_krs")
                }
            },
            {"$sort": {"fakultas_nama": 1, "rata_nilai_akhir": -1, "total_krs": -1}}
        ]
    },
    "Q3": {
        "name": "JOIN Kompleks Multi-Entitas Akademik (Dosen, Prodi, Semester, Kelas, MK)",
        "postgres": """
            SELECT
                d.nama AS nama_dosen,
                d.nip AS nip_dosen,
                p.nama_resmi AS nama_prodi,
                s.kode AS kode_semester,
                COUNT(f.krs_key) AS total_mahasiswa,
                COUNT(DISTINCT f.kelas_kuliah_key) AS total_kelas_diampu,
                COUNT(DISTINCT f.kelas_kuliah_key) * 3 AS total_sks_diampu,
                ROUND(AVG(f.nilai_akhir)::numeric, 2) AS rata_nilai_mahasiswa,
                ROUND(AVG(f.nilai_tugas)::numeric, 2) AS rata_tugas,
                ROUND(AVG(f.nilai_uts)::numeric, 2) AS rata_uts,
                COUNT(CASE WHEN f.nilai_akhir >= 85.0 THEN 1 END) AS total_nilai_a,
                ROUND((COUNT(CASE WHEN f.nilai_akhir >= 85.0 THEN 1 END) * 100.0
                       / NULLIF(COUNT(f.krs_key), 0))::numeric, 2) AS persentase_nilai_a
            FROM fact_krs f
            LEFT JOIN dim_dosen d ON f.dosen_koordinator_key = d.dosen_key
            LEFT JOIN dim_prodi p ON f.prodi_key = p.prodi_key
            LEFT JOIN dim_semester s ON f.semester_key = s.semester_key
            WHERE f.is_approved = TRUE
            GROUP BY d.nama, d.nip, p.nama_resmi, s.kode
            HAVING COUNT(f.krs_key) >= 15
            ORDER BY total_mahasiswa DESC, rata_nilai_mahasiswa DESC;
        """,
        "clickhouse": """
            SELECT
                d.nama AS nama_dosen,
                d.nip AS nip_dosen,
                p.nama_resmi AS nama_prodi,
                s.kode AS kode_semester,
                count(f.krs_key) AS total_mahasiswa,
                uniqExact(f.kelas_kuliah_key) AS total_kelas_diampu,
                uniqExact(f.kelas_kuliah_key) * 3 AS total_sks_diampu,
                round(toDecimal128(sum(f.nilai_akhir), 10) / nullIf(count(f.nilai_akhir), 0), 2) AS rata_nilai_mahasiswa,
                round(toDecimal128(sum(f.nilai_tugas), 10) / nullIf(count(f.nilai_tugas), 0), 2) AS rata_tugas,
                round(toDecimal128(sum(f.nilai_uts), 10) / nullIf(count(f.nilai_uts), 0), 2) AS rata_uts,
                countIf(f.nilai_akhir >= 85.0) AS total_nilai_a,
                round(toDecimal128(countIf(f.nilai_akhir >= 85.0), 10) * 100
                      / nullIf(count(f.krs_key), 0), 2) AS persentase_nilai_a
            FROM fact_krs f
            LEFT JOIN dim_dosen d ON f.dosen_koordinator_key = d.dosen_key
            LEFT JOIN dim_prodi p ON f.prodi_key = p.prodi_key
            LEFT JOIN dim_semester s ON f.semester_key = s.semester_key
            WHERE f.is_approved = 1
            GROUP BY d.nama, d.nip, p.nama_resmi, s.kode
            HAVING total_mahasiswa >= 15
            ORDER BY total_mahasiswa DESC, rata_nilai_mahasiswa DESC
        """,
        "mongodb": [
            {"$match": {"is_approved": True}},
            {
                "$group": {
                    "_id": {
                        "nama_dosen": "$dosen_koordinator.nama",
                        "nip_dosen": "$dosen_koordinator.nip",
                        "nama_prodi": "$prodi.nama_resmi",
                        "kode_semester": "$semester.kode"
                    },
                    "total_mahasiswa": {"$sum": 1},
                    "kelas_set": {"$addToSet": "$kelas_kuliah.id_kelas_kuliah"},
                    "rata_nilai_dec": mg_dec_avg("$nilai_akhir"),
                    "rata_tugas_dec": mg_dec_avg("$nilai_tugas"),
                    "rata_uts_dec": mg_dec_avg("$nilai_uts"),
                    "total_nilai_a": {
                        "$sum": {"$cond": [{"$gte": ["$nilai_akhir", 85.0]}, 1, 0]}
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "nama_dosen": "$_id.nama_dosen",
                    "nip_dosen": "$_id.nip_dosen",
                    "nama_prodi": "$_id.nama_prodi",
                    "kode_semester": "$_id.kode_semester",
                    "total_mahasiswa": 1,
                    "total_kelas_diampu": {"$size": "$kelas_set"},
                    "total_sks_diampu": {"$multiply": [{"$size": "$kelas_set"}, 3]},
                    "rata_nilai_mahasiswa": mg_round_half_up("$rata_nilai_dec"),
                    "rata_tugas": mg_round_half_up("$rata_tugas_dec"),
                    "rata_uts": mg_round_half_up("$rata_uts_dec"),
                    "total_nilai_a": 1,
                    "persentase_nilai_a": mg_pct("$total_nilai_a", "$total_mahasiswa")
                }
            },
            {"$match": {"total_mahasiswa": {"$gte": 15}}},
            {"$sort": {"total_mahasiswa": -1, "rata_nilai_mahasiswa": -1}}
        ]
    },
        "Q4": {
        "name": "Filter Majemuk, Pattern Matching String & Analytic Window Function (Top-5 per Prodi)",
        "postgres": """
            WITH RankedKRS AS (
                SELECT 
                    m.nim,
                    m.nama_mahasiswa,
                    p.nama_resmi AS nama_prodi,
                    f.nilai_akhir,
                    f.nilai_tugas,
                    f.nilai_uts,
                    f.nilai_uas,
                    DENSE_RANK() OVER (
                        PARTITION BY p.prodi_key 
                        ORDER BY f.nilai_akhir DESC
                    ) AS rank_di_prodi
                FROM fact_krs f
                LEFT JOIN dim_mahasiswa m ON f.mahasiswa_key = m.mahasiswa_key
                LEFT JOIN dim_prodi p ON m.prodi_key = p.prodi_key
                WHERE f.nilai_akhir BETWEEN 60.00 AND 100.00
                  AND LOWER(p.nama_resmi) LIKE '%teknik%'
                  AND f.is_approved = TRUE
            )
            SELECT 
                nim,
                nama_mahasiswa,
                nama_prodi,
                nilai_akhir,
                nilai_tugas,
                nilai_uts,
                nilai_uas,
                rank_di_prodi
            FROM RankedKRS 
            WHERE rank_di_prodi <= 5
            ORDER BY nama_prodi ASC, rank_di_prodi ASC;
        """,
        "clickhouse": """
            WITH RankedKRS AS (
                SELECT 
                    m.nim,
                    m.nama_mahasiswa,
                    p.nama_resmi AS nama_prodi,
                    f.nilai_akhir,
                    f.nilai_tugas,
                    f.nilai_uts,
                    f.nilai_uas,
                    dense_rank() OVER (
                        PARTITION BY p.prodi_key 
                        ORDER BY f.nilai_akhir DESC
                    ) AS rank_di_prodi
                FROM fact_krs f
                LEFT JOIN dim_mahasiswa m ON f.mahasiswa_key = m.mahasiswa_key
                LEFT JOIN dim_prodi p ON m.prodi_key = p.prodi_key
                WHERE f.nilai_akhir >= 60.00 AND f.nilai_akhir <= 100.00
                  AND lower(p.nama_resmi) LIKE '%teknik%'
                  AND f.is_approved = 1
            )
            SELECT 
                nim,
                nama_mahasiswa,
                nama_prodi,
                nilai_akhir,
                nilai_tugas,
                nilai_uts,
                nilai_uas,
                rank_di_prodi
            FROM RankedKRS 
            WHERE rank_di_prodi <= 5
            ORDER BY nama_prodi ASC, rank_di_prodi ASC
        """,
        "mongodb": [
            {
                "$match": {
                    "nilai_akhir": { "$gte": 60.0, "$lte": 100.0 },
                    "mahasiswa.prodi.nama_resmi": { "$regex": "teknik", "$options": "i" },
                    "is_approved": True
                }
            },
            {
                "$setWindowFields": {
                    "partitionBy": "$mahasiswa.prodi.id_prodi_neosia",
                    "sortBy": { "nilai_akhir": -1 },
                    "output": {
                        "rank_di_prodi": { "$denseRank": {} }
                    }
                }
            },
            {
                "$match": { "rank_di_prodi": { "$lte": 5 } }
            },
            {
                "$project": {
                    "_id": 0,
                    "nim": "$mahasiswa.nim",
                    "nama_mahasiswa": "$mahasiswa.nama_mahasiswa",
                    "nama_prodi": "$mahasiswa.prodi.nama_resmi",
                    "nilai_akhir": 1,
                    "nilai_tugas": 1,
                    "nilai_uts": 1,
                    "nilai_uas": 1,
                    "rank_di_prodi": 1
                }
            },
            {
                "$sort": { "nama_prodi": 1, "rank_di_prodi": 1 }
            }
        ]
    },
    "Q5": {
        "name": "Analisis Cohort, Top-100 Ranking Rata-Rata Nilai & Multi-Criteria Sort",
        "postgres": """
            WITH MahasiswaPerformance AS (
                SELECT
                    m.nim,
                    m.nama_mahasiswa,
                    p.nama_resmi AS nama_prodi,
                    m.angkatan,
                    COUNT(f.krs_key) AS total_krs,
                    COUNT(f.krs_key) * 3 AS total_sks_diambil,
                    SUM(f.nilai_akhir) AS total_akumulasi_nilai,
                    ROUND(AVG(f.nilai_akhir)::numeric, 2) AS rata_nilai_akhir
                FROM fact_krs f
                LEFT JOIN dim_mahasiswa m ON f.mahasiswa_key = m.mahasiswa_key
                LEFT JOIN dim_prodi p ON f.prodi_key = p.prodi_key
                WHERE f.is_approved = TRUE
                GROUP BY m.nim, m.nama_mahasiswa, p.nama_resmi, m.angkatan
                HAVING COUNT(f.krs_key) >= 4 AND AVG(f.nilai_akhir) >= 70.0
            )
            SELECT
                nim, nama_mahasiswa, nama_prodi, angkatan,
                total_krs, total_sks_diambil, rata_nilai_akhir, total_akumulasi_nilai
            FROM MahasiswaPerformance
            ORDER BY rata_nilai_akhir DESC,
                     total_sks_diambil DESC,
                     total_krs DESC,
                     nim COLLATE "C" ASC,
                     nama_prodi COLLATE "C" ASC,
                     angkatan ASC,
                     nama_mahasiswa COLLATE "C" ASC
            LIMIT 100;
        """,
        "clickhouse": """
            WITH MahasiswaPerformance AS (
                SELECT
                    m.nim,
                    m.nama_mahasiswa,
                    p.nama_resmi AS nama_prodi,
                    m.angkatan,
                    count(f.krs_key) AS total_krs,
                    count(f.krs_key) * 3 AS total_sks_diambil,
                    sum(f.nilai_akhir) AS total_akumulasi_nilai,
                    round(toDecimal128(sum(f.nilai_akhir), 10) / nullIf(count(f.nilai_akhir), 0), 2) AS rata_nilai_akhir
                FROM fact_krs f
                LEFT JOIN dim_mahasiswa m ON f.mahasiswa_key = m.mahasiswa_key
                LEFT JOIN dim_prodi p ON f.prodi_key = p.prodi_key
                WHERE f.is_approved = 1
                GROUP BY m.nim, m.nama_mahasiswa, p.nama_resmi, m.angkatan
                HAVING count(f.krs_key) >= 4
                   AND sum(f.nilai_akhir) >= 70 * count(f.nilai_akhir)
            )
            SELECT
                nim, nama_mahasiswa, nama_prodi, angkatan,
                total_krs, total_sks_diambil, rata_nilai_akhir, total_akumulasi_nilai
            FROM MahasiswaPerformance
            ORDER BY rata_nilai_akhir DESC,
                     total_sks_diambil DESC,
                     total_krs DESC,
                     nim ASC,
                     nama_prodi ASC,
                     angkatan ASC,
                     nama_mahasiswa ASC
            LIMIT 100
        """,
        "mongodb": [
            {"$match": {"is_approved": True}},
            {
                "$group": {
                    "_id": {
                        "id_mahasiswa": "$mahasiswa.id_mahasiswa",
                        "nim": "$mahasiswa.nim",
                        "nama_mahasiswa": "$mahasiswa.nama_mahasiswa",
                        "nama_prodi": "$prodi.nama_resmi",
                        "angkatan": "$mahasiswa.angkatan"
                    },
                    "total_krs": {"$sum": 1},
                    "total_sks_diambil": {"$sum": 3},
                    "total_akumulasi_dec": {"$sum": {"$toDecimal": "$nilai_akhir"}},
                    "rata_dec": mg_dec_avg("$nilai_akhir")
                }
            },
            {
                "$match": {
                    "total_krs": {"$gte": 4},
                    "$expr": {"$gte": ["$rata_dec", 70]}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "nim": "$_id.nim",
                    "nama_mahasiswa": "$_id.nama_mahasiswa",
                    "nama_prodi": "$_id.nama_prodi",
                    "angkatan": "$_id.angkatan",
                    "total_krs": 1,
                    "total_sks_diambil": 1,
                    "rata_nilai_akhir": mg_round_half_up("$rata_dec"),
                    "total_akumulasi_nilai": {"$toDouble": "$total_akumulasi_dec"}
                }
            },
            {
                "$sort": {
                    "rata_nilai_akhir": -1,
                    "total_sks_diambil": -1,
                    "total_krs": -1,
                    "nim": 1,
                    "nama_prodi": 1,
                    "angkatan": 1,
                    "nama_mahasiswa": 1
                }
            },
            {"$limit": 100}
        ]
    },
    "Q6": {
        "name": "Selective Filtered OLAP / Rekap Nilai & Kelulusan Mahasiswa",
        "postgres": """
            SELECT
                kk.nama_kelas AS nama_kelas,
                COUNT(f.krs_key) AS total_peserta,
                ROUND(AVG(f.nilai_akhir)::numeric, 2) AS rata_nilai,
                ROUND(STDDEV_POP(f.nilai_akhir)::numeric, 2) AS stddev_nilai,
                ROUND(
                    (COUNT(CASE WHEN f.nilai_akhir >= 60.0 THEN 1 END) * 100.0
                     / NULLIF(COUNT(f.krs_key), 0))::numeric, 2
                ) AS persentase_kelulusan
            FROM fact_krs f
            JOIN dim_mahasiswa m ON f.mahasiswa_key = m.mahasiswa_key
            LEFT JOIN dim_prodi p ON f.prodi_key = p.prodi_key
            LEFT JOIN dim_kelas_kuliah kk ON f.kelas_kuliah_key = kk.kelas_kuliah_key
            WHERE m.nim = 'V543351951001'
              AND f.is_approved = TRUE
            GROUP BY kk.nama_kelas
            ORDER BY total_peserta DESC, rata_nilai DESC;
        """,
        "clickhouse": """
            SELECT
                kk.nama_kelas AS nama_kelas,
                count(f.krs_key) AS total_peserta,
                round(toDecimal128(sum(f.nilai_akhir), 10) / nullIf(count(f.nilai_akhir), 0), 2) AS rata_nilai,
                round(stddevPop(f.nilai_akhir) + 0.000000001, 2) AS stddev_nilai,
                round(toDecimal128(countIf(f.nilai_akhir >= 60.0), 10) * 100
                      / nullIf(count(f.krs_key), 0), 2) AS persentase_kelulusan
            FROM fact_krs f
            GLOBAL JOIN dim_mahasiswa m ON f.mahasiswa_key = m.mahasiswa_key
            GLOBAL LEFT JOIN dim_prodi p ON f.prodi_key = p.prodi_key
            GLOBAL LEFT JOIN dim_kelas_kuliah kk ON f.kelas_kuliah_key = kk.kelas_kuliah_key
            WHERE m.nim = 'V543351951001'
              AND f.is_approved = 1
            GROUP BY kk.nama_kelas
            ORDER BY total_peserta DESC, rata_nilai DESC;
        """,
        "mongodb": [
            {"$match": {"mahasiswa.nim": "V543351951001", "is_approved": True}},
            {
                "$group": {
                    "_id": "$kelas_kuliah.nama_kelas",
                    "total_peserta": {"$sum": 1},
                    "rata_dec": mg_dec_avg("$nilai_akhir"),
                    "stddev_raw": {"$stdDevPop": "$nilai_akhir"},
                    "total_lulus": {
                        "$sum": {"$cond": [{"$gte": ["$nilai_akhir", 60.0]}, 1, 0]}
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "nama_kelas": "$_id",
                    "total_peserta": 1,
                    "rata_nilai": mg_round_half_up("$rata_dec"),
                    "stddev_nilai": mg_round_half_up({"$toDecimal": {"$add": ["$stddev_raw", 1e-9]}}),
                    "persentase_kelulusan": mg_pct("$total_lulus", "$total_peserta")
                }
            },
            {"$sort": {"total_peserta": -1, "rata_nilai": -1}}
        ]
    }
}

# ==============================================================================
# 3. PENGUKURAN METRIK DOCKER DAN UKURAN PENYIMPANAN
# ==============================================================================
def _parse_size_mb(value):
    """Ubah string ukuran Docker ke MiB. Satuan biner (KiB/MiB/GiB) dan desimal (kB/MB/GB) dibedakan."""
    m = re.match(r'^\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]*)\s*$', value or '')
    if not m:
        return 0.0
    factors = {'': 1, 'B': 1, 'kB': 1e3, 'KB': 1e3, 'MB': 1e6, 'GB': 1e9, 'TB': 1e12,
               'KiB': 1024, 'MiB': 1024 ** 2, 'GiB': 1024 ** 3, 'TiB': 1024 ** 4}
    return float(m.group(1)) * factors.get(m.group(2), 1) / (1024.0 ** 2)

def find_container(db_type):
    keyword = {'postgres': 'postgres', 'clickhouse': 'clickhouse', 'mongodb': 'mongo'}[db_type]
    try:
        out = subprocess.check_output(
            ['docker', 'ps', '--filter', 'status=running', '--format', '{{.Names}}'],
            stderr=subprocess.DEVNULL, timeout=5, text=True
        )
        names = [name.strip() for name in out.splitlines() if name.strip()]
        # Prefer a container matching the configured database name, not an arbitrary running container.
        exact = [name for name in names if name.lower() in [x.lower() for x in CONTAINER_NAMES[db_type]]]
        matches = exact or [name for name in names if keyword in name.lower()]
        if matches:
            return matches[0]
    except Exception:
        pass
    raise RuntimeError(f'Container Docker aktif untuk {db_type} tidak ditemukan. Periksa docker ps dan CONTAINER_NAMES.')

def get_docker_stats(db_type):
    """Snapshot Docker stats. RAM is current container usage; Block I/O is cumulative."""
    container = find_container(db_type)
    cmd = ['docker', 'stats', container, '--no-stream', '--format', '{{.CPUPerc}}|{{.MemUsage}}|{{.BlockIO}}']
    raw = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, timeout=5, text=True).strip()
    parts = raw.split('|')
    if len(parts) != 3:
        raise RuntimeError(f'Format docker stats tidak dikenali: {raw}')
    cpu = float(parts[0].replace('%', '').strip() or 0)
    ram = _parse_size_mb(parts[1].split('/')[0].strip())
    io_parts = parts[2].split('/')
    if len(io_parts) != 2:
        raise RuntimeError(f'Format Block I/O tidak dikenali: {parts[2]}')
    return {'cpu_pct': cpu, 'ram_mb': ram, 'disk_read_mb': _parse_size_mb(io_parts[0]), 'disk_write_mb': _parse_size_mb(io_parts[1])}

def measure_storage_size(db_type):
    """
    Mengukur storage footprint yang dilaporkan engine (MB).
    Catatan: metode internal tiap DBMS berbeda, sehingga hasil dibandingkan sebagai
    estimasi footprint engine-reported, bukan ukuran file identik pada level filesystem.
    """
    if db_type == 'postgres':
        with psycopg2.connect(**POSTGRES_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_database_size(current_database())")
                size_bytes = cur.fetchone()[0]
        return size_bytes / (1024.0 * 1024.0), "pg_database_size (database total; termasuk indeks)"

    if db_type == 'clickhouse':
        url = f"http://{CLICKHOUSE_CONFIG['host']}:{CLICKHOUSE_CONFIG['port']}/"
        db_name = CLICKHOUSE_CONFIG['database'].replace("'", "''")
        query = (
            "SELECT coalesce(sum(bytes_on_disk), 0) "
            f"FROM system.parts WHERE database = '{db_name}' AND active = 1 FORMAT TabSeparated"
        )
        res = requests.get(
            url,
            params={
                'database': CLICKHOUSE_CONFIG['database'],
                'user': CLICKHOUSE_CONFIG['user'],
                'password': CLICKHOUSE_CONFIG['password'],
                'query': query
            },
            timeout=15
        )
        res.raise_for_status()
        return float(res.text.strip() or 0) / (1024.0 * 1024.0), "system.parts.bytes_on_disk (active parts)"

    if db_type == 'mongodb':
        client = MongoClient(MONGODB_CONFIG['uri'], serverSelectionTimeoutMS=5000)
        try:
            stats = client[MONGODB_CONFIG['db_name']].command('dbStats')
            size_bytes = float(stats.get('storageSize', 0)) + float(stats.get('indexSize', 0))
            return size_bytes / (1024.0 * 1024.0), "dbStats.storageSize + dbStats.indexSize"
        finally:
            client.close()

    raise ValueError(f'Database tidak dikenal: {db_type}')

# ==============================================================================
# 4. EKSEKUTOR KUERI — mengembalikan seluruh hasil agar bisa divalidasi
# ==============================================================================
def execute_postgres_query(query_sql):
    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query_sql)
            columns = [d[0] for d in cursor.description] if cursor.description else []
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

def execute_clickhouse_query(query_sql):
    clean_sql = query_sql.strip().rstrip(';').strip()
    url = f"http://{CLICKHOUSE_CONFIG['host']}:{CLICKHOUSE_CONFIG['port']}/"
    params = {'database': CLICKHOUSE_CONFIG['database'], 'user': CLICKHOUSE_CONFIG['user'], 'password': CLICKHOUSE_CONFIG['password'], 'query': clean_sql + ' FORMAT JSON', 'join_use_nulls': 1, 'output_format_json_quote_64bit_integers': 0}
    res = requests.get(url, params=params, timeout=180)
    res.raise_for_status()
    return res.json().get('data', [])

def execute_mongodb_query(pipeline):
    client = MongoClient(MONGODB_CONFIG['uri'], serverSelectionTimeoutMS=5000, socketTimeoutMS=180000)
    try:
        return list(client[MONGODB_CONFIG['db_name']].krs.aggregate(pipeline, allowDiskUse=True))
    finally:
        client.close()


def _normalize_value(value):
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, Decimal128):
        value = value.to_decimal()

    if isinstance(value, Decimal):
        value = float(value)

    if isinstance(value, (int, float)):
        value = float(value)

        if not math.isfinite(value):
            return str(value)

        return value

    if isinstance(value, dict):
        return {
            key: _normalize_value(value[key])
            for key in sorted(value)
        }

    if isinstance(value, (list, tuple)):
        return [_normalize_value(item) for item in value]

    return value


def result_signature(rows, debug_name=None):
    normalized = [_normalize_value(row) for row in rows]

    normalized.sort(
        key=lambda row: json.dumps(
            row,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":")
        )
    )

    payload = json.dumps(
        normalized,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":")
    )

    signature = hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()

    # Diagnostik tambahan, tanpa mengubah hasil signature
    if debug_name is not None:
        print(f"\n=== DIAGNOSTIK SIGNATURE: {debug_name} ===")
        print(f"Jumlah baris     : {len(rows)}")
        print(f"Panjang JSON     : {len(payload)} karakter")
        print(f"Contoh JSON awal : {payload[:500]}")
        print(f"Signature        : {signature}")

    return signature


def debug_signature_difference(rows_a, rows_b, name_a, name_b):
    import json

    def normalized_payload(rows):
        normalized = [_normalize_value(row) for row in rows]
        normalized.sort(
            key=lambda row: json.dumps(
                row,
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        return [
            json.dumps(
                row,
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            for row in normalized
        ]

    a = normalized_payload(rows_a)
    b = normalized_payload(rows_b)

    print(f"\n=== PERBANDINGAN {name_a} vs {name_b} ===")
    print(f"Jumlah baris: {len(a)} vs {len(b)}")

    if len(a) != len(b):
        print("PENYEBAB: jumlah baris berbeda.")
        return

    for i, (row_a, row_b) in enumerate(zip(a, b)):
        if row_a != row_b:
            print(f"Baris berbeda pertama: indeks {i}")
            print(f"{name_a}: {row_a}")
            print(f"{name_b}: {row_b}")
            return

    print("Semua baris JSON ternormalisasi identik.")


Q2_KEYS = [
    "fakultas_nama",
    "nama_prodi",
    "angkatan",
    "tahun_semester",
]

Q2_METRICS = [
    "total_krs",
    "total_mahasiswa_unik",
    "rata_nilai_akhir",
    "stddev_nilai",
    "total_sangat_baik",
    "rasio_sangat_baik",
]


def to_number(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    return None


def compare_q2_results(rows_a, rows_b, name_a, name_b, limit=10):
    def make_map(rows):
        result = {}

        for row in rows:
            key = tuple(row.get(k) for k in Q2_KEYS)

            if key in result:
                print(f"PERINGATAN: kunci duplikat ditemukan: {key}")

            result[key] = row

        return result

    map_a = make_map(rows_a)
    map_b = make_map(rows_b)

    keys_a = set(map_a)
    keys_b = set(map_b)

    only_a = keys_a - keys_b
    only_b = keys_b - keys_a

    print(f"\n=== PERBANDINGAN {name_a} vs {name_b} ===")
    print(f"Jumlah baris: {len(rows_a)} vs {len(rows_b)}")
    print(f"Kunci hanya di {name_a}: {len(only_a)}")
    print(f"Kunci hanya di {name_b}: {len(only_b)}")

    if only_a:
        print("Contoh kunci yang hanya ada di", name_a)
        for key in list(only_a)[:limit]:
            print(key)

    if only_b:
        print("Contoh kunci yang hanya ada di", name_b)
        for key in list(only_b)[:limit]:
            print(key)

    mismatches = 0

    for key in keys_a & keys_b:
        row_a = map_a[key]
        row_b = map_b[key]

        for col in Q2_METRICS:
            a = row_a.get(col)
            b = row_b.get(col)

            na = to_number(a)
            nb = to_number(b)

            if na is not None and nb is not None:
                equal = math.isclose(
                    na, nb,
                    rel_tol=1e-9,
                    abs_tol=1e-6
                )
            else:
                equal = a == b

            if not equal:
                mismatches += 1

                if mismatches <= limit:
                    print(f"\nPerbedaan pada kunci: {key}")
                    print(f"Kolom       : {col}")
                    print(f"{name_a}: {a!r} ({type(a).__name__})")
                    print(f"{name_b}: {b!r} ({type(b).__name__})")

    print(f"\nJumlah perbedaan metrik: {mismatches}")

    if not only_a and not only_b and mismatches == 0:
        print("HASIL: setara berdasarkan kunci dan toleransi numerik.")
    else:
        print("HASIL: ditemukan perbedaan yang perlu diperiksa.")


QUERY_KEYS = {
    "Q2": ["fakultas_nama", "nama_prodi", "angkatan", "tahun_semester"],
    "Q3": ["nama_dosen", "nip_dosen", "nama_prodi", "kode_semester"],
    "Q5": ["nim", "nama_prodi", "angkatan", "nama_mahasiswa"],
    "Q6": ["nama_kelas"],
}
NUMERIC_TOLERANCE = 0.01 + 1e-9   # selisih <= 0.01 dianggap setara (pembulatan 2 desimal)


def compare_rows_with_tolerance(rows_a, rows_b, key_cols, tol=NUMERIC_TOLERANCE, limit=5):
    """Bandingkan dua hasil berdasarkan kunci; kolom numerik dengan toleransi absolut."""
    if rows_a is None or rows_b is None:
        return False, "hasil tidak tersedia"
    if key_cols is None:
        return False, "tidak ada definisi kunci (hanya hash yang dipakai)"
    if len(rows_a) != len(rows_b):
        return False, f"jumlah baris berbeda ({len(rows_a)} vs {len(rows_b)})"

    def to_map(rows):
        return {tuple(r.get(k) for k in key_cols): r for r in rows}

    ma, mb = to_map(rows_a), to_map(rows_b)
    if set(ma) != set(mb):
        sa, sb = sorted(set(ma) - set(mb), key=str)[:limit], sorted(set(mb) - set(ma), key=str)[:limit]
        return False, f"kunci berbeda; hanya di A: {sa}; hanya di B: {sb}"

    diffs = []
    for key, ra in ma.items():
        rb = mb[key]
        for col in set(ra) | set(rb):
            a, b = ra.get(col), rb.get(col)
            na, nb = to_number(a), to_number(b)
            if na is not None and nb is not None:
                same = abs(na - nb) <= tol
            else:
                same = a == b
            if not same:
                diffs.append((key, col, a, b))
    if diffs:
        sample = "; ".join(f"{k} {c}: {a!r} vs {b!r}" for k, c, a, b in diffs[:limit])
        return False, f"{len(diffs)} sel berbeda. Contoh: {sample}"
    return True, "setara dalam toleransi"

# ==============================================================================
# 5. PENGUKURAN — KLIEN PERSISTEN, COLD RUN TERKONTROL, METRIK CGROUP, STATISTIK
# ------------------------------------------------------------------------------
# Prinsip desain:
#  (1) Latensi diukur TANPA sampler apa pun yang berjalan bersamaan.
#  (2) Koneksi dibuat SEKALI di luar timer; timer hanya mencakup eksekusi + penerimaan hasil
#      (parsing JSON / konversi dict dilakukan setelah timer berhenti).
#  (3) Cold run = stop container -> drop OS page cache -> start -> tunggu siap -> sambung ulang.
#  (4) CPU/RAM/Disk diukur di fase TERPISAH memakai cgroup container (bukan docker stats).
#  (5) Urutan eksekusi antar-database digilir (interleaved) agar tidak bias.
#  (6) Setiap metrik dilaporkan dengan n, mean, median, stdev, CV, min, p95, max, CI95.
# ==============================================================================
import gc
import platform
import re
import threading
import uuid
from collections import defaultdict

T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306,
       9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
       16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086, 21: 2.080, 22: 2.074,
       23: 2.069, 24: 2.064, 25: 2.060, 26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042}


def t_crit(df):
    if df <= 0:
        return 0.0
    return T95.get(df, 2.0 if df < 60 else 1.96)


def summarize(values):
    """Statistik deskriptif + CI95 (distribusi t) untuk satu daftar nilai."""
    vals = [float(v) for v in values if v is not None]
    n = len(vals)
    if n == 0:
        return None
    srt = sorted(vals)
    mean = statistics.mean(vals)
    sd = statistics.stdev(vals) if n > 1 else 0.0

    def pct(p):
        if n == 1:
            return srt[0]
        k = (n - 1) * p
        lo, hi = math.floor(k), math.ceil(k)
        return srt[lo] + (srt[hi] - srt[lo]) * (k - lo)

    half = t_crit(n - 1) * sd / math.sqrt(n) if n > 1 else 0.0
    return {'n': n, 'mean': mean, 'median': statistics.median(vals), 'stdev': sd,
            'cv_pct': (sd / mean * 100.0) if mean else 0.0, 'min': srt[0],
            'p95': pct(0.95), 'max': srt[-1], 'ci95_low': mean - half, 'ci95_high': mean + half}


def rotate(seq, k):
    seq = list(seq)
    if not seq:
        return seq
    k %= len(seq)
    return seq[k:] + seq[:k]


def _find_key(obj, key):
    """Cari nilai pertama dari 'key' secara depth-first pada dict/list bersarang."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = _find_key(v, key)
            if r is not None:
                return r
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            r = _find_key(v, key)
            if r is not None:
                return r
    return None


# ------------------------------------------------------------------------------
# 5a. KLIEN PERSISTEN (koneksi dibuat sekali, di luar timer)
# ------------------------------------------------------------------------------
class PostgresClient:
    name = 'postgres'

    def __init__(self):
        self.conn = None

    def connect(self):
        self.close()
        self.conn = psycopg2.connect(**POSTGRES_CONFIG)
        self.conn.autocommit = True
        with self.conn.cursor() as cur:  # pemanasan koneksi saja, tidak menyentuh data
            cur.execute('SELECT 1')
            cur.fetchall()

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None

    def run(self, sql, qid=None):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            raw = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
        return cols, raw

    def decode(self, payload):
        cols, raw = payload
        return [dict(zip(cols, r)) for r in raw], {}

    def explain(self, sql):
        with self.conn.cursor() as cur:
            cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql.strip().rstrip(';'))
            data = cur.fetchone()[0]
        if isinstance(data, str):
            data = json.loads(data)
        plan = data[0]
        node = plan.get('Plan', {})
        hit = node.get('Shared Hit Blocks', 0)
        read = node.get('Shared Read Blocks', 0)
        return {'server_exec_ms': plan.get('Execution Time'),
                'server_bytes_read': read * 8192,
                'note': f"EXPLAIN ANALYZE; planning_ms={plan.get('Planning Time')}; shared_hit_blocks={hit}; shared_read_blocks={read}"}


class ClickHouseClient:
    name = 'clickhouse'

    def __init__(self):
        self.session = None
        self.url = f"http://{CLICKHOUSE_CONFIG['host']}:{CLICKHOUSE_CONFIG['port']}/"

    def _params(self, qid=None):
        p = {'database': CLICKHOUSE_CONFIG['database'], 'user': CLICKHOUSE_CONFIG['user'],
             'password': CLICKHOUSE_CONFIG['password'], 'join_use_nulls': 1,
             'output_format_json_quote_64bit_integers': 0}
        if qid:
            p['query_id'] = qid
        return p

    def connect(self):
        self.close()
        self.session = requests.Session()  # keep-alive: koneksi TCP dipakai ulang
        self._raw('SELECT 1')

    def close(self):
        if self.session is not None:
            try:
                self.session.close()
            except Exception:
                pass
            self.session = None

    def _raw(self, sql, qid=None):
        res = self.session.post(self.url, params=self._params(qid), data=sql.encode('utf-8'), timeout=180)
        res.raise_for_status()
        return res.content

    def run(self, sql, qid=None):
        clean = sql.strip().rstrip(';').strip() + ' FORMAT JSON'
        return self._raw(clean, qid)  # body diterima penuh di dalam timer

    def decode(self, payload):
        obj = json.loads(payload)
        st = obj.get('statistics', {}) or {}
        return obj.get('data', []), {
            'server_exec_ms': (st.get('elapsed') * 1000.0) if st.get('elapsed') is not None else None,
            'server_rows_read': st.get('rows_read'),
            'server_bytes_read': st.get('bytes_read')}

    def query_log_stats(self, qid):
        """Memori puncak per-kueri dari system.query_log (akurat, bukan level container)."""
        self._raw('SYSTEM FLUSH LOGS')
        sql = ("SELECT memory_usage, query_duration_ms, read_rows, read_bytes FROM system.query_log "
               f"WHERE query_id = '{qid}' AND type = 'QueryFinish' "
               "ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT JSON")
        data = json.loads(self._raw(sql)).get('data', [])
        return data[0] if data else {}


class MongoDBClient:
    name = 'mongodb'

    def __init__(self):
        self.client = None

    def connect(self):
        self.close()
        self.client = MongoClient(MONGODB_CONFIG['uri'], serverSelectionTimeoutMS=10000, socketTimeoutMS=180000)
        self.client.admin.command('ping')

    def close(self):
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass
            self.client = None

    def run(self, pipeline, qid=None):
        return list(self.client[MONGODB_CONFIG['db_name']].krs.aggregate(pipeline, allowDiskUse=True))

    def decode(self, payload):
        return payload, {}

    def explain(self, pipeline):
        res = self.client[MONGODB_CONFIG['db_name']].command({
            'explain': {'aggregate': 'krs', 'pipeline': pipeline, 'cursor': {}, 'allowDiskUse': True},
            'verbosity': 'executionStats'})
        return {'server_exec_ms': _find_key(res, 'executionTimeMillis'),
                'server_rows_read': _find_key(res, 'totalDocsExamined'),
                'note': f"explain executionStats; keys_examined={_find_key(res, 'totalKeysExamined')}"}


def make_client(db):
    return {'postgres': PostgresClient, 'clickhouse': ClickHouseClient, 'mongodb': MongoDBClient}[db]()


def wait_ready(db, timeout=300):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        probe = make_client(db)
        try:
            probe.connect()
            probe.close()
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            probe.close()
            time.sleep(1.0)
    raise RuntimeError(f'{db} tidak siap setelah restart: {last}')


# ------------------------------------------------------------------------------
# 5b. COLD RUN: stop container -> drop OS page cache -> start -> tunggu siap
# ------------------------------------------------------------------------------
_OS_CACHE = {'method': None, 'failed': False, 'warned': False}


def drop_os_page_cache():
    """Kosongkan page cache OS. Return (berhasil, metode)."""
    if _OS_CACHE['failed']:
        return False, 'tidak tersedia'
    script = 'sync; echo 3 > /proc/sys/vm/drop_caches'
    candidates = []
    if hasattr(os, 'geteuid') and os.geteuid() == 0:
        candidates.append(('root', ['sh', '-c', script]))
    candidates.append(('sudo -n', ['sudo', '-n', 'sh', '-c', script]))
    candidates.append(('docker privileged (VM Docker)', [
        'docker', 'run', '--rm', '--privileged', '--pid=host', 'alpine',
        'nsenter', '-t', '1', '-m', '-u', '-n', '-i', 'sh', '-c', script]))
    if _OS_CACHE['method']:
        candidates.sort(key=lambda c: c[0] != _OS_CACHE['method'])
    for name, cmd in candidates:
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=180)
            _OS_CACHE['method'] = name
            return True, name
        except Exception:
            continue
    _OS_CACHE['failed'] = True
    return False, 'tidak ada metode yang berhasil'


def docker_run(args_list, timeout=180):
    subprocess.run(['docker'] + args_list, check=True, capture_output=True, timeout=timeout)


# ------------------------------------------------------------------------------
# 5c. PENGUKUR RESOURCE BERBASIS CGROUP (CPU-time, memori, Block I/O level container)
# ------------------------------------------------------------------------------
class CgroupMeter:
    """Baca counter cgroup container lewat SATU shell persisten (docker exec -i sh).
    Snapshot hanya beberapa ms sehingga bisa dipakai sebelum/sesudah SETIAP run, di luar timer."""

    def __init__(self, container):
        self.container = container
        self.version = None
        self.available = False
        self.shell = None
        self.stream = None
        self.samples = []
        self.reader = None
        try:
            self.version = self._exec("test -f /sys/fs/cgroup/cpu.stat && echo v2 || echo v1").strip()
            self.available = self.snapshot() is not None
        except Exception:
            self.available = False

    def _exec(self, cmd, timeout=15):
        return subprocess.check_output(['docker', 'exec', self.container, 'sh', '-c', cmd],
                                       text=True, timeout=timeout, stderr=subprocess.DEVNULL)

    # --- shell persisten ---------------------------------------------------------------
    def _open(self):
        self._close_shell()
        self.shell = subprocess.Popen(['docker', 'exec', '-i', self.container, 'sh'],
                                      stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=subprocess.DEVNULL, text=True, bufsize=1)

    def _close_shell(self):
        if self.shell is not None:
            try:
                self.shell.stdin.close()
                self.shell.wait(timeout=2)
            except Exception:
                try:
                    self.shell.kill()
                except Exception:
                    pass
            self.shell = None

    def reset(self):
        """Dipanggil setelah container di-restart (shell lama ikut mati)."""
        self._close_shell()

    def close(self):
        self._close_shell()

    def _query(self, cmd):
        if self.shell is None or self.shell.poll() is not None:
            self._open()
        self.shell.stdin.write(cmd + "\necho '#END'\n")
        self.shell.stdin.flush()
        lines = []
        while True:
            line = self.shell.stdout.readline()
            if line == '':
                raise RuntimeError('shell cgroup berhenti')
            if line.strip() == '#END':
                break
            lines.append(line)
        return ''.join(lines)

    def _snapshot_cmd(self):
        if self.version == 'v2':
            return ("echo '#CPU'; cat /sys/fs/cgroup/cpu.stat; echo '#MEM'; cat /sys/fs/cgroup/memory.current; "
                    "echo '#IO'; cat /sys/fs/cgroup/io.stat 2>/dev/null; "
                    "echo '#STAT'; grep -E '^(anon|file) ' /sys/fs/cgroup/memory.stat 2>/dev/null; true")
        return ("echo '#CPU'; (cat /sys/fs/cgroup/cpuacct/cpuacct.usage 2>/dev/null || "
                "cat /sys/fs/cgroup/cpu,cpuacct/cpuacct.usage); echo '#MEM'; "
                "cat /sys/fs/cgroup/memory/memory.usage_in_bytes; echo '#IO'; "
                "echo '#STAT'; grep -E '^(total_rss|total_cache) ' /sys/fs/cgroup/memory/memory.stat 2>/dev/null; true")

    def snapshot(self):
        out = None
        for attempt in (1, 2):
            try:
                t_a = time.perf_counter()
                out = self._query(self._snapshot_cmd())
                t_b = time.perf_counter()
                break
            except Exception:
                self._close_shell()
                if attempt == 2:
                    raise
        sections, cur = {}, None
        for line in out.splitlines():
            if line.startswith('#') and line[1:] in ('CPU', 'MEM', 'IO', 'STAT'):
                cur = line[1:]
                sections[cur] = []
            elif cur:
                sections[cur].append(line)
        cpu_txt, mem_txt, io_txt, stat_txt = ('\n'.join(sections.get(k, [])) for k in ('CPU', 'MEM', 'IO', 'STAT'))
        if self.version == 'v2':
            m = re.search(r'usage_usec\s+(\d+)', cpu_txt)
            cpu_usec = int(m.group(1)) if m else None
        else:
            cpu_usec = int(cpu_txt.strip()) / 1000.0 if cpu_txt.strip().isdigit() else None
        mem = int(mem_txt.strip()) if mem_txt.strip().isdigit() else None
        rb = sum(int(x) for x in re.findall(r'rbytes=(\d+)', io_txt)) if io_txt else None
        wb = sum(int(x) for x in re.findall(r'wbytes=(\d+)', io_txt)) if io_txt else None
        ma = re.search(r'^(?:anon|total_rss)\s+(\d+)', stat_txt, re.M)
        anon = int(ma.group(1)) if ma else None
        if cpu_usec is None or mem is None:
            return None
        return {'cpu_usec': cpu_usec, 'mem': mem, 'anon': anon, 'rbytes': rb, 'wbytes': wb,
                't': (t_a + t_b) / 2.0}

    # --- stream memori puncak (hanya fase resource; terpisah agar tidak mengganggu CPU) ----
    def start_stream(self):
        path = '/sys/fs/cgroup/memory.current' if self.version == 'v2' else '/sys/fs/cgroup/memory/memory.usage_in_bytes'
        script = f"while true; do cat {path}; read -t 0.02 _; [ $? -eq 1 ] && break; done"
        self.samples = []
        self.stream = subprocess.Popen(['docker', 'exec', '-i', self.container, 'bash', '-c', script],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=subprocess.DEVNULL, text=True, bufsize=1)

        def reader():
            for line in self.stream.stdout:
                line = line.strip()
                if line.isdigit():
                    self.samples.append((time.perf_counter(), int(line)))

        self.reader = threading.Thread(target=reader, daemon=True)
        self.reader.start()

    def stop_stream(self):
        samples = []
        if self.stream is not None:
            try:
                self.stream.stdin.close()
                self.stream.wait(timeout=3)
            except Exception:
                self.stream.kill()
            if self.reader:
                self.reader.join(timeout=2)
            samples = list(self.samples)
        self.stream = None
        return samples


# ------------------------------------------------------------------------------
# 5d. PENCATAT CSV
# ------------------------------------------------------------------------------
RESULT_FIELDS = [
    'timestamp', 'scale_factor', 'database', 'query_id', 'query_name', 'run_type', 'iteration',
    'cache_state', 'os_cache_dropped', 'latency_ms', 'timing_scope', 'rows_returned', 'result_signature',
    'cpu_time_s', 'cpu_idle_s', 'cpu_net_s', 'cpu_util_pct_of_core', 'cpu_window_s',
    'ram_before_mb', 'ram_after_mb', 'ram_anon_after_mb', 'ram_end_delta_mb',
    'ram_peak_mb', 'ram_delta_peak_mb', 'ram_sample_count',
    'disk_read_mb', 'disk_write_mb',
    'server_exec_ms', 'server_rows_read', 'server_bytes_read', 'server_mem_mb', 'note']
SUMMARY_FIELDS = ['scale_factor', 'database', 'query_id', 'run_type', 'metric', 'n', 'mean', 'median',
                  'stdev', 'cv_pct', 'min', 'p95', 'max', 'ci95_low', 'ci95_high']
STORAGE_FIELDS = ['timestamp', 'scale_factor', 'database', 'storage_size_mb', 'measurement_method', 'status', 'error']


PER_RUN_METRICS = ['cpu_net_s', 'cpu_util_pct_of_core', 'ram_after_mb', 'ram_anon_after_mb',
                   'ram_end_delta_mb', 'disk_read_mb', 'disk_write_mb']


class CsvSink:
    def __init__(self, path, fields):
        self.path, self.fields = path, fields
        if os.path.exists(path):
            with open(path, encoding='utf-8') as f:
                header = f.readline().strip().split(',')
            if header != fields:
                raise SystemExit(f"File '{path}' sudah ada dengan kolom berbeda (format versi lama). "
                                 f"Gunakan nama file lain atau hapus file tersebut.")
        else:
            with open(path, 'w', newline='', encoding='utf-8') as f:
                csv.DictWriter(f, fieldnames=fields).writeheader()

    def write(self, row):
        with open(self.path, 'a', newline='', encoding='utf-8') as f:
            csv.DictWriter(f, fieldnames=self.fields).writerow({k: row.get(k, '') for k in self.fields})


def save_storage_summary(filepath, scale_factor, db_type, storage_mb, method='', status='OK', error=''):
    CsvSink(filepath, STORAGE_FIELDS).write({
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'scale_factor': scale_factor,
        'database': db_type, 'storage_size_mb': '' if storage_mb is None else f"{storage_mb:.2f}",
        'measurement_method': method, 'status': status, 'error': error})


def _fmt(v, nd=3):
    if v is None or v == '':
        return ''
    return f"{v:.{nd}f}" if isinstance(v, float) else v


# ------------------------------------------------------------------------------
# 5e. METADATA LINGKUNGAN (wajib dicantumkan di bab metodologi skripsi)
# ------------------------------------------------------------------------------
def collect_environment(args, dbs, containers):
    env = {'timestamp': datetime.now().isoformat(timespec='seconds'), 'python': sys.version.split()[0],
           'platform': platform.platform(), 'cpu_count_host': os.cpu_count(), 'args': vars(args), 'databases': {}}

    def safe(fn):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            return f'tidak tersedia ({exc.__class__.__name__})'

    env['docker_server'] = safe(lambda: subprocess.check_output(
        ['docker', 'version', '--format', '{{.Server.Version}}'], text=True, timeout=10, stderr=subprocess.DEVNULL).strip())
    for db in dbs:
        info = {'container': containers.get(db)}
        if containers.get(db):
            info['image|nano_cpus|mem_limit_bytes'] = safe(lambda c=containers[db]: subprocess.check_output(
                ['docker', 'inspect', '-f', '{{.Config.Image}}|{{.HostConfig.NanoCpus}}|{{.HostConfig.Memory}}', c],
                text=True, timeout=10, stderr=subprocess.DEVNULL).strip())
        if db == 'postgres':
            def pg_info():
                with psycopg2.connect(**POSTGRES_CONFIG) as conn, conn.cursor() as cur:
                    out = {}
                    cur.execute('SELECT version()')
                    out['version'] = cur.fetchone()[0]
                    for s in ('shared_buffers', 'work_mem', 'effective_cache_size',
                              'max_parallel_workers_per_gather', 'max_worker_processes'):
                        cur.execute(f'SHOW {s}')
                        out[s] = cur.fetchone()[0]
                    return out
            info['settings'] = safe(pg_info)
        elif db == 'clickhouse':
            def ch_info():
                c = ClickHouseClient()
                c.connect()
                out = {'version': c._raw('SELECT version()').decode().strip()}
                rows = c._raw("SELECT name, value FROM system.settings WHERE name IN "
                              "('max_threads','max_memory_usage') FORMAT JSON")
                for r in json.loads(rows).get('data', []):
                    out[r['name']] = r['value']
                c.close()
                return out
            info['settings'] = safe(ch_info)
        elif db == 'mongodb':
            def mg_info():
                cl = MongoClient(MONGODB_CONFIG['uri'], serverSelectionTimeoutMS=5000)
                try:
                    out = {'version': cl.admin.command('buildInfo').get('version')}
                    wt = cl.admin.command('serverStatus').get('wiredTiger', {}).get('cache', {})
                    out['wiredtiger_cache_max_bytes'] = wt.get('maximum bytes configured')
                    return out
                finally:
                    cl.close()
            info['settings'] = safe(mg_info)
        env['databases'][db] = info
    return env


# ------------------------------------------------------------------------------
# 5f. SESI BENCHMARK
# ------------------------------------------------------------------------------
class BenchmarkSession:
    def __init__(self, args, dbs, queries, containers):
        self.args, self.dbs, self.queries, self.containers = args, dbs, queries, containers
        self.clients = {db: make_client(db) for db in dbs}
        self.meters = {}
        self.sink = CsvSink(args.output, RESULT_FIELDS)
        self.metrics = defaultdict(list)      # (q, db, run_type, metric) -> [nilai]
        self.sigs = defaultdict(set)          # (q, db) -> {signature}
        self.first_rows = {}                  # (q, db) -> rows
        self.first_sig = {}
        self.failed = set()                   # (q, db)
        self.idle = {}                        # (q, db) -> CPU-detik idle per detik

    # ---- util -------------------------------------------------------------------------
    def active(self, q):
        return [d for d in self.dbs if (q, d) not in self.failed]

    def add_metric(self, q, db, run_type, metric, value):
        if value is not None and value != '':
            self.metrics[(q, db, run_type, metric)].append(float(value))

    def timed(self, db, q, qid=None):
        """Timer hanya mengelilingi eksekusi + penerimaan hasil (koneksi sudah terbuka)."""
        client, query = self.clients[db], QUERIES[q][db]
        gc.collect()
        gc.disable()
        t0 = time.perf_counter()
        try:
            payload = client.run(query, qid)
        finally:
            t1 = time.perf_counter()
            gc.enable()
        return (t1 - t0) * 1000.0, payload, t0, t1

    def record(self, q, db, run_type, it, ms, payload, extra=None, want_sig=False, metrics=None):
        rows, srv = self.clients[db].decode(payload)       # di luar timer
        sig = ''
        if want_sig:
            sig = result_signature(rows)
            self.sigs[(q, db)].add(sig)
            if (q, db) not in self.first_rows:
                self.first_rows[(q, db)], self.first_sig[(q, db)] = rows, sig
        row = {'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'scale_factor': self.args.scale,
               'database': db, 'query_id': q, 'query_name': QUERIES[q]['name'], 'run_type': run_type,
               'iteration': it, 'latency_ms': f"{ms:.3f}",
               'timing_scope': 'client_persistent_connection_execute_and_fetch',
               'rows_returned': len(rows), 'result_signature': sig,
               'server_exec_ms': _fmt(srv.get('server_exec_ms')), 'server_rows_read': srv.get('server_rows_read', ''),
               'server_bytes_read': srv.get('server_bytes_read', '')}
        row.update(extra or {})
        for k, v in (metrics or {}).items():
            row[k] = _fmt(v, 4 if k.startswith('cpu') else 3)
        self.sink.write(row)
        if run_type in ('Cold_Run', 'Warm_Run'):
            self.add_metric(q, db, run_type, 'latency_ms', ms)
            for k in PER_RUN_METRICS:
                if metrics and k in metrics:
                    self.add_metric(q, db, run_type, k, metrics[k])
            self.add_metric(q, db, run_type, 'server_exec_ms', srv.get('server_exec_ms'))
        return rows, srv

    def measure_idle(self, q, db):
        """Baseline CPU idle container (CPU-detik per detik), dikurangkan dari CPU tiap run."""
        meter = self.meters.get(db)
        if not (meter and meter.available):
            return
        try:
            a = meter.snapshot()
            time.sleep(1.0)
            b = meter.snapshot()
            self.idle[(q, db)] = (b['cpu_usec'] - a['cpu_usec']) / 1e6 / max(b['t'] - a['t'], 1e-6)
        except Exception:
            self.idle[(q, db)] = 0.0

    def timed_metrics(self, db, q, qid=None):
        """timed() + snapshot cgroup sebelum/sesudah. Snapshot berada DI LUAR timer."""
        meter = self.meters.get(db) if self.args.run_metrics else None
        s0 = None
        if meter and meter.available:
            try:
                s0 = meter.snapshot()
            except Exception:
                s0 = None
        ms, payload, _, _ = self.timed(db, q, qid)
        metrics = None
        if s0:
            try:
                s1 = meter.snapshot()
                window = max(s1['t'] - s0['t'], 1e-6)
                cpu_s = (s1['cpu_usec'] - s0['cpu_usec']) / 1e6
                cpu_idle = max(0.0, self.idle.get((q, db), 0.0)) * window
                cpu_net = max(0.0, cpu_s - cpu_idle)
                mb = 1024.0 ** 2
                metrics = {
                    'cpu_time_s': cpu_s, 'cpu_idle_s': cpu_idle, 'cpu_net_s': cpu_net,
                    'cpu_util_pct_of_core': cpu_net / window * 100.0, 'cpu_window_s': window,
                    'ram_before_mb': s0['mem'] / mb, 'ram_after_mb': s1['mem'] / mb,
                    'ram_end_delta_mb': (s1['mem'] - s0['mem']) / mb}
                if s1.get('anon') is not None:
                    metrics['ram_anon_after_mb'] = s1['anon'] / mb
                if s0['rbytes'] is not None and s1['rbytes'] is not None:
                    metrics['disk_read_mb'] = (s1['rbytes'] - s0['rbytes']) / mb
                    metrics['disk_write_mb'] = (s1['wbytes'] - s0['wbytes']) / mb
            except Exception:
                metrics = None
        return ms, payload, metrics

    # ---- siapkan kondisi cold ------------------------------------------------------------
    def prepare_cold(self, db):
        cont = self.containers[db]
        self.clients[db].close()
        docker_run(['stop', '-t', '30', cont])
        dropped, method = (False, 'tidak diminta')
        if self.args.cold_mode == 'oscache':
            dropped, method = drop_os_page_cache()
            if not dropped and not _OS_CACHE['warned']:
                _OS_CACHE['warned'] = True
                print("   ! PERINGATAN: page cache OS TIDAK berhasil dikosongkan. Cold run hanya 'restart container'.\n"
                      "     Jalankan skrip dengan sudo, atau beri sudo tanpa password / izin docker privileged.")
        docker_run(['start', cont])
        wait_ready(db)
        time.sleep(self.args.settle)
        self.clients[db].connect()
        if self.meters.get(db):
            self.meters[db].reset()
        state = 'cold(restart+os_cache_dropped)' if dropped else 'cold(restart_only)'
        return {'cache_state': state, 'os_cache_dropped': dropped, 'note': f'drop_cache={method}'}

    # ---- fase-fase ------------------------------------------------------------------------
    def cold_phase(self, q):
        if self.args.cold_mode == 'off' or self.args.cold_runs <= 0:
            return
        print(f"\n [{q}] FASE COLD ({self.args.cold_runs}x per database; mode={self.args.cold_mode})")
        for r in range(1, self.args.cold_runs + 1):
            for db in rotate(self.active(q), r - 1):
                try:
                    extra = self.prepare_cold(db)
                    ms, payload, mtr = self.timed_metrics(db, q)
                    self.record(q, db, 'Cold_Run', r, ms, payload, extra, want_sig=True, metrics=mtr)
                    print(f"   cold #{r} {db:<10}: {ms:10.3f} ms  [{extra['cache_state']}]")
                except Exception as exc:  # noqa: BLE001
                    print(f"   ✗ cold #{r} {db} GAGAL: {exc}")
                    self.failed.add((q, db))
                time.sleep(self.args.pause)

    def warm_phase(self, q):
        dbs = self.active(q)
        print(f"\n [{q}] FASE WARM (warm-up {self.args.warmup}x dibuang; ukur {self.args.iterations}x; urutan database digilir)")
        for phase, total, run_type in (('warmup', self.args.warmup, 'Warmup'), ('ukur', self.args.iterations, 'Warm_Run')):
            for it in range(1, total + 1):
                for db in rotate([d for d in dbs if (q, d) not in self.failed], it - 1):
                    try:
                        if run_type == 'Warm_Run':
                            ms, payload, mtr = self.timed_metrics(db, q)
                        else:                               # warm-up: dibuang, hanya latensi
                            ms, payload, _, _ = self.timed(db, q)
                            mtr = None
                        self.record(q, db, run_type, it, ms, payload, {'cache_state': 'warm'},
                                    want_sig=(run_type == 'Warm_Run' and it == 1), metrics=mtr)
                        if run_type == 'Warm_Run':
                            print(f"   warm #{it:<3} {db:<10}: {ms:10.3f} ms")
                    except Exception as exc:  # noqa: BLE001
                        print(f"   ✗ {run_type} #{it} {db} GAGAL: {exc}")
                        self.failed.add((q, db))
                    if self.args.pause:
                        time.sleep(self.args.pause / 4.0)

    def resource_phase(self, q):
        if self.args.skip_resource or self.args.resource_runs <= 0:
            return
        print(f"\n [{q}] FASE RESOURCE (terpisah dari latensi; cgroup container; {self.args.resource_runs}x)")
        for db in self.active(q):
            meter = self.meters.get(db)
            if not meter or not meter.available:
                print(f"   ! {db}: cgroup tidak terbaca, metrik resource dilewati")
                continue
            for i in range(1, self.args.resource_runs + 1):
                try:
                    row = self._resource_once(q, db, meter, i)
                    print(f"   resource #{i} {db:<10}: cpu_net={_fmt(row['cpu_net_s'])} s | "
                          f"ΔRAM={_fmt(row['ram_delta_peak_mb'], 2)} MB ({row['ram_sample_count']} sampel) | "
                          f"disk R/W={_fmt(row['disk_read_mb'])}/{_fmt(row['disk_write_mb'])} MB")
                except Exception as exc:  # noqa: BLE001
                    print(f"   ✗ resource #{i} {db} GAGAL: {exc}")

    def _resource_once(self, q, db, meter, i):
        client = self.clients[db]
        # (A) CPU-time + Block I/O: tanpa stream apa pun di dalam container ------------------
        a, b = meter.snapshot(), None
        time.sleep(1.0)
        b = meter.snapshot()
        idle_rate = (b['cpu_usec'] - a['cpu_usec']) / 1e6 / max(b['t'] - a['t'], 1e-6)   # CPU-detik idle per detik
        qid = uuid.uuid4().hex
        s0 = meter.snapshot()
        ms, payload, _, _ = self.timed(db, q, qid)
        s1 = meter.snapshot()
        window = max(s1['t'] - s0['t'], 1e-6)
        cpu_s = (s1['cpu_usec'] - s0['cpu_usec']) / 1e6
        cpu_idle = max(0.0, idle_rate) * window
        cpu_net = max(0.0, cpu_s - cpu_idle)
        rd = (s1['rbytes'] - s0['rbytes']) / (1024 ** 2) if s0['rbytes'] is not None else None
        wr = (s1['wbytes'] - s0['wbytes']) / (1024 ** 2) if s0['wbytes'] is not None else None
        rows, srv = client.decode(payload)                 # di luar timer; tidak ditulis sebagai baris sendiri
        sig = result_signature(rows)
        self.sigs[(q, db)].add(sig)
        if (q, db) not in self.first_rows:
            self.first_rows[(q, db)], self.first_sig[(q, db)] = rows, sig
        server_mem = None
        if db == 'clickhouse':
            try:
                ql = client.query_log_stats(qid)
                if ql.get('memory_usage') is not None:
                    server_mem = float(ql['memory_usage']) / (1024 ** 2)
            except Exception:
                pass

        # (B) RAM puncak: stream cgroup aktif hanya pada eksekusi ini -------------------------
        meter.start_stream()
        time.sleep(0.6)
        ms2, payload2, t0, t1 = self.timed(db, q)
        time.sleep(0.3)
        samples = meter.stop_stream()
        before = [v for (t, v) in samples if t <= t0]
        during = [v for (t, v) in samples if t0 <= t <= t1 + 0.25]
        ram_before = (before[-1] if before else s0['mem']) / (1024 ** 2)
        ram_peak = max([ram_before * 1024 ** 2] + during) / (1024 ** 2)

        # (C) metrik sisi-server (eksekusi tambahan, hanya iterasi pertama) -------------------
        extra_note = ''
        if i == 1 and hasattr(client, 'explain'):
            try:
                ex = client.explain(QUERIES[q][db])
                srv = {**srv, **{k: v for k, v in ex.items() if k != 'note' and v is not None}}
                extra_note = ex.get('note', '')
            except Exception as exc:  # noqa: BLE001
                extra_note = f'explain gagal: {exc.__class__.__name__}'

        row = {'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'scale_factor': self.args.scale,
               'database': db, 'query_id': q, 'query_name': QUERIES[q]['name'], 'run_type': 'Resource_Run',
               'iteration': i, 'cache_state': 'warm', 'latency_ms': f"{ms:.3f}",
               'timing_scope': 'instrumented_run_NOT_for_latency_stats',
               'cpu_time_s': _fmt(cpu_s, 4), 'cpu_idle_s': _fmt(cpu_idle, 4), 'cpu_net_s': _fmt(cpu_net, 4),
               'cpu_util_pct_of_core': _fmt(cpu_net / window * 100.0, 1), 'cpu_window_s': _fmt(window, 3),
               'ram_before_mb': _fmt(ram_before, 2), 'ram_peak_mb': _fmt(ram_peak, 2),
               'ram_delta_peak_mb': _fmt(max(0.0, ram_peak - ram_before), 2), 'ram_sample_count': len(during),
               'disk_read_mb': _fmt(rd), 'disk_write_mb': _fmt(wr),
               'server_exec_ms': _fmt(srv.get('server_exec_ms')), 'server_rows_read': srv.get('server_rows_read', ''),
               'server_bytes_read': srv.get('server_bytes_read', ''), 'server_mem_mb': _fmt(server_mem, 2),
               'note': ('container-level cgroup; CPU net = total - baseline idle; ' + extra_note).strip()}
        self.sink.write(row)
        for metric, val in (('cpu_net_s', cpu_net), ('cpu_util_pct_of_core', cpu_net / window * 100.0),
                            ('ram_delta_peak_mb', max(0.0, ram_peak - ram_before)), ('ram_peak_mb', ram_peak),
                            ('disk_read_mb', rd), ('disk_write_mb', wr), ('server_mem_mb', server_mem),
                            ('server_exec_ms', srv.get('server_exec_ms'))):
            self.add_metric(q, db, 'Resource_Run', metric, val)
        return row

    # ---- ringkasan ------------------------------------------------------------------------
    def print_query_summary(self, q):
        print(f"\n ---- RINGKASAN {q} (latensi, ms) " + "-" * 40)
        print(f"   {'database':<11} {'cold median':>12} {'warm median':>12} {'warm p95':>10} {'warm CV%':>9} {'cold/warm':>10}")
        for db in self.dbs:
            c = summarize(self.metrics.get((q, db, 'Cold_Run', 'latency_ms'), []))
            w = summarize(self.metrics.get((q, db, 'Warm_Run', 'latency_ms'), []))
            if not (c or w):
                print(f"   {db:<11} (tidak ada data)")
                continue
            ratio = (c['median'] / w['median']) if (c and w and w['median']) else None
            print(f"   {db:<11} {_fmt(c['median'], 2) if c else '-':>12} {_fmt(w['median'], 2) if w else '-':>12} "
                  f"{_fmt(w['p95'], 2) if w else '-':>10} {_fmt(w['cv_pct'], 1) if w else '-':>9} "
                  f"{_fmt(ratio, 2) if ratio else '-':>10}")
        for db in self.dbs:
            w = summarize(self.metrics.get((q, db, 'Warm_Run', 'latency_ms'), []))
            if w and w['cv_pct'] > 15:
                print(f"   ! {db}: CV warm {w['cv_pct']:.1f}% (>15%) — variasi tinggi, pertimbangkan menambah --iterations")

    def write_summary(self, path):
        sink = CsvSink(path, SUMMARY_FIELDS)
        for (q, db, run_type, metric), vals in sorted(self.metrics.items()):
            s = summarize(vals)
            if s:
                sink.write({'scale_factor': self.args.scale, 'database': db, 'query_id': q,
                            'run_type': run_type, 'metric': metric, **{k: _fmt(v, 4) for k, v in s.items()}})

    def validate_cross_db(self):
        print("\n[VALIDASI] Kesetaraan hasil antar-database (kunci + toleransi <= 0.01)")
        all_ok = True
        for q in self.queries:
            have = [db for db in self.dbs if (q, db) in self.first_rows]
            if len(have) < 2:
                print(f"   {q}: belum bisa dibandingkan (hasil < 2 database)")
                continue
            sigs = {db: self.first_sig[(q, db)] for db in have}
            text = ', '.join(f"{db}={s[:10]}" for db, s in sigs.items())
            if len(set(sigs.values())) == 1:
                print(f"   {q}: SESUAI (hash identik) | {text}")
                continue
            ref, notes, ok_all = have[0], [], True
            for other in have[1:]:
                ok, msg = compare_rows_with_tolerance(self.first_rows[(q, ref)], self.first_rows[(q, other)],
                                                      QUERY_KEYS.get(q))
                ok_all = ok_all and ok
                notes.append(f"{ref} vs {other}: {msg}")
            all_ok = all_ok and ok_all
            print(f"   {q}: {'SESUAI (toleransi <= 0.01)' if ok_all else 'TIDAK SESUAI'} | {text}")
            for n in notes:
                print(f"        - {n}")
        for (q, db), s in self.sigs.items():
            if len(s) > 1:
                print(f"   ! {q}/{db}: hasil BERBEDA antar-eksekusi ({len(s)} signature) — data berubah/tidak deterministik")
        return all_ok

    # ---- alur utama -----------------------------------------------------------------------
    def run(self):
        for db in self.dbs:
            self.clients[db].connect()
            if self.containers.get(db) and (not self.args.skip_resource or self.args.run_metrics):
                self.meters[db] = CgroupMeter(self.containers[db])
                if not self.meters[db].available:
                    print(f"   ! {db}: cgroup tidak terbaca; CPU/RAM/disk tidak tercatat untuk database ini")
        for q in self.queries:
            print("\n" + "=" * 70)
            print(f" KUERI [{q}] {QUERIES[q]['name']}")
            print("=" * 70)
            if self.args.run_metrics:
                for db in self.active(q):
                    self.measure_idle(q, db)
            self.cold_phase(q)
            self.warm_phase(q)
            self.resource_phase(q)
            self.print_query_summary(q)
        for c in self.clients.values():
            c.close()
        for m in self.meters.values():
            m.close()


# ==============================================================================
# 6. MAIN
# ==============================================================================
def main(argv=None):
    p = argparse.ArgumentParser(description="Benchmark Runner — cold run terkontrol, klien persisten, metrik cgroup")
    p.add_argument("--db", choices=["postgres", "clickhouse", "mongodb", "all"], default="all")
    p.add_argument("--query", choices=["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "all"], default="all")
    p.add_argument("--scale", default="SF0.1")
    p.add_argument("--iterations", type=int, default=15, help="jumlah warm run yang diukur per database (default 15)")
    p.add_argument("--warmup", type=int, default=2, help="warm-up dibuang sebelum warm run (default 2)")
    p.add_argument("--cold-runs", type=int, default=3, help="jumlah cold run per database per kueri (default 3)")
    p.add_argument("--cold-mode", choices=["oscache", "restart", "off"], default="oscache",
                   help="oscache = stop container + drop OS page cache + start; restart = hanya restart; off = tanpa cold run")
    p.add_argument("--resource-runs", type=int, default=3, help="jumlah run khusus CPU/RAM/Disk (default 3)")
    p.add_argument("--skip-resource", action="store_true", help="lewati fase RAM puncak + EXPLAIN (fase resource terpisah)")
    p.add_argument("--no-run-metrics", action="store_true", help="jangan catat CPU/RAM/disk pada tiap cold & warm run")
    p.add_argument("--settle", type=float, default=5.0, help="detik menunggu setelah container siap (default 5)")
    p.add_argument("--pause", type=float, default=0.5, help="jeda antar-run (detik)")
    p.add_argument("--output", default="benchmark_results_sf01.csv")
    p.add_argument("--summary-output", default="benchmark_summary_sf01.csv")
    p.add_argument("--storage-output", default="storage_summary_sf01.csv")
    p.add_argument("--env-output", default="benchmark_environment_sf01.json")
    p.add_argument("--yes", action="store_true", help="lewati konfirmasi restart container")
    args = p.parse_args(argv)

    dbs = ["postgres", "clickhouse", "mongodb"] if args.db == "all" else [args.db]
    queries = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6"] if args.query == "all" else [args.query]
    args.run_metrics = not args.no_run_metrics
    need_docker = (args.cold_mode != "off" and args.cold_runs > 0) or not args.skip_resource or args.run_metrics

    containers = {}
    if need_docker:
        for db in dbs:
            try:
                containers[db] = find_container(db)
            except Exception as exc:  # noqa: BLE001
                containers[db] = None
                print(f"! Container {db} tidak ditemukan: {exc}")
        if args.cold_mode != "off" and args.cold_runs > 0 and not all(containers.values()):
            raise SystemExit("Cold run butuh container Docker untuk semua database target. Periksa `docker ps` / CONTAINER_NAMES, "
                             "atau gunakan --cold-mode off.")

    print("=================================================================")
    print(" BENCHMARK RUNNER — COLD/WARM TERKONTROL, METRIK PRESISI")
    print("=================================================================")
    print(f" Database : {', '.join(dbs).upper()}   Kueri : {', '.join(queries)}   Skala : {args.scale}")
    print(f" Cold     : mode={args.cold_mode}, {args.cold_runs}x   Warm: warm-up {args.warmup}x + ukur {args.iterations}x")
    print(f" Per-run  : {'CPU/RAM/disk dicatat di SETIAP cold & warm run (cgroup, di luar timer)' if args.run_metrics else 'dimatikan'}")
    print(f" Resource : {'dilewati' if args.skip_resource else f'{args.resource_runs}x fase terpisah (RAM puncak + EXPLAIN)'}")
    print(f" Keluaran : {args.output} | {args.summary_output} | {args.env_output}")
    print("=================================================================")
    if args.cold_mode != "off" and args.cold_runs > 0:
        print(" PERINGATAN: cold run akan MENGHENTIKAN lalu MENYALAKAN ULANG container database"
              + (" dan mengosongkan page cache OS (butuh root/sudo)." if args.cold_mode == "oscache" else "."))
        if not args.yes and input(" Lanjutkan? [y/N] ").strip().lower() != "y":
            raise SystemExit("Dibatalkan.")

    print("\n[STEP 0] Mencatat metadata lingkungan...")
    env = collect_environment(args, dbs, containers)
    with open(args.env_output, "w", encoding="utf-8") as f:
        json.dump(env, f, ensure_ascii=False, indent=2, default=str)

    print("\n[STEP 1] Mengukur Storage Size...")
    for db_item in dbs:
        try:
            st_mb, method = measure_storage_size(db_item)
            print(f"   • {db_item.upper()}: {st_mb:.2f} MB ({method})")
            save_storage_summary(args.storage_output, args.scale, db_item, st_mb, method=method)
        except Exception as exc:  # noqa: BLE001
            print(f"   ✗ {db_item.upper()} gagal: {exc}")
            save_storage_summary(args.storage_output, args.scale, db_item, None, status='ERROR', error=str(exc))

    print("\n[STEP 2] Evaluasi kueri...")
    session = BenchmarkSession(args, dbs, queries, containers)
    session.run()

    print("\n[STEP 3] Menulis ringkasan statistik...")
    session.write_summary(args.summary_output)
    print(f"   -> {args.summary_output}")

    ok = session.validate_cross_db()
    print("\n=================================================================")
    if len(dbs) < 2:
        print(" Selesai. (Validasi lintas-database dilewati: hanya satu database dijalankan.)")
    else:
        print(" Selesai. Hasil setara antar-database." if ok else " Selesai. PERHATIAN: ada kueri yang hasilnya tidak setara.")
    print("=================================================================\n")


if __name__ == "__main__":
    main()
