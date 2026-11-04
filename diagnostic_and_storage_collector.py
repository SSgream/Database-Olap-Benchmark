import os
import sys
import json
import csv
import time
import argparse
import urllib.request
import urllib.parse
from datetime import datetime
import json
from decimal import Decimal
from bson.decimal128 import Decimal128
# Dependency imports with safety
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


# ==========================================
# KONFIGURASI KUERI ANALITIS OLAP (Q1 - Q6)
# ==========================================

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
        "mongodb_collection": "krs",
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
                round(stddevPop(f.nilai_akhir), 2) AS stddev_nilai,
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
        "mongodb_collection": "krs",
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
                    "stddev_nilai": mg_round_half_up({"$toDecimal": "$stddev_raw"}),
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
        "mongodb_collection": "krs",
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
        "mongodb_collection": "krs",
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
        "mongodb_collection": "krs",
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
                round(stddevPop(f.nilai_akhir), 2) AS stddev_nilai,
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
        "mongodb_collection": "krs",
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
                    "stddev_nilai": mg_round_half_up({"$toDecimal": "$stddev_raw"}),
                    "persentase_kelulusan": mg_pct("$total_lulus", "$total_peserta")
                }
            },
            {"$sort": {"total_peserta": -1, "rata_nilai": -1}}
        ]
    }
}

# ==========================================
# HELPER FUNCTIONS UNTUK EXECUTION & STORAGE
# ==========================================

def execute_clickhouse_query(url, user, password, query, params=None):
    """Mengeksekusi SQL pada ClickHouse via HTTP Interface (dependency-free)."""
    full_url = f"{url.rstrip('/')}/?user={urllib.parse.quote(user)}&password={urllib.parse.quote(password)}&default_format=JSON"
    req = urllib.request.Request(full_url, data=query.strip().rstrip(";").encode('utf-8'), headers={'Content-Type': 'text/plain'})
    try:
        with urllib.request.urlopen(req, timeout=300) as response:
            res_body = response.read().decode('utf-8')
            if res_body.strip():
                return json.loads(res_body)
            return None
    except Exception as e:
        # Retry with text output if format JSON is not applicable
        full_url_raw = f"{url.rstrip('/')}/?user={urllib.parse.quote(user)}&password={urllib.parse.quote(password)}"
        req_raw = urllib.request.Request(full_url_raw, data=query.strip().rstrip(";").encode('utf-8'), headers={'Content-Type': 'text/plain'})
        with urllib.request.urlopen(req_raw, timeout=300) as response_raw:
            return response_raw.read().decode('utf-8')


def capture_postgres_explain(pg_conn, query_sql, output_path):
    """Mengeksekusi EXPLAIN (ANALYZE, BUFFERS) PostgreSQL dan menyimpan ke file .txt."""
    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT TEXT) {query_sql}"
    with pg_conn.cursor() as cur:
        cur.execute(explain_sql)
        rows = cur.fetchall()
        explain_text = "\n".join([r[0] for r in rows])
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(explain_text)
    print(f"  [OK] Saved PostgreSQL EXPLAIN to: {output_path}")


def capture_clickhouse_explain(ch_url, ch_user, ch_pass, query_sql, output_path):
    """Mengeksekusi EXPLAIN PIPELINE dan EXPLAIN SYNTAX ClickHouse dan menyimpan ke file .txt."""
    explain_pipeline_sql = f"EXPLAIN PIPELINE {query_sql}"
    explain_syntax_sql = f"EXPLAIN SYNTAX {query_sql}"
    
    pipeline_res = execute_clickhouse_query(ch_url, ch_user, ch_pass, explain_pipeline_sql)
    syntax_res = execute_clickhouse_query(ch_url, ch_user, ch_pass, explain_syntax_sql)
    
    content = "=== EXPLAIN PIPELINE ===\n"
    if isinstance(pipeline_res, dict) and 'data' in pipeline_res:
        content += "\n".join([str(row) for row in pipeline_res['data']])
    else:
        content += str(pipeline_res)
        
    content += "\n\n=== EXPLAIN SYNTAX ===\n"
    if isinstance(syntax_res, dict) and 'data' in syntax_res:
        content += "\n".join([str(row) for row in syntax_res['data']])
    else:
        content += str(syntax_res)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  [OK] Saved ClickHouse EXPLAIN to: {output_path}")


def capture_mongodb_explain(mongo_db, coll_name, pipeline, output_path):
    """Mengeksekusi explain executionStats pada MongoDB dan menyimpan ke file .json."""
    explain_cmd = {
        'explain': {
            'aggregate': coll_name,
            'pipeline': pipeline,
            'cursor': {}
        },
        'verbosity': 'executionStats'
    }
    explain_res = mongo_db.command(explain_cmd)
    
    # Custom serializer for MongoDB BSON types
    def bson_serializer(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        if hasattr(obj, '__str__'):
            return str(obj)
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(explain_res, f, indent=2, default=bson_serializer)
    print(f"  [OK] Saved MongoDB EXPLAIN to: {output_path}")


# ==========================================
# STORAGE & DISK DIAGNOSTICS COLLECTORS
# ==========================================

def collect_postgres_storage_diagnostics(pg_conn):
    """Mengambil rincian ukuran tabel, ukuran B-Tree Index, dan rasio bloat pada PostgreSQL."""
    diagnostics = {
        "tables": [],
        "indexes": [],
        "summary": {}
    }
    
    # 1. Total & Table vs Index Size per Relation
    sql_tables = """
        SELECT 
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))) AS total_size_pretty,
            pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)) AS total_size_bytes,
            pg_size_pretty(pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))) AS table_size_pretty,
            pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)) AS table_size_bytes,
            pg_size_pretty(pg_indexes_size(quote_ident(schemaname) || '.' || quote_ident(tablename))) AS indexes_size_pretty,
            pg_indexes_size(quote_ident(schemaname) || '.' || quote_ident(tablename)) AS indexes_size_bytes,
            ROUND(
                (pg_indexes_size(quote_ident(schemaname) || '.' || quote_ident(tablename))::numeric / 
                 NULLIF(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)), 0) * 100), 2
            ) AS index_ratio_pct
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)) DESC;
    """
    
    with pg_conn.cursor() as cur:
        cur.execute(sql_tables)
        rows = cur.fetchall()
        total_db_bytes = 0
        total_idx_bytes = 0
        
        for r in rows:
            diagnostics["tables"].append({
                "schema": r[0],
                "table": r[1],
                "total_size": r[2],
                "total_bytes": r[3],
                "table_data_size": r[4],
                "table_data_bytes": r[5],
                "indexes_size": r[6],
                "indexes_bytes": r[7],
                "index_ratio_pct": float(r[8]) if r[8] is not None else 0.0
            })
            total_db_bytes += r[3]
            total_idx_bytes += r[7]

    # 2. Detailed B-Tree Index Sizes
    sql_indexes = """
        SELECT
            i.relname AS index_name,
            t.relname AS table_name,
            pg_size_pretty(pg_relation_size(i.oid)) AS index_size_pretty,
            pg_relation_size(i.oid) AS index_size_bytes,
            idx.indisunique AS is_unique,
            idx.indisprimary AS is_primary
        FROM pg_class t
        JOIN pg_index idx ON t.oid = idx.indrelid
        JOIN pg_class i ON i.oid = idx.indexrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY pg_relation_size(i.oid) DESC;
    """
    with pg_conn.cursor() as cur:
        cur.execute(sql_indexes)
        rows = cur.fetchall()
        for r in rows:
            diagnostics["indexes"].append({
                "index_name": r[0],
                "table_name": r[1],
                "size_pretty": r[2],
                "size_bytes": r[3],
                "is_unique": r[4],
                "is_primary": r[5]
            })

    diagnostics["summary"] = {
        "total_database_bytes": total_db_bytes,
        "total_database_mb": round(total_db_bytes / (1024 * 1024), 2),
        "total_index_bytes": total_idx_bytes,
        "total_index_mb": round(total_idx_bytes / (1024 * 1024), 2),
        "overall_index_ratio_pct": round((total_idx_bytes / total_db_bytes * 100), 2) if total_db_bytes > 0 else 0.0
    }
    
    return diagnostics


def collect_clickhouse_storage_diagnostics(ch_url, ch_user, ch_pass, db_name):
    """Mengambil rincian kompresi LZ4 per tabel dan per kolom pada ClickHouse (system.parts & system.columns)."""
    diagnostics = {
        "tables": [],
        "fact_krs_columns": [],
        "summary": {}
    }
    
    # 1. Table-level compression ratio
    sql_tables = f"""
        SELECT
            table,
            sum(data_compressed_bytes) AS compressed_bytes,
            sum(data_uncompressed_bytes) AS uncompressed_bytes,
            round(sum(data_uncompressed_bytes) / nullIf(sum(data_compressed_bytes), 0), 2) AS compression_ratio,
            sum(marks_bytes) AS marks_bytes,
            sum(rows) AS total_rows,
            count() AS total_parts
        FROM system.parts
        WHERE active = 1 AND database = '{db_name}'
        GROUP BY table
        ORDER BY sum(data_compressed_bytes) DESC
    """
    
    res = execute_clickhouse_query(ch_url, ch_user, ch_pass, sql_tables)
    total_comp = 0
    total_uncomp = 0
    total_marks = 0
    
    if isinstance(res, dict) and 'data' in res:
        for row in res['data']:
            c_bytes = int(row['compressed_bytes'])
            u_bytes = int(row['uncompressed_bytes'])
            m_bytes = int(row['marks_bytes'])
            diagnostics["tables"].append({
                "table": row['table'],
                "compressed_mb": round(c_bytes / (1024 * 1024), 2),
                "uncompressed_mb": round(u_bytes / (1024 * 1024), 2),
                "compression_ratio": float(row['compression_ratio']),
                "marks_kb": round(m_bytes / 1024, 2),
                "total_rows": int(row['total_rows']),
                "parts_count": int(row['total_parts'])
            })
            total_comp += c_bytes
            total_uncomp += u_bytes
            total_marks += m_bytes

    # 2. Column-level compression breakdown for fact_krs
    sql_cols = f"""
        SELECT
            column,
            type,
            sum(data_compressed_bytes) AS compressed_bytes,
            sum(data_uncompressed_bytes) AS uncompressed_bytes,
            round(sum(data_uncompressed_bytes) / nullIf(sum(data_compressed_bytes), 0), 2) AS column_compression_ratio
        FROM system.columns
        WHERE database = '{db_name}' AND table = 'fact_krs'
        GROUP BY column, type
        ORDER BY sum(data_compressed_bytes) DESC
    """
    res_cols = execute_clickhouse_query(ch_url, ch_user, ch_pass, sql_cols)
    if isinstance(res_cols, dict) and 'data' in res_cols:
        for row in res_cols['data']:
            diagnostics["fact_krs_columns"].append({
                "column": row['column'],
                "type": row['type'],
                "compressed_kb": round(int(row['compressed_bytes']) / 1024, 2),
                "uncompressed_kb": round(int(row['uncompressed_bytes']) / 1024, 2),
                "compression_ratio": float(row['column_compression_ratio'])
            })

    diagnostics["summary"] = {
        "total_compressed_bytes": total_comp,
        "total_compressed_mb": round(total_comp / (1024 * 1024), 2),
        "total_uncompressed_bytes": total_uncomp,
        "total_uncompressed_mb": round(total_uncomp / (1024 * 1024), 2),
        "overall_compression_ratio": round(total_uncomp / total_comp, 2) if total_comp > 0 else 0.0,
        "total_marks_kb": round(total_marks / 1024, 2)
    }
    
    return diagnostics


def collect_mongodb_storage_diagnostics(mongo_db):
    """Mengambil rincian WiredTiger storage, BSON data size, dan Compound Indexes pada MongoDB."""
    diagnostics = {
        "collections": [],
        "summary": {}
    }
    
    db_stats = mongo_db.command("dbStats")
    coll_names = mongo_db.list_collection_names()
    
    total_storage = 0
    total_data = 0
    total_indexes = 0
    
    for coll in coll_names:
        c_stats = mongo_db.command("collStats", coll)
        storage_bytes = c_stats.get('storageSize', 0)
        data_bytes = c_stats.get('size', 0)
        idx_bytes = c_stats.get('totalIndexSize', 0)
        idx_details = c_stats.get('indexSizes', {})
        
        comp_ratio = round(data_bytes / storage_bytes, 2) if storage_bytes > 0 else 1.0
        
        diagnostics["collections"].append({
            "collection": coll,
            "count": c_stats.get('count', 0),
            "storage_size_mb": round(storage_bytes / (1024 * 1024), 2),
            "uncompressed_data_size_mb": round(data_bytes / (1024 * 1024), 2),
            "wiredtiger_compression_ratio": comp_ratio,
            "total_index_size_mb": round(idx_bytes / (1024 * 1024), 2),
            "index_details": {k: round(v / (1024 * 1024), 2) for k, v in idx_details.items()}
        })
        
        total_storage += storage_bytes
        total_data += data_bytes
        total_indexes += idx_bytes

    diagnostics["summary"] = {
        "total_storage_mb": round(total_storage / (1024 * 1024), 2),
        "total_uncompressed_data_mb": round(total_data / (1024 * 1024), 2),
        "total_index_mb": round(total_indexes / (1024 * 1024), 2),
        "overall_wiredtiger_compression_ratio": round(total_data / total_storage, 2) if total_storage > 0 else 1.0
    }
    
    return diagnostics


# ==========================================
# MAIN EXECUTION WORKFLOW
# ==========================================

def main():
    parser = argparse.ArgumentParser(description="Diagnostics and Storage Footprint Collector")
    parser.add_argument("--scale-factor", default="SF1.0", help="Scale Factor name (e.g., SF0.1, SF0.5, SF1.0)")
    parser.add_argument("--output-dir", default="./diagnostics_results", help="Directory to save EXPLAIN logs and storage reports")
    
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

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    explain_dir = os.path.join(args.output_dir, "explain_logs")
    os.makedirs(explain_dir, exist_ok=True)

    print("==========================================================================")
    print(f" DIAGNOSTICS & STORAGE COLLECTOR ({args.scale_factor})")
    print(f" Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" Output Directory: {os.path.abspath(args.output_dir)}")
    print("==========================================================================")

    # ---------------------------------------------------------
    # 1. EXPLAIN LOGS COLLECTION (Q1 - Q6)
    # ---------------------------------------------------------
    print("\n[1/2] Capturing EXPLAIN Logs for PostgreSQL, ClickHouse, and MongoDB...")
    
    # PostgreSQL Connect
    pg_conn = None
    if HAS_PSYCOPG2:
        try:
            pg_conn = psycopg2.connect(
                host=args.pg_host, port=args.pg_port, dbname=args.pg_db,
                user=args.pg_user, password=args.pg_pass
            )
            print("  [CONN] Connected to PostgreSQL successfully.")
        except Exception as e:
            print(f"  [WARN] Failed to connect to PostgreSQL: {e}")

    # MongoDB Connect
    mongo_db = None
    if HAS_PYMONGO:
        try:
            m_client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=3000)
            mongo_db = m_client[args.mongo_db]
            print("  [CONN] Connected to MongoDB successfully.")
        except Exception as e:
            print(f"  [WARN] Failed to connect to MongoDB: {e}")

    for q_id, q_data in QUERIES.items():
        print(f"\n--- Processing {q_id}: {q_data['name']} ---")
        
        # PostgreSQL EXPLAIN
        if pg_conn:
            try:
                out_path = os.path.join(explain_dir, f"postgres_{q_id}_{args.scale_factor}.txt")
                capture_postgres_explain(pg_conn, q_data["postgres"], out_path)
            except Exception as e:
                print(f"  [ERROR] PostgreSQL EXPLAIN {q_id} failed: {e}")
                if pg_conn:
                    pg_conn.rollback()

        # ClickHouse EXPLAIN
        try:
            out_path = os.path.join(explain_dir, f"clickhouse_{q_id}_{args.scale_factor}.txt")
            capture_clickhouse_explain(args.ch_url, args.ch_user, args.ch_pass, q_data["clickhouse"], out_path)
        except Exception as e:
            print(f"  [WARN] ClickHouse EXPLAIN {q_id} skipped / failed: {e}")

        # MongoDB EXPLAIN
        if mongo_db is not None:
            try:
                out_path = os.path.join(explain_dir, f"mongodb_{q_id}_{args.scale_factor}.json")
                capture_mongodb_explain(mongo_db, q_data["mongodb_collection"], q_data["mongodb"], out_path)
            except Exception as e:
                print(f"  [ERROR] MongoDB EXPLAIN {q_id} failed: {e}")

    # ---------------------------------------------------------
    # 2. DISK STORAGE & COMPRESSION DIAGNOSTICS
    # ---------------------------------------------------------
    print("\n[2/2] Collecting Detailed Disk Storage & Compression Metrics...")
    storage_report = {}

    # PostgreSQL Storage
    if pg_conn:
        try:
            print("  Collecting PostgreSQL Index Bloat & Relation Sizes...")
            pg_storage = collect_postgres_storage_diagnostics(pg_conn)
            storage_report["postgres"] = pg_storage
        except Exception as e:
            print(f"  [ERROR] PostgreSQL storage collection failed: {e}")

    # ClickHouse Storage
    try:
        print("  Collecting ClickHouse LZ4 Compression & Column Sizes...")
        ch_storage = collect_clickhouse_storage_diagnostics(args.ch_url, args.ch_user, args.ch_pass, args.ch_db)
        storage_report["clickhouse"] = ch_storage
    except Exception as e:
        print(f"  [WARN] ClickHouse storage collection failed: {e}")

    # MongoDB Storage
    if mongo_db is not None:
        try:
            print("  Collecting MongoDB WiredTiger Storage & Compound Index Sizes...")
            mg_storage = collect_mongodb_storage_diagnostics(mongo_db)
            storage_report["mongodb"] = mg_storage
        except Exception as e:
            print(f"  [ERROR] MongoDB storage collection failed: {e}")

    # Save Storage JSON Report
    report_json_path = os.path.join(args.output_dir, f"storage_diagnostics_report_{args.scale_factor}.json")
    with open(report_json_path, "w", encoding="utf-8") as f:
        json.dump(storage_report, f, indent=2)
    print(f"\n[OK] Complete Storage Diagnostics JSON saved to: {report_json_path}")

    # Save CSV Summary for Easy Embedding in Skripsi Tables
    summary_csv_path = os.path.join(args.output_dir, f"storage_summary_table_{args.scale_factor}.csv")
    with open(summary_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Database", "Total_Storage_MB", "Uncompressed_Data_MB", "Total_Index_MB", "Compression_or_Index_Ratio"])
        
        if "postgres" in storage_report:
            s = storage_report["postgres"]["summary"]
            writer.writerow(["PostgreSQL", s["total_database_mb"], s["total_database_mb"] - s["total_index_mb"], s["total_index_mb"], f"{s['overall_index_ratio_pct']}% Index Ratio"])
        
        if "clickhouse" in storage_report:
            s = storage_report["clickhouse"]["summary"]
            writer.writerow(["ClickHouse", s["total_compressed_mb"], s["total_uncompressed_mb"], round(s["total_marks_kb"]/1024, 2), f"{s['overall_compression_ratio']}x Compression"])
            
        if "mongodb" in storage_report:
            s = storage_report["mongodb"]["summary"]
            writer.writerow(["MongoDB", s["total_storage_mb"], s["total_uncompressed_data_mb"], s["total_index_mb"], f"{s['overall_wiredtiger_compression_ratio']}x WiredTiger"])

    print(f"[OK] Summary CSV saved to: {summary_csv_path}")
    print("\nCollection Workflow Finished Successfully!")

    if pg_conn:
        pg_conn.close()


if __name__ == "__main__":
    main()
