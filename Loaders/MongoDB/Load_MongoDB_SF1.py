import os
import sys
import json
import gc
import warnings
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

# ==============================================================================
# 1. KONFIGURASI DATABASE & SKALA BEBAN KERJA
# ==============================================================================
SCALE_FACTOR = "SF1"

# Batas maksimum baris yang dimuat untuk SETIAP tabel fakta (KRS, Presensi,
# Penasihat Akademik, dan seluruh Riwayat Simpeg). Jika tersedia kolom tanggal
# yang relevan, data akan diurutkan dahulu sehingga N baris TERBARU yang diambil.
MAX_FACT_ROWS = 6_000_000

MONGODB_CONFIG = {
    'uri': 'mongodb://localhost:27018',
    'db_name': 'skripsi_db'
}

# ==============================================================================
# 2. DEFINISI PATH SECARA DINAMIS (BERBASIS LOKASI FILE SCRIPT)
# ==============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
DATA_DIR = os.path.join(ROOT_DIR, "DATA")

NEOSIA_FIXED_DIR = os.path.join(DATA_DIR, "NEOSIA", "fixed")
NEOSIA_SCALED_DIR = os.path.join(DATA_DIR, "NEOSIA", "scaled", SCALE_FACTOR)

SIMPEG_FIXED_DIR = os.path.join(DATA_DIR, "SIMPEG", "fixed")
SIMPEG_SCALED_DIR = os.path.join(DATA_DIR, "SIMPEG", "scaled", SCALE_FACTOR)

JSON_OUTPUT_DIR = os.path.join(ROOT_DIR, "star_schema_json")

def log(message):
    print(f"[ETL MONGO INFO - {SCALE_FACTOR}] {message}")

def log_error(message):
    print(f"[ETL MONGO ERROR - {SCALE_FACTOR}] {message}", file=sys.stderr)

class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, (np.ndarray)):
            return obj.tolist()
        elif isinstance(obj, (pd.Timestamp)):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, (np.bool_)):
            return bool(obj)
        elif pd.isna(obj):
            return None
        return super(MongoJSONEncoder, self).default(obj)

# ==============================================================================
# 3. KELAS UTAMA ETL PROSESOR MONGODB
# ==============================================================================
class MongoETLLoader:
    def __init__(self):
        self.prodi_map = {}
        self.dosen_map = {}
        self.mhs_map = {}
        self.mk_map = {}
        self.kelas_map = {}
        self.semester_map = {}
        self.kehadiran_map = {}
        self.pegawai_map = {}
        self.golongan_map = {}
        self.jabfung_map = {}
        self.jabstruk_map = {}
        self.unit_map = {}
        self.jenis_peg_map = {}
        self.status_peg_map = {}
        self.pendidikan_map = {}

    def limit_latest_rows(self, df, date_col=None, n=MAX_FACT_ROWS, label=""):
        """
        Membatasi jumlah baris sebuah DataFrame menjadi maksimum `n` baris.
        Mengikuti pola yang dipakai pada fact presensi: jika `date_col` tersedia
        pada DataFrame, data diurutkan menurun berdasarkan kolom tersebut
        terlebih dahulu sehingga N baris TERBARU yang dipertahankan. Jika
        kolom tanggal tidak tersedia/valid, N baris pertama (urutan asli) yang dipertahankan.
        """
        if df is None or len(df) <= n:
            return df

        original_len = len(df)
        if date_col and date_col in df.columns:
            df = df.copy()
            df['_sort_dt'] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.sort_values(by='_sort_dt', ascending=False).drop(columns=['_sort_dt'])
        else:
            df = df.copy()

        df = df.head(n)
        log(f"-> Batasan N={n:,} baris diterapkan pada {label or 'data'} (semula {original_len:,} baris).")
        return df

    def load_and_topn_streaming(self, folder, filename, date_lookup_df, left_on, right_on,
                                 date_col, n=MAX_FACT_ROWS, chunksize=1_000_000,
                                 tiebreak_col='id', usecols=None, label="",
                                 extra_merges=None, column_maps=None):
        """
        Versi 'streaming' dari load_csv() + merge() + limit_latest_rows() khusus untuk
        file CSV RAKSASA (ratusan juta baris) yang tidak mungkin dimuat sekaligus ke RAM
        (penyebab error 'Error tokenizing data: out of memory' pada presensi_mahasiswa_SF1.csv
        dan kelas_kuliah_has_mahasiswa_SF1.csv).

        File dibaca per-chunk (default 1 juta baris), langsung digabung dengan tabel
        lookup kecil (mis. pertemuan.csv / kartu_rencana_studi.csv), lalu sebuah buffer
        TOP-N (baris terbaru berdasarkan date_col) dipertahankan dan dipangkas ulang ke
        N baris setiap selesai satu chunk. Memori yang dipakai maksimal ~(chunksize + n)
        baris, bukan seluruh isi file.

        KONSISTENSI ANTAR DATABASE: hasil akhir algoritma ini identik dengan
        sort_values(date_col, ascending=False).head(n) pada seluruh data sekaligus,
        berapa pun ukuran chunksize-nya. Selama skrip ClickHouse/PostgreSQL/MongoDB
        memakai sumber CSV yang sama, date_col yang sama, dan tiebreak_col yang sama
        (kolom PK unik, mis. 'id', untuk memecah baris bertanggal identik), ketiga
        database akan memuat N baris IDENTIK.

        Parameter tambahan:
        - extra_merges: opsional, list of dict untuk merge tambahan PER CHUNK setelah
          merge utama, mis. join ke tabel lookup kecil lain (kelas_kuliah_koordinator).
          Setiap item: {'df': lookup_df, 'left_on': ..., 'right_on': ...,
          'how': 'left' (default), 'suffixes': (..., ...)}.
        - column_maps: opsional, dict {nama_kolom_baru: (kolom_kunci_di_chunk, dict_lookup)}
          untuk menempelkan nilai dari sebuah dict Python (bukan DataFrame) ke tiap
          chunk via .map(). Dipakai saat tabel lookup-nya sendiri terlalu besar untuk
          di-load sebagai DataFrame utuh -> lihat build_pivoted_value_maps_streaming().
        """
        path = os.path.join(folder, filename)
        if not os.path.exists(path):
            log(f"Peringatan: File {filename} tidak ditemukan di {folder}.")
            return None

        buffer = None
        total_seen = 0
        read_kwargs = dict(chunksize=chunksize, low_memory=False)
        if usecols:
            read_kwargs['usecols'] = usecols

        try:
            reader = pd.read_csv(path, **read_kwargs)
        except Exception:
            read_kwargs['encoding'] = 'utf-8-sig'
            reader = pd.read_csv(path, **read_kwargs)

        for chunk in reader:
            chunk.columns = chunk.columns.astype(str).str.strip().str.replace('\ufeff', '')
            total_seen += len(chunk)

            merged = chunk.merge(date_lookup_df, left_on=left_on, right_on=right_on,
                                  how='inner', suffixes=('', '_lk'))
            del chunk

            if merged.empty:
                continue

            if extra_merges:
                for spec in extra_merges:
                    merged = merged.merge(
                        spec['df'], left_on=spec['left_on'], right_on=spec['right_on'],
                        how=spec.get('how', 'left'), suffixes=spec.get('suffixes', ('', '_x'))
                    )

            if column_maps:
                for target_col, (key_col, map_dict) in column_maps.items():
                    merged[target_col] = merged[key_col].map(map_dict)

            merged['_sort_dt'] = pd.to_datetime(merged[date_col], errors='coerce')

            if buffer is None:
                buffer = merged
            else:
                buffer = pd.concat([buffer, merged], ignore_index=True)
            del merged

            sort_cols = ['_sort_dt']
            ascending = [False]
            if tiebreak_col and tiebreak_col in buffer.columns:
                sort_cols.append(tiebreak_col)
                ascending.append(True)

            buffer = buffer.sort_values(by=sort_cols, ascending=ascending).head(n)
            gc.collect()

        if buffer is not None:
            buffer = buffer.drop(columns=['_sort_dt']).reset_index(drop=True)
            log(f"Streaming filter {label}: {total_seen:,} baris dibaca -> {len(buffer):,} baris terbaru diambil (N={n:,}).")
        return buffer

    def build_pivoted_value_maps_streaming(self, folder, filename, key_col, category_col,
                                            value_col, category_label_map, label="pivot",
                                            chunksize=1_000_000):
        """
        Versi PIVOT dari lookup streaming, untuk tabel nilai berformat LONG (satu baris
        = satu komponen nilai per peserta), mis. kelas_kuliah_nilai_SF1.csv:
        kolomnya cuma id, id_peserta_kelas, id_bobot_nilai_kelas_kuliah, nilai,
        created_at, updated_at -- BUKAN kolom nilai_tugas/nilai_uts/nilai_uas siap pakai.
        File ini RAKSASA (seukuran kelas_kuliah_has_mahasiswa) -> JANGAN load penuh
        sebagai DataFrame, dibaca per-chunk saja.

        category_label_map: dict {id_bobot: nama_singkat}, mis.
            {1: 'uas', 2: 'presensi', 3: 'kuis', 4: 'uh', 5: 'uts'}
            (didapat dari tabel referensi tipe_bobot_nilai.csv).

        Return: dict {nama_singkat: {key_col_value: nilai}}, contoh:
            {'uas': {123: 88.0, ...}, 'uts': {...}, 'presensi': {...},
             'kuis': {...}, 'uh': {...}}
        Siap dipakai sebagai `column_maps` pada load_and_topn_streaming(), satu
        entry per komponen (nilai_uas, nilai_uts, nilai_presensi, nilai_kuis, nilai_uh).
        """
        log(f"Membangun pivot map streaming untuk {label} ({filename})...")
        path = os.path.join(folder, filename)
        result = {name: {} for name in set(category_label_map.values())}
        if not os.path.exists(path):
            log(f"Peringatan: File {filename} tidak ditemukan di {folder}. Pivot {label} kosong.")
            return result

        usecols = [key_col, category_col, value_col]
        read_kwargs = dict(chunksize=chunksize, low_memory=False, usecols=usecols)
        try:
            reader = pd.read_csv(path, **read_kwargs)
        except Exception:
            read_kwargs['encoding'] = 'utf-8-sig'
            reader = pd.read_csv(path, **read_kwargs)

        total_seen = 0
        for chunk in reader:
            chunk.columns = chunk.columns.astype(str).str.strip().str.replace('\ufeff', '')
            total_seen += len(chunk)
            chunk = chunk.copy()
            chunk['_komponen'] = chunk[category_col].map(category_label_map)
            chunk = chunk.dropna(subset=['_komponen'])
            for komponen, sub in chunk.groupby('_komponen'):
                result[komponen].update(dict(zip(sub[key_col], sub[value_col])))
            del chunk
            gc.collect()

        for komponen, d in result.items():
            log(f"  - {label}[{komponen}]: {len(d):,} baris")
        log(f"Membangun pivot streaming {label}: {total_seen:,} baris dibaca.")
        return result

    def load_csv(self, folder, filename):
        path = os.path.join(folder, filename)
        if not os.path.exists(path):
            log(f"Peringatan: File {filename} tidak ditemukan di {folder}. Mengembalikan DataFrame kosong.")
            return None
        try:
            df = pd.read_csv(path, low_memory=False)
            df.columns = df.columns.astype(str).str.strip().str.replace('\ufeff', '')
            return df
        except Exception as e:
            try:
                df = pd.read_csv(path, encoding='utf-8-sig', low_memory=False)
                df.columns = df.columns.astype(str).str.strip()
                return df
            except Exception as ex:
                log_error(f"Gagal membaca {path}: {str(ex)}")
                return None

    def save_to_json_lines(self, list_of_dicts, filename):
        os.makedirs(JSON_OUTPUT_DIR, exist_ok=True)
        out_path = os.path.join(JSON_OUTPUT_DIR, filename)
        try:
            with open(out_path, 'w', encoding='utf-8') as f:
                for doc in list_of_dicts:
                    f.write(json.dumps(doc, cls=MongoJSONEncoder) + '\n')
            log(f"-> Berhasil menyimpan {len(list_of_dicts):,} dokumen ke berkas JSON: '{out_path}'")
            return out_path
        except Exception as e:
            log_error(f"Gagal menulis berkas JSON {filename}: {str(e)}")
            return None

    def _import_chunk_to_mongodb(self, collection_name, chunk_docs, drop_first=False):
        if not chunk_docs:
            return
        try:
            import pymongo
            from pymongo import MongoClient
        except ImportError:
            return

        try:
            client = MongoClient(MONGODB_CONFIG['uri'], serverSelectionTimeoutMS=3000)
            db = client[MONGODB_CONFIG['db_name']]
            col = db[collection_name]
            if drop_first:
                col.drop()
            clean_chunk = [json.loads(json.dumps(d, cls=MongoJSONEncoder)) for d in chunk_docs]
            col.insert_many(clean_chunk)
        except Exception as e:
            log_error(f"Gagal melakukan koneksi/impor langsung ke MongoDB: {str(e)}")

    def import_to_mongodb_direct(self, collection_name, list_of_dicts, batch_size=50000):
        if not list_of_dicts:
            return

        try:
            import pymongo
            from pymongo import MongoClient
        except ImportError:
            log(f"Modul 'pymongo' tidak terpasang. Hanya mengekspor {len(list_of_dicts):,} dokumen ke JSON.")
            return

        try:
            client = MongoClient(MONGODB_CONFIG['uri'], serverSelectionTimeoutMS=3000)
            db = client[MONGODB_CONFIG['db_name']]
            col = db[collection_name]

            col.drop()

            total = len(list_of_dicts)
            log(f"Memuat {total:,} dokumen ke koleksi MongoDB '{collection_name}' (batch {batch_size:,})...")

            for start in range(0, total, batch_size):
                chunk = list_of_dicts[start:start + batch_size]
                clean_chunk = [json.loads(json.dumps(d, cls=MongoJSONEncoder)) for d in chunk]
                col.insert_many(clean_chunk)

            log(f"-> [DB DIRECT] Berhasil mengimpor {total:,} dokumen ke koleksi '{collection_name}' di MongoDB.")
        except Exception as e:
            log_error(f"Gagal melakukan koneksi/impor langsung ke MongoDB: {str(e)}")

    def run(self):
        log("=== MEMULAI PROSES ETL EMBEDDED SCHEMA (MONGODB) ===")
        log(f"Skala Data: {SCALE_FACTOR}")
        log(f"Struktur Direktori Root: {ROOT_DIR}")

        # ======================================================================
        # STEP 1: LOAD & PREPARE SEMUA DIMENSI DI MEMORI RAM
        # ======================================================================
        log("1. Memuat seluruh tabel dimensi master ke memori...")

        # 1.1 Prodi & Dependensinya
        df_p_neo = self.load_csv(NEOSIA_FIXED_DIR, "prodi.csv")
        if df_p_neo is None:
            df_p_neo = self.load_csv(NEOSIA_SCALED_DIR, "prodi.csv")

        df_pj_neo = self.load_csv(NEOSIA_FIXED_DIR, "prodi_jenjang.csv")
        df_f_neo = self.load_csv(NEOSIA_FIXED_DIR, "fakultas.csv")
        df_j_neo = self.load_csv(NEOSIA_FIXED_DIR, "jurusan.csv")
        df_p_simp = self.load_csv(SIMPEG_FIXED_DIR, "ref_program_studi.csv")

        if df_p_neo is not None:
            m_prodi = df_p_neo.copy()
            if df_pj_neo is not None:
                m_prodi = m_prodi.merge(df_pj_neo, left_on='id_prodi_jenjang', right_on='id', suffixes=('', '_jen'), how='left')
            if df_f_neo is not None:
                m_prodi = m_prodi.merge(df_f_neo, left_on='id_fakultas', right_on='id', suffixes=('', '_fak'), how='left')
            if df_j_neo is not None:
                m_prodi = m_prodi.merge(df_j_neo, left_on='id_jurusan', right_on='id', suffixes=('', '_jur'), how='left')

            if df_p_simp is not None:
                right_key = 'prodiDiktiKode' if 'prodiDiktiKode' in df_p_simp.columns else ('kode_dikti' if 'kode_dikti' in df_p_simp.columns else df_p_simp.columns[0])
                m_prodi = m_prodi.merge(df_p_simp, left_on='kode_dikti', right_on=right_key, suffixes=('', '_simp'), how='left')
                id_prodi_simpeg_series = m_prodi['prodiKode'].fillna(-1).astype(int).replace({-1: None}) if 'prodiKode' in m_prodi.columns else pd.Series([None] * len(m_prodi))
            else:
                id_prodi_simpeg_series = pd.Series([None] * len(m_prodi))

            for i, r in m_prodi.iterrows():
                p_doc = {
                    "prodi_kode": str(r['kode_dikti']) if 'kode_dikti' in r and pd.notna(r['kode_dikti']) else "Unknown",
                    "id_prodi_neosia": int(r['id']) if 'id' in r and pd.notna(r['id']) else None,
                    "id_prodi_simpeg": int(id_prodi_simpeg_series.iloc[i]) if pd.notna(id_prodi_simpeg_series.iloc[i]) else None,
                    "nama_resmi": str(r['nama_resmi']) if 'nama_resmi' in r and pd.notna(r['nama_resmi']) else "Unknown",
                    "nama_singkat": str(r['nama_singkat']) if 'nama_singkat' in r and pd.notna(r['nama_singkat']) else "Unknown",
                    "jenjang_nama": str(r['nama']) if 'nama' in r and pd.notna(r['nama']) else "S1",
                    "fakultas_nama": str(r['nama_resmi_fak']) if 'nama_resmi_fak' in r and pd.notna(r['nama_resmi_fak']) else "Unknown",
                    "jurusan_nama": str(r['nama_resmi_jur']) if 'nama_resmi_jur' in r and pd.notna(r['nama_resmi_jur']) else "Unknown",
                    "is_eksakta": bool(r['is_eksakta']) if 'is_eksakta' in r and pd.notna(r['is_eksakta']) else False,
                    "is_active": bool(r['is_active']) if 'is_active' in r and pd.notna(r['is_active']) else True
                }
                if 'id' in r and pd.notna(r['id']):
                    self.prodi_map[int(r['id'])] = p_doc
                if pd.notna(p_doc["id_prodi_simpeg"]):
                    self.prodi_map[f"simpeg_{p_doc['id_prodi_simpeg']}"] = p_doc

        # 1.2 Dosen
        df_d_neo = self.load_csv(NEOSIA_FIXED_DIR, "dosen.csv")
        if df_d_neo is None:
            df_d_neo = self.load_csv(NEOSIA_SCALED_DIR, "dosen.csv")

        df_d_simp = self.load_csv(SIMPEG_FIXED_DIR, "data_dosen.csv")
        if df_d_simp is None:
            df_d_simp = self.load_csv(SIMPEG_SCALED_DIR, "data_dosen.csv")

        df_rpd = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_prodi_dosen_neosia_SF1.csv")
        if df_rpd is None:
            df_rpd = self.load_csv(SIMPEG_SCALED_DIR, f"riwayat_prodi_dosen_neosia_{SCALE_FACTOR}.csv")

        if df_d_neo is not None:
            simpeg_dosen_lookup = {}
            if df_d_simp is not None and 'dosenIdNeosia' in df_d_simp.columns:
                for _, r in df_d_simp.iterrows():
                    if pd.notna(r['dosenIdNeosia']):
                        simpeg_dosen_lookup[int(r['dosenIdNeosia'])] = int(r['dosenPegId'])

            for i, r in df_d_neo.iterrows():
                id_dosen_neosia = int(r['id']) if 'id' in r and pd.notna(r['id']) else (i + 1)
                id_pegawai_simpeg = simpeg_dosen_lookup.get(id_dosen_neosia, None)
                p_doc = self.prodi_map.get(int(r['id_prodi'])) if 'id_prodi' in r and pd.notna(r['id_prodi']) else None

                d_doc = {
                    "id_dosen_neosia": id_dosen_neosia,
                    "id_pegawai_simpeg": int(id_pegawai_simpeg) if id_pegawai_simpeg is not None else None,
                    "nip": str(r['nip']) if 'nip' in r and pd.notna(r['nip']) else "Unknown",
                    "nidn": str(r['nidn']) if 'nidn' in r and pd.notna(r['nidn']) else "Unknown",
                    "nama": str(r['nama']) if 'nama' in r and pd.notna(r['nama']) else "Unknown",
                    "is_active": bool(r['is_active']) if 'is_active' in r and pd.notna(r['is_active']) else True,
                    "prodi": p_doc
                }
                self.dosen_map[id_dosen_neosia] = d_doc
                if id_pegawai_simpeg is not None:
                    self.dosen_map[f"simpeg_{int(id_pegawai_simpeg)}"] = d_doc

        # 1.3 Mahasiswa
        df_m = self.load_csv(NEOSIA_FIXED_DIR, "mahasiswa.csv")
        if df_m is None:
            df_m = self.load_csv(NEOSIA_SCALED_DIR, "mahasiswa.csv")

        df_jm = self.load_csv(NEOSIA_FIXED_DIR, "jalur_masuk.csv")
        df_pm = self.load_csv(NEOSIA_FIXED_DIR, "peminatan.csv")

        if df_m is not None:
            m_mhs = df_m.copy()
            if df_jm is not None:
                m_mhs = m_mhs.merge(df_jm, left_on='id_jalur_masuk', right_on='id', suffixes=('', '_jm'), how='left')
            if df_pm is not None:
                m_mhs = m_mhs.merge(df_pm, left_on='id_peminatan', right_on='id', suffixes=('', '_pm'), how='left')

            for i, r in m_mhs.iterrows():
                p_doc = self.prodi_map.get(int(r['id_prodi'])) if 'id_prodi' in r and pd.notna(r['id_prodi']) else None
                m_doc = {
                    "id_mahasiswa": int(r['id']),
                    "nim": str(r['nim']) if 'nim' in r and pd.notna(r['nim']) else "Unknown",
                    "nama_mahasiswa": str(r['nama_mahasiswa']) if 'nama_mahasiswa' in r and pd.notna(r['nama_mahasiswa']) else "Unknown",
                    "angkatan": int(r['angkatan']) if 'angkatan' in r and pd.notna(r['angkatan']) else 2022,
                    "jenis_kelamin": str(r['jenis_kelamin']) if 'jenis_kelamin' in r and pd.notna(r['jenis_kelamin']) else "L",
                    "tempat_lahir": str(r['tempat_lahir']) if 'tempat_lahir' in r and pd.notna(r['tempat_lahir']) else "Makassar",
                    "tanggal_lahir": str(r['tanggal_lahir']) if 'tanggal_lahir' in r and pd.notna(r['tanggal_lahir']) else None,
                    "kewarganegaraan": str(r['kewarganegaraan']) if 'kewarganegaraan' in r and pd.notna(r['kewarganegaraan']) else "WNI",
                    "jalur_masuk_nama": str(r['nama']) if 'nama' in r and pd.notna(r['nama']) else "SNMPTN",
                    "peminatan_nama": str(r['nama_resmi_peminatan']) if 'nama_resmi_peminatan' in r and pd.notna(r['nama_resmi_peminatan']) else "General",
                    "is_active": bool(r['is_active']) if 'is_active' in r and pd.notna(r['is_active']) else True,
                    "prodi": p_doc
                }
                self.mhs_map[int(r['id'])] = m_doc

        # 1.4 Mata Kuliah
        df_mk = self.load_csv(NEOSIA_FIXED_DIR, "mata_kuliah.csv")
        if df_mk is None:
            df_mk = self.load_csv(NEOSIA_SCALED_DIR, "mata_kuliah.csv")
        df_kur = self.load_csv(NEOSIA_FIXED_DIR, "kurikulum.csv")
        df_pj = self.load_csv(NEOSIA_FIXED_DIR, "prodi_jenjang.csv")

        if df_mk is not None:
            m_mk = df_mk.copy()
            if df_kur is not None:
                m_mk = m_mk.merge(df_kur, left_on='id_kurikulum', right_on='id', suffixes=('', '_kur'), how='left')
            if df_pj is not None:
                m_mk = m_mk.merge(df_pj, left_on='id_prodi_jenjang', right_on='id', suffixes=('', '_jen'), how='left')

            for i, r in m_mk.iterrows():
                p_doc = self.prodi_map.get(int(r['id_prodi'])) if 'id_prodi' in r and pd.notna(r['id_prodi']) else None
                mk_doc = {
                    "id_mata_kuliah": int(r['id']),
                    "nama_resmi": str(r['nama_resmi']) if 'nama_resmi' in r and pd.notna(r['nama_resmi']) else "Unknown",
                    "jenjang_nama": str(r['nama_jenjang']) if 'nama_jenjang' in r and pd.notna(r['nama_jenjang']) else "S1",
                    "kurikulum_nama": str(r['nama']) if 'nama' in r and pd.notna(r['nama']) else "Unknown",
                    "prodi": p_doc
                }
                self.mk_map[int(r['id'])] = mk_doc

        # 1.5 Kelas Kuliah
        df_kk = self.load_csv(NEOSIA_FIXED_DIR, "kelas_kuliah.csv")
        if df_kk is None:
            df_kk = self.load_csv(NEOSIA_FIXED_DIR, "kelas_kuliah.csv")
        df_j = self.load_csv(NEOSIA_FIXED_DIR, "jadwal_kuliah.csv")
        if df_j is None:
            df_j = self.load_csv(NEOSIA_FIXED_DIR, "jadwal_kuliah.csv")
        df_r = self.load_csv(NEOSIA_FIXED_DIR, "ruang.csv")

        if df_kk is not None:
            m_kk = df_kk.copy()
            if df_j is not None:
                m_kk = m_kk.merge(df_j, left_on='id', right_on='id_kelas_kuliah', suffixes=('', '_jad'), how='left')
            if df_r is not None:
                m_kk = m_kk.merge(df_r, left_on='id_ruang', right_on='id', suffixes=('', '_rg'), how='left')

            for i, r in m_kk.iterrows():
                mk_doc = self.mk_map.get(int(r['id_mata_kuliah'])) if 'id_mata_kuliah' in r and pd.notna(r['id_mata_kuliah']) else None
                kk_doc = {
                    "id_kelas_kuliah": int(r['id']),
                    "nama_kelas": str(r['nama'])[:150] if 'nama' in r and pd.notna(r['nama']) else "Unknown",
                    "jenis_pertemuan": str(r['id_jenis_pertemuan']) if 'id_jenis_pertemuan' in r and pd.notna(r['id_jenis_pertemuan']) else "Tatap Muka",
                    "ruang_nama": str(r['nama_ruang']) if 'nama_ruang' in r and pd.notna(r['nama_ruang']) else "Unknown",
                    "gedung": str(r['gedung']) if 'gedung' in r and pd.notna(r['gedung']) else "Unknown",
                    "kapasitas": int(r['kapasitas']) if 'kapasitas' in r and pd.notna(r['kapasitas']) else 40,
                    "hari": int(r['day_of_week']) if 'day_of_week' in r and pd.notna(r['day_of_week']) else 1,
                    "jam_mulai": str(r['start_time']) if 'start_time' in r and pd.notna(r['start_time']) else None,
                    "jam_selesai": str(r['end_time']) if 'end_time' in r and pd.notna(r['end_time']) else None,
                    "mata_kuliah": mk_doc
                }
                self.kelas_map[int(r['id'])] = kk_doc

        # 1.6 Semester
        df_sem = self.load_csv(NEOSIA_FIXED_DIR, "semester.csv")
        if df_sem is not None:
            for i, r in df_sem.iterrows():
                self.semester_map[int(r['id'])] = {
                    "id_semester": int(r['id']),
                    "tahun": int(r['tahun']),
                    "kode": str(r['kode'])
                }

        # 1.7 Tipe Kehadiran
        df_tk = self.load_csv(NEOSIA_FIXED_DIR, "tipe_kehadiran.csv")
        if df_tk is not None:
            for i, r in df_tk.iterrows():
                self.kehadiran_map[int(r['id'])] = {
                    "id_tipe_kehadiran": int(r['id']),
                    "nama": str(r['nama']),
                    "kode": str(r['kode'])
                }

        # 1.8 Pegawai Simpeg
        df_peg = self.load_csv(SIMPEG_FIXED_DIR, "data_pegawai.csv")
        if df_peg is None:
            df_peg = self.load_csv(SIMPEG_FIXED_DIR, "data_pegawai.csv")

        df_ag = self.load_csv(SIMPEG_FIXED_DIR, "ref_agama.csv")
        df_nk = self.load_csv(SIMPEG_FIXED_DIR, "ref_status_nikah.csv")

        if df_peg is not None:
            m_peg = df_peg.copy()
            if df_ag is not None and 'pegAgama' in m_peg.columns and 'agamaKode' in df_ag.columns:
                m_peg = m_peg.merge(df_ag, left_on='pegAgama', right_on='agamaKode', how='left')
            if df_nk is not None and 'pegStatusNikah' in m_peg.columns and 'nikahKode' in df_nk.columns:
                m_peg = m_peg.merge(df_nk, left_on='pegStatusNikah', right_on='nikahKode', how='left')

            id_col = 'id' if 'id' in m_peg.columns else ('pegId' if 'pegId' in m_peg.columns else m_peg.columns[0])
            for i, r in m_peg.iterrows():
                peg_doc = {
                    "id_pegawai": int(r[id_col]),
                    "nip": str(r['pegNip']) if 'pegNip' in r and pd.notna(r['pegNip']) else "Unknown",
                    "nama": str(r['nama']) if 'nama' in r and pd.notna(r['nama']) else ("Unknown" if 'pegNama' not in r else str(r['pegNama'])),
                    "jenis_kelamin": str(r['pegJenisKelamin']) if 'pegJenisKelamin' in r and pd.notna(r['pegJenisKelamin']) else "L",
                    "agama_nama": str(r['agamaNama']) if 'agamaNama' in r and pd.notna(r['agamaNama']) else "Islam",
                    "status_nikah_nama": str(r['nikahNama']) if 'nikahNama' in r and pd.notna(r['nikahNama']) else "Menikah",
                    "tempat_lahir": str(r['pegTempatLahir']) if 'pegTempatLahir' in r and pd.notna(r['pegTempatLahir']) else "Makassar",
                    "tanggal_lahir": str(r['pegTanggalLahir']) if 'pegTanggalLahir' in r and pd.notna(r['pegTanggalLahir']) else None,
                    "is_dosen": bool(r['pegIsDosen']) if 'pegIsDosen' in r and pd.notna(r['pegIsDosen']) else True
                }
                self.pegawai_map[int(r[id_col])] = peg_doc

        # Ref Tables
        df_gol = self.load_csv(SIMPEG_FIXED_DIR, "golongan.csv")
        if df_gol is not None:
            for i, r in df_gol.iterrows():
                self.golongan_map[str(r['golongan'])] = {
                    "golongan_kode": str(r['golongan']),
                    "pangkat": str(r['pangkat'])
                }

        df_jf = self.load_csv(SIMPEG_FIXED_DIR, "ref_jabatan_fungsional.csv")
        if df_jf is not None:
            for i, r in df_jf.iterrows():
                jf_id_col = 'jabfungrid' if 'jabfungrid' in r else ('jabfungrId' if 'jabfungrId' in r else r.keys()[0])
                jf_nama_col = 'jabfungrnama' if 'jabfungrnama' in r else ('jabfungrNama' if 'jabfungrNama' in r else r.keys()[1])
                self.jabfung_map[int(r[jf_id_col])] = {
                    "id_jabfung": int(r[jf_id_col]),
                    "nama": str(r[jf_nama_col]),
                    "tingkat": int(r['jabfungrtingkat']) if 'jabfungrtingkat' in r else 1,
                    "batas_usia_pensiun": int(r['jabfungrBatasUsiaPensiun']) if 'jabfungrBatasUsiaPensiun' in r else 65
                }

        df_js = self.load_csv(SIMPEG_FIXED_DIR, "ref_jabatan_struktural.csv")
        if df_js is not None:
            for i, r in df_js.iterrows():
                js_id_col = 'jstrukid' if 'jstrukid' in r else ('jstrukId' if 'jstrukId' in r else r.keys()[0])
                js_nama_col = 'jstruknama' if 'jstruknama' in r else ('jstrukNama' if 'jstrukNama' in r else r.keys()[1])
                self.jabstruk_map[int(r[js_id_col])] = {
                    "id_jabstruk": int(r[js_id_col]),
                    "nama": str(r[js_nama_col]),
                    "kelompok_nama": str(r['jstrukkodeunitkerja']) if 'jstrukkodeunitkerja' in r else "General",
                    "urutan": int(r['jstrukurutan']) if 'jstrukurutan' in r else 1
                }

        df_uk = self.load_csv(SIMPEG_FIXED_DIR, "ref_unit_kerja.csv")
        if df_uk is not None:
            for i, r in df_uk.iterrows():
                uk_id_col = 'unitkerjaid' if 'unitkerjaid' in r else ('unitkerjaId' if 'unitkerjaId' in r else r.keys()[0])
                uk_nama_col = 'unitkerjanama' if 'unitkerjanama' in r else ('unitkerjaNama' if 'unitkerjaNama' in r else r.keys()[1])
                self.unit_map[str(r[uk_id_col])] = {
                    "unit_kerja_id": str(r[uk_id_col]),
                    "nama": str(r[uk_nama_col]),
                    "induk_kerja_nama": str(r['unitkerjaindukkerjaid']) if 'unitkerjaindukkerjaid' in r else "Induk",
                    "eselon": int(r['unitkerjaeselonid']) if 'unitkerjaeselonid' in r else 1
                }

        df_jp = self.load_csv(SIMPEG_FIXED_DIR, "ref_jenis_pegawai.csv")
        if df_jp is not None:
            for i, r in df_jp.iterrows():
                jp_id_col = 'jnspegrid' if 'jnspegrid' in r else ('jnspegrId' if 'jnspegrId' in r else r.keys()[0])
                jp_nama_col = 'jnspegrnama' if 'jnspegrnama' in r else ('jnspegrNama' if 'jnspegrNama' in r else r.keys()[1])
                self.jenis_peg_map[int(r[jp_id_col])] = {
                    "id_jenis_pegawai": int(r[jp_id_col]),
                    "nama": str(r[jp_nama_col]),
                    "tipe_kontrak": str(r['jnspegrtipekontrak']) if 'jnspegrtipekontrak' in r else "Tetap"
                }

        df_sp = self.load_csv(SIMPEG_FIXED_DIR, "ref_status_pegawai.csv")
        if df_sp is not None:
            for i, r in df_sp.iterrows():
                sp_id_col = 'statid' if 'statid' in r else ('statId' if 'statId' in r else r.keys()[0])
                sp_nama_col = 'statnama' if 'statnama' in r else ('statNama' if 'statNama' in r else r.keys()[1])
                self.status_peg_map[int(r[sp_id_col])] = {
                    "id_status": int(r[sp_id_col]),
                    "nama": str(r[sp_nama_col])
                }

        df_pend = self.load_csv(SIMPEG_FIXED_DIR, "ref_pendidikan.csv")
        if df_pend is not None:
            for i, r in df_pend.iterrows():
                pend_id_col = 'pendid' if 'pendid' in r else ('pendId' if 'pendId' in r else r.keys()[0])
                pend_nama_col = 'pendnama' if 'pendnama' in r else ('pendNama' if 'pendNama' in r else r.keys()[1])
                self.pendidikan_map[int(r[pend_id_col])] = {
                    "id_pendidikan": int(r[pend_id_col]),
                    "nama": str(r[pend_nama_col])
                }

        # ======================================================================
        # STEP 2: PROSES DAN EMBED TABEL FAKTA AKADEMIK (NEOSIA) WITH DEDUPLICATION
        # ======================================================================
        log("2. Memproses dan membungkus data fakta Akademik (KRS, Presensi, PA) dengan deduplikasi...")

        # 2.1 KRS (Embedded Document krs)
        # --- kartu_rencana_studi & kelas_kuliah_koordinator relatif lebih kecil -> load penuh ---
        df_krs = self.load_csv(NEOSIA_SCALED_DIR, "kartu_rencana_studi_SF0.5.csv")
        df_kkc = self.load_csv(NEOSIA_FIXED_DIR, "kelas_kuliah_koordinator.csv")
        df_kkc_clean = (df_kkc.drop_duplicates(subset=['id_kelas_kuliah'])
                        if df_kkc is not None and 'id_kelas_kuliah' in df_kkc.columns else df_kkc)

        # Tabel referensi tipe bobot nilai (KECIL) -> id -> nama_singkat
        # (uas, presensi, kuis, uh, uts). SESUAIKAN nama file jika berbeda.
        df_bobot = self.load_csv(NEOSIA_FIXED_DIR, "tipe_bobot_nilai.csv")
        bobot_label_map = (dict(zip(df_bobot['id'], df_bobot['nama_singkat']))
                            if df_bobot is not None else {})

        # Tabel persentase bobot per kelas kuliah (KECIL) -> untuk hitung nilai akhir.
        # SESUAIKAN nama file jika berbeda.
        df_bnkk = self.load_csv(NEOSIA_FIXED_DIR, "bobot_nilai_kelas_kuliah.csv")
        if df_bnkk is not None and bobot_label_map:
            df_bnkk = df_bnkk.copy()
            df_bnkk['komponen'] = df_bnkk['id_tipe_bobot_nilai'].map(bobot_label_map)
            df_persentase = (
                df_bnkk.dropna(subset=['komponen'])
                .pivot_table(index='id_kelas_kuliah', columns='komponen',
                            values='persentase', aggfunc='first')
                .reset_index()
            )
            df_persentase.columns = ['id_kelas_kuliah'] + [
                f'persentase_{c}' for c in df_persentase.columns[1:]
            ]

            # FIX: id_bobot_nilai_kelas_kuliah di kelas_kuliah_nilai_SF1 adalah FK ke
            # bobot_nilai_kelas_kuliah.id (PK per kelas+komponen, mis. 1..5, 6..10, ...,
            # 209711..209715) -- BUKAN ke tipe_bobot_nilai.id (cuma 1-5). Sebelumnya
            # kode ini pakai bobot_label_map langsung, jadi hampir semua baris nilai
            # gagal mapping (fallback ke 0) kecuali kelas yang PK bobotnya kebetulan
            # berimpit dengan range 1-5. Perbaikan: mapping dua langkah lewat id
            # (PK bobot_nilai_kelas_kuliah) -> nama_singkat komponen.
            bnkk_to_komponen_map = dict(zip(df_bnkk['id'], df_bnkk['komponen']))
        else:
            df_persentase = None
            bnkk_to_komponen_map = {}

        m = None
        if df_krs is not None:
            # kelas_kuliah_nilai_SF1.csv: format LONG (id_peserta_kelas, id_bobot,
            # nilai) dan RAKSASA -> pivot streaming per komponen (uas/uts/presensi/
            # kuis/uh) berdasarkan id_peserta_kelas. SESUAIKAN category_col jika nama
            # kolom id tipe bobot di file ini bukan 'id_bobot_nilai_kelas_kuliah'.
            nilai_pivot_maps = self.build_pivoted_value_maps_streaming(
                folder=NEOSIA_SCALED_DIR,
                filename="kelas_kuliah_nilai_SF0.5.csv",
                key_col='id_peserta_kelas',
                category_col='id_bobot_nilai_kelas_kuliah',
                value_col='nilai',
                category_label_map=bnkk_to_komponen_map,  # FIX: bukan bobot_label_map
                label="kelas_kuliah_nilai",
            )

            column_maps = {
                f'nilai_{komponen}': ('id', peta)
                for komponen, peta in nilai_pivot_maps.items()
            }

            # kelas_kuliah_has_mahasiswa_SF1.csv adalah file dasar TERBESAR ->
            # dibaca per-chunk, sambil mempertahankan kandidat "N data terbaru"
            # (berdasarkan created_at). extra_merges untuk join ke koordinator
            # (kecil), column_maps untuk menempelkan nilai per komponen dari pivot
            # streaming di atas.
            m = self.load_and_topn_streaming(
                folder=NEOSIA_SCALED_DIR,
                filename="kelas_kuliah_has_mahasiswa_SF0.5.csv",
                date_lookup_df=df_krs,
                left_on='id_kartu_rencana_studi',
                right_on='id',
                date_col='created_at',
                n=MAX_FACT_ROWS,
                chunksize=1_000_000,
                tiebreak_col='id',  # PK kelas_kuliah_has_mahasiswa (unsuffixed karena suffixes=('', '_krs'))
                label="KRS",
                extra_merges=(
                    [{'df': df_kkc_clean, 'left_on': 'id_kelas_kuliah', 'right_on': 'id_kelas_kuliah',
                    'how': 'left', 'suffixes': ('', '_koor')}]
                    if df_kkc_clean is not None else None
                ),
                column_maps=column_maps,
            )

            # Setelah m ada id_kelas_kuliah -> tempelkan persentase bobot per kelas,
            # lalu hitung nilai akhir tertimbang:
            #   nilai_akhir = sum(nilai_komponen * persentase_komponen) / 100
            if m is not None and not m.empty and df_persentase is not None:
                m = m.merge(df_persentase, on='id_kelas_kuliah', how='left', suffixes=('', '_persen'))

                komponen_list = ['uas', 'uts', 'presensi', 'kuis', 'uh']
                for komponen in komponen_list:
                    col_nilai = f'nilai_{komponen}'
                    col_persen = f'persentase_{komponen}'
                    if col_nilai not in m.columns:
                        m[col_nilai] = 0.0
                    if col_persen not in m.columns:
                        m[col_persen] = 0.0
                    m[col_nilai] = m[col_nilai].fillna(0.0)
                    m[col_persen] = m[col_persen].fillna(0.0)

                # Hitung nilai akhir secara vektor dan bulatkan secara eksplisit
                # ke 2 angka desimal dengan aturan half-up sebelum dikirim ke DB.
                # Ini mencegah nilai float seperti 68.24799999999999 dikirim ke
                # kolom Decimal ClickHouse lalu terpotong menjadi 68.24.
                nilai_akhir_raw = sum(
                    m[f'nilai_{k}'] * m[f'persentase_{k}'] / 100.0
                    for k in komponen_list
                )
                m['nilai_akhir_hitung'] = np.where(
                    nilai_akhir_raw >= 0,
                    np.floor(nilai_akhir_raw * 100.0 + 0.500000001),
                    np.ceil(nilai_akhir_raw * 100.0 - 0.500000001)
                ) / 100.0

            del df_krs, df_kkc, df_kkc_clean, df_bobot, df_bnkk, df_persentase, bnkk_to_komponen_map, nilai_pivot_maps
            gc.collect()

            # FIX FK KRS: turunkan id_prodi dan id_semester melalui prodi_semester.
            # kartu_rencana_studi hanya menyimpan id_prodi_semester, bukan id_prodi.
            df_ps = self.load_csv(NEOSIA_FIXED_DIR, "prodi_semester.csv")
            if df_ps is not None and 'id_prodi_semester' in m.columns:
                ps_cols = [c for c in ['id', 'id_prodi', 'id_semester'] if c in df_ps.columns]
                if 'id' in ps_cols and 'id_prodi' in ps_cols and 'id_semester' in ps_cols:
                    df_ps_lookup = df_ps[ps_cols].drop_duplicates(subset=['id']).rename(columns={
                        'id': '_lookup_id_prodi_semester',
                        'id_prodi': '_lookup_id_prodi',
                        'id_semester': '_lookup_id_semester'
                    })
                    m = m.merge(
                        df_ps_lookup,
                        left_on='id_prodi_semester',
                        right_on='_lookup_id_prodi_semester',
                        how='left'
                    )
                    if 'id_prodi' not in m.columns:
                        m['id_prodi'] = m['_lookup_id_prodi']
                    else:
                        m['id_prodi'] = m['id_prodi'].where(m['id_prodi'].notna(), m['_lookup_id_prodi'])
                    if 'id_semester' not in m.columns:
                        m['id_semester'] = m['_lookup_id_semester']
                    else:
                        m['id_semester'] = m['id_semester'].where(m['id_semester'].notna(), m['_lookup_id_semester'])
                    m.drop(columns=['_lookup_id_prodi_semester', '_lookup_id_prodi', '_lookup_id_semester'], inplace=True)

        if m is not None and not m.empty:
            log(f"-> KRS terekstraksi {len(m):,} baris. Memproses dokumen KRS...")
            chunk_size = 50000
            total_rows = len(m)
            out_path = os.path.join(JSON_OUTPUT_DIR, "krs.json")
            if os.path.exists(out_path):
                os.remove(out_path)

            col_cleared = False
            for start_idx in range(0, total_rows, chunk_size):
                m_chunk = m.iloc[start_idx : start_idx + chunk_size]
                records = m_chunk.to_dict('records')
                chunk_docs = []
                for idx_in_chunk, r in enumerate(records):
                    global_idx = start_idx + idx_in_chunk

                    # CATATAN: tabel bobot referensi tidak punya komponen "tugas"
                    # (hanya: uas, presensi, kuis, uh, uts). nilai_tugas diambil
                    # dari komponen 'kuis'. Ganti ke nilai_uh jika "tugas" sebenarnya
                    # komponen lain.
                    doc = {
                        "krs_key": global_idx + 1,
                        "nilai_tugas": float(r['nilai_kuis']) if 'nilai_kuis' in r and pd.notna(r['nilai_kuis']) else 0.0,
                        "nilai_uts": float(r['nilai_uts']) if 'nilai_uts' in r and pd.notna(r['nilai_uts']) else 0.0,
                        "nilai_uas": float(r['nilai_uas']) if 'nilai_uas' in r and pd.notna(r['nilai_uas']) else 0.0,
                        "nilai_akhir": float(r['nilai_akhir_hitung']) if 'nilai_akhir_hitung' in r and pd.notna(r['nilai_akhir_hitung']) else 0.0,
                        "is_approved": bool(r['is_approved']) if 'is_approved' in r and pd.notna(r['is_approved']) else True,
                        "is_current": bool(r['is_current']) if 'is_current' in r and pd.notna(r['is_current']) else True,
                        "is_locked": bool(r['is_locked']) if 'is_locked' in r and pd.notna(r['is_locked']) else True,
                        "created_at": str(r['created_at']) if 'created_at' in r and pd.notna(r['created_at']) else None,
                        "mahasiswa": self.mhs_map.get(int(r['id_mahasiswa']), None) if 'id_mahasiswa' in r and pd.notna(r['id_mahasiswa']) else None,
                        "kelas_kuliah": self.kelas_map.get(int(r['id_kelas_kuliah']), None) if 'id_kelas_kuliah' in r and pd.notna(r['id_kelas_kuliah']) else None,
                        "semester": self.semester_map.get(int(r['id_semester']), None) if 'id_semester' in r and pd.notna(r['id_semester']) else None,
                        "prodi": self.prodi_map.get(int(r['id_prodi']), None) if 'id_prodi' in r and pd.notna(r['id_prodi']) else None,
                        "dosen_koordinator": self.dosen_map.get(int(r['id_dosen']), None) if 'id_dosen' in r and pd.notna(r['id_dosen']) else None
                    }
                    chunk_docs.append(doc)

                with open(out_path, 'a', encoding='utf-8') as f:
                    for d in chunk_docs:
                        f.write(json.dumps(d, cls=MongoJSONEncoder) + '\n')

                self._import_chunk_to_mongodb("krs", chunk_docs, drop_first=(not col_cleared))
                col_cleared = True
                del m_chunk, chunk_docs, records
                gc.collect()

            del m
            gc.collect()
            log(f"-> Selesai memuat {total_rows:,} dokumen KRS.")

        # 2.2 Presensi (Embedded Document presensi)
        # pertemuan.csv relatif kecil (bukan file scaled) -> aman di-load penuh
        df_p = self.load_csv(NEOSIA_FIXED_DIR, "pertemuan.csv")
        pm_filename = "presensi_mahasiswa_SF0.5.csv"
        if not os.path.exists(os.path.join(NEOSIA_SCALED_DIR, pm_filename)):
            pm_filename = f"presensi_mahasiswa_{SCALE_FACTOR}.csv"

        df_p_clean = (df_p.drop_duplicates(subset=['id']) if 'id' in df_p.columns else df_p) if df_p is not None else None

        m = None
        if df_p_clean is not None:
            # --- FILE PRESENSI BISA RATUSAN JUTA BARIS -> JANGAN LOAD PENUH ---
            # Dibaca per-chunk sambil langsung mempertahankan kandidat "N data
            # terbaru" (berdasarkan tanggal_rencana) di memori. Mencegah error
            # "out of memory" tanpa mengubah hasil akhir: 3.000.000 baris yang
            # dipilih tetap identik dengan yang dimuat ke ClickHouse/PostgreSQL,
            # selama tiebreak_col ('id' presensi) sama di ketiga skrip.
            m = self.load_and_topn_streaming(
                folder=NEOSIA_SCALED_DIR,
                filename=pm_filename,
                date_lookup_df=df_p_clean,
                left_on='id_pertemuan',
                right_on='id',
                date_col='tanggal_rencana',
                n=MAX_FACT_ROWS,
                chunksize=1_000_000,
                tiebreak_col='id',  # kolom PK presensi_mahasiswa; sesuaikan jika nama aslinya berbeda
                label="Presensi",
            )
            del df_p, df_p_clean
            gc.collect()

        if m is not None and not m.empty:
            log(f"-> Presensi terekstraksi {len(m):,} baris. Memproses dokumen presensi...")

            chunk_size = 50000
            total_rows = len(m)
            out_path = os.path.join(JSON_OUTPUT_DIR, "presensi.json")
            if os.path.exists(out_path):
                os.remove(out_path)

            col_cleared = False
            for start_idx in range(0, total_rows, chunk_size):
                m_chunk = m.iloc[start_idx : start_idx + chunk_size]
                records = m_chunk.to_dict('records')
                chunk_docs = []
                for idx_in_chunk, r in enumerate(records):
                    global_idx = start_idx + idx_in_chunk
                    doc = {
                        "presensi_key": global_idx + 1,
                        "pertemuan_ke": int(r['pertemuan_ke']) if 'pertemuan_ke' in r and pd.notna(r['pertemuan_ke']) else 1,
                        "tanggal_pertemuan": str(r['tanggal_rencana']) if 'tanggal_rencana' in r and pd.notna(r['tanggal_rencana']) else None,
                        "hadir_flag": bool(r['id_tipe_kehadiran'] == 1) if 'id_tipe_kehadiran' in r and pd.notna(r['id_tipe_kehadiran']) else False,
                        "mahasiswa": self.mhs_map.get(int(r['id_mahasiswa']), None) if 'id_mahasiswa' in r and pd.notna(r['id_mahasiswa']) else None,
                        "kelas_kuliah": self.kelas_map.get(int(r['id_kelas_kuliah']), None) if 'id_kelas_kuliah' in r and pd.notna(r['id_kelas_kuliah']) else None,
                        "tipe_kehadiran": self.kehadiran_map.get(int(r['id_tipe_kehadiran']), None) if 'id_tipe_kehadiran' in r and pd.notna(r['id_tipe_kehadiran']) else None
                    }
                    chunk_docs.append(doc)

                with open(out_path, 'a', encoding='utf-8') as f:
                    for d in chunk_docs:
                        f.write(json.dumps(d, cls=MongoJSONEncoder) + '\n')

                self._import_chunk_to_mongodb("presensi", chunk_docs, drop_first=(not col_cleared))
                col_cleared = True
                del m_chunk, chunk_docs, records
                gc.collect()

            del m
            gc.collect()
            log(f"-> Selesai memuat {total_rows:,} dokumen presensi.")

        # 2.3 Penasihat Akademik
        df_pa = self.load_csv(NEOSIA_SCALED_DIR, "penasihat_akademik_SF1.csv")

        pa_docs = []
        if df_pa is not None:
            # --- FILTER N DATA TERBARU ---
            df_pa = self.limit_latest_rows(df_pa, date_col=None, n=MAX_FACT_ROWS, label="Penasihat Akademik")

            for i, r in df_pa.iterrows():
                doc = {
                    "pa_key": i + 1,
                    "is_active": bool(r['is_active']) if 'is_active' in r and pd.notna(r['is_active']) else True,
                    "mahasiswa": self.mhs_map.get(int(r['id_mahasiswa']), None) if 'id_mahasiswa' in r and pd.notna(r['id_mahasiswa']) else None,
                    "dosen": self.dosen_map.get(int(r['id_dosen']), None) if 'id_dosen' in r and pd.notna(r['id_dosen']) else None,
                    "semester": self.semester_map.get(int(r['id_prodi_semester']), None) if 'id_prodi_semester' in r and pd.notna(r['id_prodi_semester']) else None
                }
                pa_docs.append(doc)

            self.save_to_json_lines(pa_docs, "penasihat_akademik.json")
            self.import_to_mongodb_direct("penasihat_akademik", pa_docs)
            del pa_docs, df_pa
            gc.collect()

        # ======================================================================
        # STEP 3: PROSES DAN EMBED TABEL FAKTA KEPEGAWAIAN (SIMPEG)
        # ======================================================================
        log("3. Memproses dan membungkus data riwayat Simpeg...")

        def process_riwayat_collection(csv_name, mapping_key, map_dict, target_col, extra_fields_fn, date_col=None):
            df_rw = self.load_csv(SIMPEG_SCALED_DIR, csv_name)
            if df_rw is None:
                alt_name = csv_name.replace(f"_{SCALE_FACTOR}.csv", ".csv")
                df_rw = self.load_csv(SIMPEG_SCALED_DIR, alt_name)

            docs = []
            if df_rw is not None:
                # --- FILTER N DATA TERBARU ---
                df_rw = self.limit_latest_rows(df_rw, date_col=date_col, n=MAX_FACT_ROWS, label=f"riwayat_{target_col}")

                records = df_rw.to_dict('records')
                for i, r in enumerate(records):
                    peg_cols = [k for k in r.keys() if 'PegId' in k or 'PegawaiId' in k or 'pegId' in k]
                    peg_id = int(r[peg_cols[0]]) if peg_cols and pd.notna(r[peg_cols[0]]) else (int(r[list(r.keys())[1]]) if len(r.keys()) > 1 and pd.notna(r[list(r.keys())[1]]) else None)
                    peg_doc = self.pegawai_map.get(peg_id, None) if peg_id else None

                    val_key = r.get(mapping_key, None)
                    dim_doc = None
                    if pd.notna(val_key):
                        dim_doc = map_dict.get(str(val_key) if isinstance(val_key, str) else int(val_key) if isinstance(val_key, (int, float, np.integer)) else None, None)
                        if dim_doc is None:
                            dim_doc = map_dict.get(val_key, None)

                    doc = {
                        f"riwayat_{target_col}_key": i + 1,
                        "pegawai": peg_doc,
                        target_col: dim_doc
                    }
                    doc.update(extra_fields_fn(r))
                    docs.append(doc)

                self.save_to_json_lines(docs, f"riwayat_{target_col}.json")
                self.import_to_mongodb_direct(f"riwayat_{target_col}", docs)
                del docs, df_rw
                gc.collect()

        process_riwayat_collection(
            "riwayat_golongan_SF1.csv", "rgGolongan", self.golongan_map, "golongan",
            lambda r: {
                "tmt_golongan": str(r["rgTMTGolongan"]) if 'rgTMTGolongan' in r and pd.notna(r["rgTMTGolongan"]) else None,
                "no_sk": str(r["rgNoSK"]) if 'rgNoSK' in r and pd.notna(r["rgNoSK"]) else None,
                "tgl_sk": str(r["rgTglSK"]) if 'rgTglSK' in r and pd.notna(r["rgTglSK"]) else None,
                "is_aktif": bool(r["rgAktif"]) if 'rgAktif' in r and pd.notna(r["rgAktif"]) else True
            },
            date_col="rgTMTGolongan"
        )

        process_riwayat_collection(
            "riwayat_jabatan_fungsional_SF1.csv", "rjfJabatanId", self.jabfung_map, "jabatan_fungsional",
            lambda r: {
                "tmt_jabatan": str(r["rjfTMTJabatan"]) if 'rjfTMTJabatan' in r and pd.notna(r["rjfTMTJabatan"]) else None,
                "angka_kredit": float(r["rjfAngkaKredit"]) if 'rjfAngkaKredit' in r and pd.notna(r["rjfAngkaKredit"]) else 0.0,
                "is_aktif": bool(r["rjfAktif"]) if 'rjfAktif' in r and pd.notna(r["rjfAktif"]) else True
            },
            date_col="rjfTMTJabatan"
        )

        process_riwayat_collection(
            "riwayat_unit_kerja_SF1.csv", "rwyUKunitkerjaId", self.unit_map, "unit_kerja",
            lambda r: {
                "tmt": str(r["rwyUKTMT"]) if 'rwyUKTMT' in r and pd.notna(r["rwyUKTMT"]) else None,
                "no_sk": str(r["rwyUKNoSK"]) if 'rwyUKNoSK' in r and pd.notna(r["rwyUKNoSK"]) else None,
                "is_aktif": bool(r["rwyUKAktif"]) if 'rwyUKAktif' in r and pd.notna(r["rwyUKAktif"]) else True
            },
            date_col="rwyUKTMT"
        )

        process_riwayat_collection(
            "riwayat_jenis_pegawai_SF1.csv", "rjpegJnspegrId", self.jenis_peg_map, "jenis_pegawai",
            lambda r: {
                "no_sk": str(r["rjpegNoSK"]) if 'rjpegNoSK' in r and pd.notna(r["rjpegNoSK"]) else None,
                "tgl_sk": str(r["rjpegTglSK"]) if 'rjpegTglSK' in r and pd.notna(r["rjpegTglSK"]) else None,
                "is_aktif": bool(r["rjpegAktif"]) if 'rjpegAktif' in r and pd.notna(r["rjpegAktif"]) else True
            },
            date_col="rjpegTglSK"
        )

        process_riwayat_collection(
            "riwayat_status_pegawai_SF1.csv", "rwystatStatId", self.status_peg_map, "status_pegawai",
            lambda r: {
                "no_sk": str(r["rwystatNoSK"]) if 'rwystatNoSK' in r and pd.notna(r["rwystatNoSK"]) else None,
                "tgl_sk": str(r["rwystatTglSK"]) if 'rwystatTglSK' in r and pd.notna(r["rwystatTglSK"]) else None,
                "is_aktif": bool(r["rwystatAktif"]) if 'rwystatAktif' in r and pd.notna(r["rwystatAktif"]) else True
            },
            date_col="rwystatTglSK"
        )

        df_rps = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_program_studi_SF1.csv")

        rps_docs = []
        if df_rps is not None:
            # --- FILTER N DATA TERBARU ---
            df_rps = self.limit_latest_rows(df_rps, date_col="rwyprodiTMT", n=MAX_FACT_ROWS, label="riwayat_prodi")

            for i, r in df_rps.iterrows():
                peg_doc = self.pegawai_map.get(int(r['rwyprodiPegId']), None) if 'rwyprodiPegId' in r and pd.notna(r['rwyprodiPegId']) else None
                prodi_id_sim = r.get('rwyprodiProdiKode', None)
                p_doc = self.prodi_map.get(f"simpeg_{int(prodi_id_sim)}", None) if pd.notna(prodi_id_sim) else None

                doc = {
                    "riwayat_prodi_key": i + 1,
                    "pegawai": peg_doc,
                    "prodi": p_doc,
                    "tmt": str(r["rwyprodiTMT"]) if 'rwyprodiTMT' in r and pd.notna(r["rwyprodiTMT"]) else None,
                    "no_sk": str(r["rwyprodiNoSK"]) if 'rwyprodiNoSK' in r and pd.notna(r["rwyprodiNoSK"]) else None,
                    "is_aktif": bool(r["rwyprodiAktif"]) if 'rwyprodiAktif' in r and pd.notna(r["rwyprodiAktif"]) else True
                }
                rps_docs.append(doc)

            self.save_to_json_lines(rps_docs, "riwayat_prodi.json")
            self.import_to_mongodb_direct("riwayat_prodi", rps_docs)
            del rps_docs, df_rps
            gc.collect()

        process_riwayat_collection(
            "riwayat_mutasi_struktural_SF1.csv", "rmsJstrukId", self.jabstruk_map, "jabatan_struktural",
            lambda r: {
                "tanggal_mulai": str(r["rmsTanggalMulai"]) if 'rmsTanggalMulai' in r and pd.notna(r["rmsTanggalMulai"]) else None,
                "tanggal_selesai": str(r["rmsTanggalSelesai"]) if 'rmsTanggalSelesai' in r and pd.notna(r["rmsTanggalSelesai"]) else None,
                "is_jbt_aktif": bool(r["rmsJbtAktif"]) if 'rmsJbtAktif' in r and pd.notna(r["rmsJbtAktif"]) else True,
                "status_pejabat": str(r["rmsStatusPejabat"]) if 'rmsStatusPejabat' in r and pd.notna(r["rmsStatusPejabat"]) else "Definitif"
            },
            date_col="rmsTanggalMulai"
        )

        process_riwayat_collection(
            "riwayat_pendidikan_SF1.csv", "rwpendJenjang", self.pendidikan_map, "pendidikan",
            lambda r: {
                "nama_sekolah": str(r["rwpendNamaSekolah"]) if 'rwpendNamaSekolah' in r and pd.notna(r["rwpendNamaSekolah"]) else "Unknown",
                "jurusan": str(r["rwpendJurusan"]) if 'rwpendJurusan' in r and pd.notna(r["rwpendJurusan"]) else "Unknown",
                "tahun_masuk": str(r["rwpendThnMasuk"]) if 'rwpendThnMasuk' in r and pd.notna(r["rwpendThnMasuk"]) else None,
                "tahun_lulus": str(r["rwpendThnLulus"]) if 'rwpendThnLulus' in r and pd.notna(r["rwpendThnLulus"]) else None,
                "negara_nama": str(r["rwpendNegara"]) if 'rwpendNegara' in r and pd.notna(r["rwpendNegara"]) else "Indonesia",
                "is_terakhir": bool(r["rwpendTerakhir"]) if 'rwpendTerakhir' in r and pd.notna(r["rwpendTerakhir"]) else True
            },
            date_col="rwpendThnMasuk"
        )

        log("===============================================================")
        log("PROSES ETL MONGODB EMBEDDED SCHEMA SELESAI DENGAN SUKSES!")
        log(f"Berkas JSON Lines tersimpan di folder: {JSON_OUTPUT_DIR}")
        log("===============================================================")

if __name__ == "__main__":
    loader = MongoETLLoader()
    loader.run()
