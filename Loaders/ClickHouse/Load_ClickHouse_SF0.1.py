import os
import sys
import io
import gc
import pandas as pd
import numpy as np
import requests
import warnings
warnings.filterwarnings("ignore")

# ==============================================================================
# 1. KONFIGURASI DATABASE & SKALA BEBAN KERJA
# ==============================================================================
SCALE_FACTOR = "SF0.1"  # Cukup ubah ke 'SF0.1' atau 'SF0.1' untuk script lainnya!

# Batas maksimum jumlah baris yang dimuat untuk SETIAP tabel fakta (fact table).
# Jika data hasil transformasi melebihi angka ini, hanya N baris TERBARU
# (berdasarkan kolom tanggal yang relevan bila tersedia) yang akan dimuat ke ClickHouse.
MAX_ROWS_PER_LOAD = 600000

DB_CONFIG = {
    'host': 'localhost',          # Host database ClickHouse
    'port': '8124',               # Port HTTP interface ClickHouse
    'user': 'default',            # User default ClickHouse
    'password': 'clickhouse123',               # Password jika ada
    'database': 'default'      # Nama database target
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

def log(message):
    print(f"[ETL CLICKHOUSE - {SCALE_FACTOR}] {message}")

def log_error(message):
    print(f"[ETL CLICKHOUSE ERROR - {SCALE_FACTOR}] {message}", file=sys.stderr)

# ==============================================================================
# 3. KELAS UTAMA ETL PROSESOR CLICKHOUSE
# ==============================================================================
class ClickHouseETLLoader:
    def __init__(self):
        # Map untuk melacak Surrogate Keys
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

    def load_csv(self, folder, filename):
        path = os.path.join(folder, filename)
        if not os.path.exists(path):
            log(f"Peringatan: File {filename} tidak ditemukan di {folder}. Membuat data tiruan kosong.")
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

    def apply_row_limit(self, df, date_column=None, n=MAX_ROWS_PER_LOAD, label=""):
        """
        Membatasi jumlah baris suatu DataFrame maksimal N baris sebelum dimuat.
        Pola ini diadaptasi dari filter 'N data terbaru' pada fact_presensi:
        - Jika `date_column` tersedia di DataFrame, data diurutkan dahulu
          berdasarkan tanggal TERBARU (descending) sebelum dipotong.
        - Jika tidak ada kolom tanggal yang relevan, data langsung dipotong
          menjadi N baris pertama (tanpa pengurutan).
        """
        if df is None or df.empty or n is None:
            # n=None berarti tidak ada batas jumlah baris untuk DataFrame ini.
            return df

        total_rows = len(df)
        if total_rows <= n:
            return df

        if date_column and date_column in df.columns:
            df = df.copy()
            df['_sort_dt'] = pd.to_datetime(df[date_column], errors='coerce')
            df = df.sort_values(by='_sort_dt', ascending=False).drop(columns=['_sort_dt'])

        df = df.head(n)
        log(f"Membatasi data {label}: {total_rows:,} baris -> {len(df):,} baris (N={n:,}).")
        return df

    def load_and_topn_streaming(self, folder, filename, date_lookup_df, left_on, right_on,
                                 date_col, n=MAX_ROWS_PER_LOAD, chunksize=1_000_000,
                                 tiebreak_col='id', usecols=None, label="",
                                 extra_merges=None, column_maps=None):
        """
        Versi 'streaming' dari load_csv() + merge() + apply_row_limit() yang dirancang
        khusus untuk file CSV RAKSASA (puluhan-ratusan juta baris) yang TIDAK MUNGKIN
        dimuat sekaligus ke RAM (inilah penyebab error 'Error tokenizing data:
        out of memory' pada file presensi_mahasiswa_SF0.1.csv).

        Alih-alih:
            df = pd.read_csv(path)              # <- memuat SEMUA baris ke RAM
            df = df.merge(lookup, ...)
            df = apply_row_limit(df, ...)        # <- sort seluruh 250 juta baris

        Fungsi ini membaca file per-chunk (mis. 1 juta baris per chunk), langsung
        menggabungkannya dengan tabel lookup kecil (mis. pertemuan.csv), lalu
        menjaga sebuah "buffer" yang hanya berisi kandidat TOP-N (baris paling
        baru berdasarkan date_col) yang pernah dilihat sejauh ini. Setelah tiap
        chunk diproses, buffer dipangkas kembali ke N baris. Dengan begitu,
        memori yang dipakai maksimal hanya sekitar (chunksize + n) baris,
        BUKAN seluruh isi file.

        PENTING - KONSISTENSI ANTAR DATABASE (ClickHouse / PostgreSQL / MongoDB):
        Hasil akhir dari algoritma ini SELALU tepat sama dengan hasil
        `sort_values(date_col, ascending=False).head(n)` yang dilakukan pada
        seluruh data sekaligus - tidak peduli berapa pun ukuran chunksize yang
        dipakai. Ini karena di setiap langkah kita mempertahankan kandidat
        TOP-N terbaik yang pernah terlihat, bukan sekadar top-N dari tiap
        chunk. Jadi selama ketiga skrip loader (ClickHouse/Postgres/MongoDB):
          1) membaca sumber CSV mentah yang sama persis,
          2) menggunakan `date_col` yang sama sebagai kunci urutan, dan
          3) menggunakan `tiebreak_col` yang sama untuk memecah baris dengan
             tanggal yang identik (WAJIB diisi kolom unik, misalnya primary key
             'id' dari presensi_mahasiswa, supaya urutan tidak ambigu),
        maka ketiga database akan memuat 3.000.000 baris IDENTIK, walaupun
        masing-masing skrip memakai chunksize yang berbeda.

        Parameter:
        - date_lookup_df: DataFrame kecil (mis. pertemuan.csv, sudah di-load penuh)
          yang akan di-merge ke tiap chunk untuk mendapatkan date_col.
        - left_on/right_on: kunci join antara chunk (file besar) dan date_lookup_df.
        - date_col: nama kolom tanggal (ada di hasil merge) yang dipakai untuk
          menentukan "N data terbaru".
        - tiebreak_col: kolom unik (biasanya primary key) pada file besar, dipakai
          sebagai kunci urutan kedua supaya hasil deterministik saat tanggal sama.
        - extra_merges: opsional, list of dict untuk merge tambahan yang dilakukan
          PER CHUNK setelah merge utama, mis. join ke tabel lookup kecil lain.
          Setiap item: {'df': lookup_df, 'left_on': ..., 'right_on': ...,
          'how': 'left' (default), 'suffixes': (..., ...)}.
          Cocok untuk lookup yang MASIH MUAT di RAM (tabel kecil/menengah).
        - column_maps: opsional, dict {nama_kolom_baru: (kolom_kunci_di_chunk, dict_lookup)}
          untuk menempelkan nilai dari sebuah dict Python (bukan DataFrame) ke tiap
          chunk via .map(). Dipakai saat tabel lookup-nya SENDIRI terlalu besar untuk
          di-load sebagai DataFrame utuh -> lihat build_column_maps_streaming(), yang
          membangun dict tersebut dengan cara streaming juga (hanya menyimpan kolom
          yang benar-benar dibutuhkan, bukan seluruh baris/kolom asli).
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
            buffer = buffer.drop(columns=['_sort_dt'])
            log(f"Streaming filter {label}: {total_seen:,} baris dibaca -> {len(buffer):,} baris terbaru diambil (N={n:,}).")
        return buffer

    def build_column_maps_streaming(self, folder, filename, key_col, value_cols,
                                     chunksize=1_000_000, label=""):
        """
        Membangun lookup Python dict (bukan DataFrame) dari sebuah file CSV RAKSASA,
        tanpa pernah memuat seluruh file itu ke RAM sekaligus.

        Dipakai untuk kasus file yang punya kolom nilai TETAP (fixed columns) per
        baris, dan file itu terlalu besar untuk di-load penuh (`pd.read_csv` biasa)
        hanya demi di-merge (LEFT JOIN) ke tabel lain. Solusinya: baca per-chunk, dan
        untuk tiap chunk hanya simpan kolom kunci (key_col) + kolom nilai yang benar-
        benar dipakai (value_cols) ke dalam dict Python biasa. Dict inilah yang
        nantinya ditempelkan ke data lain lewat .map(), jauh lebih hemat memori
        dibanding menyimpan DataFrame lengkap + melakukan `.merge()`.

        Return: dict {value_col: {key: value}} - satu dict lookup per kolom nilai,
        supaya bisa langsung dipakai di parameter `column_maps` pada
        load_and_topn_streaming().
        """
        path = os.path.join(folder, filename)
        maps = {c: {} for c in value_cols}
        if not os.path.exists(path):
            log(f"Peringatan: File {filename} tidak ditemukan di {folder}. Lookup {label} kosong.")
            return maps

        usecols = [key_col] + [c for c in value_cols if c != key_col]
        read_kwargs = dict(chunksize=chunksize, low_memory=False, usecols=usecols)
        try:
            reader = pd.read_csv(path, **read_kwargs)
        except Exception:
            read_kwargs['encoding'] = 'utf-8-sig'
            reader = pd.read_csv(path, **read_kwargs)

        total_seen = 0
        for chunk in reader:
            chunk.columns = chunk.columns.astype(str).str.strip().str.replace('\ufeff', '')
            chunk = chunk.drop_duplicates(subset=[key_col], keep='last')
            total_seen += len(chunk)
            keys = chunk[key_col]
            for c in value_cols:
                maps[c].update(dict(zip(keys, chunk[c])))
            del chunk
            gc.collect()

        n_keys = len(maps[value_cols[0]]) if value_cols else 0
        log(f"Membangun lookup streaming {label}: {total_seen:,} baris dibaca -> {n_keys:,} kunci unik disimpan.")
        return maps

    def build_pivoted_value_maps_streaming(self, folder, filename, key_col, category_col,
                                            value_col, category_label_map, label="pivot",
                                            chunksize=1_000_000):
        """
        Versi PIVOT dari build_column_maps_streaming(), untuk tabel nilai berformat
        LONG (satu baris = satu komponen nilai per peserta), mis. kelas_kuliah_nilai:
        kolomnya cuma id, id_peserta_kelas, id_bobot/id_tipe_bobot_nilai, nilai,
        created_at, updated_at -- BUKAN kolom nilai_tugas/nilai_uts/nilai_uas siap pakai.

        category_label_map: dict {id_bobot: nama_singkat}, mis.
            {1: 'uas', 2: 'presensi', 3: 'kuis', 4: 'uh', 5: 'uts'}
            (didapat dari tabel referensi tipe_bobot_nilai/bobot).

        Return: dict {nama_singkat: {key_col_value: nilai}}, contoh:
            {'uas': {123: 88.0, ...}, 'uts': {...}, 'presensi': {...},
             'kuis': {...}, 'uh': {...}}
        Siap dipakai untuk membangun `column_maps` pada load_and_topn_streaming(),
        satu entry per komponen.
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

    def execute_query(self, query):
        """Menjalankan query DDL/DML ke ClickHouse via HTTP Interface"""
        url = f"http://{DB_CONFIG['host']}:{DB_CONFIG['port']}/"
        params = {
            'database': DB_CONFIG['database'],
            'user': DB_CONFIG['user'],
            'password': DB_CONFIG['password'],
            'query': query
        }
        try:
            response = requests.post(url, params=params, timeout=30)
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text}")
            return response.text
        except Exception as e:
            log_error(f"Gagal mengeksekusi query '{query[:100]}...': {str(e)}")
            raise e

    def bulk_load_to_clickhouse(self, table_name, df, chunk_size=500000, max_rows=MAX_ROWS_PER_LOAD):
        """Memasukkan DataFrame ke ClickHouse dalam format CSVWithNames via HTTP POST secara bertahap"""
        if df is None or df.empty:
            log(f"Melewati pemuatan ke {table_name} karena data kosong.")
            return

        # --- BATAS MAKSIMUM BARIS (BERLAKU UNTUK SELURUH TABEL: DIM & FACT) ---
        # Diterapkan di sini (titik sentral) supaya seluruh pemanggilan
        # bulk_load_to_clickhouse otomatis terkena batas MAX_ROWS_PER_LOAD,
        # termasuk tabel dimensi yang sebelumnya tidak difilter.
        # Untuk tabel fakta yang sudah difilter lebih dulu lewat apply_row_limit()
        # dengan date_column spesifik, baris ini tidak akan memotong ulang
        # karena jumlah barisnya sudah <= MAX_ROWS_PER_LOAD.
        df = self.apply_row_limit(df, n=max_rows, label=table_name)

        total_rows = len(df)
        if total_rows <= chunk_size:
            self._insert_chunk_to_clickhouse(table_name, df)
        else:
            log(f"Memuat {total_rows:,} baris ke ClickHouse '{table_name}' secara bertahap (chunk {chunk_size:,} baris)...")
            for start in range(0, total_rows, chunk_size):
                chunk = df.iloc[start:start + chunk_size]
                self._insert_chunk_to_clickhouse(table_name, chunk)
                log(f"  -> Ter-load {min(start + chunk_size, total_rows):,} / {total_rows:,} baris.")

        log(f"Berhasil memuat {total_rows:,} baris ke tabel ClickHouse '{table_name}'.")

    def _insert_chunk_to_clickhouse(self, table_name, df_chunk):
        df_copy = df_chunk.copy()

        # 1. Konversi Boolean & Flag ke UInt8 (0 atau 1)
        for col in df_copy.columns:
            col_lower = str(col).lower()
            if col_lower.startswith('is_') or col_lower.endswith('_flag') or col_lower in ['is_eksakta', 'is_active', 'is_approved', 'is_current', 'is_locked', 'is_aktif', 'is_jbt_aktif', 'is_terakhir', 'hadir_flag']:
                df_copy[col] = df_copy[col].apply(
                    lambda x: 1 if pd.notna(x) and str(x).lower() in ['true', '1', '1.0', 't', 'y'] else 0
                )

            elif df_copy[col].dtype == 'object':
                df_copy[col] = df_copy[col].apply(
                    lambda x: x.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ').strip()
                    if isinstance(x, str) else x
                )

            is_key_or_id = (
                col_lower.endswith('_key') or 
                col_lower.endswith('_id') or 
                col_lower.startswith('id_') or 
                col_lower == 'id' or 
                'key' in col_lower or
                col_lower in ['angkatan', 'pertemuan_ke', 'tahun', 'kapasitas', 'hari', 'tahun_masuk', 'tahun_lulus', 'urutan', 'eselon', 'batas_usia_pensiun']
            )
            if is_key_or_id:
                def format_int_val(x):
                    if pd.isna(x) or x is None or str(x).strip() in ['', 'nan', 'None', '<NA>']:
                        return ''
                    try:
                        return str(int(round(float(x))))
                    except (ValueError, TypeError):
                        return str(x).strip()
                df_copy[col] = df_copy[col].apply(format_int_val)

        csv_buffer = io.StringIO()
        df_copy.to_csv(csv_buffer, index=False, header=True, na_rep="")
        csv_payload = csv_buffer.getvalue().encode('utf-8')

        url = f"http://{DB_CONFIG['host']}:{DB_CONFIG['port']}/"
        params = {
            'database': DB_CONFIG['database'],
            'user': DB_CONFIG['user'],
            'password': DB_CONFIG['password'],
            'query': f"INSERT INTO {table_name} FORMAT CSVWithNames"
        }

        try:
            response = requests.post(url, params=params, data=csv_payload, timeout=180)
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text}")
        except Exception as e:
            log_error(f"Gagal memuat chunk ke tabel ClickHouse '{table_name}': {str(e)}")
            raise e
        finally:
            del df_copy, csv_buffer, csv_payload
            gc.collect()

    def truncate_all_tables(self):
        """Mengosongkan semua tabel target di ClickHouse sebelum reload"""
        tables = [
            'fact_riwayat_pendidikan', 'fact_riwayat_mutasi_struktural', 'fact_riwayat_program_studi',
            'fact_riwayat_status_pegawai', 'fact_riwayat_jenis_pegawai', 'fact_riwayat_unit_kerja',
            'fact_riwayat_jabatan_fungsional', 'fact_riwayat_golongan', 'fact_penasihat_akademik',
            'fact_presensi', 'fact_krs', 'dim_pendidikan', 'dim_status_pegawai', 'dim_jenis_pegawai',
            'dim_unit_kerja', 'dim_jabatan_struktural', 'dim_jabatan_fungsional', 'dim_golongan',
            'dim_pegawai', 'dim_tipe_kehadiran', 'dim_semester', 'dim_kelas_kuliah', 'dim_mata_kuliah',
            'dim_mahasiswa', 'dim_dosen', 'dim_prodi'
        ]
        log("Mengosongkan (TRUNCATE) seluruh tabel di ClickHouse...")
        for table in tables:
            try:
                self.execute_query(f"TRUNCATE TABLE IF EXISTS {table}")
                log(f"-> TRUNCATE TABLE {table} sukses.")
            except Exception as e:
                log_error(f"-> Gagal mengosongkan tabel {table}: {str(e)}")

    def run(self):
        log("=== MEMULAI PROSES ETL STAR SCHEMA (CLICKHOUSE) ===")
        log(f"Path Root Project: {ROOT_DIR}")
        log(f"Path Data Source: {DATA_DIR}")
        
        confirm = input("Apakah Anda ingin mengosongkan (TRUNCATE) semua tabel terlebih dahulu? (y/n): ").strip().lower()
        if confirm == 'y':
            self.truncate_all_tables()

        # ======================================================================
        # 1. PROSES DAN PEMUATAN TABEL DIMENSI
        # ======================================================================
        self.build_dim_prodi()
        self.build_dim_dosen()
        self.build_dim_mahasiswa()
        self.build_dim_mata_kuliah()
        self.build_dim_kelas_kuliah()
        self.build_dim_semester()
        self.build_dim_tipe_kehadiran()
        self.build_dim_pegawai()
        self.build_dim_golongan()
        self.build_dim_jabatan_fungsional()
        self.build_dim_jabatan_struktural()
        self.build_dim_unit_kerja()
        self.build_dim_jenis_pegawai()
        self.build_dim_status_pegawai()
        self.build_dim_pendidikan()

        # ======================================================================
        # 2. PROSES DAN PEMUATAN TABEL FAKTA (Scaled)
        # ======================================================================
        self.build_fact_krs()
        self.build_fact_presensi()
        self.build_fact_penasihat_akademik()
        self.build_fact_riwayat_golongan()
        self.build_fact_riwayat_jabfung()
        self.build_fact_riwayat_unit_kerja()
        self.build_fact_riwayat_jenis_pegawai()
        self.build_fact_riwayat_status_pegawai()
        self.build_fact_riwayat_program_studi()
        self.build_fact_riwayat_mutasi_struktural()
        self.build_fact_riwayat_pendidikan()

        log("=====================================================")
        log(f"Proses ETL ClickHouse untuk {SCALE_FACTOR} BERHASIL!")
        log("=====================================================")

    # --------------------------------------------------------------------------
    # METODE BUILD TABEL DIMENSI
    # --------------------------------------------------------------------------
    def build_dim_prodi(self):
        log("Memproses dim_prodi...")
        df_p_neo = self.load_csv(NEOSIA_FIXED_DIR, "prodi.csv")
        df_pj_neo = self.load_csv(NEOSIA_FIXED_DIR, "prodi_jenjang.csv")
        df_f_neo = self.load_csv(NEOSIA_FIXED_DIR, "fakultas.csv")
        df_j_neo = self.load_csv(NEOSIA_FIXED_DIR, "jurusan.csv")
        df_p_simp = self.load_csv(SIMPEG_FIXED_DIR, "ref_program_studi.csv")

        if df_p_neo is not None:
            m = df_p_neo.merge(df_pj_neo, left_on='id_prodi_jenjang', right_on='id', suffixes=('', '_jen'), how='left')
            m = m.merge(df_f_neo, left_on='id_fakultas', right_on='id', suffixes=('', '_fak'), how='left')
            
            if df_j_neo is not None:
                m = m.merge(df_j_neo, left_on='id_jurusan', right_on='id', suffixes=('', '_jur'), how='left')
            else:
                m['nama_resmi_jur'] = np.nan

            if df_p_simp is not None:
                if 'prodiDiktiKode' in df_p_simp.columns:
                    right_key = 'prodiDiktiKode'
                    id_col_raw = 'prodiKode'
                elif 'kode_dikti' in df_p_simp.columns:
                    right_key = 'kode_dikti'
                    id_col_raw = 'id'
                else:
                    right_key = df_p_simp.columns[0]
                    id_col_raw = df_p_simp.columns[0]

                m = m.merge(df_p_simp, left_on='kode_dikti', right_on=right_key, suffixes=('', '_simp'), how='left')
                target_id_col = f"{id_col_raw}_simp" if f"{id_col_raw}_simp" in m.columns else id_col_raw
                df_dim_prodi_simp_id = m[target_id_col] if target_id_col in m.columns else np.nan
            else:
                df_dim_prodi_simp_id = np.nan

            df_dim = pd.DataFrame()
            df_dim['prodi_kode'] = m['kode_dikti']
            df_dim['id_prodi_neosia'] = m['id']
            df_dim['id_prodi_simpeg'] = df_dim_prodi_simp_id
            df_dim['nama_resmi'] = m['nama_resmi']
            df_dim['nama_singkat'] = m['nama_singkat']
            df_dim['jenjang_nama'] = m['nama']
            df_dim['fakultas_nama'] = m['nama_resmi_fak']
            df_dim['jurusan_nama'] = m['nama_resmi_jur'] if 'nama_resmi_jur' in m.columns else np.nan
            df_dim['is_eksakta'] = m['is_eksakta']
            df_dim['is_active'] = m['is_active']
            df_dim.insert(0, 'prodi_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=[
                'prodi_key', 'prodi_kode', 'id_prodi_neosia', 'id_prodi_simpeg',
                'nama_resmi', 'nama_singkat', 'jenjang_nama', 'fakultas_nama',
                'jurusan_nama', 'is_eksakta', 'is_active'
            ])

        self.bulk_load_to_clickhouse("dim_prodi", df_dim)
        for _, r in df_dim.iterrows():
            pk = int(r['prodi_key'])
            if pd.notna(r['id_prodi_neosia']):
                self.prodi_map[int(r['id_prodi_neosia'])] = pk
            if pd.notna(r['id_prodi_simpeg']):
                self.prodi_map[f"simpeg_{int(r['id_prodi_simpeg'])}"] = pk

    def build_dim_dosen(self):
        log("Memproses dim_dosen...")
        df_d_neo = self.load_csv(NEOSIA_FIXED_DIR, "dosen.csv")
        df_d_simp = self.load_csv(SIMPEG_FIXED_DIR, "data_dosen.csv")
        df_rpd = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_prodi_dosen_neosia_SF0.1.csv")
        if df_rpd is None:
            df_rpd = self.load_csv(SIMPEG_SCALED_DIR, f"riwayat_prodi_dosen_neosia_{SCALE_FACTOR}.csv")
        df_peg_raw = self.load_csv(SIMPEG_FIXED_DIR, "data_pegawai.csv")

        if df_d_neo is not None:
            id_dosen_col = 'id' if 'id' in df_d_neo.columns else ('id_dosen' if 'id_dosen' in df_d_neo.columns else df_d_neo.columns[0])
            df_dim = pd.DataFrame()
            df_dim['id_dosen_neosia'] = df_d_neo[id_dosen_col]

            if df_d_simp is not None:
                right_key = 'dosenIdNeosia' if 'dosenIdNeosia' in df_d_simp.columns else ('id_dosen_neosia' if 'id_dosen_neosia' in df_d_simp.columns else df_d_simp.columns[0])
                m = df_d_neo.merge(df_d_simp, left_on=id_dosen_col, right_on=right_key, how='left')
                peg_id_col = 'dosenPegId' if 'dosenPegId' in m.columns else ('pegawai_id' if 'pegawai_id' in m.columns else np.nan)
                df_dim['id_pegawai_simpeg'] = m[peg_id_col] if peg_id_col in m.columns else np.nan
            else:
                df_dim['id_pegawai_simpeg'] = np.nan

            if df_rpd is not None and df_peg_raw is not None:
                id_peg_col = 'id' if 'id' in df_peg_raw.columns else ('pegId' if 'pegId' in df_peg_raw.columns else df_peg_raw.columns[0])
                nip_col_peg = 'pegNip' if 'pegNip' in df_peg_raw.columns else ('nip' if 'nip' in df_peg_raw.columns else df_peg_raw.columns[0])
                nip_to_peg_id = dict(zip(df_peg_raw[nip_col_peg].astype(str), df_peg_raw[id_peg_col]))
                rpd_nips = set(df_rpd['nip'].dropna().astype(str)) if 'nip' in df_rpd.columns else set()

                for idx, row in df_d_neo.iterrows():
                    if pd.isna(df_dim.loc[idx, 'id_pegawai_simpeg']) and 'nip' in row:
                        nip_str = str(row['nip'])
                        if nip_str in rpd_nips and nip_str in nip_to_peg_id:
                            df_dim.loc[idx, 'id_pegawai_simpeg'] = nip_to_peg_id[nip_str]
                        elif nip_str in nip_to_peg_id:
                            df_dim.loc[idx, 'id_pegawai_simpeg'] = nip_to_peg_id[nip_str]

            df_dim['nip'] = df_d_neo['nip'] if 'nip' in df_d_neo.columns else np.nan
            df_dim['nidn'] = df_d_neo['nidn'] if 'nidn' in df_d_neo.columns else np.nan
            df_dim['nama'] = df_d_neo['nama'] if 'nama' in df_d_neo.columns else "Unknown"
            df_dim['prodi_key'] = df_d_neo['id_prodi'].map(self.prodi_map) if 'id_prodi' in df_d_neo.columns else np.nan
            df_dim['is_active'] = df_d_neo['is_active'] if 'is_active' in df_d_neo.columns else 1
            df_dim.insert(0, 'dosen_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=[
                'dosen_key', 'id_dosen_neosia', 'id_pegawai_simpeg', 'nip', 'nidn', 'nama', 'prodi_key', 'is_active'
            ])

        self.bulk_load_to_clickhouse("dim_dosen", df_dim)
        for _, r in df_dim.iterrows():
            dk = int(r['dosen_key'])
            if pd.notna(r['id_dosen_neosia']):
                self.dosen_map[int(r['id_dosen_neosia'])] = dk
            if pd.notna(r['id_pegawai_simpeg']):
                self.dosen_map[f"simpeg_{int(r['id_pegawai_simpeg'])}"] = dk

    def build_dim_mahasiswa(self):
        log("Memproses dim_mahasiswa...")
        df_m = self.load_csv(NEOSIA_FIXED_DIR, "mahasiswa.csv")
        df_jm = self.load_csv(NEOSIA_FIXED_DIR, "jalur_masuk.csv")
        df_pm = self.load_csv(NEOSIA_FIXED_DIR, "peminatan.csv")

        if df_m is not None:
            m = df_m.copy()
            if df_jm is not None:
                m = m.merge(df_jm, left_on='id_jalur_masuk', right_on='id', suffixes=('', '_jm'), how='left')
            else:
                m['nama'] = np.nan

            if df_pm is not None:
                m = m.merge(df_pm, left_on='id_peminatan', right_on='id', suffixes=('', '_pm'), how='left')
            else:
                m['nama_resmi_peminatan'] = np.nan

            df_dim = pd.DataFrame()
            df_dim['id_mahasiswa'] = m['id']
            df_dim['nim'] = m['nim']
            df_dim['nama_mahasiswa'] = m['nama_mahasiswa']
            df_dim['angkatan'] = m['angkatan']
            df_dim['jenis_kelamin'] = m['jenis_kelamin']
            df_dim['tempat_lahir'] = m['tempat_lahir']
            df_dim['tanggal_lahir'] = m['tanggal_lahir']
            df_dim['kewarganegaraan'] = m['kewarganegaraan']
            df_dim['prodi_key'] = m['id_prodi'].map(self.prodi_map)
            df_dim['jalur_masuk_nama'] = m['nama'] if 'nama' in m.columns else np.nan
            df_dim['peminatan_nama'] = m['nama_resmi_peminatan'] if 'nama_resmi_peminatan' in m.columns else np.nan
            df_dim['is_active'] = m['is_active']
            df_dim.insert(0, 'mahasiswa_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=[
                'mahasiswa_key', 'id_mahasiswa', 'nim', 'nama_mahasiswa', 'angkatan',
                'jenis_kelamin', 'tempat_lahir', 'tanggal_lahir', 'kewarganegaraan',
                'prodi_key', 'jalur_masuk_nama', 'peminatan_nama', 'is_active'
            ])

        self.bulk_load_to_clickhouse("dim_mahasiswa", df_dim)
        for _, r in df_dim.iterrows():
            self.mhs_map[int(r['id_mahasiswa'])] = int(r['mahasiswa_key'])

    def build_dim_mata_kuliah(self):
        log("Memproses dim_mata_kuliah...")
        df_mk = self.load_csv(NEOSIA_FIXED_DIR, "mata_kuliah.csv")
        if df_mk is None:
            df_mk = self.load_csv(NEOSIA_SCALED_DIR, "mata_kuliah.csv")
        df_kur = self.load_csv(NEOSIA_FIXED_DIR, "kurikulum.csv")
        df_pj = self.load_csv(NEOSIA_FIXED_DIR, "prodi_jenjang.csv")

        if df_mk is not None:
            m = df_mk.copy()
            if df_kur is not None:
                m = m.merge(df_kur, left_on='id_kurikulum', right_on='id', suffixes=('', '_kur'), how='left')
            if df_pj is not None:
                m = m.merge(df_pj, left_on='id_prodi_jenjang', right_on='id', suffixes=('', '_jen'), how='left')

            df_dim = pd.DataFrame()
            df_dim['id_mata_kuliah'] = m['id'] if 'id' in m.columns else m['id_mata_kuliah']
            df_dim['nama_resmi'] = m['nama_resmi'] if 'nama_resmi' in m.columns else "Unknown"
            df_dim['prodi_key'] = m['id_prodi'].map(self.prodi_map) if 'id_prodi' in m.columns else np.nan

            if 'nama_jen' in m.columns:
                df_dim['jenjang_nama'] = m['nama_jen'].fillna('S1')
            elif 'nama_jenjang' in m.columns:
                df_dim['jenjang_nama'] = m['nama_jenjang'].fillna('S1')
            elif 'nama' in m.columns:
                df_dim['jenjang_nama'] = m['nama'].fillna('S1')
            else:
                df_dim['jenjang_nama'] = "S1"

            if 'nama_kur' in m.columns:
                df_dim['kurikulum_nama'] = m['nama_kur'].fillna('Kurikulum Standard')
            elif 'nama' in m.columns:
                df_dim['kurikulum_nama'] = m['nama'].fillna('Kurikulum Standard')
            else:
                df_dim['kurikulum_nama'] = "Kurikulum Standard"

            df_dim.insert(0, 'mata_kuliah_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=['mata_kuliah_key', 'id_mata_kuliah', 'nama_resmi', 'prodi_key', 'jenjang_nama', 'kurikulum_nama'])

        self.bulk_load_to_clickhouse("dim_mata_kuliah", df_dim)
        for _, r in df_dim.iterrows():
            self.mk_map[int(r['id_mata_kuliah'])] = int(r['mata_kuliah_key'])

    def build_dim_kelas_kuliah(self):
        log("Memproses dim_kelas_kuliah...")
        df_kk = self.load_csv(NEOSIA_FIXED_DIR, "kelas_kuliah.csv")
        df_j = self.load_csv(NEOSIA_FIXED_DIR, "jadwal_kuliah.csv")
        df_r = self.load_csv(NEOSIA_FIXED_DIR, "ruang.csv")

        if df_kk is not None:
            m = df_kk.copy()
            if df_j is not None:
                m = m.merge(df_j, left_on='id', right_on='id_kelas_kuliah', suffixes=('', '_j'), how='left')
            if df_r is not None:
                m = m.merge(df_r, left_on='id_ruang', right_on='id', suffixes=('', '_r'), how='left')

            df_dim = pd.DataFrame()
            df_dim['id_kelas_kuliah'] = m['id']
            df_dim['nama_kelas'] = m['nama'].astype(str).str.slice(0, 150)
            df_dim['mata_kuliah_key'] = m['id_mata_kuliah'].map(self.mk_map)
            df_dim['jenis_pertemuan'] = m['id_jenis_pertemuan']
            df_dim['ruang_nama'] = m['nama_ruang'] if 'nama_ruang' in m.columns else "Kelas"
            df_dim['gedung'] = m['gedung'] if 'gedung' in m.columns else "Gedung FT"
            df_dim['kapasitas'] = m['kapasitas'] if 'kapasitas' in m.columns else 40
            df_dim['hari'] = m['day_of_week'] if 'day_of_week' in m.columns else 1
            df_dim['jam_mulai'] = m['start_time'] if 'start_time' in m.columns else "08:00:00"
            df_dim['jam_selesai'] = m['end_time'] if 'end_time' in m.columns else "09:40:00"
            df_dim.insert(0, 'kelas_kuliah_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=[
                'kelas_kuliah_key', 'id_kelas_kuliah', 'nama_kelas', 'mata_kuliah_key',
                'jenis_pertemuan', 'ruang_nama', 'gedung', 'kapasitas', 'hari', 'jam_mulai', 'jam_selesai'
            ])

        # Dimensi kelas kuliah harus memuat semua ID agar pemetaan fact tidak kehilangan kelas.
        # Batas MAX_ROWS_PER_LOAD tetap berlaku untuk tabel lain.
        self.bulk_load_to_clickhouse("dim_kelas_kuliah", df_dim, max_rows=None)
        for _, r in df_dim.iterrows():
            self.kelas_map[int(r['id_kelas_kuliah'])] = int(r['kelas_kuliah_key'])

    def build_dim_semester(self):
        log("Memproses dim_semester...")
        df_sem = self.load_csv(NEOSIA_FIXED_DIR, "semester.csv")
        if df_sem is not None:
            df_dim = pd.DataFrame()
            df_dim['id_semester'] = df_sem['id']
            df_dim['tahun'] = df_sem['tahun']
            df_dim['kode'] = df_sem['kode']
            df_dim.insert(0, 'semester_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=['semester_key', 'id_semester', 'tahun', 'kode'])

        self.bulk_load_to_clickhouse("dim_semester", df_dim)
        for _, r in df_dim.iterrows():
            self.semester_map[int(r['id_semester'])] = int(r['semester_key'])

    def build_dim_tipe_kehadiran(self):
        log("Memproses dim_tipe_kehadiran...")
        df_tk = self.load_csv(NEOSIA_FIXED_DIR, "tipe_kehadiran.csv")
        if df_tk is not None:
            df_dim = pd.DataFrame()
            df_dim['id_tipe_kehadiran'] = df_tk['id']
            df_dim['nama'] = df_tk['nama']
            df_dim['kode'] = df_tk['kode']
            df_dim.insert(0, 'tipe_kehadiran_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=['tipe_kehadiran_key', 'id_tipe_kehadiran', 'nama', 'kode'])

        self.bulk_load_to_clickhouse("dim_tipe_kehadiran", df_dim)
        for _, r in df_dim.iterrows():
            self.kehadiran_map[int(r['id_tipe_kehadiran'])] = int(r['tipe_kehadiran_key'])

    def build_dim_pegawai(self):
        log("Memproses dim_pegawai...")
        df_peg = self.load_csv(SIMPEG_FIXED_DIR, "data_pegawai.csv")
        if df_peg is None:
            df_peg = self.load_csv(SIMPEG_SCALED_DIR, "data_pegawai.csv")
        df_ag = self.load_csv(SIMPEG_FIXED_DIR, "ref_agama.csv")
        df_nk = self.load_csv(SIMPEG_FIXED_DIR, "ref_status_nikah.csv")

        if df_peg is not None:
            m = df_peg.copy()
            if df_ag is not None:
                left_ag = 'pegAgama' if 'pegAgama' in m.columns else ('agama_id' if 'agama_id' in m.columns else None)
                right_ag = 'agamaKode' if 'agamaKode' in df_ag.columns else ('id' if 'id' in df_ag.columns else (df_ag.columns[0] if len(df_ag.columns)>0 else None))
                if left_ag and right_ag:
                    m = m.merge(df_ag, left_on=left_ag, right_on=right_ag, suffixes=('', '_ag'), how='left')

            if df_nk is not None:
                left_nk = 'pegStatusNikah' if 'pegStatusNikah' in m.columns else ('nikah_id' if 'nikah_id' in m.columns else None)
                right_nk = 'nikahKode' if 'nikahKode' in df_nk.columns else ('id' if 'id' in df_nk.columns else (df_nk.columns[0] if len(df_nk.columns)>0 else None))
                if left_nk and right_nk:
                    m = m.merge(df_nk, left_on=left_nk, right_on=right_nk, suffixes=('', '_nk'), how='left')

            df_dim = pd.DataFrame()
            id_col = 'id' if 'id' in m.columns else ('pegId' if 'pegId' in m.columns else m.columns[0])
            df_dim['id_pegawai'] = m[id_col]

            nip_col = 'pegNip' if 'pegNip' in m.columns else ('nip' if 'nip' in m.columns else None)
            df_dim['nip'] = m[nip_col] if nip_col and nip_col in m.columns else np.nan

            nama_col = 'nama' if 'nama' in m.columns else ('pegNama' if 'pegNama' in m.columns else None)
            df_dim['nama'] = m[nama_col] if nama_col and nama_col in m.columns else "Unknown"

            jk_col = 'pegJenisKelamin' if 'pegJenisKelamin' in m.columns else ('jenis_kelamin' if 'jenis_kelamin' in m.columns else None)
            df_dim['jenis_kelamin'] = m[jk_col] if jk_col and jk_col in m.columns else "L"

            ag_nama_col = 'agamaNama' if 'agamaNama' in m.columns else ('nama_ag' if 'nama_ag' in m.columns else None)
            df_dim['agama_nama'] = m[ag_nama_col] if ag_nama_col and ag_nama_col in m.columns else np.nan

            nk_nama_col = 'nikahNama' if 'nikahNama' in m.columns else ('nama_nk' if 'nama_nk' in m.columns else None)
            df_dim['status_nikah_nama'] = m[nk_nama_col] if nk_nama_col and nk_nama_col in m.columns else np.nan

            tl_col = 'pegTempatLahir' if 'pegTempatLahir' in m.columns else ('tempat_lahir' if 'tempat_lahir' in m.columns else None)
            df_dim['tempat_lahir'] = m[tl_col] if tl_col and tl_col in m.columns else np.nan

            tgl_col = 'pegTanggalLahir' if 'pegTanggalLahir' in m.columns else ('tanggal_lahir' if 'tanggal_lahir' in m.columns else None)
            df_dim['tanggal_lahir'] = m[tgl_col] if tgl_col and tgl_col in m.columns else np.nan

            is_dosen_col = 'pegIsDosen' if 'pegIsDosen' in m.columns else ('is_dosen' if 'is_dosen' in m.columns else None)
            df_dim['is_dosen'] = m[is_dosen_col] if is_dosen_col and is_dosen_col in m.columns else False

            df_dim.insert(0, 'pegawai_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=[
                'pegawai_key', 'id_pegawai', 'nip', 'nama', 'jenis_kelamin',
                'agama_nama', 'status_nikah_nama', 'tempat_lahir', 'tanggal_lahir', 'is_dosen'
            ])

        self.bulk_load_to_clickhouse("dim_pegawai", df_dim)
        for _, r in df_dim.iterrows():
            self.pegawai_map[int(r['id_pegawai'])] = int(r['pegawai_key'])

    def build_dim_golongan(self):
        log("Memproses dim_golongan...")
        df_gol = self.load_csv(SIMPEG_FIXED_DIR, "golongan.csv")
        if df_gol is not None:
            df_dim = pd.DataFrame()
            df_dim['golongan_code'] = df_gol['golongan']
            df_dim['pangkat'] = df_gol['pangkat']
            df_dim.insert(0, 'golongan_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=['golongan_key', 'golongan_code', 'pangkat'])

        self.bulk_load_to_clickhouse("dim_golongan", df_dim)
        for _, r in df_dim.iterrows():
            self.golongan_map[r['golongan_code']] = int(r['golongan_key'])

    def build_dim_jabatan_fungsional(self):
        log("Memproses dim_jabatan_fungsional...")
        df_jf = self.load_csv(SIMPEG_FIXED_DIR, "ref_jabatan_fungsional.csv")
        if df_jf is not None:
            df_dim = pd.DataFrame()
            df_dim['id_jabfung'] = df_jf['jabfungrid']
            df_dim['nama'] = df_jf['jabfungrnama']
            df_dim['tingkat'] = df_jf['jabfungrtingkat']
            df_dim['batas_usia_pensiun'] = df_jf['jabfungrbatasusiapensiun']
            df_dim.insert(0, 'jabfung_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=['jabfung_key', 'id_jabfung', 'nama', 'tingkat', 'batas_usia_pensiun'])

        self.bulk_load_to_clickhouse("dim_jabatan_fungsional", df_dim)
        for _, r in df_dim.iterrows():
            self.jabfung_map[int(r['id_jabfung'])] = int(r['jabfung_key'])

    def build_dim_jabatan_struktural(self):
        log("Memproses dim_jabatan_struktural...")
        df_js = self.load_csv(SIMPEG_FIXED_DIR, "ref_jabatan_struktural.csv")
        if df_js is not None:
            df_dim = pd.DataFrame()
            df_dim['id_jabstruk'] = df_js['jstrukid']
            df_dim['nama'] = df_js['jstruknama']
            df_dim['kelompok_nama'] = df_js['jstrukkodeunitkerja']
            df_dim['urutan'] = df_js['jstrukurutan']
            df_dim.insert(0, 'jabstruk_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=['jabstruk_key', 'id_jabstruk', 'nama', 'kelompok_nama', 'urutan'])

        self.bulk_load_to_clickhouse("dim_jabatan_struktural", df_dim)
        for _, r in df_dim.iterrows():
            self.jabstruk_map[int(r['id_jabstruk'])] = int(r['jabstruk_key'])

    def build_dim_unit_kerja(self):
        log("Memproses dim_unit_kerja...")
        df_uk = self.load_csv(SIMPEG_FIXED_DIR, "ref_unit_kerja.csv")
        if df_uk is not None:
            df_dim = pd.DataFrame()
            df_dim['unit_kerja_id'] = df_uk['unitkerjaid']
            df_dim['nama'] = df_uk['unitkerjanama']
            df_dim['induk_kerja_nama'] = df_uk['unitkerjaindukkerjaid']
            df_dim['eselon'] = df_uk['unitkerjaeselonid']
            df_dim.insert(0, 'unit_kerja_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=['unit_kerja_key', 'unit_kerja_id', 'nama', 'induk_kerja_nama', 'eselon'])

        self.bulk_load_to_clickhouse("dim_unit_kerja", df_dim)
        for _, r in df_dim.iterrows():
            self.unit_map[r['unit_kerja_id']] = int(r['unit_kerja_key'])

    def build_dim_jenis_pegawai(self):
        log("Memproses dim_jenis_pegawai...")
        df_jp = self.load_csv(SIMPEG_FIXED_DIR, "ref_jenis_pegawai.csv")
        if df_jp is not None:
            df_dim = pd.DataFrame()
            df_dim['id_jenis_pegawai'] = df_jp['jnspegrid']
            df_dim['nama'] = df_jp['jnspegrnama']
            df_dim['tipe_kontrak'] = df_jp['jnspegrtipekontrak']
            df_dim.insert(0, 'jenis_pegawai_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=['jenis_pegawai_key', 'id_jenis_pegawai', 'nama', 'tipe_kontrak'])

        self.bulk_load_to_clickhouse("dim_jenis_pegawai", df_dim)
        for _, r in df_dim.iterrows():
            self.jenis_peg_map[int(r['id_jenis_pegawai'])] = int(r['jenis_pegawai_key'])

    def build_dim_status_pegawai(self):
        log("Memproses dim_status_pegawai...")
        df_sp = self.load_csv(SIMPEG_FIXED_DIR, "ref_status_pegawai.csv")
        if df_sp is not None:
            df_dim = pd.DataFrame()
            df_dim['id_status'] = df_sp['statid']
            df_dim['nama'] = df_sp['statnama']
            df_dim.insert(0, 'status_pegawai_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=['status_pegawai_key', 'id_status', 'nama'])

        self.bulk_load_to_clickhouse("dim_status_pegawai", df_dim)
        for _, r in df_dim.iterrows():
            self.status_peg_map[int(r['id_status'])] = int(r['status_pegawai_key'])

    def build_dim_pendidikan(self):
        log("Memproses dim_pendidikan...")
        df_pend = self.load_csv(SIMPEG_FIXED_DIR, "ref_pendidikan.csv")
        if df_pend is not None:
            df_dim = pd.DataFrame()
            df_dim['id_pendidikan'] = df_pend['pendid']
            df_dim['nama'] = df_pend['pendnama']
            df_dim.insert(0, 'pendidikan_key', range(1, len(df_dim) + 1))
        else:
            df_dim = pd.DataFrame(columns=['pendidikan_key', 'id_pendidikan', 'nama'])

        self.bulk_load_to_clickhouse("dim_pendidikan", df_dim)
        for _, r in df_dim.iterrows():
            self.pendidikan_map[int(r['id_pendidikan'])] = int(r['pendidikan_key'])

    # --------------------------------------------------------------------------
    # METODE BUILD TABEL FAKTA (Scaled)
    # --------------------------------------------------------------------------
    def build_fact_krs(self):
        log("Memproses fact_krs...")

        # --- kartu_rencana_studi & kelas_kuliah_koordinator relatif lebih kecil -> load penuh ---
        df_krs = self.load_csv(NEOSIA_SCALED_DIR, "kartu_rencana_studi_SF0.1.csv")
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
        else:
            df_persentase = None

        m = None
        if df_krs is not None:
            # --- kelas_kuliah_nilai_SF0.1.csv: format LONG (id_peserta_kelas, id_bobot,
            # nilai) dan RAKSASA (seukuran kelas_kuliah_has_mahasiswa) -> JANGAN load
            # penuh sebagai DataFrame. Pivot streaming per komponen (uas/uts/presensi/
            # kuis/uh) berdasarkan id_peserta_kelas. SESUAIKAN category_col jika nama
            # kolom id tipe bobot di file ini bukan 'id_bobot'.
            # bangun mapping: id (PK bobot_nilai_kelas_kuliah) -> nama_singkat komponen
            bnkk_to_komponen_map = {}
            if df_bnkk is not None and bobot_label_map:
                tmp = df_bnkk.copy()
                tmp['komponen'] = tmp['id_tipe_bobot_nilai'].map(bobot_label_map)
                bnkk_to_komponen_map = dict(zip(tmp['id'], tmp['komponen']))

            nilai_pivot_maps = self.build_pivoted_value_maps_streaming(
                folder=NEOSIA_SCALED_DIR,
                filename="kelas_kuliah_nilai_SF0.1.csv",
                key_col='id_peserta_kelas',
                category_col='id_bobot_nilai_kelas_kuliah',
                value_col='nilai',
                category_label_map=bnkk_to_komponen_map, 
                label="kelas_kuliah_nilai",
            )

            column_maps = {
                f'nilai_{komponen}': ('id', peta)
                for komponen, peta in nilai_pivot_maps.items()
            }

            # --- kelas_kuliah_has_mahasiswa_SF0.1.csv adalah file dasar TERBESAR ->
            # dibaca per-chunk (bukan load penuh), sambil langsung mempertahankan
            # kandidat "N data terbaru" (berdasarkan created_at). extra_merges dipakai
            # untuk join ke kelas_kuliah_koordinator (kecil, muat di RAM), sedangkan
            # column_maps dipakai untuk menempelkan nilai per komponen dari pivot
            # streaming di atas (bukan lewat .merge() yang mahal secara memori).
            m = self.load_and_topn_streaming(
                folder=NEOSIA_SCALED_DIR,
                filename="kelas_kuliah_has_mahasiswa_SF0.1.csv",
                date_lookup_df=df_krs,
                left_on='id_kartu_rencana_studi',
                right_on='id',
                date_col='created_at',
                n=MAX_ROWS_PER_LOAD,
                chunksize=1_000_000,
                tiebreak_col='id',  # PK kelas_kuliah_has_mahasiswa (unsuffixed karena suffixes=('', '_krs'))
                label="fact_krs",
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

            del df_krs, df_kkc, df_kkc_clean, df_bobot, df_bnkk, df_persentase, nilai_pivot_maps
            gc.collect()

        # Samakan pemetaan prodi dan semester dengan loader PostgreSQL:
        # KRS menyimpan id_prodi_semester, sehingga id_prodi dan id_semester
        # harus diturunkan melalui prodi_semester.id terlebih dahulu.
        if m is not None and not m.empty and 'id_prodi_semester' in m.columns:
            df_ps = self.load_csv(NEOSIA_FIXED_DIR, "prodi_semester.csv")
            if df_ps is not None and all(c in df_ps.columns for c in ['id', 'id_prodi', 'id_semester']):
                df_ps_lookup = df_ps[['id', 'id_prodi', 'id_semester']].drop_duplicates(subset=['id']).rename(columns={
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
            df_fact = pd.DataFrame()
            df_fact['mahasiswa_key'] = m['id_mahasiswa'].map(self.mhs_map)
            df_fact['kelas_kuliah_key'] = pd.to_numeric(m['id_kelas_kuliah'], errors='coerce').map(self.kelas_map)
            df_fact['semester_key'] = m['id_semester'].map(self.semester_map) if 'id_semester' in m.columns else np.nan
            df_fact['prodi_key'] = m['id_prodi'].map(self.prodi_map) if 'id_prodi' in m.columns else np.nan
            df_fact['dosen_koordinator_key'] = m['id_dosen'].map(self.dosen_map) if 'id_dosen' in m.columns else np.nan

            # CATATAN: tabel bobot referensi tidak punya komponen "tugas"
            # (hanya: uas, presensi, kuis, uh, uts). Sementara di-nolkan.
            # Ganti ke nilai_kuis / nilai_uh jika "tugas" sebenarnya salah satu dari itu.
            df_fact['nilai_tugas'] = m['nilai_kuis'] if 'nilai_kuis' in m.columns else 0.0
            df_fact['nilai_uts'] = m['nilai_uts'] if 'nilai_uts' in m.columns else 0.0
            df_fact['nilai_uas'] = m['nilai_uas'] if 'nilai_uas' in m.columns else 0.0
            df_fact['nilai_akhir'] = m['nilai_akhir_hitung'] if 'nilai_akhir_hitung' in m.columns else 0.0

            df_fact['is_approved'] = m['is_approved'] if 'is_approved' in m.columns else True
            df_fact['is_current'] = m['is_current'] if 'is_current' in m.columns else True
            df_fact['is_locked'] = m['is_locked'] if 'is_locked' in m.columns else False
            df_fact['created_at'] = m['created_at'] if 'created_at' in m.columns else np.nan
            df_fact.insert(0, 'krs_key', range(1, len(df_fact) + 1))
        else:
            df_fact = pd.DataFrame(columns=[
                'krs_key', 'mahasiswa_key', 'kelas_kuliah_key', 'semester_key', 'prodi_key',
                'dosen_koordinator_key', 'nilai_tugas', 'nilai_uts', 'nilai_uas', 'nilai_akhir',
                'is_approved', 'is_current', 'is_locked', 'created_at'
            ])

        self.bulk_load_to_clickhouse("fact_krs", df_fact)

    def build_fact_presensi(self):
        log("Memproses fact_presensi...")

        # --- pertemuan.csv relatif kecil (bukan file scaled) -> aman di-load penuh ---
        df_p = self.load_csv(NEOSIA_FIXED_DIR, "pertemuan.csv")
        pm_filename = "presensi_mahasiswa_SF0.1.csv"
        pm_path_exists = os.path.exists(os.path.join(NEOSIA_SCALED_DIR, pm_filename))
        if not pm_path_exists:
            pm_filename = f"presensi_mahasiswa_{SCALE_FACTOR}.csv"

        if df_p is not None:
            df_p_clean = df_p.drop_duplicates(subset=['id']) if 'id' in df_p.columns else df_p
        else:
            df_p_clean = None

        m = None
        if df_p_clean is not None:
            # --- FILE PRESENSI BISA MENCAPAI RATUSAN JUTA BARIS -> JANGAN LOAD PENUH ---
            # Dibaca per-chunk sambil langsung mempertahankan hanya kandidat
            # "N data terbaru" (berdasarkan tanggal_rencana) di dalam memori.
            # Ini yang mencegah error "out of memory" tanpa mengubah hasil akhir:
            # 3.000.000 baris yang dipilih tetap identik dengan hasil sort penuh,
            # dan tetap identik dengan yang dimuat ke PostgreSQL/MongoDB selama
            # ketiga skrip memakai tiebreak_col yang sama (kolom 'id' presensi).
            m = self.load_and_topn_streaming(
                folder=NEOSIA_SCALED_DIR,
                filename=pm_filename,
                date_lookup_df=df_p_clean,
                left_on='id_pertemuan',
                right_on='id',
                date_col='tanggal_rencana',
                n=MAX_ROWS_PER_LOAD,
                chunksize=1_000_000,
                tiebreak_col='id',  # kolom PK presensi_mahasiswa; ganti sesuai nama kolom asli jika berbeda
                label="fact_presensi",
            )

            del df_p, df_p_clean
            gc.collect()

        if m is not None and not m.empty:
            df_fact = pd.DataFrame()
            df_fact['mahasiswa_key'] = m['id_mahasiswa'].map(self.mhs_map)
            df_fact['kelas_kuliah_key'] = m['id_kelas_kuliah'].map(self.kelas_map)
            df_fact['pertemuan_ke'] = m['pertemuan_ke']
            df_fact['tanggal_pertemuan'] = m['tanggal_rencana'] if 'tanggal_rencana' in m.columns else np.nan
            df_fact['tipe_kehadiran_key'] = m['id_tipe_kehadiran'].map(self.kehadiran_map)
            df_fact['hadir_flag'] = (m['id_tipe_kehadiran'] == 1)
            df_fact.insert(0, 'presensi_key', range(1, len(df_fact) + 1))

            del m
            gc.collect()
        else:
            df_fact = pd.DataFrame(columns=[
                'presensi_key', 'mahasiswa_key', 'kelas_kuliah_key', 'pertemuan_ke',
                'tanggal_pertemuan', 'tipe_kehadiran_key', 'hadir_flag'
            ])

        self.bulk_load_to_clickhouse("fact_presensi", df_fact)
        del df_fact
        gc.collect()

    def build_fact_penasihat_akademik(self):
        log("Memproses fact_penasihat_akademik...")
        df_pa = self.load_csv(NEOSIA_SCALED_DIR, "penasihat_akademik_SF0.1.csv")
        if df_pa is not None:
            # --- FILTER N DATA TERBARU ---
            df_pa = self.apply_row_limit(df_pa, label="fact_penasihat_akademik")

            df_fact = pd.DataFrame()
            df_fact['mahasiswa_key'] = df_pa['id_mahasiswa'].map(self.mhs_map)
            df_fact['dosen_key'] = df_pa['id_dosen'].map(self.dosen_map)
            df_fact['semester_key'] = df_pa['id_prodi_semester'].map(self.semester_map)
            df_fact['is_active'] = df_pa['is_active']
            df_fact.insert(0, 'pa_key', range(1, len(df_fact) + 1))
        else:
            df_fact = pd.DataFrame(columns=['pa_key', 'mahasiswa_key', 'dosen_key', 'semester_key', 'is_active'])

        self.bulk_load_to_clickhouse("fact_penasihat_akademik", df_fact)

    def build_fact_riwayat_golongan(self):
        log("Memproses fact_riwayat_golongan...")
        df_rg = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_golongan_SF0.1.csv")
        if df_rg is not None:
            # --- FILTER N DATA TERBARU (BERDASARKAN TMT GOLONGAN) ---
            df_rg = self.apply_row_limit(df_rg, date_column='rgTMTGolongan', label="fact_riwayat_golongan")

            df_fact = pd.DataFrame()
            df_fact['pegawai_key'] = df_rg['rgPegId'].map(self.pegawai_map)
            df_fact['golongan_key'] = df_rg['rgGolongan'].map(self.golongan_map)
            df_fact['tmt_golongan'] = df_rg['rgTMTGolongan']
            df_fact['no_sk'] = df_rg['rgNoSK']
            df_fact['tgl_sk'] = df_rg['rgTglSK']
            df_fact['is_aktif'] = df_rg['rgAktif']
            df_fact.insert(0, 'riwayat_golongan_key', range(1, len(df_fact) + 1))
        else:
            df_fact = pd.DataFrame(columns=['riwayat_golongan_key', 'pegawai_key', 'golongan_key', 'tmt_golongan', 'no_sk', 'tgl_sk', 'is_aktif'])

        self.bulk_load_to_clickhouse("fact_riwayat_golongan", df_fact)

    def build_fact_riwayat_jabfung(self):
        log("Memproses fact_riwayat_jabatan_fungsional...")
        df_rjf = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_jabatan_fungsional_SF0.1.csv")
        if df_rjf is not None:
            # --- FILTER N DATA TERBARU (BERDASARKAN TMT JABATAN) ---
            df_rjf = self.apply_row_limit(df_rjf, date_column='rjfTMTJabatan', label="fact_riwayat_jabatan_fungsional")

            df_fact = pd.DataFrame()
            df_fact['pegawai_key'] = df_rjf['rjfPegId'].map(self.pegawai_map)
            df_fact['jabfung_key'] = df_rjf['rjfJabatanId'].map(self.jabfung_map)
            df_fact['tmt_jabatan'] = df_rjf['rjfTMTJabatan']
            df_fact['angka_kredit'] = df_rjf['rjfAngkaKredit']
            df_fact['is_aktif'] = df_rjf['rjfAktif']
            df_fact.insert(0, 'riwayat_jabfung_key', range(1, len(df_fact) + 1))
        else:
            df_fact = pd.DataFrame(columns=['riwayat_jabfung_key', 'pegawai_key', 'jabfung_key', 'tmt_jabatan', 'angka_kredit', 'is_aktif'])

        self.bulk_load_to_clickhouse("fact_riwayat_jabatan_fungsional", df_fact)

    def build_fact_riwayat_unit_kerja(self):
        log("Memproses fact_riwayat_unit_kerja...")
        df_ruk = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_unit_kerja_SF0.1.csv")
        if df_ruk is not None:
            # --- FILTER N DATA TERBARU (BERDASARKAN TMT) ---
            df_ruk = self.apply_row_limit(df_ruk, date_column='rwyUKTMT', label="fact_riwayat_unit_kerja")

            df_fact = pd.DataFrame()
            df_fact['pegawai_key'] = df_ruk['rwyUKPegawaiId'].map(self.pegawai_map)
            df_fact['unit_kerja_key'] = df_ruk['rwyUKunitkerjaId'].map(self.unit_map)
            df_fact['tmt'] = df_ruk['rwyUKTMT']
            df_fact['no_sk'] = df_ruk['rwyUKNoSK']
            df_fact['is_aktif'] = df_ruk['rwyUKAktif']
            df_fact.insert(0, 'riwayat_unitkerja_key', range(1, len(df_fact) + 1))
        else:
            df_fact = pd.DataFrame(columns=['riwayat_unitkerja_key', 'pegawai_key', 'unit_kerja_key', 'tmt', 'no_sk', 'is_aktif'])

        self.bulk_load_to_clickhouse("fact_riwayat_unit_kerja", df_fact)

    def build_fact_riwayat_jenis_pegawai(self):
        log("Memproses fact_riwayat_jenis_pegawai...")
        df_rjp = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_jenis_pegawai_SF0.1.csv")
        if df_rjp is not None:
            # --- FILTER N DATA TERBARU (BERDASARKAN TGL SK) ---
            df_rjp = self.apply_row_limit(df_rjp, date_column='rjpegTglSK', label="fact_riwayat_jenis_pegawai")

            df_fact = pd.DataFrame()
            df_fact['pegawai_key'] = df_rjp['rjpegPegId'].map(self.pegawai_map)
            df_fact['jenis_pegawai_key'] = df_rjp['rjpegJnspegrId'].map(self.jenis_peg_map)
            df_fact['no_sk'] = df_rjp['rjpegNoSK']
            df_fact['tgl_sk'] = df_rjp['rjpegTglSK']
            df_fact['is_aktif'] = df_rjp['rjpegAktif']
            df_fact.insert(0, 'riwayat_jenispeg_key', range(1, len(df_fact) + 1))
        else:
            df_fact = pd.DataFrame(columns=['riwayat_jenispeg_key', 'pegawai_key', 'jenis_pegawai_key', 'no_sk', 'tgl_sk', 'is_aktif'])

        self.bulk_load_to_clickhouse("fact_riwayat_jenis_pegawai", df_fact)

    def build_fact_riwayat_status_pegawai(self):
        log("Memproses fact_riwayat_status_pegawai...")
        df_rsp = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_status_pegawai_SF0.1.csv")
        if df_rsp is not None:
            # --- FILTER N DATA TERBARU (BERDASARKAN TGL SK) ---
            df_rsp = self.apply_row_limit(df_rsp, date_column='rwystatTglSK', label="fact_riwayat_status_pegawai")

            df_fact = pd.DataFrame()
            df_fact['pegawai_key'] = df_rsp['rwystatpegId'].map(self.pegawai_map)
            df_fact['status_pegawai_key'] = df_rsp['rwystatStatId'].map(self.status_peg_map)
            df_fact['no_sk'] = df_rsp['rwystatNoSK']
            df_fact['tgl_sk'] = df_rsp['rwystatTglSK']
            df_fact['is_aktif'] = df_rsp['rwystatAktif']
            df_fact.insert(0, 'riwayat_status_key', range(1, len(df_fact) + 1))
        else:
            df_fact = pd.DataFrame(columns=['riwayat_status_key', 'pegawai_key', 'status_pegawai_key', 'no_sk', 'tgl_sk', 'is_aktif'])

        self.bulk_load_to_clickhouse("fact_riwayat_status_pegawai", df_fact)

    def build_fact_riwayat_program_studi(self):
        log("Memproses fact_riwayat_program_studi...")
        df_rps = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_program_studi_SF0.1.csv")
        if df_rps is not None:
            # --- FILTER N DATA TERBARU (BERDASARKAN TMT) ---
            df_rps = self.apply_row_limit(df_rps, date_column='rwyprodiTMT', label="fact_riwayat_program_studi")

            df_fact = pd.DataFrame()
            df_fact['pegawai_key'] = df_rps['rwyprodiPegId'].map(self.pegawai_map)
            df_fact['prodi_key'] = df_rps['rwyprodiProdiKode'].apply(lambda x: self.prodi_map.get(f"simpeg_{int(x)}") if pd.notna(x) else None)
            df_fact['tmt'] = df_rps['rwyprodiTMT']
            df_fact['no_sk'] = df_rps['rwyprodiNoSK']
            df_fact['is_aktif'] = df_rps['rwyprodiAktif']
            df_fact.insert(0, 'riwayat_prodi_key', range(1, len(df_fact) + 1))
        else:
            df_fact = pd.DataFrame(columns=['riwayat_prodi_key', 'pegawai_key', 'prodi_key', 'tmt', 'no_sk', 'is_aktif'])

        self.bulk_load_to_clickhouse("fact_riwayat_program_studi", df_fact)

    def build_fact_riwayat_mutasi_struktural(self):
        log("Memproses fact_riwayat_mutasi_struktural...")
        df_rms = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_mutasi_struktural_SF0.1.csv")
        if df_rms is not None:
            # --- FILTER N DATA TERBARU (BERDASARKAN TANGGAL MULAI) ---
            df_rms = self.apply_row_limit(df_rms, date_column='rmsTanggalMulai', label="fact_riwayat_mutasi_struktural")

            df_fact = pd.DataFrame()
            df_fact['pegawai_key'] = df_rms['rmsPegId'].map(self.pegawai_map)
            df_fact['jabstruk_key'] = df_rms['rmsJstrukId'].map(self.jabstruk_map)
            df_fact['tanggal_mulai'] = df_rms['rmsTanggalMulai']
            df_fact['tanggal_selesai'] = df_rms['rmsTanggalSelesai']
            df_fact['is_jbt_aktif'] = df_rms['rmsJbtAktif']
            df_fact['status_pejabat'] = df_rms['rmsStatusPejabat']
            df_fact.insert(0, 'riwayat_mutasi_key', range(1, len(df_fact) + 1))
        else:
            df_fact = pd.DataFrame(columns=['riwayat_mutasi_key', 'pegawai_key', 'jabstruk_key', 'tanggal_mulai', 'tanggal_selesai', 'is_jbt_aktif', 'status_pejabat'])

        self.bulk_load_to_clickhouse("fact_riwayat_mutasi_struktural", df_fact)

    def build_fact_riwayat_pendidikan(self):
        log("Memproses fact_riwayat_pendidikan...")
        df_rp = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_pendidikan_SF0.1.csv")
        if df_rp is not None:
            # --- FILTER N DATA TERBARU (BERDASARKAN TAHUN LULUS) ---
            tahun_col = 'rwpendThnMasuk' if 'rwpendThnMasuk' in df_rp.columns else None
            df_rp = self.apply_row_limit(df_rp, date_column=tahun_col, label="fact_riwayat_pendidikan")

            df_fact = pd.DataFrame()
            df_fact['pegawai_key'] = df_rp['rwpendPegId'].map(self.pegawai_map) if 'rwpendPegId' in df_rp.columns else np.nan
            df_fact['pendidikan_key'] = df_rp['rwpendJenjang'].map(self.pendidikan_map) if 'rwpendJenjang' in df_rp.columns else np.nan
            df_fact['nama_sekolah'] = df_rp['rwpendNamaSekolah'] if 'rwpendNamaSekolah' in df_rp.columns else np.nan
            
            if 'rwpendJurusan' in df_rp.columns:
                df_fact['jurusan'] = df_rp['rwpendJurusan']
            elif 'jurusan' in df_rp.columns:
                df_fact['jurusan'] = df_rp['jurusan']
            else:
                df_fact['jurusan'] = np.nan

            df_fact['tahun_masuk'] = df_rp['rwpendThnMasuk'] if 'rwpendThnMasuk' in df_rp.columns else np.nan
            df_fact['tahun_lulus'] = df_rp['rwpendThnLulus'] if 'rwpendThnLulus' in df_rp.columns else np.nan
            df_fact['negara_nama'] = df_rp['rwpendNegara'] if 'rwpendNegara' in df_rp.columns else np.nan
            
            if 'rwpendTerakhir' in df_rp.columns:
                df_fact['is_terakhir'] = df_rp['rwpendTerakhir']
            elif 'is_terakhir' in df_rp.columns:
                df_fact['is_terakhir'] = df_rp['is_terakhir']
            else:
                df_fact['is_terakhir'] = True

            df_fact.insert(0, 'riwayat_pendidikan_key', range(1, len(df_fact) + 1))
        else:
            df_fact = pd.DataFrame(columns=[
                'riwayat_pendidikan_key', 'pegawai_key', 'pendidikan_key',
                'nama_sekolah', 'jurusan', 'tahun_masuk', 'tahun_lulus', 'negara_nama', 'is_terakhir'
            ])

        self.bulk_load_to_clickhouse("fact_riwayat_pendidikan", df_fact)

if __name__ == "__main__":
    loader = ClickHouseETLLoader()
    loader.run()
