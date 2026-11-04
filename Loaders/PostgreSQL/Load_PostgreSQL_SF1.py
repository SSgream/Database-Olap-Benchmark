import os
import sys
import io
import pandas as pd
import numpy as np
import psycopg2
import gc
import warnings
warnings.filterwarnings("ignore")

# ==============================================================================
# 1. KONFIGURASI DATABASE & SKALA BEBAN KERJA
# ==============================================================================
SCALE_FACTOR = "SF1"  # Cukup ubah ke 'SF1' atau 'SF1' untuk script lainnya!

DB_CONFIG = {
    'dbname': 'skripsi_db',      # Sesuaikan dengan nama DB PostgreSQL Anda
    'user': 'user',               # Sesuaikan dengan user database Anda
    'password': 'password',       # Sesuaikan dengan password database Anda
    'host': 'localhost',          # Gunakan 'localhost' jika jalan di host
    'port': '5434'                # Port host Docker yang dipetakan (misal 5434)
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

# Batas maksimum baris yang dimuat PER TABEL. Jika data sumber melebihi batas ini,
# baris yang dipertahankan adalah yang paling BARU (lihat _apply_row_limit()).
MAX_ROWS_PER_TABLE = 6_000_000

def log(message):
    print(f"[ETL INFO - {SCALE_FACTOR}] {message}")

def log_error(message):
    print(f"[ETL ERROR - {SCALE_FACTOR}] {message}", file=sys.stderr)

# ==============================================================================
# 3. KELAS UTAMA ETL PROSESOR
# ==============================================================================
class PostgresETLLoader:
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

    def load_and_topn_streaming(self, folder, filename, date_lookup_df, left_on, right_on,
                                 date_col, n=MAX_ROWS_PER_TABLE, chunksize=1_000_000,
                                 tiebreak_col='id', usecols=None, label="",
                                 extra_merges=None, column_maps=None):
        """
        Versi 'streaming' dari load_csv() + merge() + _apply_row_limit() khusus untuk
        file CSV RAKSASA (ratusan juta baris) yang tidak mungkin dimuat sekaligus ke RAM
        (penyebab error 'Error tokenizing data: out of memory' pada presensi_mahasiswa_SF1.csv).

        File dibaca per-chunk (default 1 juta baris), langsung digabung dengan tabel
        lookup kecil (mis. pertemuan.csv), lalu sebuah buffer TOP-N (baris terbaru
        berdasarkan date_col) dipertahankan dan dipangkas ulang ke N baris setiap
        selesai satu chunk. Memori yang dipakai maksimal ~(chunksize + n) baris,
        bukan seluruh isi file.

        KONSISTENSI ANTAR DATABASE: hasil akhir algoritma ini identik dengan
        sort_values(date_col, ascending=False).head(n) pada seluruh data sekaligus,
        berapa pun ukuran chunksize-nya. Selama skrip ClickHouse/PostgreSQL/MongoDB
        memakai sumber CSV yang sama, date_col yang sama, dan tiebreak_col yang sama
        (kolom PK unik presensi, mis. 'id', untuk memecah baris bertanggal identik),
        ketiga database akan memuat 3.000.000 baris IDENTIK.

        - extra_merges: opsional, list of dict untuk merge tambahan PER CHUNK setelah
          merge utama, mis. {'df': lookup_df, 'left_on':..., 'right_on':...,
          'how':'left', 'suffixes':(...,...)}. Cocok untuk lookup kecil/menengah
          yang masih muat penuh di RAM.
        - column_maps: opsional, dict {kolom_baru: (kolom_kunci, dict_lookup)} untuk
          menempelkan nilai dari dict Python (bukan DataFrame) via .map(). Dipakai
          saat tabel lookup-nya sendiri terlalu besar untuk di-load penuh -> lihat
          build_column_maps_streaming().
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

    def build_column_maps_streaming(self, folder, filename, key_col, value_cols,
                                     chunksize=1_000_000, label=""):
        """
        Membangun lookup Python dict (bukan DataFrame) dari sebuah file CSV RAKSASA,
        tanpa pernah memuat seluruh file itu ke RAM sekaligus.

        Dipakai untuk kasus file yang punya kolom nilai tetap (fixed columns) per
        baris, dan file itu terlalu besar untuk di-load penuh hanya demi di-LEFT
        JOIN ke tabel lain. Solusinya: baca per-chunk, simpan hanya kolom kunci +
        kolom nilai yang benar-benar dipakai ke dalam dict Python biasa, jauh lebih
        hemat memori dibanding DataFrame lengkap + `.merge()`.

        Return: dict {value_col: {key: value}}, siap dipakai di parameter
        `column_maps` pada load_and_topn_streaming().
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

    def _apply_row_limit(self, df, table_name, max_rows=MAX_ROWS_PER_TABLE, date_col=None, key_col=None):
        """
        Membatasi jumlah baris yang akan dimuat ke `table_name` maksimal `max_rows` baris.

        - Jika total baris <= max_rows: dikembalikan apa adanya (tidak ada perubahan).
        - Jika total baris > max_rows DAN `date_col` tersedia di df: baris diurutkan
          menurun berdasarkan kolom tanggal tersebut, lalu diambil `max_rows` baris
          TERBARU saja.
        - Jika `date_col` tidak tersedia/tidak ditemukan: diambil `max_rows` baris
          pertama apa adanya (fallback, tidak ada acuan waktu untuk menentukan "terbaru").
        - Jika `key_col` diberikan (kolom surrogate key), kolom tersebut dinomori ulang
          1..N agar tetap kontigu setelah pemotongan.
        """
        if df is None or df.empty or max_rows is None or len(df) <= max_rows:
            return df

        original_rows = len(df)
        df = df.copy()

        if date_col and date_col in df.columns:
            df['_row_limit_sort_dt'] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.sort_values(by='_row_limit_sort_dt', ascending=False).drop(columns=['_row_limit_sort_dt'])
            log(f"[{table_name}] Data ({original_rows:,} baris) melebihi batas {max_rows:,} baris. "
                f"Mengambil {max_rows:,} baris TERBARU berdasarkan kolom '{date_col}'.")
        else:
            log(f"[{table_name}] Data ({original_rows:,} baris) melebihi batas {max_rows:,} baris. "
                f"Tidak ada kolom tanggal acuan, mengambil {max_rows:,} baris pertama apa adanya.")

        df = df.head(max_rows).reset_index(drop=True)

        if key_col and key_col in df.columns:
            df[key_col] = range(1, len(df) + 1)

        return df

    def bulk_load_to_postgres(self, conn, table_name, df, chunk_size=500000,
                            max_rows=MAX_ROWS_PER_TABLE, date_col=None, key_col=None):
        if df is None or df.empty:
            log(f"Melewati pemuatan ke {table_name} karena data kosong.")
            return df

        df = self._apply_row_limit(df, table_name, max_rows=max_rows, date_col=date_col, key_col=key_col)

        total_rows = len(df)
        cursor = conn.cursor()
        try:
            if total_rows <= chunk_size:
                self._load_chunk_to_postgres(cursor, table_name, df)
            else:
                log(f"Memuat {total_rows:,} baris ke '{table_name}' secara bertahap (chunk {chunk_size:,} baris)...")
                for start in range(0, total_rows, chunk_size):
                    chunk = df.iloc[start:start + chunk_size]
                    self._load_chunk_to_postgres(cursor, table_name, chunk)
                    log(f"  -> Ter-load {min(start + chunk_size, total_rows):,} / {total_rows:,} baris.")
            conn.commit()
            log(f"Berhasil memuat {total_rows:,} baris ke tabel '{table_name}'.")
        except Exception as e:
            conn.rollback()
            log_error(f"Gagal memuat data ke tabel '{table_name}': {str(e)}")
            raise e
        finally:
            cursor.close()

        return df 

    def _load_chunk_to_postgres(self, cursor, table_name, df_chunk):
        df_copy = df_chunk.copy()

        for col in df_copy.columns:
            col_lower = str(col).lower()
            
            if df_copy[col].dtype == 'object':
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
                col_lower in ['angkatan', 'pertemuan_ke', 'tahun', 'kapasitas', 'hari', 'tahun_masuk', 'tahun_lulus', 'urutan', 'eselon', 'batas_usia_pensiun', 'angka_kredit']
            )
            
            if is_key_or_id or df_copy[col].dtype in ['float64', 'float32']:
                non_nulls = df_copy[col].dropna()
                def format_int_val(x):
                    if pd.isna(x) or x is None or str(x).strip() in ['', 'nan', 'None', '<NA>']:
                        return ''
                    try:
                        val = float(x)
                        return str(int(round(val)))
                    except (ValueError, TypeError):
                        return str(x).strip()

                if is_key_or_id or (len(non_nulls) > 0 and (non_nulls % 1 == 0).all()):
                    df_copy[col] = df_copy[col].apply(format_int_val)

        df_copy = df_copy.replace({np.nan: None})
        
        buffer = io.StringIO()
        df_copy.to_csv(buffer, index=False, header=False, sep='\t')
        buffer.seek(0)
        
        cursor.copy_from(buffer, table_name, sep='\t', null='')
        del df_copy, buffer
        gc.collect()

    def reset_sequences(self, conn, table_name, key_column):
        """Menyetel ulang sekuens PostgreSQL agar tidak bentrok setelah bulk load manual"""
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT setval(pg_get_serial_sequence('{table_name}', '{key_column}'), COALESCE(MAX({key_column}), 1)) FROM {table_name};")
            conn.commit()
            log(f"Sekuens auto-increment untuk '{table_name}' berhasil disinkronkan.")
        except Exception as e:
            conn.rollback()
            log_error(f"Gagal mereset sekuens {table_name}: {str(e)}")
        finally:
            cursor.close()

    def truncate_target_tables(self, conn):
        """Mengosongkan semua tabel tujuan sebelum memuat data baru"""
        cursor = conn.cursor()
        tables_to_truncate = [
            'fact_riwayat_pendidikan', 'fact_riwayat_mutasi_struktural', 'fact_riwayat_program_studi',
            'fact_riwayat_status_pegawai', 'fact_riwayat_jenis_pegawai', 'fact_riwayat_unit_kerja',
            'fact_riwayat_jabatan_fungsional', 'fact_riwayat_golongan', 'fact_penasihat_akademik',
            'fact_presensi', 'fact_krs', 'dim_pendidikan', 'dim_status_pegawai', 'dim_jenis_pegawai',
            'dim_unit_kerja', 'dim_jabatan_struktural', 'dim_jabatan_fungsional', 'dim_golongan',
            'dim_pegawai', 'dim_tipe_kehadiran', 'dim_semester', 'dim_kelas_kuliah', 'dim_mata_kuliah',
            'dim_mahasiswa', 'dim_dosen', 'dim_prodi'
        ]
        
        log("Mengosongkan tabel target di PostgreSQL (TRUNCATE)...")
        try:
            for table in tables_to_truncate:
                cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table}');")
                if cursor.fetchone()[0]:
                    cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")
            conn.commit()
            log("Tabel target berhasil dikosongkan.")
        except Exception as e:
            conn.rollback()
            log_error(f"Gagal mengosongkan tabel: {str(e)}")
            raise e
        finally:
            cursor.close()

    def run(self):
        log("Menghubungkan ke database PostgreSQL...")
        try:
            conn = psycopg2.connect(**DB_CONFIG)
        except Exception as e:
            log_error(f"Gagal tersambung ke database: {str(e)}")
            return

        try:
            confirm = input("Apakah Anda ingin mengosongkan tabel target di PostgreSQL sebelum memuat data? (y/n): ")
            if confirm.lower() == 'y':
                self.truncate_target_tables(conn)

            # ==================================================================
            # 2. PROSES DAN PEMUATAN TABEL DIMENSI
            # ==================================================================
            
            # --- DIM_PRODI (Conformed) ---
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
                    if target_id_col in m.columns:
                        df_dim_prodi_simp_id = m[target_id_col]
                    else:
                        df_dim_prodi_simp_id = np.nan
                else:
                    df_dim_prodi_simp_id = np.nan

                df_dim_prodi = pd.DataFrame()
                df_dim_prodi['prodi_kode'] = m['kode_dikti']
                df_dim_prodi['id_prodi_neosia'] = m['id']
                df_dim_prodi['id_prodi_simpeg'] = df_dim_prodi_simp_id
                    
                df_dim_prodi['nama_resmi'] = m['nama_resmi']
                df_dim_prodi['nama_singkat'] = m['nama_singkat']
                df_dim_prodi['jenjang_nama'] = m['nama']
                df_dim_prodi['fakultas_nama'] = m['nama_resmi_fak']
                df_dim_prodi['jurusan_nama'] = m['nama_resmi_jur'] if 'nama_resmi_jur' in m.columns else np.nan
                df_dim_prodi['is_eksakta'] = m['is_eksakta']
                df_dim_prodi['is_active'] = m['is_active']
                
                df_dim_prodi.insert(0, 'prodi_key', range(1, len(df_dim_prodi) + 1))
            else:
                df_dim_prodi = pd.DataFrame(columns=[
                    'prodi_key', 'prodi_kode', 'id_prodi_neosia', 'id_prodi_simpeg',
                    'nama_resmi', 'nama_singkat', 'jenjang_nama', 'fakultas_nama',
                    'jurusan_nama', 'is_eksakta', 'is_active'
                ])

            self.bulk_load_to_postgres(conn, "dim_prodi", df_dim_prodi, key_col="prodi_key")
            self.reset_sequences(conn, "dim_prodi", "prodi_key")
            
            for _, r in df_dim_prodi.iterrows():
                pk = int(r['prodi_key'])
                if pd.notna(r['id_prodi_neosia']):
                    self.prodi_map[int(r['id_prodi_neosia'])] = pk
                if pd.notna(r['id_prodi_simpeg']):
                    self.prodi_map[f"simpeg_{int(r['id_prodi_simpeg'])}"] = pk

            # --- DIM_DOSEN ---
            log("Memproses dim_dosen...")
            df_d_neo = self.load_csv(NEOSIA_FIXED_DIR, "dosen.csv")
            df_d_simp = self.load_csv(SIMPEG_FIXED_DIR, "data_dosen.csv")
            df_rpd = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_prodi_dosen_neosia_SF1.csv")
            if df_rpd is None:
                df_rpd = self.load_csv(SIMPEG_SCALED_DIR, f"riwayat_prodi_dosen_neosia_{SCALE_FACTOR}.csv")
            df_peg_raw = self.load_csv(SIMPEG_FIXED_DIR, "data_pegawai.csv")

            if df_d_neo is not None:
                id_dosen_col = 'id' if 'id' in df_d_neo.columns else ('id_dosen' if 'id_dosen' in df_d_neo.columns else df_d_neo.columns[0])

                df_dim_dosen = pd.DataFrame()
                df_dim_dosen['id_dosen_neosia'] = df_d_neo[id_dosen_col]
                
                if df_d_simp is not None:
                    right_key = 'dosenIdNeosia' if 'dosenIdNeosia' in df_d_simp.columns else ('id_dosen_neosia' if 'id_dosen_neosia' in df_d_simp.columns else df_d_simp.columns[0])
                    m = df_d_neo.merge(df_d_simp, left_on=id_dosen_col, right_on=right_key, how='left')
                    
                    peg_id_col = 'dosenPegId' if 'dosenPegId' in m.columns else ('pegawai_id' if 'pegawai_id' in m.columns else np.nan)
                    df_dim_dosen['id_pegawai_simpeg'] = m[peg_id_col] if peg_id_col in m.columns else np.nan
                else:
                    df_dim_dosen['id_pegawai_simpeg'] = np.nan
                
                if df_rpd is not None and df_peg_raw is not None:
                    id_peg_col = 'id' if 'id' in df_peg_raw.columns else ('pegId' if 'pegId' in df_peg_raw.columns else ('id_pegawai' if 'id_pegawai' in df_peg_raw.columns else df_peg_raw.columns[0]))
                    nip_col_peg = 'pegNip' if 'pegNip' in df_peg_raw.columns else ('nip' if 'nip' in df_peg_raw.columns else df_peg_raw.columns[0])

                    nip_to_peg_id = dict(zip(df_peg_raw[nip_col_peg].astype(str), df_peg_raw[id_peg_col]))
                    rpd_nips = set(df_rpd['nip'].dropna().astype(str)) if 'nip' in df_rpd.columns else set()
                    
                    for idx, row in df_d_neo.iterrows():
                        if pd.isna(df_dim_dosen.loc[idx, 'id_pegawai_simpeg']) and 'nip' in row:
                            nip_str = str(row['nip'])
                            if nip_str in rpd_nips and nip_str in nip_to_peg_id:
                                df_dim_dosen.loc[idx, 'id_pegawai_simpeg'] = nip_to_peg_id[nip_str]
                            elif nip_str in nip_to_peg_id:
                                df_dim_dosen.loc[idx, 'id_pegawai_simpeg'] = nip_to_peg_id[nip_str]
                                
                df_dim_dosen['nip'] = df_d_neo['nip'] if 'nip' in df_d_neo.columns else np.nan
                df_dim_dosen['nidn'] = df_d_neo['nidn'] if 'nidn' in df_d_neo.columns else np.nan
                df_dim_dosen['nama'] = df_d_neo['nama'] if 'nama' in df_d_neo.columns else "Unknown"
                df_dim_dosen['prodi_key'] = df_d_neo['id_prodi'].map(self.prodi_map) if 'id_prodi' in df_d_neo.columns else np.nan
                df_dim_dosen['is_active'] = df_d_neo['is_active'] if 'is_active' in df_d_neo.columns else 1
                df_dim_dosen.insert(0, 'dosen_key', range(1, len(df_dim_dosen) + 1))
            else:
                df_dim_dosen = pd.DataFrame(columns=[
                    'dosen_key', 'id_dosen_neosia', 'id_pegawai_simpeg', 'nip', 'nidn', 'nama', 'prodi_key', 'is_active'
                ])

            df_dim_dosen['id_pegawai_simpeg'] = df_dim_dosen['id_pegawai_simpeg'].apply(lambda x: int(x) if pd.notna(x) and x != '' else None) 
            if 'prodi_key' in df_dim_dosen.columns: 
                df_dim_dosen['prodi_key'] = df_dim_dosen['prodi_key'].apply(lambda x: int(x) if pd.notna(x) and x != '' else None)

            self.bulk_load_to_postgres(conn, "dim_dosen", df_dim_dosen, key_col="dosen_key")
            self.reset_sequences(conn, "dim_dosen", "dosen_key")
            
            for _, r in df_dim_dosen.iterrows():
                dk = int(r['dosen_key'])
                if pd.notna(r['id_dosen_neosia']):
                    self.dosen_map[int(r['id_dosen_neosia'])] = dk
                if pd.notna(r['id_pegawai_simpeg']):
                    self.dosen_map[f"simpeg_{int(r['id_pegawai_simpeg'])}"] = dk

            # --- DIM_MAHASISWA ---
            log("Memproses dim_mahasiswa...")
            df_m = self.load_csv(NEOSIA_FIXED_DIR, "mahasiswa.csv")
            df_jm = self.load_csv(NEOSIA_FIXED_DIR, "jalur_masuk.csv")
            df_pm = self.load_csv(NEOSIA_FIXED_DIR, "peminatan.csv")

            if df_m is not None:
                m = df_m.copy()
                if df_jm is not None: 
                    m = m.merge(df_jm, left_on='id_jalur_masuk', right_on='id', suffixes=('', '_jm'), how='left') 
                else: m['nama'] = np.nan
                
                if df_pm is not None: 
                    m = m.merge(df_pm, left_on='id_peminatan', right_on='id', suffixes=('', '_pm'), how='left') 
                else: m['nama_resmi_peminatan'] = np.nan
                
                df_dim_mhs = pd.DataFrame()
                df_dim_mhs['id_mahasiswa'] = m['id']
                df_dim_mhs['nim'] = m['nim']
                df_dim_mhs['nama_mahasiswa'] = m['nama_mahasiswa']
                df_dim_mhs['angkatan'] = m['angkatan']
                df_dim_mhs['jenis_kelamin'] = m['jenis_kelamin']
                df_dim_mhs['tempat_lahir'] = m['tempat_lahir']
                df_dim_mhs['tanggal_lahir'] = m['tanggal_lahir']
                df_dim_mhs['kewarganegaraan'] = m['kewarganegaraan']
                df_dim_mhs['prodi_key'] = m['id_prodi'].map(self.prodi_map)
                df_dim_mhs['jalur_masuk_nama'] = m['nama'] if 'nama' in m.columns else np.nan
                df_dim_mhs['peminatan_nama'] = m['nama_resmi_peminatan'] if 'nama_resmi_peminatan' in m.columns else np.nan
                df_dim_mhs['is_active'] = m['is_active']
                df_dim_mhs.insert(0, 'mahasiswa_key', range(1, len(df_dim_mhs) + 1))
            else:
                df_dim_mhs = pd.DataFrame(columns=[
                    'mahasiswa_key', 'id_mahasiswa', 'nim', 'nama_mahasiswa', 'angkatan',
                    'jenis_kelamin', 'tempat_lahir', 'tanggal_lahir', 'kewarganegaraan',
                    'prodi_key', 'jalur_masuk_nama', 'peminatan_nama', 'is_active'
                ])

            self.bulk_load_to_postgres(conn, "dim_mahasiswa", df_dim_mhs, key_col="mahasiswa_key")
            self.reset_sequences(conn, "dim_mahasiswa", "mahasiswa_key")
            
            for _, r in df_dim_mhs.iterrows():
                self.mhs_map[int(r['id_mahasiswa'])] = int(r['mahasiswa_key'])

            # --- DIM_MATA_KULIAH ---
            log("Memproses dim_mata_kuliah...")
            df_mk = self.load_csv(NEOSIA_FIXED_DIR, "mata_kuliah.csv")
            if df_mk is None:
                df_mk = self.load_csv(NEOSIA_FIXED_DIR, "mata_kuliah.csv")
            df_kur = self.load_csv(NEOSIA_FIXED_DIR, "kurikulum.csv")
            df_pj = self.load_csv(NEOSIA_FIXED_DIR, "prodi_jenjang.csv")

            if df_mk is not None:
                m = df_mk.copy()
                if df_kur is not None:
                    m = m.merge(df_kur, left_on='id_kurikulum', right_on='id', suffixes=('', '_kur'), how='left')
                if df_pj is not None:
                    m = m.merge(df_pj, left_on='id_prodi_jenjang', right_on='id', suffixes=('', '_jen'), how='left')
                
                df_dim_mk = pd.DataFrame()
                df_dim_mk['id_mata_kuliah'] = m['id'] if 'id' in m.columns else m['id_mata_kuliah']
                df_dim_mk['nama_resmi'] = m['nama_resmi'] if 'nama_resmi' in m.columns else "Unknown"
                df_dim_mk['prodi_key'] = m['id_prodi'].map(self.prodi_map) if 'id_prodi' in m.columns else np.nan
                
                if 'nama_jen' in m.columns:
                    df_dim_mk['jenjang_nama'] = m['nama_jen'].fillna('S1')
                elif 'nama_jenjang' in m.columns:
                    df_dim_mk['jenjang_nama'] = m['nama_jenjang'].fillna('S1')
                elif 'nama' in m.columns:
                    df_dim_mk['jenjang_nama'] = m['nama'].fillna('S1')
                else:
                    df_dim_mk['jenjang_nama'] = "S1"

                if 'nama_kur' in m.columns:
                    df_dim_mk['kurikulum_nama'] = m['nama_kur'].fillna('Kurikulum Standard')
                elif 'nama' in m.columns:
                    df_dim_mk['kurikulum_nama'] = m['nama'].fillna('Kurikulum Standard')
                else:
                    df_dim_mk['kurikulum_nama'] = "Kurikulum Standard"

                df_dim_mk.insert(0, 'mata_kuliah_key', range(1, len(df_dim_mk) + 1))
            else:
                df_dim_mk = pd.DataFrame(columns=['mata_kuliah_key', 'id_mata_kuliah', 'nama_resmi', 'prodi_key', 'jenjang_nama', 'kurikulum_nama'])

            self.bulk_load_to_postgres(conn, "dim_mata_kuliah", df_dim_mk, key_col="mata_kuliah_key")
            self.reset_sequences(conn, "dim_mata_kuliah", "mata_kuliah_key")
            
            for _, r in df_dim_mk.iterrows():
                self.mk_map[int(r['id_mata_kuliah'])] = int(r['mata_kuliah_key'])

            # --- DIM_KELAS_KULIAH ---
            log("Memproses dim_kelas_kuliah...")
            df_kk = self.load_csv(NEOSIA_FIXED_DIR, "kelas_kuliah.csv")
            df_j = self.load_csv(NEOSIA_FIXED_DIR, "jadwal_kuliah.csv")
            df_r = self.load_csv(NEOSIA_FIXED_DIR, "ruang.csv")

            if df_kk is not None:
                m = df_kk.merge(df_j, left_on='id', right_on='id_kelas_kuliah', suffixes=('', '_j'), how='left')
                m = m.merge(df_r, left_on='id_ruang', right_on='id', suffixes=('', '_r'), how='left')
                
                df_dim_kk = pd.DataFrame()
                df_dim_kk['id_kelas_kuliah'] = m['id']
                df_dim_kk['nama_kelas'] = m['nama'].astype(str).str.slice(0, 150)
                df_dim_kk['mata_kuliah_key'] = m['id_mata_kuliah'].map(self.mk_map)
                df_dim_kk['jenis_pertemuan'] = m['id_jenis_pertemuan']
                df_dim_kk['ruang_nama'] = m['nama'].astype(str).str.slice(0, 150)
                df_dim_kk['gedung'] = m['gedung']
                df_dim_kk['kapasitas'] = m['kapasitas']
                df_dim_kk['hari'] = m['day_of_week']
                df_dim_kk['jam_mulai'] = m['start_time']
                df_dim_kk['jam_selesai'] = m['end_time']
                df_dim_kk.insert(0, 'kelas_kuliah_key', range(1, len(df_dim_kk) + 1))
            else:
                df_dim_kk = pd.DataFrame(columns=[
                    'kelas_kuliah_key', 'id_kelas_kuliah', 'nama_kelas', 'mata_kuliah_key',
                    'jenis_pertemuan', 'ruang_nama', 'gedung', 'kapasitas', 'hari', 'jam_mulai', 'jam_selesai'
                ])

            # DIMENSI kelas harus memuat seluruh kelas agar semua FK dari fact_krs dapat dipetakan.
            # Jangan gunakan batas MAX_ROWS_PER_TABLE pada tabel dimensi ini.
            df_dim_kk = self.bulk_load_to_postgres(
                conn, "dim_kelas_kuliah", df_dim_kk,
                key_col="kelas_kuliah_key", max_rows=None
            )
            self.reset_sequences(conn, "dim_kelas_kuliah", "kelas_kuliah_key")
            
            for _, r in df_dim_kk.iterrows():
                self.kelas_map[int(r['id_kelas_kuliah'])] = int(r['kelas_kuliah_key'])

            # --- DIM_SEMESTER ---
            log("Memproses dim_semester...")
            df_sem = self.load_csv(NEOSIA_FIXED_DIR, "semester.csv")
            if df_sem is not None:
                df_dim_sem = pd.DataFrame()
                df_dim_sem['id_semester'] = df_sem['id']
                df_dim_sem['tahun'] = df_sem['tahun']
                df_dim_sem['kode'] = df_sem['kode']
                df_dim_sem.insert(0, 'semester_key', range(1, len(df_dim_sem) + 1))
            else:
                df_dim_sem = pd.DataFrame(columns=['semester_key', 'id_semester', 'tahun', 'kode'])

            self.bulk_load_to_postgres(conn, "dim_semester", df_dim_sem, key_col="semester_key")
            self.reset_sequences(conn, "dim_semester", "semester_key")
            
            for _, r in df_dim_sem.iterrows():
                self.semester_map[int(r['id_semester'])] = int(r['semester_key'])

            # --- DIM_TIPE_KEHADIRAN ---
            log("Memproses dim_tipe_kehadiran...")
            df_tk = self.load_csv(NEOSIA_FIXED_DIR, "tipe_kehadiran.csv")
            if df_tk is not None:
                df_dim_tk = pd.DataFrame()
                df_dim_tk['id_tipe_kehadiran'] = df_tk['id']
                df_dim_tk['nama'] = df_tk['nama']
                df_dim_tk['kode'] = df_tk['kode']
                df_dim_tk.insert(0, 'tipe_kehadiran_key', range(1, len(df_dim_tk) + 1))
            else:
                df_dim_tk = pd.DataFrame(columns=['tipe_kehadiran_key', 'id_tipe_kehadiran', 'nama', 'kode'])

            self.bulk_load_to_postgres(conn, "dim_tipe_kehadiran", df_dim_tk, key_col="tipe_kehadiran_key")
            self.reset_sequences(conn, "dim_tipe_kehadiran", "tipe_kehadiran_key")
            
            for _, r in df_dim_tk.iterrows():
                self.kehadiran_map[int(r['id_tipe_kehadiran'])] = int(r['tipe_kehadiran_key'])

            # --- DIM_PEGAWAI ---
            log("Memproses dim_pegawai...")
            df_peg = self.load_csv(SIMPEG_FIXED_DIR, "data_pegawai.csv")
            df_ag = self.load_csv(SIMPEG_FIXED_DIR, "ref_agama.csv")
            df_nk = self.load_csv(SIMPEG_FIXED_DIR, "ref_status_nikah.csv")

            if df_peg is not None:
                m = df_peg.merge(df_ag, left_on='pegAgama', right_on='agamakode', how='left')
                m = m.merge(df_nk, left_on='pegStatusNikah', right_on='nikahkode', how='left')
                
                df_dim_peg = pd.DataFrame()
                df_dim_peg['id_pegawai'] = m['pegId']
                df_dim_peg['nip'] = m['pegNip']
                df_dim_peg['nama'] = m['pegNama']
                df_dim_peg['jenis_kelamin'] = m['pegJenisKelamin']
                df_dim_peg['agama_nama'] = m['agamanama']
                df_dim_peg['status_nikah_nama'] = m['nikahnama']
                df_dim_peg['tempat_lahir'] = m['pegTempatLahir']
                df_dim_peg['tanggal_lahir'] = m['pegTanggalLahir']
                df_dim_peg['is_dosen'] = m['pegIsDosen'].astype(bool)
                df_dim_peg.insert(0, 'pegawai_key', range(1, len(df_dim_peg) + 1))
            else:
                df_dim_peg = pd.DataFrame(columns=[
                    'pegawai_key', 'id_pegawai', 'nip', 'nama', 'jenis_kelamin',
                    'agama_nama', 'status_nikah_nama', 'tempat_lahir', 'tanggal_lahir', 'is_dosen'
                ])

            self.bulk_load_to_postgres(conn, "dim_pegawai", df_dim_peg, key_col="pegawai_key")
            self.reset_sequences(conn, "dim_pegawai", "pegawai_key")
            
            for _, r in df_dim_peg.iterrows():
                self.pegawai_map[int(r['id_pegawai'])] = int(r['pegawai_key'])

            # --- DIM_GOLONGAN ---
            log("Memproses dim_golongan...")
            df_gol = self.load_csv(SIMPEG_FIXED_DIR, "golongan.csv")
            if df_gol is not None:
                df_dim_gol = pd.DataFrame()
                df_dim_gol['golongan_code'] = df_gol['golongan']
                df_dim_gol['pangkat'] = df_gol['pangkat']
                df_dim_gol.insert(0, 'golongan_key', range(1, len(df_dim_gol) + 1))
            else:
                df_dim_gol = pd.DataFrame(columns=['golongan_key', 'golongan_code', 'pangkat'])

            self.bulk_load_to_postgres(conn, "dim_golongan", df_dim_gol, key_col="golongan_key")
            self.reset_sequences(conn, "dim_golongan", "golongan_key")
            
            for _, r in df_dim_gol.iterrows():
                self.golongan_map[r['golongan_code']] = int(r['golongan_key'])

            # --- DIM_JABATAN_FUNGSIONAL ---
            log("Memproses dim_jabatan_fungsional...")
            df_jf = self.load_csv(SIMPEG_FIXED_DIR, "ref_jabatan_fungsional.csv")
            if df_jf is not None:
                df_dim_jf = pd.DataFrame()
                df_dim_jf['id_jabfung'] = df_jf['jabfungrid']
                df_dim_jf['nama'] = df_jf['jabfungrnama']
                df_dim_jf['tingkat'] = df_jf['jabfungrtingkat']
                df_dim_jf['batas_usia_pensiun'] = df_jf['jabfungrbatasusiapensiun']
                df_dim_jf.insert(0, 'jabfung_key', range(1, len(df_dim_jf) + 1))
            else:
                df_dim_jf = pd.DataFrame(columns=['jabfung_key', 'id_jabfung', 'nama', 'tingkat', 'batas_usia_pensiun'])

            self.bulk_load_to_postgres(conn, "dim_jabatan_fungsional", df_dim_jf, key_col="jabfung_key")
            self.reset_sequences(conn, "dim_jabatan_fungsional", "jabfung_key")
            
            for _, r in df_dim_jf.iterrows():
                self.jabfung_map[int(r['id_jabfung'])] = int(r['jabfung_key'])

            # --- DIM_JABATAN_STRUKTURAL ---
            log("Memproses dim_jabatan_struktural...")
            df_js = self.load_csv(SIMPEG_FIXED_DIR, "ref_jabatan_struktural.csv")
            if df_js is not None:
                df_dim_js = pd.DataFrame()
                df_dim_js['id_jabstruk'] = df_js['jstrukid']
                df_dim_js['nama'] = df_js['jstruknama']
                df_dim_js['kelompok_nama'] = df_js['jstrukkodeunitkerja']
                df_dim_js['urutan'] = df_js['jstrukurutan']
                df_dim_js.insert(0, 'jabstruk_key', range(1, len(df_dim_js) + 1))
            else:
                df_dim_js = pd.DataFrame(columns=['jabstruk_key', 'id_jabstruk', 'nama', 'kelompok_nama', 'urutan'])

            self.bulk_load_to_postgres(conn, "dim_jabatan_struktural", df_dim_js, key_col="jabstruk_key")
            self.reset_sequences(conn, "dim_jabatan_struktural", "jabstruk_key")
            
            for _, r in df_dim_js.iterrows():
                self.jabstruk_map[int(r['id_jabstruk'])] = int(r['jabstruk_key'])

            # --- DIM_UNIT_KERJA ---
            log("Memproses dim_unit_kerja...")
            df_uk = self.load_csv(SIMPEG_FIXED_DIR, "ref_unit_kerja.csv")
            if df_uk is not None:
                df_dim_uk = pd.DataFrame()
                df_dim_uk['unit_kerja_id'] = df_uk['unitkerjaid']
                df_dim_uk['nama'] = df_uk['unitkerjanama']
                df_dim_uk['induk_kerja_nama'] = df_uk['unitkerjaindukkerjaid']
                df_dim_uk['eselon'] = df_uk['unitkerjaeselonid']
                df_dim_uk.insert(0, 'unit_kerja_key', range(1, len(df_dim_uk) + 1))
            else:
                df_dim_uk = pd.DataFrame(columns=['unit_kerja_key', 'unit_kerja_id', 'nama', 'induk_kerja_nama', 'eselon'])

            self.bulk_load_to_postgres(conn, "dim_unit_kerja", df_dim_uk, key_col="unit_kerja_key")
            self.reset_sequences(conn, "dim_unit_kerja", "unit_kerja_key")
            
            for _, r in df_dim_uk.iterrows():
                self.unit_map[r['unit_kerja_id']] = int(r['unit_kerja_key'])

            # --- DIM_JENIS_PEGAWAI ---
            log("Memproses dim_jenis_pegawai...")
            df_jp = self.load_csv(SIMPEG_FIXED_DIR, "ref_jenis_pegawai.csv")
            if df_jp is not None:
                df_dim_jp = pd.DataFrame()
                df_dim_jp['id_jenis_pegawai'] = df_jp['jnspegrid']
                df_dim_jp['nama'] = df_jp['jnspegrnama']
                df_dim_jp['tipe_kontrak'] = df_jp['jnspegrtipekontrak']
                df_dim_jp.insert(0, 'jenis_pegawai_key', range(1, len(df_dim_jp) + 1))
            else:
                df_dim_jp = pd.DataFrame(columns=['jenis_pegawai_key', 'id_jenis_pegawai', 'nama', 'tipe_kontrak'])

            self.bulk_load_to_postgres(conn, "dim_jenis_pegawai", df_dim_jp, key_col="jenis_pegawai_key")
            self.reset_sequences(conn, "dim_jenis_pegawai", "jenis_pegawai_key")
            
            for _, r in df_dim_jp.iterrows():
                self.jenis_peg_map[int(r['id_jenis_pegawai'])] = int(r['jenis_pegawai_key'])

            # --- DIM_STATUS_PEGAWAI ---
            log("Memproses dim_status_pegawai...")
            df_sp = self.load_csv(SIMPEG_FIXED_DIR, "ref_status_pegawai.csv")
            if df_sp is not None:
                df_dim_sp = pd.DataFrame()
                df_dim_sp['id_status'] = df_sp['statid']
                df_dim_sp['nama'] = df_sp['statnama']
                df_dim_sp.insert(0, 'status_pegawai_key', range(1, len(df_dim_sp) + 1))
            else:
                df_dim_sp = pd.DataFrame(columns=['status_pegawai_key', 'id_status', 'nama'])

            self.bulk_load_to_postgres(conn, "dim_status_pegawai", df_dim_sp, key_col="status_pegawai_key")
            self.reset_sequences(conn, "dim_status_pegawai", "status_pegawai_key")
            
            for _, r in df_dim_sp.iterrows():
                self.status_peg_map[int(r['id_status'])] = int(r['status_pegawai_key'])

            # --- DIM_PENDIDIKAN ---
            log("Memproses dim_pendidikan...")
            df_pend = self.load_csv(SIMPEG_FIXED_DIR, "ref_pendidikan.csv")
            if df_pend is not None:
                df_dim_pend = pd.DataFrame()
                df_dim_pend['id_pendidikan'] = df_pend['pendid']
                df_dim_pend['nama'] = df_pend['pendnama']
                df_dim_pend.insert(0, 'pendidikan_key', range(1, len(df_dim_pend) + 1))
            else:
                df_dim_pend = pd.DataFrame(columns=['pendidikan_key', 'id_pendidikan', 'nama'])

            self.bulk_load_to_postgres(conn, "dim_pendidikan", df_dim_pend, key_col="pendidikan_key")
            self.reset_sequences(conn, "dim_pendidikan", "pendidikan_key")
            
            for _, r in df_dim_pend.iterrows():
                self.pendidikan_map[int(r['id_pendidikan'])] = int(r['pendidikan_key'])

            # ==================================================================
            # 3. PROSES DAN PEMUATAN TABEL FAKTA (Scaled)
            # ==================================================================
            
            # --- FACT_KRS ---
            log("Memproses fact_krs...")

            # kartu_rencana_studi & kelas_kuliah_koordinator relatif lebih kecil -> load penuh
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

                # FIX: id_bobot_nilai_kelas_kuliah di file nilai (kelas_kuliah_nilai_SF1)
                # itu FK ke PK bobot_nilai_kelas_kuliah.id (per kelas+komponen, misal 1..5,
                # 6..10, ..., 209711..209715) -- BUKAN ke tipe_bobot_nilai.id (cuma 1-5).
                # Sebelumnya kode ini pakai bobot_label_map langsung (key 1-5), sehingga
                # nyaris semua id_bobot_nilai_kelas_kuliah gagal mapping (jadi NaN -> 0),
                # kecuali kelas yang PK bobotnya kebetulan berimpit dengan range 1-5.
                # Perbaikan: bangun mapping dua langkah -> id (PK bobot_nilai_kelas_kuliah)
                # -> nama_singkat komponen, lewat id_tipe_bobot_nilai.
                bnkk_to_komponen_map = dict(zip(df_bnkk['id'], df_bnkk['komponen']))
            else:
                df_persentase = None
                bnkk_to_komponen_map = {}

            m = None
            if df_krs is not None:
                # kelas_kuliah_nilai_SF1.csv: format LONG (id_peserta_kelas, id_bobot, nilai)
                # dan RAKSASA -> pivot streaming per komponen (uas/uts/presensi/kuis/uh)
                # berdasarkan id_peserta_kelas. SESUAIKAN category_col jika nama kolom
                # id tipe bobot di file ini bukan 'id_bobot'.
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
                # (kecil), column_maps untuk menempelkan nilai per komponen dari pivot streaming.
                m = self.load_and_topn_streaming(
                    folder=NEOSIA_SCALED_DIR,
                    filename="kelas_kuliah_has_mahasiswa_SF0.5.csv",
                    date_lookup_df=df_krs,
                    left_on='id_kartu_rencana_studi',
                    right_on='id',
                    date_col='created_at',
                    n=MAX_ROWS_PER_TABLE,
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
                df_fact_krs = pd.DataFrame()
                df_fact_krs['mahasiswa_key'] = m['id_mahasiswa'].map(self.mhs_map)
                df_fact_krs['kelas_kuliah_key'] = pd.to_numeric(m['id_kelas_kuliah'], errors='coerce').map(self.kelas_map)
                df_fact_krs['semester_key'] = m['id_semester'].map(self.semester_map) if 'id_semester' in m.columns else np.nan
                df_fact_krs['prodi_key'] = m['id_prodi'].map(self.prodi_map) if 'id_prodi' in m.columns else np.nan
                df_fact_krs['dosen_koordinator_key'] = m['id_dosen'].map(self.dosen_map) if 'id_dosen' in m.columns else np.nan

                # CATATAN: tabel bobot referensi tidak punya komponen "tugas"
                # (hanya: uas, presensi, kuis, uh, uts). Sementara di-nolkan.
                # Ganti ke nilai_kuis / nilai_uh jika "tugas" sebenarnya salah satu dari itu.
                df_fact_krs['nilai_tugas'] = m['nilai_kuis'] if 'nilai_kuis' in m.columns else 0.0
                df_fact_krs['nilai_uts'] = m['nilai_uts'] if 'nilai_uts' in m.columns else 0.0
                df_fact_krs['nilai_uas'] = m['nilai_uas'] if 'nilai_uas' in m.columns else 0.0
                df_fact_krs['nilai_akhir'] = m['nilai_akhir_hitung'] if 'nilai_akhir_hitung' in m.columns else 0.0

                df_fact_krs['is_approved'] = m['is_approved'] if 'is_approved' in m.columns else True
                df_fact_krs['is_current'] = m['is_current'] if 'is_current' in m.columns else True
                df_fact_krs['is_locked'] = m['is_locked'] if 'is_locked' in m.columns else False
                df_fact_krs['created_at'] = m['created_at'] if 'created_at' in m.columns else np.nan
                df_fact_krs.insert(0, 'krs_key', range(1, len(df_fact_krs) + 1))
            else:
                df_fact_krs = pd.DataFrame(columns=[
                    'krs_key', 'mahasiswa_key', 'kelas_kuliah_key', 'semester_key', 'prodi_key',
                    'dosen_koordinator_key', 'nilai_tugas', 'nilai_uts', 'nilai_uas', 'nilai_akhir',
                    'is_approved', 'is_current', 'is_locked', 'created_at'
                ])

            self.bulk_load_to_postgres(conn, "fact_krs", df_fact_krs, date_col='created_at', key_col='krs_key')
            self.reset_sequences(conn, "fact_krs", "krs_key")

            # --- FACT_PRESENSI ---
            log("Memproses fact_presensi...")

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
                # dipilih tetap identik dengan yang dimuat ke ClickHouse/MongoDB,
                # selama tiebreak_col ('id' presensi) sama di ketiga skrip.
                m = self.load_and_topn_streaming(
                    folder=NEOSIA_SCALED_DIR,
                    filename=pm_filename,
                    date_lookup_df=df_p_clean,
                    left_on='id_pertemuan',
                    right_on='id',
                    date_col='tanggal_rencana',
                    n=MAX_ROWS_PER_TABLE,
                    chunksize=1_000_000,
                    tiebreak_col='id',  # kolom PK presensi_mahasiswa; sesuaikan jika nama aslinya berbeda
                    label="fact_presensi",
                )
                del df_p, df_p_clean
                gc.collect()

            if m is not None and not m.empty:
                df_fact_p = pd.DataFrame()
                df_fact_p['mahasiswa_key'] = m['id_mahasiswa'].map(self.mhs_map)
                df_fact_p['kelas_kuliah_key'] = m['id_kelas_kuliah'].map(self.kelas_map)
                df_fact_p['pertemuan_ke'] = m['pertemuan_ke']
                df_fact_p['tanggal_pertemuan'] = m['tanggal_rencana'] if 'tanggal_rencana' in m.columns else np.nan
                df_fact_p['tipe_kehadiran_key'] = m['id_tipe_kehadiran'].map(self.kehadiran_map)
                df_fact_p['hadir_flag'] = (m['id_tipe_kehadiran'] == 1)
                df_fact_p.insert(0, 'presensi_key', range(1, len(df_fact_p) + 1))
                
                del m 
                gc.collect()
            else:
                df_fact_p = pd.DataFrame(columns=[
                    'presensi_key', 'mahasiswa_key', 'kelas_kuliah_key', 'pertemuan_ke',
                    'tanggal_pertemuan', 'tipe_kehadiran_key', 'hadir_flag'
                ])

            self.bulk_load_to_postgres(conn, "fact_presensi", df_fact_p, date_col='tanggal_pertemuan', key_col='presensi_key')
            self.reset_sequences(conn, "fact_presensi", "presensi_key")

            # --- FACT_PENASIHAT_AKADEMIK ---
            log("Memproses fact_penasihat_akademik...")
            df_pa = self.load_csv(NEOSIA_SCALED_DIR, "penasihat_akademik_SF1.csv")
            if df_pa is not None:
                df_fact_pa = pd.DataFrame()
                df_fact_pa['mahasiswa_key'] = df_pa['id_mahasiswa'].map(self.mhs_map)
                df_fact_pa['dosen_key'] = df_pa['id_dosen'].map(self.dosen_map)
                df_fact_pa['semester_key'] = df_pa['id_prodi_semester'].map(self.semester_map)
                df_fact_pa['is_active'] = df_pa['is_active']
                df_fact_pa.insert(0, 'pa_key', range(1, len(df_fact_pa) + 1))
            else:
                df_fact_pa = pd.DataFrame(columns=['pa_key', 'mahasiswa_key', 'dosen_key', 'semester_key', 'is_active'])

            self.bulk_load_to_postgres(conn, "fact_penasihat_akademik", df_fact_pa, key_col='pa_key')
            self.reset_sequences(conn, "fact_penasihat_akademik", "pa_key")

            # --- FACT_RIWAYAT_GOLONGAN ---
            log("Memproses fact_riwayat_golongan...")
            df_rg = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_golongan_SF1.csv")
            if df_rg is not None:
                df_fact_rg = pd.DataFrame()
                df_fact_rg['pegawai_key'] = df_rg['rgPegId'].map(self.pegawai_map)
                df_fact_rg['golongan_key'] = df_rg['rgGolongan'].map(self.golongan_map)
                df_fact_rg['tmt_golongan'] = df_rg['rgTMTGolongan']
                df_fact_rg['no_sk'] = df_rg['rgNoSK']
                df_fact_rg['tgl_sk'] = df_rg['rgTglSK']
                df_fact_rg['is_aktif'] = df_rg['rgAktif'].astype(bool)
                df_fact_rg.insert(0, 'riwayat_golongan_key', range(1, len(df_fact_rg) + 1))
            else:
                df_fact_rg = pd.DataFrame(columns=['riwayat_golongan_key', 'pegawai_key', 'golongan_key', 'tmt_golongan', 'no_sk', 'tgl_sk', 'is_aktif'])

            self.bulk_load_to_postgres(conn, "fact_riwayat_golongan", df_fact_rg, date_col='tmt_golongan', key_col='riwayat_golongan_key')
            self.reset_sequences(conn, "fact_riwayat_golongan", "riwayat_golongan_key")

            # --- FACT_RIWAYAT_JABFUNG ---
            log("Memproses fact_riwayat_jabatan_fungsional...")
            df_rjf = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_jabatan_fungsional_SF1.csv")
            if df_rjf is not None:
                df_fact_rjf = pd.DataFrame()
                df_fact_rjf['pegawai_key'] = df_rjf['rjfPegId'].map(self.pegawai_map)
                df_fact_rjf['jabfung_key'] = df_rjf['rjfJabatanId'].map(self.jabfung_map)
                df_fact_rjf['tmt_jabatan'] = df_rjf['rjfTMTJabatan']
                df_fact_rjf['angka_kredit'] = df_rjf['rjfAngkaKredit']
                df_fact_rjf['is_aktif'] = df_rjf['rjfAktif'].astype(bool)
                df_fact_rjf.insert(0, 'riwayat_jabfung_key', range(1, len(df_fact_rjf) + 1))
            else:
                df_fact_rjf = pd.DataFrame(columns=['riwayat_jabfung_key', 'pegawai_key', 'jabfung_key', 'tmt_jabatan', 'angka_kredit', 'is_aktif'])

            self.bulk_load_to_postgres(conn, "fact_riwayat_jabatan_fungsional", df_fact_rjf, date_col='tmt_jabatan', key_col='riwayat_jabfung_key')
            self.reset_sequences(conn, "fact_riwayat_jabatan_fungsional", "riwayat_jabfung_key")

            # --- FACT_RIWAYAT_UNIT_KERJA ---
            log("Memproses fact_riwayat_unit_kerja...")
            df_ruk = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_unit_kerja_SF1.csv")
            if df_ruk is not None:
                df_fact_ruk = pd.DataFrame()
                df_fact_ruk['pegawai_key'] = df_ruk['rwyUKPegawaiId'].map(self.pegawai_map)
                df_fact_ruk['unit_kerja_key'] = df_ruk['rwyUKunitkerjaId'].map(self.unit_map)
                df_fact_ruk['tmt'] = df_ruk['rwyUKTMT']
                df_fact_ruk['no_sk'] = df_ruk['rwyUKNoSK']
                df_fact_ruk['is_aktif'] = df_ruk['rwyUKAktif'].astype(bool)
                df_fact_ruk.insert(0, 'riwayat_unitkerja_key', range(1, len(df_fact_ruk) + 1))
            else:
                df_fact_ruk = pd.DataFrame(columns=['riwayat_unitkerja_key', 'pegawai_key', 'unit_kerja_key', 'tmt', 'no_sk', 'is_aktif'])

            self.bulk_load_to_postgres(conn, "fact_riwayat_unit_kerja", df_fact_ruk, date_col='tmt', key_col='riwayat_unitkerja_key')
            self.reset_sequences(conn, "fact_riwayat_unit_kerja", "riwayat_unitkerja_key")

            # --- FACT_RIWAYAT_JENIS_PEGAWAI ---
            log("Memproses fact_riwayat_jenis_pegawai...")
            df_rjp = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_jenis_pegawai_SF1.csv")
            if df_rjp is not None:
                df_fact_rjp = pd.DataFrame()
                df_fact_rjp['pegawai_key'] = df_rjp['rjpegPegId'].map(self.pegawai_map)
                df_fact_rjp['jenis_pegawai_key'] = df_rjp['rjpegJnspegrId'].map(self.jenis_peg_map)
                df_fact_rjp['no_sk'] = df_rjp['rjpegNoSK']
                df_fact_rjp['tgl_sk'] = df_rjp['rjpegTglSK']
                df_fact_rjp['is_aktif'] = df_rjp['rjpegAktif'].astype(bool)
                df_fact_rjp.insert(0, 'riwayat_jenispeg_key', range(1, len(df_fact_rjp) + 1))
            else:
                df_fact_rjp = pd.DataFrame(columns=['riwayat_jenispeg_key', 'pegawai_key', 'jenis_pegawai_key', 'no_sk', 'tgl_sk', 'is_aktif'])

            self.bulk_load_to_postgres(conn, "fact_riwayat_jenis_pegawai", df_fact_rjp, date_col='tgl_sk', key_col='riwayat_jenispeg_key')
            self.reset_sequences(conn, "fact_riwayat_jenis_pegawai", "riwayat_jenispeg_key")

            # --- FACT_RIWAYAT_STATUS_PEGAWAI ---
            log("Memproses fact_riwayat_status_pegawai...")
            df_rsp = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_status_pegawai_SF1.csv")
            if df_rsp is not None:
                df_fact_rsp = pd.DataFrame()
                df_fact_rsp['pegawai_key'] = df_rsp['rwystatpegId'].map(self.pegawai_map)
                df_fact_rsp['status_pegawai_key'] = df_rsp['rwystatStatId'].map(self.status_peg_map)
                df_fact_rsp['no_sk'] = df_rsp['rwystatNoSK']
                df_fact_rsp['tgl_sk'] = df_rsp['rwystatTglSK']
                df_fact_rsp['is_aktif'] = df_rsp['rwystatAktif'].astype(bool)
                df_fact_rsp.insert(0, 'riwayat_status_key', range(1, len(df_fact_rsp) + 1))
            else:
                df_fact_rsp = pd.DataFrame(columns=['riwayat_status_key', 'pegawai_key', 'status_pegawai_key', 'no_sk', 'tgl_sk', 'is_aktif'])

            self.bulk_load_to_postgres(conn, "fact_riwayat_status_pegawai", df_fact_rsp, date_col='tgl_sk', key_col='riwayat_status_key')
            self.reset_sequences(conn, "fact_riwayat_status_pegawai", "riwayat_status_key")

            # --- FACT_RIWAYAT_PROGRAM_STUDI ---
            log("Memproses fact_riwayat_program_studi...")
            df_rps = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_program_studi_SF1.csv")
            if df_rps is not None:
                df_fact_rps = pd.DataFrame()
                df_fact_rps['pegawai_key'] = df_rps['rwyprodiPegId'].map(self.pegawai_map)
                df_fact_rps['prodi_key'] = df_rps['rwyprodiProdiKode'].apply(lambda x: self.prodi_map.get(f"simpeg_{int(x)}") if pd.notna(x) else None)
                df_fact_rps['tmt'] = df_rps['rwyprodiTMT']
                df_fact_rps['no_sk'] = df_rps['rwyprodiNoSK']
                df_fact_rps['is_aktif'] = df_rps['rwyprodiAktif'].astype(bool)
                df_fact_rps.insert(0, 'riwayat_prodi_key', range(1, len(df_fact_rps) + 1))
            else:
                df_fact_rps = pd.DataFrame(columns=['riwayat_prodi_key', 'pegawai_key', 'prodi_key', 'tmt', 'no_sk', 'is_aktif'])

            self.bulk_load_to_postgres(conn, "fact_riwayat_program_studi", df_fact_rps, date_col='tmt', key_col='riwayat_prodi_key')
            self.reset_sequences(conn, "fact_riwayat_program_studi", "riwayat_prodi_key")

            # --- FACT_RIWAYAT_MUTASI_STRUKTURAL ---
            log("Memproses fact_riwayat_mutasi_struktural...")
            df_rms = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_mutasi_struktural_SF1.csv")
            if df_rms is not None:
                df_fact_rms = pd.DataFrame()
                df_fact_rms['pegawai_key'] = df_rms['rmsPegId'].map(self.pegawai_map)
                df_fact_rms['jabstruk_key'] = df_rms['rmsJstrukId'].map(self.jabstruk_map)
                df_fact_rms['tanggal_mulai'] = df_rms['rmsTanggalMulai']
                df_fact_rms['tanggal_selesai'] = df_rms['rmsTanggalSelesai']
                df_fact_rms['is_jbt_aktif'] = df_rms['rmsJbtAktif'].astype(bool)
                df_fact_rms['status_pejabat'] = df_rms['rmsStatusPejabat']
                df_fact_rms.insert(0, 'riwayat_mutasi_key', range(1, len(df_fact_rms) + 1))
            else:
                df_fact_rms = pd.DataFrame(columns=['riwayat_mutasi_key', 'pegawai_key', 'jabstruk_key', 'tanggal_mulai', 'tanggal_selesai', 'is_jbt_aktif', 'status_pejabat'])

            self.bulk_load_to_postgres(conn, "fact_riwayat_mutasi_struktural", df_fact_rms, date_col='tanggal_mulai', key_col='riwayat_mutasi_key')
            self.reset_sequences(conn, "fact_riwayat_mutasi_struktural", "riwayat_mutasi_key")

            # --- FACT_RIWAYAT_PENDIDIKAN ---
            log("Memproses fact_riwayat_pendidikan...")
            df_rp = self.load_csv(SIMPEG_SCALED_DIR, "riwayat_pendidikan_SF1.csv")
            if df_rp is not None:
                df_fact_rp = pd.DataFrame()
                df_fact_rp['pegawai_key'] = df_rp['rwpendPegId'].map(self.pegawai_map) if 'rwpendPegId' in df_rp.columns else np.nan
                df_fact_rp['pendidikan_key'] = df_rp['rwpendJenjang'].map(self.pendidikan_map) if 'rwpendJenjang' in df_rp.columns else np.nan
                df_fact_rp['nama_sekolah'] = df_rp['rwpendNamaSekolah'] if 'rwpendNamaSekolah' in df_rp.columns else np.nan
                
                if 'rwpendJurusan' in df_rp.columns:
                    df_fact_rp['jurusan'] = df_rp['rwpendJurusan']
                elif 'jurusan' in df_rp.columns:
                    df_fact_rp['jurusan'] = df_rp['jurusan']
                else:
                    df_fact_rp['jurusan'] = np.nan

                df_fact_rp['tahun_masuk'] = df_rp['rwpendThnMasuk'] if 'rwpendThnMasuk' in df_rp.columns else np.nan
                df_fact_rp['tahun_lulus'] = df_rp['rwpendThnLulus'] if 'rwpendThnLulus' in df_rp.columns else np.nan
                df_fact_rp['negara_nama'] = df_rp['rwpendNegara'] if 'rwpendNegara' in df_rp.columns else np.nan
                
                if 'rwpendTerakhir' in df_rp.columns:
                    df_fact_rp['is_terakhir'] = df_rp['rwpendTerakhir'].astype(bool)
                elif 'is_terakhir' in df_rp.columns:
                    df_fact_rp['is_terakhir'] = df_rp['is_terakhir'].astype(bool)
                else:
                    df_fact_rp['is_terakhir'] = True

                df_fact_rp.insert(0, 'riwayat_pendidikan_key', range(1, len(df_fact_rp) + 1))
            else:
                df_fact_rp = pd.DataFrame(columns=[
                    'riwayat_pendidikan_key', 'pegawai_key', 'pendidikan_key',
                    'nama_sekolah', 'jurusan', 'tahun_masuk', 'tahun_lulus', 'negara_nama', 'is_terakhir'
                ])

            self.bulk_load_to_postgres(conn, "fact_riwayat_pendidikan", df_fact_rp, date_col='tahun_masuk', key_col='riwayat_pendidikan_key')
            self.reset_sequences(conn, "fact_riwayat_pendidikan", "riwayat_pendidikan_key")

            log(f"Proses ETL untuk {SCALE_FACTOR} (Fixed + Scaled) BERHASIL!")
        
        except Exception as e:
            log_error(f"ETL Berhenti karena kesalahan kritis: {str(e)}")
        finally:
            conn.close()
            log("Koneksi database ditutup.")

if __name__ == "__main__":
    loader = PostgresETLLoader()
    loader.run()
