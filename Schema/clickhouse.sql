-- ============================================================
-- STAR SCHEMA - ClickHouse
-- Studi Kasus: Satu Data Universitas Hasanuddin
-- Catatan: ClickHouse tidak enforce FK -> kolom *_key tetap dipertahankan
-- untuk join, dipilih ORDER BY berdasarkan kolom yang paling sering
-- difilter/di-join (mendukung pemangkasan data / data skipping index).
-- ============================================================

-- ============================================================
-- 1. CONFORMED DIMENSIONS
-- ============================================================

CREATE TABLE dim_prodi (
    prodi_key           UInt64,
    prodi_kode          String,
    id_prodi_neosia      Nullable(UInt64),
    id_prodi_simpeg      Nullable(Int32),
    nama_resmi           String,
    nama_singkat         String,
    jenjang_nama         LowCardinality(String),
    fakultas_nama        LowCardinality(String),
    jurusan_nama         LowCardinality(String),
    is_eksakta           UInt8,
    is_active            UInt8
) ENGINE = MergeTree()
ORDER BY (prodi_key);

CREATE TABLE dim_dosen (
    dosen_key            UInt64,
    id_dosen_neosia       Nullable(UInt64),
    id_pegawai_simpeg     Nullable(Int32),
    nip                   String,
    nidn                  String,
    nama                  String,
    prodi_key             UInt64,
    is_active             UInt8
) ENGINE = MergeTree()
ORDER BY (dosen_key);

-- ============================================================
-- 2. DIMENSIONS - AKADEMIK (Neosia)
-- ============================================================

CREATE TABLE dim_mahasiswa (
    mahasiswa_key         UInt64,
    id_mahasiswa          UInt64,
    nim                   String,
    nama_mahasiswa        String,
    angkatan              UInt16,
    jenis_kelamin         FixedString(1),
    tempat_lahir          String,
    tanggal_lahir         Date,
    kewarganegaraan       LowCardinality(String),
    prodi_key             UInt64,
    jalur_masuk_nama      LowCardinality(String),
    peminatan_nama        String,
    is_active             UInt8
) ENGINE = MergeTree()
ORDER BY (prodi_key, mahasiswa_key);

CREATE TABLE dim_mata_kuliah (
    mata_kuliah_key       UInt64,
    id_mata_kuliah        UInt64,
    nama_resmi            String,
    prodi_key             UInt64,
    jenjang_nama          LowCardinality(String),
    kurikulum_nama        String
) ENGINE = MergeTree()
ORDER BY (mata_kuliah_key);

CREATE TABLE dim_kelas_kuliah (
    kelas_kuliah_key      UInt64,
    id_kelas_kuliah       UInt64,
    nama_kelas            String,
    mata_kuliah_key       UInt64,
    jenis_pertemuan       LowCardinality(String),
    ruang_nama            String,
    gedung                String,
    kapasitas             UInt16,
    hari                  UInt8,
    jam_mulai             String,
    jam_selesai           String
) ENGINE = MergeTree()
ORDER BY (kelas_kuliah_key);

CREATE TABLE dim_semester (
    semester_key          UInt64,
    id_semester           UInt64,
    tahun                 UInt16,
    kode                  LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (semester_key);

CREATE TABLE dim_tipe_kehadiran (
    tipe_kehadiran_key    UInt64,
    id_tipe_kehadiran     UInt32,
    nama                  LowCardinality(String),
    kode                  FixedString(3)
) ENGINE = MergeTree()
ORDER BY (tipe_kehadiran_key);

-- ============================================================
-- 3. DIMENSIONS - HR (Simpeg)
-- ============================================================

CREATE TABLE dim_pegawai (
    pegawai_key           UInt64,
    id_pegawai            UInt32,
    nip                   String,
    nama                  String,
    jenis_kelamin         FixedString(1),
    agama_nama            LowCardinality(String),
    status_nikah_nama     LowCardinality(String),
    tempat_lahir          String,
    tanggal_lahir         Date,
    is_dosen              UInt8
) ENGINE = MergeTree()
ORDER BY (pegawai_key);

CREATE TABLE dim_golongan (
    golongan_key          UInt64,
    golongan_kode         String,
    pangkat               String
) ENGINE = MergeTree()
ORDER BY (golongan_key);

CREATE TABLE dim_jabatan_fungsional (
    jabfung_key           UInt64,
    id_jabfung            UInt32,
    nama                  String,
    tingkat               UInt8,
    batas_usia_pensiun    UInt8
) ENGINE = MergeTree()
ORDER BY (jabfung_key);

CREATE TABLE dim_jabatan_struktural (
    jabstruk_key          UInt64,
    id_jabstruk           UInt32,
    nama                  String,
    kelompok_nama         String,
    urutan                UInt16
) ENGINE = MergeTree()
ORDER BY (jabstruk_key);

CREATE TABLE dim_unit_kerja (
    unit_kerja_key        UInt64,
    unit_kerja_id         String,
    nama                  String,
    induk_kerja_nama      String,
    eselon                UInt8
) ENGINE = MergeTree()
ORDER BY (unit_kerja_key);

CREATE TABLE dim_jenis_pegawai (
    jenis_pegawai_key     UInt64,
    id_jenis_pegawai      UInt8,
    nama                  String,
    tipe_kontrak          LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (jenis_pegawai_key);

CREATE TABLE dim_status_pegawai (
    status_pegawai_key    UInt64,
    id_status             UInt8,
    nama                  LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (status_pegawai_key);

CREATE TABLE dim_pendidikan (
    pendidikan_key        UInt64,
    id_pendidikan         UInt8,
    nama                  LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (pendidikan_key);

-- ============================================================
-- 4. FACT TABLES - AKADEMIK (Neosia) [BERSKALA / SF]
-- ============================================================

CREATE TABLE fact_krs (
    krs_key                 UInt64,
    mahasiswa_key            UInt64,
    kelas_kuliah_key        UInt64,
    semester_key             UInt64,
    prodi_key                UInt64,
    dosen_koordinator_key   UInt64,
    nilai_tugas              Decimal(8,2),
    nilai_uts                Decimal(8,2),
    nilai_uas                Decimal(8,2),
    nilai_akhir              Decimal(8,2),
    is_approved              UInt8,
    is_current               UInt8,
    is_locked                UInt8,
    created_at               DateTime
) ENGINE = MergeTree()
ORDER BY (prodi_key, semester_key, mahasiswa_key);

CREATE TABLE fact_presensi (
    presensi_key             UInt64,
    mahasiswa_key             UInt64,
    kelas_kuliah_key         UInt64,
    pertemuan_ke              UInt16,
    tanggal_pertemuan         Date,
    tipe_kehadiran_key        UInt64,
    hadir_flag                UInt8
) ENGINE = MergeTree()
ORDER BY (kelas_kuliah_key, mahasiswa_key, pertemuan_ke);

CREATE TABLE fact_penasihat_akademik (
    pa_key                    UInt64,
    mahasiswa_key              UInt64,
    dosen_key                  UInt64,
    semester_key                UInt64,
    is_active                   UInt8
) ENGINE = MergeTree()
ORDER BY (dosen_key, semester_key);

-- ============================================================
-- 5. FACT TABLES - HR (Simpeg) [BERSKALA / SF, pisah per jenis riwayat]
-- ============================================================

CREATE TABLE fact_riwayat_golongan (
    riwayat_golongan_key     UInt64,
    pegawai_key               UInt64,
    golongan_key               UInt64,
    tmt_golongan                Date,
    no_sk                       String,
    tgl_sk                      Date,
    is_aktif                    UInt8
) ENGINE = MergeTree()
ORDER BY (pegawai_key, tmt_golongan);

CREATE TABLE fact_riwayat_jabatan_fungsional (
    riwayat_jabfung_key         UInt64,
    pegawai_key                  UInt64,
    jabfung_key                   UInt64,
    tmt_jabatan                    Date,
    angka_kredit                    Float32,
    is_aktif                        UInt8
) ENGINE = MergeTree()
ORDER BY (pegawai_key, tmt_jabatan);

CREATE TABLE fact_riwayat_unit_kerja (
    riwayat_unitkerja_key        UInt64,
    pegawai_key                    UInt64,
    unit_kerja_key                   UInt64,
    tmt                               Date,
    no_sk                             String,
    is_aktif                          UInt8
) ENGINE = MergeTree()
ORDER BY (pegawai_key, tmt);

CREATE TABLE fact_riwayat_jenis_pegawai (
    riwayat_jenispeg_key          UInt64,
    pegawai_key                     UInt64,
    jenis_pegawai_key                 UInt64,
    no_sk                              String,
    tgl_sk                             Date,
    is_aktif                           UInt8
) ENGINE = MergeTree()
ORDER BY (pegawai_key, tgl_sk);

CREATE TABLE fact_riwayat_status_pegawai (
    riwayat_status_key            UInt64,
    pegawai_key                     UInt64,
    status_pegawai_key                UInt64,
    no_sk                              String,
    tgl_sk                             Date,
    is_aktif                           UInt8
) ENGINE = MergeTree()
ORDER BY (pegawai_key, tgl_sk);

CREATE TABLE fact_riwayat_program_studi (
    riwayat_prodi_key              UInt64,
    pegawai_key                      UInt64,
    prodi_key                          UInt64,
    tmt                                 Date,
    no_sk                                String,
    is_aktif                             UInt8
) ENGINE = MergeTree()
ORDER BY (pegawai_key, tmt);

CREATE TABLE fact_riwayat_mutasi_struktural (
    riwayat_mutasi_key              UInt64,
    pegawai_key                       UInt64,
    jabstruk_key                        UInt64,
    tanggal_mulai                        Date,
    tanggal_selesai                      Nullable(Date),
    is_jbt_aktif                          UInt8,
    status_pejabat                        String
) ENGINE = MergeTree()
ORDER BY (pegawai_key, tanggal_mulai);

CREATE TABLE fact_riwayat_pendidikan (
    riwayat_pendidikan_key           UInt64,
    pegawai_key                        UInt64,
    pendidikan_key                       UInt64,
    nama_sekolah                          String,
    jurusan                               String,
    tahun_masuk                            String,
    tahun_lulus                            String,
    negara_nama                             LowCardinality(String),
    is_terakhir                             UInt8
) ENGINE = MergeTree()
ORDER BY (pegawai_key, pendidikan_key);
