-- ============================================================
-- POSTGRESQL SCHEMA
-- Skripsi:
-- Analisis Performa dan Skalabilitas Arsitektur Database
-- pada Workload OLAP
--
-- Studi Kasus: Satu Data Universitas Hasanuddin
--
-- Domain:
--   1. Akademik (Neosia)
--   2. Kepegawaian (Simpeg)
--
-- Model:
--   Galaxy Schema / Star Schema
--   dengan Conformed Dimension
-- ============================================================


DROP TABLE IF EXISTS
    fact_riwayat_pendidikan,
    fact_riwayat_mutasi_struktural,
    fact_riwayat_program_studi,
    fact_riwayat_status_pegawai,
    fact_riwayat_jenis_pegawai,
    fact_riwayat_jabatan_fungsional,
    fact_riwayat_unit_kerja,
    fact_riwayat_golongan,
    fact_penasihat_akademik,
    fact_presensi,
    fact_krs,
    dim_pendidikan,
    dim_status_pegawai,
    dim_jenis_pegawai,
    dim_unit_kerja,
    dim_jabatan_struktural,
    dim_jabatan_fungsional,
    dim_golongan,
    dim_pegawai,
    dim_tipe_kehadiran,
    dim_semester,
    dim_kelas_kuliah,
    dim_mata_kuliah,
    dim_mahasiswa,
    dim_dosen,
    dim_prodi
CASCADE;


-- ============================================================
-- 1. CONFORMED DIMENSIONS
-- Dipakai lintas domain Neosia + Simpeg
-- ============================================================

CREATE TABLE dim_prodi (
    prodi_key           BIGSERIAL PRIMARY KEY,
    prodi_kode          VARCHAR(50) UNIQUE,
    id_prodi_neosia     BIGINT,
    id_prodi_simpeg     INT,
    nama_resmi          VARCHAR(255),
    nama_singkat        VARCHAR(255),
    jenjang_nama        VARCHAR(100),
    fakultas_nama       VARCHAR(255),
    jurusan_nama        VARCHAR(255),
    is_eksakta          BOOLEAN,
    is_active           BOOLEAN
);


CREATE TABLE dim_dosen (
    dosen_key           BIGSERIAL PRIMARY KEY,
    id_dosen_neosia     BIGINT,
    id_pegawai_simpeg   INT,
    nip                 VARCHAR(20),
    nidn                VARCHAR(20),
    nama                VARCHAR(150) NOT NULL,
    prodi_key           BIGINT REFERENCES dim_prodi(prodi_key),
    is_active            BOOLEAN
);


-- ============================================================
-- 2. DIMENSIONS - DOMAIN AKADEMIK (NEOSIA)
-- ============================================================

CREATE TABLE dim_mahasiswa (
    mahasiswa_key       BIGSERIAL PRIMARY KEY,
    id_mahasiswa        BIGINT NOT NULL,
    nim                 VARCHAR(30) UNIQUE,
    nama_mahasiswa      VARCHAR(150),
    angkatan            SMALLINT,
    jenis_kelamin       CHAR(1),
    tempat_lahir        VARCHAR(100),
    tanggal_lahir       DATE,
    kewarganegaraan     VARCHAR(50),
    prodi_key           BIGINT REFERENCES dim_prodi(prodi_key),
    jalur_masuk_nama    VARCHAR(100),
    peminatan_nama      VARCHAR(150),
    is_active            BOOLEAN
);


CREATE TABLE dim_mata_kuliah (
    mata_kuliah_key     BIGSERIAL PRIMARY KEY,
    id_mata_kuliah      BIGINT NOT NULL,
    nama_resmi          VARCHAR(255),
    prodi_key           BIGINT REFERENCES dim_prodi(prodi_key),
    jenjang_nama        VARCHAR(100),
    kurikulum_nama      VARCHAR(150)
);


CREATE TABLE dim_kelas_kuliah (
    kelas_kuliah_key    BIGSERIAL PRIMARY KEY,
    id_kelas_kuliah     BIGINT NOT NULL,
    nama_kelas          VARCHAR(150),
    mata_kuliah_key     BIGINT REFERENCES dim_mata_kuliah(mata_kuliah_key),
    jenis_pertemuan     VARCHAR(100),
    ruang_nama          VARCHAR(150),
    gedung              VARCHAR(100),
    kapasitas           SMALLINT,
    hari                SMALLINT,
    jam_mulai           TIME,
    jam_selesai         TIME
);


CREATE TABLE dim_semester (
    semester_key        BIGSERIAL PRIMARY KEY,
    id_semester         BIGINT NOT NULL,
    tahun               SMALLINT,
    kode                VARCHAR(20)
);


CREATE TABLE dim_tipe_kehadiran (
    tipe_kehadiran_key  BIGSERIAL PRIMARY KEY,
    id_tipe_kehadiran   INT NOT NULL,
    nama                VARCHAR(50),
    kode                CHAR(3)
);


-- ============================================================
-- 3. DIMENSIONS - DOMAIN KEPEGAWAIAN (SIMPEG)
-- ============================================================

CREATE TABLE dim_pegawai (
    pegawai_key         BIGSERIAL PRIMARY KEY,
    id_pegawai          INT NOT NULL,
    nip                 VARCHAR(20) UNIQUE,
    nama                VARCHAR(150),
    jenis_kelamin       CHAR(1),
    agama_nama          VARCHAR(30),
    status_nikah_nama   VARCHAR(20),
    tempat_lahir        VARCHAR(100),
    tanggal_lahir       DATE,
    is_dosen            BOOLEAN
);


CREATE TABLE dim_golongan (
    golongan_key        BIGSERIAL PRIMARY KEY,
    golongan_kode       VARCHAR(10) UNIQUE,
    pangkat             VARCHAR(50)
);


CREATE TABLE dim_jabatan_fungsional (
    jabfung_key         BIGSERIAL PRIMARY KEY,
    id_jabfung          INT NOT NULL,
    nama                VARCHAR(200),
    tingkat             INT,
    batas_usia_pensiun  INT
);


CREATE TABLE dim_jabatan_struktural (
    jabstruk_key        BIGSERIAL PRIMARY KEY,
    id_jabstruk         INT NOT NULL,
    nama                VARCHAR(150),
    kelompok_nama       VARCHAR(150),
    urutan              INT
);


CREATE TABLE dim_unit_kerja (
    unit_kerja_key      BIGSERIAL PRIMARY KEY,
    unit_kerja_id       VARCHAR(10) UNIQUE,
    nama                VARCHAR(255),
    induk_kerja_nama    VARCHAR(100),
    eselon              INT
);


CREATE TABLE dim_jenis_pegawai (
    jenis_pegawai_key   BIGSERIAL PRIMARY KEY,
    id_jenis_pegawai    SMALLINT NOT NULL,
    nama                VARCHAR(255),
    tipe_kontrak        VARCHAR(20)
);


CREATE TABLE dim_status_pegawai (
    status_pegawai_key  BIGSERIAL PRIMARY KEY,
    id_status           SMALLINT NOT NULL,
    nama                VARCHAR(50)
);


CREATE TABLE dim_pendidikan (
    pendidikan_key      BIGSERIAL PRIMARY KEY,
    id_pendidikan       SMALLINT NOT NULL,
    nama                VARCHAR(50)
);


-- ============================================================
-- 4. FACT TABLES - DOMAIN AKADEMIK (NEOSIA)
-- ============================================================

CREATE TABLE fact_krs (
    krs_key               BIGSERIAL PRIMARY KEY,
    mahasiswa_key         BIGINT REFERENCES dim_mahasiswa(mahasiswa_key),
    kelas_kuliah_key      BIGINT REFERENCES dim_kelas_kuliah(kelas_kuliah_key),
    semester_key          BIGINT REFERENCES dim_semester(semester_key),
    prodi_key             BIGINT REFERENCES dim_prodi(prodi_key),
    dosen_koordinator_key BIGINT REFERENCES dim_dosen(dosen_key),
    nilai_tugas           DECIMAL(8,2),
    nilai_uts             DECIMAL(8,2),
    nilai_uas             DECIMAL(8,2),
    nilai_akhir           DECIMAL(8,2),
    is_approved           BOOLEAN,
    is_current            BOOLEAN,
    is_locked             BOOLEAN,
    created_at            TIMESTAMP
);


CREATE TABLE fact_presensi (
    presensi_key        BIGSERIAL PRIMARY KEY,
    mahasiswa_key       BIGINT REFERENCES dim_mahasiswa(mahasiswa_key),
    kelas_kuliah_key    BIGINT REFERENCES dim_kelas_kuliah(kelas_kuliah_key),
    pertemuan_ke        INT,
    tanggal_pertemuan   DATE,
    tipe_kehadiran_key  BIGINT REFERENCES dim_tipe_kehadiran(tipe_kehadiran_key),
    hadir_flag          BOOLEAN
);


CREATE TABLE fact_penasihat_akademik (
    pa_key               BIGSERIAL PRIMARY KEY,
    mahasiswa_key        BIGINT REFERENCES dim_mahasiswa(mahasiswa_key),
    dosen_key             BIGINT REFERENCES dim_dosen(dosen_key),
    semester_key         BIGINT REFERENCES dim_semester(semester_key),
    is_active             BOOLEAN
);


-- ============================================================
-- 5. FACT TABLES - DOMAIN KEPEGAWAIAN (SIMPEG)
-- ============================================================

CREATE TABLE fact_riwayat_golongan (
    riwayat_golongan_key BIGSERIAL PRIMARY KEY,
    pegawai_key          BIGINT REFERENCES dim_pegawai(pegawai_key),
    golongan_key         BIGINT REFERENCES dim_golongan(golongan_key),
    tmt_golongan         DATE,
    no_sk                VARCHAR(100),
    tgl_sk               DATE,
    is_aktif             BOOLEAN
);


CREATE TABLE fact_riwayat_jabatan_fungsional (
    riwayat_jabfung_key  BIGSERIAL PRIMARY KEY,
    pegawai_key          BIGINT REFERENCES dim_pegawai(pegawai_key),
    jabfung_key          BIGINT REFERENCES dim_jabatan_fungsional(jabfung_key),
    tmt_jabatan          DATE,
    angka_kredit         FLOAT,
    is_aktif             BOOLEAN
);


CREATE TABLE fact_riwayat_unit_kerja (
    riwayat_unitkerja_key BIGSERIAL PRIMARY KEY,
    pegawai_key           BIGINT REFERENCES dim_pegawai(pegawai_key),
    unit_kerja_key        BIGINT REFERENCES dim_unit_kerja(unit_kerja_key),
    tmt                   DATE,
    no_sk                 VARCHAR(40),
    is_aktif              BOOLEAN
);


CREATE TABLE fact_riwayat_jenis_pegawai (
    riwayat_jenispeg_key BIGSERIAL PRIMARY KEY,
    pegawai_key          BIGINT REFERENCES dim_pegawai(pegawai_key),
    jenis_pegawai_key    BIGINT REFERENCES dim_jenis_pegawai(jenis_pegawai_key),
    no_sk                VARCHAR(100),
    tgl_sk               DATE,
    is_aktif             BOOLEAN
);


CREATE TABLE fact_riwayat_status_pegawai (
    riwayat_status_key   BIGSERIAL PRIMARY KEY,
    pegawai_key          BIGINT REFERENCES dim_pegawai(pegawai_key),
    status_pegawai_key   BIGINT REFERENCES dim_status_pegawai(status_pegawai_key),
    no_sk                VARCHAR(50),
    tgl_sk               DATE,
    is_aktif             BOOLEAN
);


CREATE TABLE fact_riwayat_program_studi (
    riwayat_prodi_key    BIGSERIAL PRIMARY KEY,
    pegawai_key          BIGINT REFERENCES dim_pegawai(pegawai_key),
    prodi_key             BIGINT REFERENCES dim_prodi(prodi_key),
    tmt                   DATE,
    no_sk                 VARCHAR(60),
    is_aktif              BOOLEAN
);


CREATE TABLE fact_riwayat_mutasi_struktural (
    riwayat_mutasi_key   BIGSERIAL PRIMARY KEY,
    pegawai_key          BIGINT REFERENCES dim_pegawai(pegawai_key),
    jabstruk_key          BIGINT REFERENCES dim_jabatan_struktural(jabstruk_key),
    tanggal_mulai        DATE,
    tanggal_selesai      DATE,
    is_jbt_aktif         BOOLEAN,
    status_pejabat       VARCHAR(100)
);


CREATE TABLE fact_riwayat_pendidikan (
    riwayat_pendidikan_key BIGSERIAL PRIMARY KEY,
    pegawai_key            BIGINT REFERENCES dim_pegawai(pegawai_key),
    pendidikan_key         BIGINT REFERENCES dim_pendidikan(pendidikan_key),
    nama_sekolah           VARCHAR(255),
    jurusan                VARCHAR(255),
    tahun_masuk            VARCHAR(4),
    tahun_lulus            VARCHAR(4),
    negara_nama            VARCHAR(100),
    is_terakhir            BOOLEAN
);


-- ============================================================
-- 6. INDEXES
-- Digunakan untuk mempercepat JOIN dan filtering workload OLAP
-- ============================================================

-- ----------------------------
-- Dimension indexes
-- ----------------------------

CREATE INDEX idx_dim_dosen_prodi
    ON dim_dosen(prodi_key);

CREATE INDEX idx_dim_mahasiswa_prodi
    ON dim_mahasiswa(prodi_key);

CREATE INDEX idx_dim_mk_prodi
    ON dim_mata_kuliah(prodi_key);

CREATE INDEX idx_dim_kelas_mk
    ON dim_kelas_kuliah(mata_kuliah_key);

CREATE INDEX idx_dim_pegawai_nip
    ON dim_pegawai(nip);


-- ----------------------------
-- Fact Akademik
-- ----------------------------

CREATE INDEX idx_fact_krs_mhs
    ON fact_krs(mahasiswa_key);

CREATE INDEX idx_fact_krs_kelas
    ON fact_krs(kelas_kuliah_key);

CREATE INDEX idx_fact_krs_semester
    ON fact_krs(semester_key);

CREATE INDEX idx_fact_krs_prodi
    ON fact_krs(prodi_key);

CREATE INDEX idx_fact_krs_dosen
    ON fact_krs(dosen_koordinator_key);


CREATE INDEX idx_fact_presensi_mhs
    ON fact_presensi(mahasiswa_key);

CREATE INDEX idx_fact_presensi_kelas
    ON fact_presensi(kelas_kuliah_key);

CREATE INDEX idx_fact_presensi_tipe
    ON fact_presensi(tipe_kehadiran_key);


CREATE INDEX idx_fact_pa_mhs
    ON fact_penasihat_akademik(mahasiswa_key);

CREATE INDEX idx_fact_pa_dosen
    ON fact_penasihat_akademik(dosen_key);

CREATE INDEX idx_fact_pa_semester
    ON fact_penasihat_akademik(semester_key);


-- ----------------------------
-- Fact Simpeg
-- ----------------------------

CREATE INDEX idx_fact_golongan_peg
    ON fact_riwayat_golongan(pegawai_key);

CREATE INDEX idx_fact_golongan_gol
    ON fact_riwayat_golongan(golongan_key);


CREATE INDEX idx_fact_jabfung_peg
    ON fact_riwayat_jabatan_fungsional(pegawai_key);

CREATE INDEX idx_fact_jabfung_jab
    ON fact_riwayat_jabatan_fungsional(jabfung_key);


CREATE INDEX idx_fact_unitkerja_peg
    ON fact_riwayat_unit_kerja(pegawai_key);

CREATE INDEX idx_fact_unitkerja_unit
    ON fact_riwayat_unit_kerja(unit_kerja_key);


CREATE INDEX idx_fact_jenispeg_peg
    ON fact_riwayat_jenis_pegawai(pegawai_key);

CREATE INDEX idx_fact_jenispeg_jenis
    ON fact_riwayat_jenis_pegawai(jenis_pegawai_key);


CREATE INDEX idx_fact_status_peg
    ON fact_riwayat_status_pegawai(pegawai_key);

CREATE INDEX idx_fact_status_status
    ON fact_riwayat_status_pegawai(status_pegawai_key);


CREATE INDEX idx_fact_prodi_peg
    ON fact_riwayat_program_studi(pegawai_key);

CREATE INDEX idx_fact_prodi_prodi
    ON fact_riwayat_program_studi(prodi_key);


CREATE INDEX idx_fact_mutasi_peg
    ON fact_riwayat_mutasi_struktural(pegawai_key);

CREATE INDEX idx_fact_mutasi_jab
    ON fact_riwayat_mutasi_struktural(jabstruk_key);


CREATE INDEX idx_fact_pendidikan_peg
    ON fact_riwayat_pendidikan(pegawai_key);

CREATE INDEX idx_fact_pendidikan_pendidikan
    ON fact_riwayat_pendidikan(pendidikan_key);


-- ============================================================
-- SELESAI
-- ============================================================

SELECT 'PostgreSQL schema berhasil dibuat.' AS status;
