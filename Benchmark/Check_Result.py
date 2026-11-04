import argparse
import sys

import pandas as pd

PER_RUN_COLS = ['cpu_net_s', 'cpu_util_pct_of_core', 'ram_before_mb', 'ram_after_mb',
                'ram_anon_after_mb', 'ram_end_delta_mb', 'disk_read_mb', 'disk_write_mb']
RESOURCE_COLS = ['cpu_net_s', 'ram_before_mb', 'ram_peak_mb', 'ram_delta_peak_mb', 'ram_sample_count',
                 'disk_read_mb', 'disk_write_mb']

results = []


def report(level, msg):
    results.append(level)
    icon = {'PASS': '✓', 'WARN': '!', 'FAIL': '✗'}[level]
    print(f"  [{level}] {icon} {msg}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('csv')
    ap.add_argument('--iterations', type=int)
    ap.add_argument('--cold-runs', type=int)
    ap.add_argument('--resource-runs', type=int)
    ap.add_argument('--warmup', type=int)
    ap.add_argument('--min-fill', type=float, default=0.95, help='minimal proporsi kolom terisi (default 0.95)')
    a = ap.parse_args()

    df = pd.read_csv(a.csv)
    print(f"\nFile: {a.csv} | {len(df)} baris | skala: {', '.join(map(str, df.scale_factor.unique()))}")

    # 1. Kolom ----------------------------------------------------------------------------------
    print("\n[1] Kolom CSV")
    need = set(PER_RUN_COLS + ['ram_peak_mb', 'os_cache_dropped', 'result_signature', 'run_type'])
    missing = sorted(need - set(df.columns))
    if missing:
        report('FAIL', f"kolom tidak ada: {missing} — file dibuat oleh versi skrip lama")
        print("\nBerhenti: perbaiki versi skrip lebih dulu.")
        sys.exit(1)
    report('PASS', "semua kolom yang dibutuhkan ada")

    dbs = sorted(df.database.unique())
    qs = sorted(df.query_id.unique())

    # 2. Jumlah baris ---------------------------------------------------------------------------
    print("\n[2] Jumlah baris per jenis run (per database x kueri)")
    expect = {'Cold_Run': a.cold_runs, 'Warm_Run': a.iterations, 'Resource_Run': a.resource_runs, 'Warmup': a.warmup}
    counts = df.groupby(['run_type', 'database', 'query_id']).size()
    for rt, exp in expect.items():
        sub = counts.get(rt) if rt in counts.index.get_level_values(0) else None
        if sub is None:
            report('FAIL' if exp else 'WARN', f"{rt}: tidak ada baris")
            continue
        obs = sorted(set(sub.values))
        full = len(sub) == len(dbs) * len(qs)
        if exp is None:
            report('PASS', f"{rt}: jumlah per kombinasi = {obs} (tidak dinilai, parameter tidak diberikan)")
        elif obs == [exp] and full:
            report('PASS', f"{rt}: {exp} baris untuk setiap kombinasi ({len(sub)} kombinasi)")
        else:
            report('FAIL', f"{rt}: diharapkan {exp} per kombinasi, ditemukan {obs}; kombinasi ada {len(sub)}/{len(dbs) * len(qs)}")

    # 3. Cold benar-benar dingin ----------------------------------------------------------------
    print("\n[3] Cold run")
    cold = df[df.run_type == 'Cold_Run']
    if cold.empty:
        report('WARN', "tidak ada Cold_Run")
    else:
        dropped = cold.os_cache_dropped.astype(str).str.lower().eq('true').mean()
        if dropped == 1:
            report('PASS', "semua Cold_Run: restart container + page cache OS dikosongkan")
        elif dropped == 0:
            report('FAIL', "page cache OS TIDAK dikosongkan (hanya restart). Jalankan dengan sudo/root")
        else:
            report('WARN', f"hanya {dropped:.0%} Cold_Run yang cache OS-nya dikosongkan")
        w = df[df.run_type == 'Warm_Run'].groupby(['database', 'query_id']).latency_ms.median()
        c = cold.groupby(['database', 'query_id']).latency_ms.median()
        ratio = (c / w).dropna()
        share = (ratio > 1.0).mean() if len(ratio) else float('nan')
        if len(ratio):
            (report('PASS', f"cold lebih lambat dari warm pada {share:.0%} kombinasi (wajar)") if share >= 0.8 else
             report('WARN', f"cold lebih lambat dari warm hanya pada {share:.0%} kombinasi — cek apakah cache benar-benar dingin"))

    # 4. Kolom resource terisi ------------------------------------------------------------------
    print("\n[4] Kolom CPU / RAM / Disk terisi")
    for rt, cols in (('Cold_Run', PER_RUN_COLS), ('Warm_Run', PER_RUN_COLS), ('Resource_Run', RESOURCE_COLS)):
        sub = df[df.run_type == rt]
        if sub.empty:
            continue
        for db in dbs:
            s = sub[sub.database == db]
            if s.empty:
                continue
            fill = s[cols].notna().mean()
            bad = fill[fill < a.min_fill]
            if bad.empty:
                report('PASS', f"{rt} / {db}: semua kolom resource terisi")
            else:
                detail = ', '.join(f"{k} {v:.0%}" for k, v in bad.items())
                hint = ("cgroup tidak terbaca" if fill.max() == 0 else
                        "kemungkinan io.stat / memory.stat tidak tersedia di Docker Anda")
                report('FAIL', f"{rt} / {db}: kolom kurang terisi -> {detail}. {hint}")
    rs = df[df.run_type == 'Resource_Run']
    if not rs.empty:
        zero = (rs.ram_sample_count == 0).mean()
        (report('PASS', "RAM puncak: semua Resource_Run punya sampel") if zero == 0 else
         report('WARN', f"RAM puncak: {zero:.0%} Resource_Run tanpa sampel (kueri terlalu singkat)"))

    # 5. Kolom server --------------------------------------------------------------------------
    print("\n[5] Metrik sisi server")
    for db in dbs:
        s = df[df.database == db]
        n = s.server_exec_ms.notna().sum()
        (report('PASS', f"{db}: server_exec_ms terisi di {n} baris") if n else
         report('WARN', f"{db}: server_exec_ms kosong seluruhnya"))

    # 6. Kesetaraan hasil ----------------------------------------------------------------------
    print("\n[6] Kesetaraan hasil antar-database")
    sig = df[df.result_signature.notna()]
    for q in qs:
        g = sig[sig.query_id == q]
        per = g.groupby('database').result_signature.nunique()
        sigs = set(g.result_signature)
        rows = g.groupby('database').rows_returned.agg(lambda s: sorted(set(s)))
        if len(per) < 2:
            report('WARN', f"{q}: hanya {len(per)} database, tidak bisa dibandingkan")
        elif (per > 1).any():
            report('FAIL', f"{q}: hasil BERBEDA antar-eksekusi pada {list(per[per > 1].index)} (tidak deterministik)")
        elif len(sigs) == 1:
            report('PASS', f"{q}: hash identik di semua database ({int(rows.iloc[0][0])} baris)")
        else:
            report('FAIL', f"{q}: hash berbeda antar database; jumlah baris: {rows.to_dict()}. Jalankan validate_equivalence.py")

    # 7. Kestabilan & kewajaran ----------------------------------------------------------------
    print("\n[7] Kestabilan dan kewajaran")
    wr = df[df.run_type == 'Warm_Run']
    if len(wr):
        cv = wr.groupby(['database', 'query_id']).latency_ms.agg(lambda s: s.std() / s.mean() * 100 if len(s) > 1 else float('nan'))
        high = cv[cv > 15].dropna()
        (report('PASS', "CV warm <= 15% di semua kombinasi") if high.empty else
         report('WARN', f"CV warm > 15% pada {len(high)} kombinasi (angka kecil rentan jitter): " +
                ', '.join(f"{d}/{q}={v:.0f}%" for (d, q), v in high.head(6).items())))
    if (df.latency_ms <= 0).any():
        report('FAIL', "ada latensi <= 0")
    else:
        report('PASS', "semua latensi positif")

    # Indikasi pemindaian penuh pada kueri selektif (mis. indeks hilang)
    q6 = df[(df.query_id == 'Q6') & (df.run_type == 'Resource_Run') & (df.iteration == 1)]
    for _, r in q6.iterrows():
        if str(r.get('note', '')).find('keys_examined=0') >= 0 and (r.server_rows_read or 0) > 1e5:
            report('WARN', f"Q6/{r.database}: memeriksa {int(r.server_rows_read):,} dokumen tanpa indeks — periksa indeks nim")

    print()
    f, w = results.count('FAIL'), results.count('WARN')
    print(f"HASIL: {results.count('PASS')} PASS, {w} WARN, {f} FAIL")
    print("=> SIAP dijalankan penuh." if f == 0 else "=> JANGAN jalankan penuh dulu; perbaiki FAIL di atas.")
    sys.exit(1 if f else 0)


if __name__ == '__main__':
    main()
