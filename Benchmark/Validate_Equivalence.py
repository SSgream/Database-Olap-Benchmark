import argparse
import json
import math
import sys
from decimal import Decimal

import benchmark_runner_v2 as bench

TOL = 0.01 + 1e-9

# Kolom kunci (dimensi) per kueri. None = tanpa kunci -> dibandingkan sebagai multiset terurut.
KEYS = {
    "Q1": [],
    "Q2": ["fakultas_nama", "nama_prodi", "angkatan", "tahun_semester"],
    "Q3": ["nama_dosen", "nip_dosen", "nama_prodi", "kode_semester"],
    "Q4": None,
    "Q5": ["nim", "nama_prodi", "angkatan", "nama_mahasiswa"],
    "Q6": ["nama_kelas"],
}
CHECK_ORDER = {"Q5"}  # kueri yang urutan barisnya bermakna (ORDER BY + LIMIT)

DBS = ["postgres", "clickhouse", "mongodb"]


# ------------------------------------------------------------------ util
def _num(v):
    """Kembalikan float bila v numerik, selain itu None."""
    if isinstance(v, bool):
        return None
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (int, float)):
        return float(v)
    if hasattr(v, "to_decimal"):  # bson Decimal128
        return float(v.to_decimal())
    return None


def _same(a, b):
    na, nb = _num(a), _num(b)
    if na is not None and nb is not None:
        if math.isnan(na) and math.isnan(nb):
            return True
        return abs(na - nb) <= TOL
    return a == b


def _canon(row, cols):
    """Bentuk terurut untuk pengurutan tanpa kunci (angka dibulatkan 2 desimal)."""
    out = []
    for c in cols:
        v = row.get(c)
        n = _num(v)
        out.append((0, round(n, 2)) if n is not None else (1, "" if v is None else str(v)))
    return tuple(out)


def _fmt(v):
    return f"{v!r}" if not isinstance(v, float) else f"{v:.6g}"


# ------------------------------------------------------------------ perbandingan
def compare(q, rows_a, rows_b, name_a, name_b, show):
    res = {"pair": f"{name_a} vs {name_b}", "ok": True, "identical": False,
           "rows": [len(rows_a), len(rows_b)], "issues": []}

    def fail(msg):
        res["ok"] = False
        res["issues"].append(msg)

    if len(rows_a) != len(rows_b):
        fail(f"jumlah baris berbeda: {len(rows_a)} vs {len(rows_b)}")

    keys = KEYS.get(q)
    cols = sorted(set().union(*[r.keys() for r in rows_a + rows_b])) if (rows_a or rows_b) else []

    pairs = []  # (label, row_a, row_b)
    if keys is not None:
        map_a = {tuple(r.get(k) for k in keys): r for r in rows_a}
        map_b = {tuple(r.get(k) for k in keys): r for r in rows_b}
        dup = (len(map_a) != len(rows_a)) or (len(map_b) != len(rows_b))
        if dup:
            fail("kunci tidak unik (ada baris dengan kunci yang sama); periksa daftar KEYS")
        only_a = sorted(set(map_a) - set(map_b), key=str)
        only_b = sorted(set(map_b) - set(map_a), key=str)
        if only_a:
            fail(f"{len(only_a)} kunci hanya di {name_a}, contoh: {only_a[:min(show,5)]}")
        if only_b:
            fail(f"{len(only_b)} kunci hanya di {name_b}, contoh: {only_b[:min(show,5)]}")
        for k in map_a.keys() & map_b.keys():
            pairs.append((k, map_a[k], map_b[k]))
        if q in CHECK_ORDER and not only_a and not only_b:
            seq_a = [tuple(r.get(k) for k in keys) for r in rows_a]
            seq_b = [tuple(r.get(k) for k in keys) for r in rows_b]
            if seq_a != seq_b:
                idx = next(i for i, (x, y) in enumerate(zip(seq_a, seq_b)) if x != y)
                fail(f"urutan baris berbeda mulai indeks {idx}: {seq_a[idx]} vs {seq_b[idx]}")
    else:
        sa = sorted(rows_a, key=lambda r: _canon(r, cols))
        sb = sorted(rows_b, key=lambda r: _canon(r, cols))
        for i, (ra, rb) in enumerate(zip(sa, sb)):
            pairs.append((f"baris#{i}", ra, rb))

    cell_diffs = []
    exact = True
    for label, ra, rb in pairs:
        for c in cols:
            a, b = ra.get(c), rb.get(c)
            if not _same(a, b):
                cell_diffs.append((label, c, a, b))
            elif _num(a) is not None and _num(b) is not None and abs(_num(a) - _num(b)) > 1e-9:
                exact = False  # setara dalam toleransi, tapi tidak persis

    if cell_diffs:
        fail(f"{len(cell_diffs)} sel numerik/teks berbeda")
        for label, c, a, b in cell_diffs[:show]:
            res["issues"].append(f"   {label} | {c}: {name_a}={_fmt(a)}  {name_b}={_fmt(b)}")

    res["identical"] = res["ok"] and exact
    return res


# ------------------------------------------------------------------ eksekusi
def fetch(q, db):
    info = bench.QUERIES[q]
    if db == "postgres":
        return bench.execute_postgres_query(info["postgres"])
    if db == "clickhouse":
        return bench.execute_clickhouse_query(info["clickhouse"])
    return bench.execute_mongodb_query(info["mongodb"])


def run(queries, dbs, show, fetcher=fetch):
    report, all_ok = {}, True
    for q in queries:
        print(f"\n[{q}] {bench.QUERIES[q]['name']}")
        data, errors = {}, {}
        for db in dbs:
            try:
                data[db] = fetcher(q, db)
                print(f"   {db:<11}: {len(data[db])} baris")
            except Exception as exc:  # noqa: BLE001
                errors[db] = str(exc)
                print(f"   {db:<11}: GAGAL -> {exc}")

        pair_results = []
        names = list(data)
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                pair_results.append(compare(q, data[names[i]], data[names[j]], names[i], names[j], show))

        ok = bool(pair_results) and not errors and all(p["ok"] for p in pair_results)
        all_ok = all_ok and ok
        for p in pair_results:
            tag = "IDENTIK" if p["identical"] else ("SETARA (toleransi)" if p["ok"] else "BEDA")
            print(f"   - {p['pair']:<24}: {tag}")
            for msg in p["issues"]:
                print(f"       {msg}")
        print(f"   => {q}: {'SETARA' if ok else 'TIDAK SETARA'}")
        report[q] = {"ok": ok, "errors": errors, "pairs": pair_results}
    return report, all_ok


def main():
    ap = argparse.ArgumentParser(description="Validasi kesetaraan hasil Q1-Q6 antar database")
    ap.add_argument("--query", choices=list(bench.QUERIES) + ["all"], default="all")
    ap.add_argument("--db", choices=DBS + ["all"], default="all")
    ap.add_argument("--show", type=int, default=10, help="maks. perbedaan yang ditampilkan per pasangan")
    ap.add_argument("--report", default="validation_report.json")
    args = ap.parse_args()

    queries = list(bench.QUERIES) if args.query == "all" else [args.query]
    dbs = DBS if args.db == "all" else [args.db]

    report, all_ok = run(queries, dbs, args.show)

    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print("\n" + "=" * 60)
    for q, r in report.items():
        print(f" {q}: {'SETARA' if r['ok'] else 'TIDAK SETARA'}")
    print("=" * 60)
    print(f" Laporan lengkap: {args.report}")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
