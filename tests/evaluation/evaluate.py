"""DataSentinel evaluation: replay with anomalies in its own lake, then score what was caught
(spec: Testing and evaluation, "DataSentinel evaluation").

    python tests/evaluation/evaluate.py run --until 2017-09-30      # the tuning period
    python tests/evaluation/evaluate.py report --period tuning     # false alarms to tune on
    python tests/evaluation/evaluate.py run --until 2018-09-03      # then the test period
    python tests/evaluation/evaluate.py report --period test       # the held-out result

It lives under tests/ because it reads the generator's ground-truth manifest, which pipeline
code never may (CLAUDE.md). Everything it writes is its own: the chronos_eval database (source,
serving and metrics), lake/eval/ and the eval_ namespaces. Nothing reaches the business tables.

Nobody intervenes during the replay: a blocked day's batches stay held, so the report can count
the good data that false alarms held back, as well as the bad data that got through.
"""
import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from generator.anomalies import TYPES, Anomalies
from generator.defects import Defects
from generator.replay import replay
from generator.source_db import conninfo

OLIST = ROOT / "data" / "raw" / "olist"
# Set by --name: "eval" for the real evaluation, anything else for a trial run beside it.
DB, NAMESPACE, LAKE, MANIFEST = "chronos_eval", "eval_", ROOT / "lake" / "eval", None


def use(name: str) -> None:
    global DB, NAMESPACE, LAKE, MANIFEST
    DB, NAMESPACE, LAKE = f"chronos_{name}", f"{name}_", ROOT / "lake" / name
    MANIFEST = LAKE / "ground_truth.json"


@dataclass(frozen=True)
class Plan:
    """The evaluation's periods (spec: the test period has weekends and the Nov 2017 sale)."""
    start: date = date(2017, 1, 1)
    tuning_end: date = date(2017, 9, 30)
    feed_end: date = date(2018, 8, 31)
    per_type: int = 30
    # Runs after the feed ends, so a file missing on the last days still gets its 2-day alert.
    tail_days: int = 3

    @property
    def test_start(self) -> date:
        return self.tuning_end + timedelta(days=1)

    @property
    def last_run(self) -> date:
        return self.feed_end + timedelta(days=self.tail_days)

    def anomalies(self) -> Anomalies:
        return Anomalies(self.test_start, self.feed_end, per_type=self.per_type)


# Which checks are meant to catch each anomaly type: used for "caught by the right check".
# The spec's reversible defects, without the announced schema change: the evaluation was first
# run before it existed, and the feed must stay the same for a resumed or repeated run to agree.
# The schema change has its own tests (scenario tests and the truth test).
EVAL_DEFECTS = Defects(new_field_from=None, schema_v2_from=None)

EXPECTED_CHECKS = {
    "missing_files": ("completeness:missing_files", "completeness:missing_after_2_days"),
    "duplicate_file": ("completeness:unlisted_files",),
    "volume_spike": ("volume",),
    "mix_shift": ("drift:payment_type", "drift:price"),
    "schema_break": ("schema_contract:quarantine_share",),
    "null_burst": ("schema_contract:quarantine_share", "known_defect:delivered_without_date",
                   "null_rate:"),
}
# A late or missing listed file keeps raising these on later runs; they are explained, not new.
FILE_CHECKS = ("completeness:missing_files", "completeness:missing_after_2_days")
# A run judges only its own batch (held batches are never re-judged), so an anomaly counts as
# detected only by its own day's run. Crediting later runs would credit other anomalies' checks.
DETECTION_RUNS = 1


def ensure_database() -> None:
    with psycopg.connect(conninfo("postgres"), autocommit=True) as admin:
        if not admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB,)).fetchone():
            admin.execute(f'CREATE DATABASE "{DB}"')


def last_run_date(conn) -> date | None:
    try:
        row = conn.execute("SELECT max(business_date) FROM metrics.releases WHERE namespace = %s",
                           (NAMESPACE,)).fetchone()
    except psycopg.errors.UndefinedTable:
        conn.rollback()
        return None
    return row[0]


def run(plan: Plan, until: date) -> None:
    from generator.source_db import apply_schema
    from jobs.context import Context
    from jobs.maintenance import maintain
    from jobs.run_day import run_day
    from jobs.spark import build_session
    from sentinel.gate import SentinelBlocked

    ensure_database()
    landing, warehouse = LAKE / "landing", LAKE / "warehouse"
    spark = build_session(str(warehouse.resolve()), app_name="chronos-eval")
    with psycopg.connect(conninfo(DB)) as conn:
        apply_schema(conn)
        conn.commit()
        done = last_run_date(conn)
        first = done + timedelta(days=1) if done else plan.start
        ctx = Context(spark, landing, namespace_prefix=NAMESPACE, source_conninfo=conninfo(DB),
                      reference_dir=OLIST)
        LAKE.mkdir(parents=True, exist_ok=True)
        log = (LAKE / "runs.jsonl").open("a")

        def after_day(d: date) -> None:
            try:
                out = run_day(ctx, d)
                status = "already_published" if out.get("already_published") else "published"
            except SentinelBlocked as e:
                status, out = "blocked", {"blocked": str(e)}
            if d.weekday() == 6:
                maintain(ctx, d)
            log.write(json.dumps({"business_date": str(d), "status": status}) + "\n")
            log.flush()
            print(d, status, flush=True)

        replay(OLIST, landing, first, until, purchased_from=plan.start, conn=conn,
               after_day=after_day, defects=EVAL_DEFECTS, feed_end=plan.feed_end,
               manifest=MANIFEST, anomalies=plan.anomalies())


# ---------------------------------------------------------------- scoring

def load_results(conn) -> tuple[dict, dict]:
    """Failed checks by date, and each date's release status."""
    checks = defaultdict(list)
    for d, check, table, severity, status, observed, detail in conn.execute("""
            SELECT business_date, check_name, table_name, severity, status, observed, detail
            FROM metrics.check_results WHERE namespace = %s AND status = 'fail'""",
            (NAMESPACE,)):
        checks[d].append({"check": check, "table": table, "severity": severity,
                          "observed": observed, "detail": detail})
    releases = dict(conn.execute("SELECT business_date, status FROM metrics.releases "
                                 "WHERE namespace = %s", (NAMESPACE,)).fetchall())
    return checks, releases


def rescore_warm_up(checks: dict, first_release: date) -> dict:
    """The stored failures with the severities today's warm-up rule would give them.

    The runs recorded severities under the old warm-up, which counted published days in the
    trailing window (docs/decisions.md, "Warm-up counts days since the first release"). Today
    a drift or known-defect failure blocks once 28 days have passed since the first release,
    drift only from its block limit. Scores are unchanged: only severities are recomputed.

    Post-hoc and approximate: a day this turns into a block would have been held, so later
    days' batches and baselines would have differed a little. It is not held-out evidence.
    """
    from sentinel import config
    from sentinel.anomaly.scores import warm_up
    out = defaultdict(list)
    for d, failures in checks.items():
        past_warm_up = not warm_up((d - first_release).days)
        for f in failures:
            f = dict(f)
            if f["check"].startswith("known_defect:"):
                f["severity"] = "block" if past_warm_up else "warn"
            elif f["check"].startswith("drift:"):
                limit = config.DRIFT_BLOCK.get(f["check"].split(":", 1)[1])
                f["severity"] = ("block" if past_warm_up and limit is not None
                                 and f["observed"] >= limit else "warn")
            out[d].append(f)
    return out


def explained(failure: dict, expected_files: set[str]) -> bool:
    """A missing-file warning about a file the manifest says was late or never sent."""
    if failure["check"] not in FILE_CHECKS:
        return False
    files = set((failure["detail"] or {}).get("files", []))
    return bool(files) and files <= expected_files


def expected_missing(manifest: dict) -> set[str]:
    out = set()
    for n in manifest["defects"]:
        hours = ([n["hour"]] if n.get("defect") == "late_file" else
                 n.get("hours", []) if n.get("anomaly") == "missing_files" else [])
        out |= {f"date={n['business_date']}/events_{n['business_date']}T{h}.jsonl" for h in hours}
    return out


def published_event_ids(event_ids: set[str]) -> dict[str, str]:
    """{event id: the file its published copy came from}, for those in published silver (main).

    The file matters: an anomaly changes the lines of its own day's files, but a reversible
    defect may resend the same event, with Olist's content, in a later day's file. Silver keeps
    the first copy it accepts, so only a copy from the anomaly day's folder is the bad data.
    """
    if not event_ids:
        return {}
    from jobs.spark import build_session
    spark = build_session(str((LAKE / "warehouse").resolve()), app_name="chronos-eval-report")
    ids = spark.createDataFrame([(i,) for i in event_ids], "event_id string")
    events = spark.table(f"local.{NAMESPACE}silver.events").select("event_id", "source_file")
    return {r.event_id: r.source_file for r in events.join(ids, "event_id").collect()}


def score(plan: Plan, period: str, rescore: bool = False) -> dict:
    manifest = json.loads(MANIFEST.read_text())
    anomalies = {n["business_date"]: n for n in manifest["defects"] if "anomaly" in n}
    files = expected_missing(manifest)
    lo, hi = ((plan.start, plan.tuning_end) if period == "tuning"
              else (plan.test_start, plan.feed_end))
    with psycopg.connect(conninfo(DB)) as conn:
        checks, releases = load_results(conn)
        incomplete = {str(d) for (d,) in conn.execute(
            """SELECT business_date FROM metrics.releases WHERE namespace = %s
               AND status = 'published' AND NOT complete""", (NAMESPACE,)).fetchall()}
    if rescore:
        checks = rescore_warm_up(checks, min(d for d, st in releases.items()
                                             if st == "published"))

    def outcome(d: date) -> str:
        return {"blocked": "blocked"}.get(releases.get(d), "published")

    rows, matrix = [], defaultdict(Counter)
    fired_by = defaultdict(Counter)  # check -> anomaly type (or "clean") -> days it fired
    d = lo
    while d <= hi:
        if d not in releases:  # not run yet
            d += timedelta(days=1)
            continue
        note = anomalies.get(str(d))
        kind = note["anomaly"] if note else "clean"
        own = [f for f in checks.get(d, []) if not (kind == "clean" and explained(f, files))]
        for f in own:
            fired_by[f["check"].split(":")[0] if f["check"].startswith(("drift", "null_rate"))
                     else f["check"]][kind] += 1
        blocks = [f for f in own if f["severity"] == "block"]
        warns = [f for f in own if f["severity"] != "block"]
        result = "blocked" if blocks else "warned" if warns else "passed"
        matrix[kind][result] += 1
        release = ("blocked" if result == "blocked" else "published") if rescore else outcome(d)
        row = {"date": str(d), "type": kind, "result": result, "release": release,
               "checks": sorted({f["check"] for f in own})}
        if note:
            hits = []
            for k in range(DETECTION_RUNS):
                later = checks.get(d + timedelta(days=k), [])
                if any(f["check"].startswith(EXPECTED_CHECKS[kind]) for f in later):
                    hits.append(k)
            row["detected_after_runs"] = hits[0] if hits else None
            row["event_ids"] = note.get("event_ids", [])
        rows.append(row)
        d += timedelta(days=1)

    # Did any anomaly's data reach a published release?
    leaked = published_event_ids({i for r in rows for i in r.get("event_ids", [])})
    per_type = {}
    for kind in TYPES:
        days = [r for r in rows if r["type"] == kind]
        if not days:
            continue
        found = [r for r in days if r["detected_after_runs"] is not None]
        per_type[kind] = {
            "injected": len(days), "detected": len(found),
            "detection_rate": round(len(found) / len(days), 3),
            "blocked": sum(r["result"] == "blocked" for r in days),
            "mean_runs_to_detection": round(sum(r["detected_after_runs"] for r in found)
                                            / len(found), 2) if found else None,
            "published": sum(r["release"] == "published" for r in days),
            # Missing files leave nothing bad behind, only a gap: those days count as published
            # marked incomplete instead (spec: Checks table, Completeness).
            "published_with_bad_data": None if kind == "missing_files" else sum(
                r["release"] == "published" and any(
                    leaked.get(i, "").startswith(f"date={r['date']}/") for i in r["event_ids"])
                for r in days),
            "published_incomplete": sum(r["release"] == "published" and r["date"] in incomplete
                                        for r in days),
        }
    clean = [r for r in rows if r["type"] == "clean"]
    held_clean = sum(1 for r in clean if r["release"] == "blocked")
    for r in rows:
        r.pop("event_ids", None)
    return {"period": period, "from": str(lo), "to": str(hi),
            "scoring": ("re-scored post-hoc with today's warm-up rule" if rescore
                        else "as measured"),
            "matrix": {k: dict(v) for k, v in matrix.items()},
            "per_type": per_type,
            "clean_days": len(clean),
            "false_blocks": held_clean,
            "false_block_rate": round(held_clean / len(clean), 3) if clean else None,
            "false_warnings": sum(1 for r in clean if r["result"] == "warned"),
            "checks_firing": {c: dict(v) for c, v in sorted(fired_by.items())},
            "days": rows}


def write_report(result: dict, suffix: str = "") -> Path:
    name = f"{result['period']}{suffix}.json"
    out = ROOT / "docs" / "evaluation" / name if NAMESPACE == "eval_" else LAKE / name
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=1, default=str) + "\n")
    return out


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run")
    r.add_argument("--until", type=date.fromisoformat, required=True)
    s = sub.add_parser("report")
    s.add_argument("--period", choices=["tuning", "test"], required=True)
    s.add_argument("--rescore-warm-up", action="store_true",
                   help="recompute severities with today's warm-up rule (post-hoc)")
    for q in (r, s):
        q.add_argument("--name", default="eval", help="database chronos_<name>, lake/<name>")
        q.add_argument("--per-type", type=int, default=30)
        q.add_argument("--start", type=date.fromisoformat, default=Plan.start)
        q.add_argument("--tuning-end", type=date.fromisoformat, default=Plan.tuning_end)
        q.add_argument("--feed-end", type=date.fromisoformat, default=Plan.feed_end)
    a = p.parse_args(argv)
    use(a.name)
    plan = Plan(a.start, a.tuning_end, a.feed_end, a.per_type)
    if a.command == "run":
        run(plan, min(a.until, plan.last_run))
    else:
        result = score(plan, a.period, a.rescore_warm_up)
        path = write_report(result, "_rescored" if a.rescore_warm_up else "")
        summary = {k: v for k, v in result.items() if k != "days"}
        print(json.dumps(summary, indent=1, default=str))
        print(f"written to {path}")


if __name__ == "__main__":
    main()
