from __future__ import annotations

import datetime
from typing import Any

from google.cloud import bigquery

import config

_F = bigquery.SchemaField

_SCHEMA_ACTIVE = [
    _F("filename",             "STRING"),
    _F("udid",                 "STRING"),
    _F("error_category",       "STRING"),
    _F("uploader",             "STRING"),
    _F("date",                 "DATE"),
    _F("notes",                "STRING"),
    _F("error_log",            "STRING"),
    _F("humana_subcategories", "STRING"),
    _F("updated_at",           "TIMESTAMP"),   # from CertifyOS updatedAt — used for change detection
    _F("first_seen_at",        "TIMESTAMP"),
    _F("last_audited_at",      "TIMESTAMP"),
    _F("run_count",            "INTEGER"),
]

_SCHEMA_CHANGED = [
    _F("filename",                  "STRING"),
    _F("udid",                      "STRING"),
    _F("last_error_category",       "STRING"),
    _F("uploader",                  "STRING"),
    _F("date",                      "DATE"),
    _F("current_status",            "STRING"),
    _F("last_notes",                "STRING"),
    _F("last_error_log",            "STRING"),
    _F("last_humana_subcategories", "STRING"),
    _F("last_updated_at",           "TIMESTAMP"),   # carried over from active
    _F("first_seen_at",             "TIMESTAMP"),
    _F("last_audited_at",           "TIMESTAMP"),
    _F("status_changed_at",         "TIMESTAMP"),
]

_SCHEMA_HISTORY = [
    _F("run_timestamp",        "TIMESTAMP"),
    _F("total_active",         "INTEGER"),
    _F("total_status_changed", "INTEGER"),
    _F("new_this_run",         "INTEGER"),
    _F("updated_this_run",     "INTEGER"),
    _F("moved_this_run",       "INTEGER"),
]

_SCHEMA_PATTERNS = [
    _F("pattern",     "STRING"),
    _F("subcategory", "STRING"),
    _F("source",      "STRING"),
    _F("added_at",    "DATE"),
    _F("match_count", "INTEGER"),
]


def _now_ts() -> str:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()


class BQClient:
    def __init__(self, credentials=None):
        self.client = bigquery.Client(project=config.BQ_PROJECT, credentials=credentials)
        for table_id, schema in [
            (config.BQ_TABLE_ACTIVE,   _SCHEMA_ACTIVE),
            (config.BQ_TABLE_CHANGED,  _SCHEMA_CHANGED),
            (config.BQ_TABLE_HISTORY,  _SCHEMA_HISTORY),
            (config.BQ_TABLE_PATTERNS, _SCHEMA_PATTERNS),
        ]:
            self.client.create_table(bigquery.Table(table_id, schema=schema), exists_ok=True)

    def _q(self, sql: str) -> list[dict]:
        try:
            return [dict(r) for r in self.client.query(sql).result()]
        except Exception as e:
            print(f"[bq] query error: {e}")
            return []

    def _insert(self, table: str, rows: list[dict]) -> None:
        if not rows:
            return
        errs = self.client.insert_rows_json(table, rows)
        if errs:
            print(f"[bq] insert errors on {table}: {errs}")

    def load_pattern_library(self) -> dict[str, str]:
        rows = self._q(f"SELECT pattern, subcategory FROM `{config.BQ_TABLE_PATTERNS}`")
        if rows:
            return {r["pattern"].lower(): r["subcategory"] for r in rows if r["pattern"]}
        self.save_patterns(
            [{"pattern": p, "subcategory": c, "source": "Seed"} for p, c in config.HUMANA_SEED_PATTERNS],
            run_ts=_now_ts(),
        )
        return {p.lower(): c for p, c in config.HUMANA_SEED_PATTERNS}

    def save_patterns(self, new_patterns: list[dict], run_ts: str) -> None:
        existing = {r["p"] for r in self._q(f"SELECT LOWER(pattern) as p FROM `{config.BQ_TABLE_PATTERNS}`")}
        seen: set = set()
        rows = []
        for p in new_patterns:
            key = p["pattern"].lower().strip()
            if key and key not in existing and key not in seen:
                seen.add(key)
                rows.append({"pattern": p["pattern"], "subcategory": p["subcategory"],
                              "source": p.get("source", "Gemini"), "added_at": run_ts[:10], "match_count": 0})
        self._insert(config.BQ_TABLE_PATTERNS, rows)

    def clear_active_table(self) -> None:
        self.client.query(f"DELETE FROM `{config.BQ_TABLE_ACTIVE}` WHERE TRUE").result()
        print("[bq] active table cleared")

    def get_active_rows(self) -> dict[str, dict]:
        rows = self._q(f"SELECT * FROM `{config.BQ_TABLE_ACTIVE}`")
        return {r["udid"]: r for r in rows}

    def upsert_results(self, results: list[dict], run_ts: str) -> dict[str, int]:
        if not results:
            return {"new": 0, "updated": 0}

        existing = self.get_active_rows()
        to_update = [r["udid"] for r in results if r.get("udid") and r["udid"] in existing]

        if to_update:
            q = ", ".join(f"'{u}'" for u in to_update)
            self.client.query(f"DELETE FROM `{config.BQ_TABLE_ACTIVE}` WHERE udid IN ({q})").result()

        ts = _now_ts()
        rows = []
        for res in results:
            uid = res.get("udid", "")
            if not uid:
                continue
            old = existing.get(uid, {})
            rows.append({
                "filename":             res.get("filename", ""),
                "udid":                 uid,
                "error_category":       res.get("error_category", ""),
                "uploader":             res.get("uploader", ""),
                "date":                 res.get("date", ""),
                "notes":                res.get("notes", ""),
                "error_log":            res.get("error_log", "")[:50000],
                "humana_subcategories": res.get("humana_subcategories", ""),
                "updated_at":           res.get("updated_at") or None,
                "first_seen_at":        str(old.get("first_seen_at") or ts),
                "last_audited_at":      ts,
                "run_count":            int(old.get("run_count") or 0) + 1 if uid in existing else 1,
            })
        self._insert(config.BQ_TABLE_ACTIVE, rows)

        stats = {"new": len(rows) - len(to_update), "updated": len(to_update)}
        print(f"[bq] upsert: new={stats['new']} updated={stats['updated']}")
        return stats

    def move_to_status_changed(self, disappeared: set[str], snapshot: dict[str, dict], run_ts: str) -> int:
        if not disappeared:
            return 0
        ts = _now_ts()
        rows = []
        for uid in disappeared:
            old = snapshot.get(uid, {})
            rows.append({
                "filename":                  old.get("filename", ""),
                "udid":                      uid,
                "last_error_category":       old.get("error_category", ""),
                "uploader":                  old.get("uploader", ""),
                "date":                      str(old.get("date", "")),
                "current_status":            old.get("_current_status", ""),
                "last_notes":                old.get("notes", ""),
                "last_error_log":            str(old.get("error_log", ""))[:50000],
                "last_humana_subcategories": old.get("humana_subcategories", ""),
                "last_updated_at":           old.get("updated_at") or None,
                "first_seen_at":             str(old.get("first_seen_at") or ts),
                "last_audited_at":           str(old.get("last_audited_at") or ts),
                "status_changed_at":         ts,
            })
        self._insert(config.BQ_TABLE_CHANGED, rows)
        q = ", ".join(f"'{u}'" for u in disappeared)
        self.client.query(f"DELETE FROM `{config.BQ_TABLE_ACTIVE}` WHERE udid IN ({q})").result()
        print(f"[bq] moved {len(disappeared)} udid(s) to status_changed")
        return len(disappeared)

    def append_run_history(self, run_ts: str, stats: dict) -> None:
        total_a = (self._q(f"SELECT COUNT(*) as c FROM `{config.BQ_TABLE_ACTIVE}`")  or [{"c": 0}])[0]["c"]
        total_c = (self._q(f"SELECT COUNT(*) as c FROM `{config.BQ_TABLE_CHANGED}`") or [{"c": 0}])[0]["c"]
        self._insert(config.BQ_TABLE_HISTORY, [{
            "run_timestamp": _now_ts(), "total_active": total_a, "total_status_changed": total_c,
            "new_this_run": stats.get("new", 0), "updated_this_run": stats.get("updated", 0),
            "moved_this_run": stats.get("moved", 0),
        }])

    def get_dashboard_data(self) -> dict[str, Any]:
        def _clean(rows: list[dict]) -> list[dict]:
            return [{k: ("" if v is None else str(v)) for k, v in r.items()} for r in rows]

        active = self._q(f"""
            SELECT filename, udid, error_category, uploader, CAST(date AS STRING) AS date,
                   notes, error_log, humana_subcategories,
                   CAST(first_seen_at AS STRING)  AS first_seen_at,
                   CAST(last_audited_at AS STRING) AS last_audited_at, run_count
            FROM `{config.BQ_TABLE_ACTIVE}` ORDER BY last_audited_at DESC
        """)
        # updated_at is intentionally excluded — internal change-detection column only
        changed = self._q(f"""
            SELECT filename, udid, last_error_category, uploader, CAST(date AS STRING) AS date,
                   current_status, last_notes, last_error_log, last_humana_subcategories,
                   CAST(first_seen_at AS STRING) AS first_seen_at,
                   CAST(last_audited_at AS STRING) AS last_audited_at,
                   CAST(status_changed_at AS STRING) AS status_changed_at
            FROM `{config.BQ_TABLE_CHANGED}` ORDER BY status_changed_at DESC
        """)
        history = self._q(f"""
            SELECT CAST(run_timestamp AS STRING) AS run_timestamp
            FROM `{config.BQ_TABLE_HISTORY}` ORDER BY run_timestamp DESC LIMIT 1
        """)
        return {
            "active":      _clean(active),
            "changed":     _clean(changed),
            "lastUpdated": history[0]["run_timestamp"] if history else "",
            "errors":      [],
        }
