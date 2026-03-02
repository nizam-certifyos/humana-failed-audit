from __future__ import annotations

import concurrent.futures
import csv
import io
import json
import random
import re
import threading
import time
import datetime
from typing import Optional

import requests
import vertexai
from google.cloud import secretmanager
from google.oauth2 import service_account
from vertexai.generative_models import GenerativeModel

import config
from bq_client import BQClient


def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.datetime.utcnow().strftime("%H:%M:%S")
    tag = {"OK": "✅", "WARN": "⚠️", "ERROR": "❌"}.get(level, "  ")
    print(f"[{ts}] {tag} {msg}")


# ── Credentials ────────────────────────────────────────────────────────────────

def _load_secret(name: str) -> dict:
    sm   = secretmanager.SecretManagerServiceClient()
    path = f"projects/{config.SECRET_PROJECT_ID}/secrets/{name}/versions/latest"
    return json.loads(sm.access_secret_version(request={"name": path}).payload.data.decode())


def _load_sa_credentials() -> service_account.Credentials:
    return service_account.Credentials.from_service_account_info(
        _load_secret(config.SECRET_NAME),
        scopes=["https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/bigquery"],
    )


def _load_certifyos_creds() -> tuple[str, str]:
    data = _load_secret(config.CERTIFYOS_M2M_SECRET_NAME)
    return data["client_id"], data["client_secret"]


# ── CertifyOS API ──────────────────────────────────────────────────────────────

class CertifyOSClient:
    def __init__(self, token: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "tenant-id":     config.TENANT_ID,
            "Content-Type":  "application/json",
        })

    def _get(self, path: str, params: dict = None, stream: bool = False):
        url = f"{config.CERTIFYOS_BASE_URL}{path}"
        for attempt in range(config.MAX_RETRIES):
            try:
                r = self.session.get(url, params=params, stream=stream, timeout=60)
                if r.status_code in (429,) or r.status_code >= 500:
                    wait = min(config.BASE_BACKOFF * 2 ** attempt + random.uniform(0, 1), config.MAX_BACKOFF)
                    log(f"HTTP {r.status_code} on {path} — retry in {wait:.1f}s", "WARN")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r
            except requests.RequestException as e:
                if attempt == config.MAX_RETRIES - 1:
                    raise
                wait = min(config.BASE_BACKOFF * 2 ** attempt, config.MAX_BACKOFF)
                log(f"Request error {path}: {e} — retry in {wait:.1f}s", "WARN")
                time.sleep(wait)

    @staticmethod
    def authenticate(client_id: str, client_secret: str) -> "CertifyOSClient":
        r = requests.post(
            f"{config.CERTIFYOS_BASE_URL}/auth/client-credentials",
            headers={"Content-Type": "application/json", "tenant-id": config.TENANT_ID},
            json={"clientId": client_id, "clientSecret": client_secret},
            timeout=30,
        )
        r.raise_for_status()
        token = r.json().get("accessToken")
        if not token:
            raise ValueError(f"No accessToken in auth response: {r.text[:200]}")
        return CertifyOSClient(token)

    def fetch_failed_rosters(self) -> list[dict]:
        from urllib.parse import urlencode
        import json as _json
        rosters, page, total_count = [], 0, None
        while True:
            filter_str = _json.dumps({
                "data.status": {"in": [config.FAILED_STATUS]},
                "tenantId":    {"eq": config.TENANT_ID},
            })
            url = f"/roster?{urlencode({'filter': filter_str, 'page': page, 'size': config.PAGE_SIZE})}"
            data  = self._get(url).json()
            items = data.get("data") or []
            if total_count is None:
                total_count = data.get("totalCount")
            if not items:
                break
            for r in items:
                uid = r.get("id", "")
                if not uid:
                    continue
                filename = (
                    r.get("initialFilename")
                    or r.get("data", {}).get("initialFilename")
                    or r.get("fileName")
                    or r.get("name")
                    or f"unknown_{uid}.csv"
                )
                file_path = (
                    r.get("filePath")
                    or r.get("data", {}).get("filePath")
                    or r.get("data", {}).get("fileUrl")
                    or ""
                )
                uploader = (
                    r.get("createdBy")
                    or r.get("data", {}).get("createdBy")
                    or ""
                )
                created_at = (
                    r.get("createdAt")
                    or r.get("data", {}).get("createdAt")
                    or ""
                )
                updated_at = (
                    r.get("updatedAt")
                    or r.get("data", {}).get("updatedAt")
                    or ""
                )
                rosters.append({
                    "udid":       uid,
                    "filename":   filename,
                    "filepath":   file_path,
                    "uploader":   uploader,
                    "date":       str(created_at)[:10],
                    "updated_at": updated_at,
                })
            if total_count and len(rosters) >= int(total_count):
                break
            if len(items) < config.PAGE_SIZE:
                break
            page += 1
        log(f"Fetched {len(rosters)} FAILED rosters", "OK")
        return rosters

    def get_udid_status(self, udid: str) -> str:
        try:
            data   = self._get(f"/roster/{udid}").json()
            status = (
                data.get("status")
                or data.get("data", {}).get("status")
                or "Unknown"
            )
            return str(status).upper()
        except Exception as e:
            log(f"Status fetch failed for {udid[:8]}: {e}", "WARN")
            return "Unknown"

    def download_csv(self, udid: str, filepath: str) -> Optional[str]:
        from urllib.parse import urlparse, unquote, quote

        # ── Step 1: get signed GCS URL from download-records ─────────────────
        signed_url = None
        try:
            resp = self._get(f"/roster/{udid}/download-records")
            body = resp.json()
            signed_url = body.get("fileUrl", "")
            if not signed_url:
                log(f"  download-records JSON has no fileUrl for {udid[:8]}: {str(body)[:200]}", "WARN")
        except Exception as e:
            log(f"  download-records failed {udid[:8]}: {e}", "WARN")

        # ── Step 2: fetch actual CSV from signed URL ──────────────────────────
        if signed_url:
            try:
                r = requests.get(signed_url, timeout=120)
                r.raise_for_status()
                return r.content.decode("utf-8-sig", errors="replace")
            except Exception as e:
                log(f"  GCS signed URL fetch failed {udid[:8]}: {e}", "WARN")

        # ── Step 3: /storage/download fallback with roster-exports path ───────
        export_path = ""
        if signed_url:
            try:
                gcs_path    = unquote(urlparse(signed_url).path)
                export_path = gcs_path.split("/pdm_roster_bucket/", 1)[-1]
            except Exception:
                pass
        if not export_path and filepath:
            fn = filepath.split("/", 2)[-1]
            fn = fn.split("-", 1)[-1] if "-" in fn[:20] else fn
            export_path = f"roster-exports/{udid}/{fn}"

        if export_path:
            try:
                fallback_url = f"/storage/download?filePath={quote(export_path, safe='')}"
                return self._get(fallback_url).content.decode("utf-8-sig", errors="replace")
            except Exception as e:
                log(f"  Fallback download failed {udid[:8]}: {e}", "WARN")

        return None


# ── CSV parsing ────────────────────────────────────────────────────────────────

def _pick_col(headers: dict, *candidates: str) -> Optional[str]:
    lower = {k.lower(): k for k in headers}
    return next((lower[c.lower()] for c in candidates if c.lower() in lower), None)


def parse_csv(csv_text: str) -> dict:
    reader     = csv.DictReader(io.StringIO(csv_text))
    hdr        = {k: k for k in (reader.fieldnames or [])}
    col_dv     = _pick_col(hdr, "Data Validations", "DataValidations")
    col_bv     = _pick_col(hdr, "Business Validations", "BusinessValidations")
    col_npi    = _pick_col(hdr, "NPI", "Humana: Practitioner NPI", "Practitioner NPI")
    col_fn     = _pick_col(hdr, "First Name", "Humana: First Name")
    col_ln     = _pick_col(hdr, "Last Name",  "Humana: Last Name")

    data_errors, biz_errors, rows_info = [], [], []
    seen_dv, seen_bv = set(), set()

    for row in reader:
        dv  = (row.get(col_dv, "") or "").strip() if col_dv else ""
        bv  = (row.get(col_bv, "") or "").strip() if col_bv else ""
        npi = (row.get(col_npi, "") or "").strip() if col_npi else ""
        fn  = (row.get(col_fn,  "") or "").strip() if col_fn  else ""
        ln  = (row.get(col_ln,  "") or "").strip() if col_ln  else ""

        for line in (dv.splitlines() or []):
            if line.strip() and line.strip() not in seen_dv:
                seen_dv.add(line.strip())
                data_errors.append(line.strip())
        for line in (bv.splitlines() or []):
            if line.strip() and line.strip() not in seen_bv:
                seen_bv.add(line.strip())
                biz_errors.append(line.strip())
        if dv or bv:
            rows_info.append({"npi": npi, "first": fn, "last": ln, "dv": dv, "bv": bv})

    parts = []
    if data_errors:
        parts.append("=== Data Validations ===\n" + "\n".join(data_errors))
    if biz_errors:
        parts.append("=== Business Validations ===\n" + "\n".join(biz_errors))

    return {
        "data_errors":      data_errors,
        "business_errors":  biz_errors,
        "error_log":        "\n\n".join(parts),
        "rows_with_errors": rows_info,
    }


# ── Error classification ───────────────────────────────────────────────────────

def _is_platform(line: str) -> bool:
    ll = line.lower()
    return any(k in ll for k in config.PLATFORM_KEYWORDS)


def _is_core_preproc(line: str) -> bool:
    ll = line.lower()
    return any(ll.startswith(p) or (": " + p) in ll for p in config.CORE_PREPROC_COLS)


def _is_certifyos_col(line: str) -> bool:
    return "certifyos" in line.lower()


def classify_errors(data_errors: list, biz_errors: list) -> str:
    all_errors = data_errors + biz_errors
    if not all_errors:
        return "Blank — No Validation Data"

    has_platform      = any(_is_platform(e)     for e in all_errors)
    has_core4         = any(_is_core_preproc(e)  for e in data_errors)
    has_ext_certifyos = any(_is_certifyos_col(e) and not _is_core_preproc(e) for e in data_errors)
    has_humana        = any(not _is_platform(e) and not _is_certifyos_col(e) for e in all_errors)

    if has_platform:
        if has_core4 and has_humana: return "Platform + Pre-processing + Humana Errors"
        if has_core4:                return "Platform + Pre-processing Errors"
        if has_humana:               return "Platform + Humana Errors"
        return "Platform Errors"

    if has_core4 or has_ext_certifyos:
        if not has_humana:           return "Pre-processing Errors"
        if has_core4 and has_humana: return "Pre-processing + Humana Errors"

    return "Humana Errors"


# ── NPPES check ────────────────────────────────────────────────────────────────

def _nppes_check(npi: str, first: str, last: str, gemini: GenerativeModel) -> bool:
    if not npi:
        return False
    try:
        data    = requests.get(config.NPPES_API_URL,
                               params={"number": npi, "enumeration_type": "NPI-1", "version": "2.1"},
                               timeout=15).json()
        results = data.get("results", [])
        if not results:
            return False
        basic     = results[0].get("basic", {})
        api_first = (basic.get("first_name") or "").lower()
        api_last  = (basic.get("last_name")  or "").lower()
        return (first.lower() in api_first or api_first in first.lower()) and \
               (last.lower()  in api_last  or api_last  in last.lower())
    except Exception:
        try:
            resp = gemini.generate_content(
                f"Does NPI={npi} First='{first}' Last='{last}' match an NPPES record? Reply YES or NO only."
            )
            return "YES" in resp.text.upper()
        except Exception:
            return False


# ── Humana sub-categorisation ──────────────────────────────────────────────────

_NPI_NAMECHECK_MARKER = "provider name is not matching with nppes"
_SECTION_HEADERS      = {"=== data validations ===", "=== business validations ==="}

_BASE_SUBCATS = [
    "Degree / Title Not Valid",
    "Degree / Title Missing",
    "NPI Blank",
    "First / Last Name Blank",
    "NPPES Name Mismatch",
    "TIN Error",
    "Geographic Market Error",
    "Provider License / Credential Error",
    "Provider Type Error",
    "Individual vs Group Error",
    "Duplicate Provider Error",
]

MAX_WORKERS = 8   # parallel CSV downloads


def _gemini_tag(lines: list[str], gemini: GenerativeModel, known_subcats: list[str] | None = None) -> list[dict]:
    if not lines:
        return []

    all_subcats = list(dict.fromkeys(_BASE_SUBCATS + (known_subcats or [])))
    subcat_list = "\n".join(f"- {s}" for s in all_subcats)
    numbered    = "\n".join(f"{i+1}. {l}" for i, l in enumerate(lines))

    prompt = (
        "You are classifying healthcare roster validation errors into Humana sub-categories.\n\n"
        "STEP 1 — SKIP THESE (return 'Skip — Pre-processing'):\n"
        "  • Any error mentioning CertifyOS Group Name / Group NPI / Group TIN / Network Name\n"
        "  • Any error about 'Provider Name is not matching with NPPES'\n\n"
        f"STEP 2 — APPROVED SUB-CATEGORY LIST (your complete knowledge base):\n{subcat_list}\n\n"
        "STEP 3 — CLASSIFICATION RULES:\n"
        "  • ALWAYS try to match one of the existing labels above. Stretch to fit.\n"
        "  • NEVER invent a new sub-category unless you have exhausted ALL existing options.\n"
        "  • If you must create a new label, name it similarly to the existing ones (Title Case, short noun phrase).\n"
        "  • NEVER use vague labels like 'Other' or 'Unknown'.\n\n"
        "Return a JSON array only (no explanation), one object per line:\n"
        '[{"line_number":<int>,"subcategory":"<label>","pattern":"<3-7 word key phrase>"}]\n\n'
        f"Lines:\n{numbered}"
    )
    try:
        raw = gemini.generate_content(prompt).text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        results = []
        for item in json.loads(raw.strip()):
            subcat = (item.get("subcategory") or "").strip()
            if not subcat or subcat in ("Skip — Pre-processing", "Skip"):
                continue
            idx = item.get("line_number", 1) - 1
            if 0 <= idx < len(lines):
                results.append({
                    "line":        lines[idx],
                    "subcategory": subcat,
                    "pattern":     item.get("pattern", lines[idx][:60]).lower().strip(),
                })
        return results
    except Exception as e:
        log(f"Gemini tagging failed: {e}", "WARN")
        # keyword-based fallback against known subcats
        fallback = []
        for line in lines:
            ll = line.lower()
            matched = next((s for s in all_subcats if any(w.lower() in ll for w in s.split())), None)
            if matched:
                fallback.append({"line": line, "subcategory": matched, "pattern": ll[:60]})
        return fallback


def classify_humana_subcategories(
    error_log: str,
    pattern_db: dict,
    pending: list,
    gemini: GenerativeModel,
    error_category: str = "",
) -> list[str]:
    """
    Classify error_log into Humana sub-category tags.

    NPI name-check lines are skipped ONLY for mixed 'Pre-processing NPI + Humana'
    files. For pure Humana files they are confirmed real mismatches and should
    map to 'NPPES Name Mismatch'.
    """
    if not error_log or not error_log.strip():
        return []

    matched: set = set()
    unknown: list = []
    skip_npi_lines = "Pre-processing NPI" in error_category

    for raw in error_log.splitlines():
        line = raw.strip()
        if not line or line.lower() in _SECTION_HEADERS:
            continue
        ll = line.lower()
        if any(ll.startswith(p) or (": " + p) in ll for p in config.CORE_PREPROC_COLS):
            continue
        if skip_npi_lines and _NPI_NAMECHECK_MARKER in ll:
            continue

        found = next((subcat for pat, subcat in pattern_db.items() if pat in ll), None)
        if found:
            matched.add(found)
        else:
            unknown.append(line)

    unknown = list(dict.fromkeys(unknown))
    known_subcats = sorted(set(pattern_db.values()))
    for gr in _gemini_tag(unknown, gemini, known_subcats):
        subcat = gr.get("subcategory", "").strip()
        if not subcat:
            continue
        matched.add(subcat)
        pending.append({"pattern": gr["pattern"], "subcategory": subcat, "source": "Gemini"})
        pattern_db[gr["pattern"]] = subcat

    return sorted(matched) if matched else []


# ── Main orchestrator ──────────────────────────────────────────────────────────

def run_audit(force_reaudit: bool = False) -> dict:
    run_ts = datetime.datetime.utcnow().isoformat()
    log(f"Audit start — {run_ts}  mode={'force_reaudit' if force_reaudit else 'incremental'}")

    sa_creds = _load_sa_credentials()

    vertexai.init(project=config.GEMINI_PROJECT_ID, location=config.GEMINI_LOCATION, credentials=sa_creds)
    gemini = GenerativeModel(config.GEMINI_MODEL)
    log(f"Gemini ready ({config.GEMINI_MODEL})", "OK")

    bq  = BQClient(credentials=sa_creds)
    c_id, c_secret = _load_certifyos_creds()
    api = CertifyOSClient.authenticate(c_id, c_secret)
    log("CertifyOS auth OK", "OK")

    failed_rosters = api.fetch_failed_rosters()
    all_api_udids  = {r["udid"] for r in failed_rosters}

    pattern_db = bq.load_pattern_library()
    pending_patterns: list = []
    log(f"Pattern library: {len(pattern_db)} entries", "OK")

    if force_reaudit:
        log("Force re-audit: clearing active table", "WARN")
        bq.clear_active_table()
        active_snapshot, known_udids, disappeared = {}, set(), set()
        new_udids      = all_api_udids
        changed_udids  = set()
    else:
        active_snapshot = bq.get_active_rows()
        known_udids     = set(active_snapshot.keys())
        new_udids       = all_api_udids - known_udids
        disappeared     = known_udids   - all_api_udids

        # Detect UDIDs that are still FAILED but were updated since last audit.
        # If updatedAt from API differs from what's stored in BQ → re-download + re-audit.
        changed_udids: set = set()
        api_updated_map = {r["udid"]: r.get("updated_at", "") for r in failed_rosters}
        for uid in known_udids & all_api_udids:
            bq_ts  = str(active_snapshot[uid].get("updated_at") or "")
            api_ts = api_updated_map.get(uid, "")
            if api_ts and api_ts != bq_ts:
                changed_udids.add(uid)

        if changed_udids:
            log(f"Detected {len(changed_udids)} UDID(s) with changed updatedAt — will re-audit", "WARN")

    to_audit_udids = new_udids | changed_udids
    log(f"Known={len(known_udids)}  New={len(new_udids)}  "
        f"Changed={len(changed_udids)}  Disappeared={len(disappeared)}  "
        f"To audit={len(to_audit_udids)}", "OK")

    if disappeared:
        log(f"Fetching current status for {len(disappeared)} disappeared UDID(s)…")
        for uid in disappeared:
            active_snapshot[uid]["_current_status"] = api.get_udid_status(uid)
    moved = bq.move_to_status_changed(disappeared, active_snapshot, run_ts)

    new_list = [r for r in failed_rosters if r["udid"] in to_audit_udids]
    results  = []

    # ── Parallel CSV pre-download ─────────────────────────────────────────────
    _csv_cache: dict[str, str | None] = {}
    _dl_token_lock = threading.Lock()

    def _dl_worker(roster: dict) -> tuple[str, str | None]:
        uid      = roster["udid"]
        filepath = roster["filepath"]
        try:
            text = api.download_csv(uid, filepath)
            return uid, text
        except Exception as e:
            log(f"  [DL] {uid[:8]}: {e}", "WARN")
            return uid, None

    if new_list:
        log(f"Pre-downloading {len(new_list)} CSV(s) with {MAX_WORKERS} workers…")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futs = {pool.submit(_dl_worker, r): r["udid"] for r in new_list}
            done = 0
            for fut in concurrent.futures.as_completed(futs):
                uid, text = fut.result()
                _csv_cache[uid] = text
                done += 1
                if done % 10 == 0 or done == len(new_list):
                    log(f"  [DL] {done}/{len(new_list)} downloaded")
        ok  = sum(1 for v in _csv_cache.values() if v)
        bad = len(_csv_cache) - ok
        log(f"Pre-download complete — {ok} OK, {bad} failed", "OK" if not bad else "WARN")

    for i in range(0, len(new_list), config.BATCH_SIZE):
        batch = new_list[i: i + config.BATCH_SIZE]
        log(f"Batch {i // config.BATCH_SIZE + 1}: {len(batch)} file(s)")
        for roster in batch:
            uid, filename, filepath = roster["udid"], roster["filename"], roster["filepath"]
            uploader, date          = roster["uploader"], roster["date"]
            updated_at              = roster.get("updated_at", "")
            is_reaudit              = uid in changed_udids
            log(f"  {'[re-audit] ' if is_reaudit else ''}{filename[:60]} ({uid[:8]}…)")

            csv_text = _csv_cache.get(uid)
            if not csv_text:
                results.append({"filename": filename, "udid": uid, "error_category": "Download Failed",
                                 "uploader": uploader, "date": date, "notes": "Download failed",
                                 "error_log": "", "humana_subcategories": "", "updated_at": updated_at})
                continue

            parsed = parse_csv(csv_text)
            de, be = parsed["data_errors"], parsed["business_errors"]

            if not de and not be:
                results.append({"filename": filename, "udid": uid,
                                 "error_category": "Blank — No Validation Data",
                                 "uploader": uploader, "date": date,
                                 "notes": "Validation columns empty", "error_log": "",
                                 "humana_subcategories": "", "updated_at": updated_at})
                continue

            category    = classify_errors(de, be)
            nppes_notes = []

            if any(_NPI_NAMECHECK_MARKER in e.lower() for e in be):
                fp = sum(1 for row in parsed["rows_with_errors"]
                         if _NPI_NAMECHECK_MARKER in row.get("bv", "").lower()
                         and _nppes_check(row["npi"], row["first"], row["last"], gemini))
                if fp:
                    other_errors = [e for e in de + be if _NPI_NAMECHECK_MARKER not in e.lower()]
                    has_other    = any(not _is_platform(e) and not _is_certifyos_col(e) for e in other_errors)
                    category     = ("Pre-processing NPI name check + Humana Errors" if has_other
                                    else "Pre-processing NPI name check Errors")
                    nppes_notes.append(f"{fp} NPPES false positives confirmed")

            humana_subcats = ""
            if "Humana" in category:
                subs = classify_humana_subcategories(parsed["error_log"], pattern_db, pending_patterns, gemini, category)
                humana_subcats = ", ".join(subs)
                log(f"    Humana sub-cats: {humana_subcats}", "OK")

            if is_reaudit:
                old_cat = active_snapshot.get(uid, {}).get("error_category", "")
                if old_cat != category:
                    log(f"    Category changed: {old_cat} → {category}", "WARN")

            log(f"    → {category}", "OK")
            results.append({
                "filename":             filename,
                "udid":                 uid,
                "error_category":       category,
                "uploader":             uploader,
                "date":                 date,
                "notes":                "; ".join(nppes_notes),
                "error_log":            parsed["error_log"],
                "humana_subcategories": humana_subcats,
                "updated_at":           updated_at,
            })

    upsert_stats = bq.upsert_results(results, run_ts)
    if pending_patterns:
        bq.save_patterns(pending_patterns, run_ts)
    bq.append_run_history(run_ts, {**upsert_stats, "moved": moved})

    summary = {
        "run_ts":              run_ts,
        "total_from_api":      len(all_api_udids),
        "new_audited":         upsert_stats.get("new", 0),
        "re_audited_changed":  len(changed_udids),
        "skipped_unchanged":   len(known_udids & all_api_udids) - len(changed_udids),
        "moved_to_changed":    moved,
    }
    log(f"Audit complete: {summary}", "OK")
    return summary
