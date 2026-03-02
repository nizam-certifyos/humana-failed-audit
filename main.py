from __future__ import annotations

import os
import threading
import traceback

from flask import Flask, jsonify, render_template, request

import config
from bq_client import BQClient
from google.oauth2 import service_account
from auditor import _load_secret, _load_sa_credentials, run_audit

app = Flask(__name__)

AUDIT_SECRET = os.environ.get("AUDIT_SECRET", config.AUDIT_SECRET_VALUE)

_bq_client: BQClient | None = None
_bq_lock = threading.Lock()


def _get_bq() -> BQClient:
    global _bq_client
    with _bq_lock:
        if _bq_client is None:
            _bq_client = BQClient(credentials=_load_sa_credentials())
    return _bq_client


def _start_audit_thread(force_reaudit: bool = False) -> None:
    def _run():
        try:
            global _bq_client
            with _bq_lock:
                _bq_client = None
            summary = run_audit(force_reaudit=force_reaudit)
            print(f"[audit] completed: {summary}")
        except Exception:
            traceback.print_exc()

    threading.Thread(target=_run, daemon=True).start()


def _check_secret() -> bool:
    return request.headers.get(config.AUDIT_SECRET_HEADER, "") == AUDIT_SECRET


@app.route("/")
def dashboard():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    try:
        return jsonify(_get_bq().get_dashboard_data())
    except Exception as e:
        traceback.print_exc()
        return jsonify({"active": [], "changed": [], "lastUpdated": "", "errors": [str(e)]}), 500


@app.route("/run-audit", methods=["POST"])
def trigger_audit():
    if not _check_secret():
        return jsonify({"error": "Unauthorized"}), 403
    _start_audit_thread(force_reaudit=False)
    return jsonify({"status": "started", "mode": "incremental"}), 200


@app.route("/reset-and-audit", methods=["POST"])
def reset_and_audit():
    if not _check_secret():
        return jsonify({"error": "Unauthorized"}), 403
    _start_audit_thread(force_reaudit=True)
    return jsonify({"status": "started", "mode": "full_reaudit"}), 200


@app.route("/seed-patterns", methods=["POST"])
def seed_patterns():
    """
    One-time endpoint: copies the Error Pattern Library from the Google Sheet
    (SOURCE_SHEET_ID in config) into the BQ patterns table, replacing all rows.
    Requires the X-Audit-Secret header.
    """
    if not _check_secret():
        return jsonify({"error": "Unauthorized"}), 403
    try:
        # Sheets API needs the spreadsheets.readonly scope which is NOT in the
        # default SA credentials (those only have cloud-platform + bigquery).
        # Load a fresh credential set with the extra scope for this one call.
        sa_info     = _load_secret(config.SECRET_NAME)
        sheets_creds = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets.readonly",
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/bigquery",
            ],
        )
        result = _get_bq().seed_from_sheet(sheets_creds, config.SOURCE_SHEET_ID)
        return jsonify({"status": "ok", **result}), 200
    except Exception:
        traceback.print_exc()
        return jsonify({"error": "seed failed — check Cloud Run logs"}), 500


@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
