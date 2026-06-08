"""
pipeline.py
===========
Parallel processing pipeline for IndiaMart Lead Enricher.

Architecture:
  - ThreadPoolExecutor with configurable concurrency
  - Thread-safe shared state via threading.Lock
  - Graceful stop via threading.Event
  - Incremental BytesIO Excel output after every row
  - Resume detection from existing output data

State is stored in a plain dict (pipeline_state) that Streamlit's
session_state holds onto. All mutations are lock-protected.
"""

import io
import logging
import threading
import time
from typing import Optional

import pandas as pd
import requests

from scraper_core import (
    process_company,
    QuotaExhaustedError,
    InvalidKeyError,
    create_selenium_driver,
)

log = logging.getLogger("pipeline")


# ──────────────────────────────────────────────────────────────────────────────
#  STATE SCHEMA  (all keys the pipeline writes)
# ──────────────────────────────────────────────────────────────────────────────

def make_initial_state() -> dict:
    """Returns a fresh pipeline state dict."""
    return {
        # Core data
        "rows": [],           # list of row dicts (company_name, pincode, address, ...)
        "total": 0,
        "processed": 0,
        "failed": 0,
        "in_progress": 0,

        # Control
        "running": False,
        "paused_quota": False,
        "stop_requested": False,

        # Current batch statuses: row_index → "processing" | "done" | "failed" | "pending"
        "row_status": {},

        # Log entries (last N)
        "log_lines": [],

        # API tracking
        "api_key":   "",
        "api_valid": None,    # None = unchecked, True/False
        "use_selenium": True, # Use Selenium (Chrome) for phone extraction
        "total_credits_used": 0,

        # Input metadata
        "input_filename": "",
    }


# ──────────────────────────────────────────────────────────────────────────────
#  EXCEL I/O  (BytesIO — Streamlit Cloud compatible)
# ──────────────────────────────────────────────────────────────────────────────

OUTPUT_COLUMNS = [
    "company_name", "pincode", "address",
    "url_fetched", "match_type", "contact_number", "status", "error"
]

DISPLAY_COLUMN_NAMES = {
    "company_name": "Company Name",
    "pincode": "Pincode",
    "address": "Address",
    "url_fetched": "IndiaMart URL",
    "match_type": "Match Type",
    "contact_number": "Contact Number",
    "status": "Status",
    "error": "Error",
}


def rows_to_dataframe(rows: list) -> pd.DataFrame:
    """Converts list of row dicts to a clean DataFrame for display/export."""
    if not rows:
        return pd.DataFrame(columns=list(DISPLAY_COLUMN_NAMES.values()))

    df = pd.DataFrame(rows)
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[OUTPUT_COLUMNS].copy()
    df = df.rename(columns=DISPLAY_COLUMN_NAMES)
    return df


def export_to_excel(rows: list) -> bytes:
    """
    Exports processed rows to an Excel file in memory (BytesIO).
    Returns raw bytes suitable for st.download_button.
    """
    df = rows_to_dataframe(rows)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Enriched Leads")

        ws = writer.sheets["Enriched Leads"]
        # Auto-width columns
        for col_cells in ws.columns:
            max_len = max(
                (len(str(cell.value)) if cell.value else 0 for cell in col_cells),
                default=10
            )
            col_letter = col_cells[0].column_letter
            ws.column_dimensions[col_letter].width = min(max_len + 4, 60)

    buf.seek(0)
    return buf.read()


def load_input_excel(file_bytes: bytes, filename: str) -> tuple:
    """
    Reads input Excel and normalises column names.
    Returns (df, error_message).
    df has columns: company_name, pincode, address
    """
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
        df.columns = df.columns.str.strip()
        df = df.fillna("")

        col_map = {}
        for col in df.columns:
            cl = col.lower()
            if any(k in cl for k in ("name", "company", "firm", "business", "trade")):
                col_map.setdefault("company_name", col)
            elif any(k in cl for k in ("pin", "zip", "postal", "post")):
                col_map.setdefault("pincode", col)
            elif any(k in cl for k in ("addr", "location", "area", "city", "locality")):
                col_map.setdefault("address", col)

        # Fallback: positional
        cols = list(df.columns)
        col_map.setdefault("company_name", cols[0] if cols else "company_name")
        col_map.setdefault("pincode", cols[1] if len(cols) > 1 else "pincode")
        col_map.setdefault("address", cols[2] if len(cols) > 2 else "address")

        df = df.rename(columns={v: k for k, v in col_map.items()})
        for c in ("company_name", "pincode", "address"):
            if c not in df.columns:
                df[c] = ""

        df = df[["company_name", "pincode", "address"]].copy()
        df = df[df["company_name"].str.strip().astype(bool)].reset_index(drop=True)

        if len(df) == 0:
            return None, "No valid rows found. Ensure Excel has company_name, pincode, address columns."

        return df, None

    except Exception as exc:
        return None, f"Failed to read Excel: {str(exc)}"


# ──────────────────────────────────────────────────────────────────────────────
#  PIPELINE RUNNER
# ──────────────────────────────────────────────────────────────────────────────

class LeadEnricherPipeline:
    """
    Thread-safe pipeline that processes companies in parallel batches.
    Designed to run in a background thread while Streamlit UI refreshes.
    """

    def __init__(self, state: dict, state_lock: threading.Lock, stop_event: threading.Event):
        self.state = state
        self.lock = state_lock
        self.stop_event = stop_event

    # ── Logging helper ────────────────────────────────────────────────────────

    def _log(self, level: str, msg: str):
        import datetime
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {level:7s} {msg}"
        with self.lock:
            self.state["log_lines"].append(line)
            # Keep only last 200 lines
            if len(self.state["log_lines"]) > 200:
                self.state["log_lines"] = self.state["log_lines"][-200:]

    def _info(self, msg):  self._log("INFO",    msg)
    def _warn(self, msg):  self._log("WARNING", msg)
    def _error(self, msg): self._log("ERROR",   msg)

    # ── Row update (thread-safe) ──────────────────────────────────────────────

    def _update_row(self, idx: int, **kwargs):
        """Update fields on a single row dict. Thread-safe."""
        with self.lock:
            if 0 <= idx < len(self.state["rows"]):
                self.state["rows"][idx].update(kwargs)

    def _set_row_status(self, idx: int, status: str):
        with self.lock:
            self.state["row_status"][idx] = status

    # ── Worker: process one company ───────────────────────────────────────────

    def _process_one(self, idx: int, row: dict, api_key: str,
                     session: requests.Session, driver=None):
        """
        Processes a single company row in a worker thread.
        Updates state in-place. Raises on quota/key errors.
        """
        name    = str(row.get("company_name", "")).strip()
        pincode = str(row.get("pincode", "")).strip().split(".")[0]
        address = str(row.get("address", "")).strip()

        self._set_row_status(idx, "processing")
        with self.lock:
            self.state["in_progress"] += 1

        self._info(f"[{idx+1}] Processing: {name[:50]}")

        try:
            result = process_company(name, pincode, address, api_key,
                                     session=session, driver=driver)

            with self.lock:
                self.state["rows"][idx].update({
                    "url_fetched":    result["url_fetched"],
                    "match_type":     result["match_type"],
                    "contact_number": result["contact_number"],
                    "status":         "done",
                    "error":          result.get("error", ""),
                })
                self.state["processed"]    += 1
                self.state["in_progress"]  -= 1
                self.state["total_credits_used"] += result.get("credits_used", 0)

            self._set_row_status(idx, "done")

            found = result["url_fetched"]
            phone = result["contact_number"]
            self._info(f"[{idx+1}] Done → {('✓ ' + found[:50]) if found else '✗ No URL'} | 📞 {phone or '—'}")

        except QuotaExhaustedError:
            self._update_row(idx, status="pending", url_fetched="", contact_number="", error="quota_exhausted")
            self._set_row_status(idx, "pending")
            with self.lock:
                self.state["in_progress"] -= 1
                self.state["paused_quota"] = True
            self._error("Serper API quota exhausted — processing paused. Enter a new API key.")
            raise

        except InvalidKeyError:
            self._update_row(idx, status="failed", error="invalid_api_key")
            self._set_row_status(idx, "failed")
            with self.lock:
                self.state["in_progress"] -= 1
                self.state["failed"] += 1
                self.state["paused_quota"] = True
            self._error("Invalid Serper API key. Please check and re-enter.")
            raise

        except Exception as exc:
            err = str(exc)[:100]
            self._update_row(idx, status="failed", error=err)
            self._set_row_status(idx, "failed")
            with self.lock:
                self.state["in_progress"] -= 1
                self.state["failed"] += 1
            self._warn(f"[{idx+1}] Failed: {err}")

    # ── Main run loop ─────────────────────────────────────────────────────────

    def run(self):
        """
        Main processing loop — simple sequential row-by-row.
        Spins up one headless Chrome driver (if use_selenium=True in state),
        reuses it for every row, then quits it cleanly on exit.
        """
        with self.lock:
            self.state["running"]        = True
            self.state["stop_requested"] = False
            api_key      = self.state["api_key"]
            use_selenium = self.state.get("use_selenium", True)

        self._info("Pipeline started — sequential mode"
                   + (" | Selenium phone extraction" if use_selenium else " | Static phone extraction"))

        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})

        # ── Start Selenium driver ──────────────────────────────────────────────
        driver = None
        if use_selenium:
            try:
                self._info("Starting headless Chrome for phone extraction...")
                driver = create_selenium_driver(headless=True)
                self._info("Chrome driver ready ✓")
            except Exception as exc:
                self._warn(
                    f"Could not start Selenium ({exc}). "
                    "Falling back to static phone extraction — "
                    "phone numbers may not be found on modern IndiaMart pages."
                )
                driver = None

        try:
            # Collect indices that still need processing
            pending_indices = []
            with self.lock:
                for i, row in enumerate(self.state["rows"]):
                    if row.get("status") != "done":
                        pending_indices.append(i)
                        row["status"] = "pending"
                        self.state["row_status"][i] = "pending"

            self._info(f"Rows to process: {len(pending_indices)}")

            for idx in pending_indices:
                if self.stop_event.is_set():
                    self._info("Stop requested — halting.")
                    break

                with self.lock:
                    if self.state["paused_quota"]:
                        break
                    row = self.state["rows"][idx]

                try:
                    self._process_one(idx, row, api_key, session, driver=driver)
                except (QuotaExhaustedError, InvalidKeyError):
                    break
                except Exception:
                    pass

        except Exception as exc:
            self._error(f"Pipeline error: {str(exc)}")

        finally:
            session.close()
            if driver is not None:
                try:
                    driver.quit()
                    self._info("Chrome driver closed.")
                except Exception:
                    pass

            with self.lock:
                self.state["running"]     = False
                self.state["in_progress"] = 0

            with self.lock:
                paused = self.state["paused_quota"]
            if paused:
                self._warn("Pipeline paused — quota exhausted. Progress saved.")
            else:
                self._info("Pipeline finished.")


# ──────────────────────────────────────────────────────────────────────────────
#  RESUME DETECTION
# ──────────────────────────────────────────────────────────────────────────────

def detect_resume_rows(rows: list) -> int:
    """
    Returns the count of already-processed rows (status == 'done').
    Used after loading existing output data back into state.
    """
    return sum(1 for r in rows if r.get("status") == "done")


def load_processed_excel(file_bytes: bytes, filename: str) -> tuple:
    """
    Reads a previously exported output Excel (the enriched results file) and
    returns a list of row dicts with status='done' for rows that were
    successfully processed, and status='pending' for failed/not-found rows.

    Supported output column formats:
      - New format: Company Name, Pincode, Address, Status, IndiaMart URL, Match Info, Phone Number
      - Legacy pipeline export: company_name / Company Name, pincode, address,
        url_fetched / IndiaMart URL, match_type / Match Type, contact_number / Contact Number

    Returns (rows: list[dict], done_count: int, error: str | None)
    """
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
        df.columns = df.columns.str.strip()
        df = df.fillna("")

        # Normalise column names → internal keys
        col_aliases = {
            "company_name":    ["company name", "company_name", "companyname", "firm", "business"],
            "pincode":         ["pincode", "pin code", "zip", "postal"],
            "address":         ["address", "addr", "location"],
            "url_fetched":     ["indiamart url", "url_fetched", "url", "indiamart link"],
            "match_type":      ["match info", "match_type", "match type", "matchinfo"],
            "contact_number":  ["phone number", "contact_number", "contact number", "phone", "mobile"],
            "status":          ["status"],
        }

        col_map = {}
        for internal_key, aliases in col_aliases.items():
            for col in df.columns:
                if col.lower() in aliases:
                    col_map[internal_key] = col
                    break

        # Must at minimum have company name
        if "company_name" not in col_map:
            return None, 0, (
                "Could not detect a company name column. "
                "Expected 'Company Name' or 'company_name' in the processed Excel."
            )

        rows = []
        done_count = 0

        for _, raw in df.iterrows():
            def g(key, default=""):
                col = col_map.get(key)
                return str(raw[col]).strip() if col else default

            company = g("company_name")
            if not company:
                continue  # skip blank rows

            # Determine if this row was successfully enriched
            raw_status = g("status", "").lower()
            url        = g("url_fetched")
            phone      = g("contact_number")

            # Accept rows that have a URL or whose status indicates found/done
            is_done = bool(url) or any(
                kw in raw_status for kw in ("✔", "found", "done", "✓")
            )

            row = {
                "company_name":   company,
                "pincode":        g("pincode"),
                "address":        g("address"),
                "url_fetched":    url,
                "match_type":     g("match_type"),
                "contact_number": phone,
                "status":         "done" if is_done else "pending",
                "error":          "",
            }
            rows.append(row)
            if is_done:
                done_count += 1

        if not rows:
            return None, 0, "No valid rows found in the processed Excel file."

        return rows, done_count, None

    except Exception as exc:
        return None, 0, f"Failed to read processed Excel: {str(exc)}"


def merge_output_with_input(
    input_df: pd.DataFrame,
    existing_rows: list,
    match_by_name: bool = False,
) -> list:
    """
    Merges new input DataFrame with existing output rows (for resume).
    Existing 'done' rows are preserved; new rows are added as pending.

    When match_by_name=True (used after loading a processed Excel that may
    have a different row count / ordering) the merge is done by company_name
    lookup instead of positional index.
    """
    input_records = input_df.to_dict("records")

    # Build a name→row lookup for name-based matching (from processed Excel upload)
    if match_by_name:
        done_by_name = {
            r.get("company_name", "").strip().lower(): r
            for r in existing_rows
            if r.get("status") == "done"
        }

    result_rows = []

    for i, inp in enumerate(input_records):
        matched_existing = None

        if match_by_name:
            key = inp.get("company_name", "").strip().lower()
            matched_existing = done_by_name.get(key)
        elif i < len(existing_rows) and existing_rows[i].get("status") == "done":
            matched_existing = existing_rows[i]

        if matched_existing:
            # Preserve existing result but refresh input fields
            row = matched_existing.copy()
            row["company_name"] = inp.get("company_name", row.get("company_name", ""))
            row["pincode"]      = inp.get("pincode",      row.get("pincode", ""))
            row["address"]      = inp.get("address",      row.get("address", ""))
        else:
            row = {
                "company_name":   inp.get("company_name", ""),
                "pincode":        inp.get("pincode", ""),
                "address":        inp.get("address", ""),
                "url_fetched":    "",
                "match_type":     "",
                "contact_number": "",
                "status":         "pending",
                "error":          "",
            }
        result_rows.append(row)

    return result_rows