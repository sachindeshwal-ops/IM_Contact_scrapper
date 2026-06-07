"""
app.py — IndiaMart Lead Enricher
==================================
Main Streamlit entry point.

Run locally:
  streamlit run app.py

Deploy to Streamlit Cloud:
  Entry point: app.py
"""

import threading
import time
import datetime
import logging

import streamlit as st
import pandas as pd

from scraper_core import validate_api_key
from pipeline import (
    LeadEnricherPipeline,
    make_initial_state,
    load_input_excel,
    load_processed_excel,
    export_to_excel,
    rows_to_dataframe,
    detect_resume_rows,
    merge_output_with_input,
)

# ──────────────────────────────────────────────────────────────────────────────
#  PAGE CONFIG (must be first Streamlit call)
# ──────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="IndiaMart Lead Enricher",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("app")


# ──────────────────────────────────────────────────────────────────────────────
#  SESSION STATE INIT
# ──────────────────────────────────────────────────────────────────────────────

def init_session():
    """Initialise all session state keys exactly once."""
    if "pipeline_state" not in st.session_state:
        st.session_state.pipeline_state = make_initial_state()
    if "state_lock" not in st.session_state:
        st.session_state.state_lock = threading.Lock()
    if "stop_event" not in st.session_state:
        st.session_state.stop_event = threading.Event()
    if "pipeline_thread" not in st.session_state:
        st.session_state.pipeline_thread = None
    if "input_file_bytes" not in st.session_state:
        st.session_state.input_file_bytes = None
    if "processed_file_rows" not in st.session_state:
        st.session_state.processed_file_rows = None   # rows loaded from prior output Excel
    if "confirm_clear" not in st.session_state:
        st.session_state.confirm_clear = False
    if "new_api_key_input" not in st.session_state:
        st.session_state.new_api_key_input = ""


init_session()

# Shorthand references
PS    = st.session_state.pipeline_state
LOCK  = st.session_state.state_lock
STOP  = st.session_state.stop_event


# ──────────────────────────────────────────────────────────────────────────────
#  HELPER FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────

def is_running() -> bool:
    t = st.session_state.pipeline_thread
    return t is not None and t.is_alive()


def start_pipeline(max_workers: int):
    """Starts (or resumes) the background pipeline thread."""
    if is_running():
        return

    STOP.clear()
    with LOCK:
        PS["stop_requested"] = False
        PS["running"] = True

    pipeline = LeadEnricherPipeline(PS, LOCK, STOP)
    t = threading.Thread(target=pipeline.run, args=(max_workers,), daemon=True)
    t.start()
    st.session_state.pipeline_thread = t


def stop_pipeline():
    """Signals the pipeline to stop after the current batch."""
    STOP.set()
    with LOCK:
        PS["stop_requested"] = True


def get_stats() -> dict:
    with LOCK:
        total     = PS["total"]
        processed = PS["processed"]
        failed    = PS["failed"]
        in_prog   = PS["in_progress"]
        remaining = max(0, total - processed - in_prog - failed)
        credits   = PS["total_credits_used"]
    return dict(total=total, processed=processed, failed=failed,
                in_progress=in_prog, remaining=remaining, credits=credits)


def get_completion_pct() -> float:
    with LOCK:
        total     = PS["total"]
        processed = PS["processed"]
        failed    = PS["failed"]
    if total == 0:
        return 0.0
    return min(1.0, (processed + failed) / total)


def status_emoji(status: str) -> str:
    return {
        "done":       "✅",
        "processing": "🔄",
        "failed":     "❌",
        "pending":    "⏳",
    }.get(status, "⏳")


def api_badge(valid: bool | None) -> str:
    if valid is True:
        return "🟢 API Key Valid"
    if valid is False:
        return "🔴 API Key Invalid"
    return "🟡 API Key Not Verified"


# ──────────────────────────────────────────────────────────────────────────────
#  SIDEBAR
# ──────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🏭 IndiaMart\nLead Enricher")
    st.markdown("---")

    # ── API Key ────────────────────────────────────────────────────────────────
    st.subheader("🔑 Serper API Key")
    api_key_input = st.text_input(
        "Enter your Serper API key",
        type="password",
        value=PS.get("api_key", ""),
        placeholder="Paste key from serper.dev",
        help="Get a free key at https://serper.dev",
    )

    col_api1, col_api2 = st.columns(2)
    with col_api1:
        if st.button("Verify Key", use_container_width=True):
            if api_key_input.strip():
                with st.spinner("Checking..."):
                    valid, msg = validate_api_key(api_key_input.strip())
                with LOCK:
                    PS["api_key"]   = api_key_input.strip()
                    PS["api_valid"] = valid
                if valid:
                    st.success(msg)
                else:
                    st.error(msg)
            else:
                st.warning("Enter an API key first.")
    with col_api2:
        with LOCK:
            badge_state = PS.get("api_valid")
        st.markdown(f"**{api_badge(badge_state)}**")

    # Save key to state when changed
    if api_key_input.strip() and api_key_input.strip() != PS.get("api_key", ""):
        with LOCK:
            PS["api_key"]   = api_key_input.strip()
            PS["api_valid"] = None  # Reset verification on change

    st.markdown("---")

    # ── Already-Processed Output Upload ───────────────────────────────────────
    st.subheader("🔄 Resume from Previous Output")
    st.caption(
        "Upload a previously exported result Excel to skip already-processed "
        "rows and continue from where you left off."
    )
    processed_file = st.file_uploader(
        "Upload previously exported Excel (optional)",
        type=["xlsx", "xls"],
        key="processed_excel_uploader",
        help=(
            "This should be the output file downloaded from a prior run. "
            "Matched rows (by company name) will be marked as done automatically."
        ),
    )

    if processed_file is not None:
        proc_bytes = processed_file.read()
        proc_rows, proc_done, proc_err = load_processed_excel(proc_bytes, processed_file.name)

        if proc_err:
            st.error(f"❌ {proc_err}")
        else:
            st.session_state.processed_file_rows = proc_rows
            st.success(
                f"✅ Loaded **{processed_file.name}** — "
                f"**{proc_done}** rows already done, will be skipped. "
                f"Upload the sellers input file below to begin merging."
            )

            # If input is already loaded, immediately merge
            if PS.get("rows") and PS.get("input_filename"):
                import io as _io
                with LOCK:
                    existing_input_bytes = st.session_state.input_file_bytes
                if existing_input_bytes:
                    df_in, err_in = load_input_excel(existing_input_bytes, PS["input_filename"])
                    if not err_in:
                        merged = merge_output_with_input(df_in, proc_rows, match_by_name=True)
                        done_c = detect_resume_rows(merged)
                        with LOCK:
                            PS["rows"]       = merged
                            PS["total"]      = len(merged)
                            PS["processed"]  = done_c
                            PS["failed"]     = sum(1 for r in merged if r.get("status") == "failed")
                            PS["row_status"] = {i: r.get("status", "pending") for i, r in enumerate(merged)}
                        st.info(f"▶ Merged — {done_c}/{len(merged)} rows already done.")

    elif st.session_state.processed_file_rows is not None:
        proc_done = detect_resume_rows(st.session_state.processed_file_rows)
        st.info(f"ℹ️ Prior output active: **{proc_done}** rows will be skipped.")
        if st.button("🗑 Clear Prior Output", use_container_width=True, key="clear_proc"):
            st.session_state.processed_file_rows = None
            st.rerun()

    st.markdown("---")

    # ── File Upload ────────────────────────────────────────────────────────────
    st.subheader("📂 Input File")
    uploaded_file = st.file_uploader(
        "Upload Excel with company data",
        type=["xlsx", "xls"],
        help="Expected columns: company_name, pincode, address (names detected automatically)",
    )

    if uploaded_file is not None:
        file_bytes = uploaded_file.read()
        # Only reload if it's a new file
        if uploaded_file.name != PS.get("input_filename", "") or not PS["rows"]:
            df, err = load_input_excel(file_bytes, uploaded_file.name)
            if err:
                st.error(f"❌ {err}")
            else:
                # Determine resume source: prefer processed-output Excel over same-file resume
                proc_rows = st.session_state.processed_file_rows

                with LOCK:
                    existing = PS.get("rows", [])

                    if proc_rows:
                        # Resume from previously exported output Excel (name-based match)
                        new_rows = merge_output_with_input(df, proc_rows, match_by_name=True)
                        done_count = detect_resume_rows(new_rows)
                        st.info(
                            f"▶ Resuming from prior output — "
                            f"**{done_count}/{len(new_rows)}** rows already done, "
                            f"continuing from next pending row."
                        )
                    elif existing and uploaded_file.name == PS.get("input_filename", ""):
                        # Same file → positional resume: keep done rows
                        new_rows = merge_output_with_input(df, existing)
                        done_count = detect_resume_rows(new_rows)
                        st.info(f"▶ Resuming — {done_count}/{len(new_rows)} rows already done.")
                    else:
                        # New file, no prior output → fresh start
                        new_rows = [
                            {
                                "company_name": r["company_name"],
                                "pincode": r["pincode"],
                                "address": r["address"],
                                "url_fetched": "",
                                "match_type": "",
                                "contact_number": "",
                                "status": "pending",
                                "error": "",
                            }
                            for _, r in df.iterrows()
                        ]
                        done_count = 0

                    PS["rows"]           = new_rows
                    PS["total"]          = len(new_rows)
                    PS["processed"]      = done_count
                    PS["failed"]         = sum(1 for r in new_rows if r.get("status") == "failed")
                    PS["row_status"]     = {i: r.get("status", "pending") for i, r in enumerate(new_rows)}
                    PS["input_filename"] = uploaded_file.name
                    PS["paused_quota"]   = False
                    st.session_state.input_file_bytes = file_bytes

                st.success(f"✅ Loaded {len(new_rows)} companies from **{uploaded_file.name}**")

    st.markdown("---")

    # ── Concurrency Slider ────────────────────────────────────────────────────
    st.subheader("⚙️ Settings")
    max_workers = st.slider(
        "Parallel Workers",
        min_value=1, max_value=10, value=5,
        help="Number of concurrent company lookups. Higher = faster but more API load.",
    )

    st.markdown("---")

    # ── Control Buttons ───────────────────────────────────────────────────────
    st.subheader("🎛️ Controls")

    has_data    = bool(PS.get("rows"))
    has_api_key = bool(PS.get("api_key", "").strip())
    running     = is_running()

    col_s1, col_s2 = st.columns(2)
    with col_s1:
        start_disabled = not has_data or not has_api_key or running
        if st.button("▶ Start", disabled=start_disabled, use_container_width=True, type="primary"):
            with LOCK:
                PS["paused_quota"] = False
            start_pipeline(max_workers)
            st.rerun()

    with col_s2:
        if st.button("⏹ Stop", disabled=not running, use_container_width=True):
            stop_pipeline()
            st.info("Stop signal sent — finishing current batch...")
            time.sleep(1)
            st.rerun()

    if st.button("🗑 Clear All", use_container_width=True):
        st.session_state.confirm_clear = True

    if st.session_state.confirm_clear:
        st.warning("⚠️ This will clear ALL data and results.")
        col_c1, col_c2 = st.columns(2)
        with col_c1:
            if st.button("Yes, Clear", type="primary", use_container_width=True):
                stop_pipeline()
                time.sleep(0.5)
                new_state = make_initial_state()
                new_state["api_key"] = PS.get("api_key", "")
                st.session_state.pipeline_state = new_state
                PS = st.session_state.pipeline_state  # re-bind
                st.session_state.input_file_bytes = None
                st.session_state.processed_file_rows = None
                st.session_state.confirm_clear = False
                st.rerun()
        with col_c2:
            if st.button("Cancel", use_container_width=True):
                st.session_state.confirm_clear = False
                st.rerun()

    st.markdown("---")

    # ── Download Button (always visible if data exists) ───────────────────────
    st.subheader("⬇️ Export")
    with LOCK:
        all_rows = list(PS.get("rows", []))
    done_rows = [r for r in all_rows if r.get("status") == "done"]

    if done_rows:
        excel_bytes = export_to_excel(done_rows)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        st.download_button(
            label=f"⬇ Download Excel ({len(done_rows)} rows)",
            data=excel_bytes,
            file_name=f"indiamart_leads_{ts}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary",
        )
    else:
        st.button("⬇ Download Excel", disabled=True, use_container_width=True,
                  help="Process some rows first to enable download.")


# ──────────────────────────────────────────────────────────────────────────────
#  MAIN DASHBOARD
# ──────────────────────────────────────────────────────────────────────────────

st.title("🏭 IndiaMart Lead Enricher")
st.caption("Automated B2B contact enrichment via Serper API + IndiaMart scraping")

# ── Quota Exhausted Alert ─────────────────────────────────────────────────────
with LOCK:
    paused_quota = PS.get("paused_quota", False)

if paused_quota:
    st.error(
        "⚠️ **Serper API credits exhausted.** "
        "Please enter a new API key below to continue. "
        "Your progress has been saved and will resume from where it stopped.",
        icon="🚨"
    )
    new_key = st.text_input(
        "New Serper API Key",
        type="password",
        placeholder="Paste new key here...",
        key="new_api_key_resume",
    )
    if st.button("▶ Resume with New Key", type="primary"):
        if new_key.strip():
            valid, msg = validate_api_key(new_key.strip())
            if valid:
                with LOCK:
                    PS["api_key"]      = new_key.strip()
                    PS["api_valid"]    = True
                    PS["paused_quota"] = False
                start_pipeline(max_workers)
                st.success("✅ Resuming with new API key...")
                time.sleep(1)
                st.rerun()
            else:
                st.error(f"❌ {msg}")
        else:
            st.warning("Please enter a key first.")
    st.markdown("---")

# ── Progress Bar ──────────────────────────────────────────────────────────────
stats = get_stats()
pct   = get_completion_pct()

st.markdown("### 📊 Progress")
st.progress(pct, text=f"{int(pct*100)}% complete")

# ── Stats Row ─────────────────────────────────────────────────────────────────
col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("🏢 Total",       stats["total"])
col2.metric("✅ Processed",   stats["processed"])
col3.metric("🔄 In Progress", stats["in_progress"])
col4.metric("❌ Failed",      stats["failed"])
col5.metric("⏳ Remaining",   stats["remaining"])
col6.metric("💳 API Credits", stats["credits"])

# Running indicator
if is_running():
    st.info("🔄 **Processing in progress...** Dashboard refreshes automatically.")
elif stats["processed"] > 0 and stats["remaining"] == 0 and stats["total"] > 0:
    st.success("🎉 **All companies processed!** Download your results from the sidebar.")

st.markdown("---")

# ── Live Processing Table ─────────────────────────────────────────────────────
with LOCK:
    all_rows    = list(PS.get("rows", []))
    row_status  = dict(PS.get("row_status", {}))

if all_rows:
    st.markdown("### 📋 Live Processing Status")

    # Show current batch (processing + recently done), then rest
    batch_size = max_workers * 2
    processing_indices = [i for i, s in row_status.items() if s == "processing"]
    recent_done_indices = [
        i for i, r in enumerate(all_rows)
        if r.get("status") == "done"
    ][-batch_size:]

    show_indices = sorted(set(processing_indices + recent_done_indices))
    if not show_indices and all_rows:
        # Show first N if nothing is processing yet
        show_indices = list(range(min(20, len(all_rows))))

    live_display = []
    for i in show_indices:
        if i < len(all_rows):
            r = all_rows[i]
            s = row_status.get(i, r.get("status", "pending"))
            live_display.append({
                "#":       i + 1,
                "Status":  status_emoji(s) + " " + s.upper(),
                "Company": r.get("company_name", ""),
                "Pincode": r.get("pincode", ""),
                "URL":     r.get("url_fetched", "")[:60] or "—",
                "Match":   r.get("match_type", "") or "—",
                "Phone":   r.get("contact_number", "") or "—",
            })

    if live_display:
        st.dataframe(
            pd.DataFrame(live_display),
            use_container_width=True,
            height=300,
            hide_index=True,
        )
else:
    st.info("📂 Upload an Excel file from the sidebar to get started.")

st.markdown("---")

# ── Full Results Table ────────────────────────────────────────────────────────
done_rows = [r for r in all_rows if r.get("status") == "done"]

if done_rows:
    st.markdown(f"### 📈 Results ({len(done_rows)} completed)")

    # Filters
    filter_col1, filter_col2 = st.columns(2)
    with filter_col1:
        filter_match = st.selectbox(
            "Filter by Match Type",
            options=["All", "name+pincode", "name+locality", "name_only", "— No URL"],
            index=0,
        )
    with filter_col2:
        filter_phone = st.selectbox(
            "Filter by Contact",
            options=["All", "Has Phone", "No Phone"],
            index=0,
        )

    results_df = rows_to_dataframe(done_rows)

    # Apply filters
    if filter_match != "All":
        match_val = "" if filter_match == "— No URL" else filter_match
        if filter_match == "— No URL":
            results_df = results_df[results_df["IndiaMart URL"].str.strip() == ""]
        else:
            results_df = results_df[results_df["Match Type"].str.contains(filter_match, na=False)]

    if filter_phone == "Has Phone":
        results_df = results_df[
            results_df["Contact Number"].str.strip().astype(bool) &
            ~results_df["Contact Number"].isin(["Not Found", "No URL", "Scrape Error", ""])
        ]
    elif filter_phone == "No Phone":
        results_df = results_df[
            results_df["Contact Number"].isin(["Not Found", "No URL", "", "Scrape Error"]) |
            results_df["Contact Number"].str.strip().eq("")
        ]

    st.dataframe(
        results_df.drop(columns=["Status", "Error"], errors="ignore"),
        use_container_width=True,
        height=400,
        hide_index=True,
    )

st.markdown("---")

# ── Log Console ───────────────────────────────────────────────────────────────
st.markdown("### 🖥️ Processing Log")
with LOCK:
    log_lines = list(PS.get("log_lines", []))

if log_lines:
    # Show last 100 lines
    log_text = "\n".join(log_lines[-100:])
    st.text_area(
        "Log Output (last 100 lines)",
        value=log_text,
        height=200,
        label_visibility="collapsed",
    )
else:
    st.text_area(
        "Log Output",
        value="Log entries will appear here once processing starts...",
        height=150,
        label_visibility="collapsed",
        disabled=True,
    )

# ── Auto-refresh while running ────────────────────────────────────────────────
if is_running():
    time.sleep(2)
    st.rerun()