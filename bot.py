import os
import sys
import io
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
import smtplib
from email.message import EmailMessage

import pandas as pd
import requests

NERC_URL = "https://www.nerc.com/pa/Stand/AlignRep/One%20Stop%20Shop.xlsx"

DATA_DIR = "data"
OUT_DIR = "output"
CURRENT_XLSX = os.path.join(DATA_DIR, "nerc_current.xlsx")
PREV_XLSX = os.path.join(DATA_DIR, "nerc_previous.xlsx")
SUMMARY_MD = os.path.join(OUT_DIR, "summary.md")

def log(msg: str):
    print(msg, flush=True)

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)

def is_8am_central_now():
    """Return True iff the current time in America/Chicago is 08:00 (hour == 8)."""
    now_utc = datetime.now(ZoneInfo("UTC"))
    now_ct = now_utc.astimezone(ZoneInfo("America/Chicago"))
    return now_ct.hour == 8

def download_excel(url: str, path: str):
    log("Downloading latest spreadsheet...")
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NERC-OneStop-Bot/1.0)"}
    r = requests.get(url, headers=headers, timeout=120)
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)
    log(f"Saved to {path}")

def load_first_sheet(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0, dtype=str, engine="openpyxl")
    df = df.fillna("")
    # normalize column names a bit
    df.columns = [str(c).strip() for c in df.columns]
    return df

def guess_primary_key(df: pd.DataFrame):
    # Try to find a column that is unique and non-null; prefer obvious id names first.
    preferred = ["ID", "Id", "id", "RowID", "Row Id", "Row #", "Requirement ID", "Req ID", "Req#", "Standard", "Project ID", "Project #"]
    for col in preferred:
        if col in df.columns:
            s = df[col].astype(str)
            if not s.isna().any() and s.is_unique:
                return col
    # Fallback: any unique non-null column
    for col in df.columns:
        s = df[col].astype(str)
        if not s.isna().any() and s.is_unique:
            return col
    return None  # will use row hashes fallback

def row_hash(row, cols):
    # Deterministic hash for a row across given columns
    buff = "||".join(str(row[c]) for c in cols)
    return hashlib.sha256(buff.encode("utf-8")).hexdigest()

def compare_dataframes(prev: pd.DataFrame, curr: pd.DataFrame):
    """Return a dict with added, removed, and changed details."""
    all_cols = sorted(set(prev.columns) | set(curr.columns))
    prev = prev.reindex(columns=all_cols, fill_value="")
    curr = curr.reindex(columns=all_cols, fill_value="")

    key = guess_primary_key(curr) or guess_primary_key(prev)
    result = {
        "key": key,
        "columns": all_cols,
        "added_keys": [],
        "removed_keys": [],
        "changed_rows": [],  # list of dicts: {"key": k or None, "diffs": [(col, old, new), ...]}
        "added_count": 0,
        "removed_count": 0,
        "changed_cells_count": 0
    }

    if key:
        prev_idx = prev.set_index(key, drop=False)
        curr_idx = curr.set_index(key, drop=False)

        prev_keys = set(prev_idx.index)
        curr_keys = set(curr_idx.index)

        added_keys = sorted(curr_keys - prev_keys)
        removed_keys = sorted(prev_keys - curr_keys)
        common_keys = sorted(prev_keys & curr_keys)

        result["added_keys"] = added_keys
        result["removed_keys"] = removed_keys
        result["added_count"] = len(added_keys)
        result["removed_count"] = len(removed_keys)

        for k in common_keys:
            rp = prev_idx.loc[k]
            rc = curr_idx.loc[k]
            diffs = []
            for c in all_cols:
                vp = str(rp[c])
                vc = str(rc[c])
                if vp != vc:
                    diffs.append((c, vp, vc))
            if diffs:
                result["changed_rows"].append({"key": k, "diffs": diffs})
                result["changed_cells_count"] += len(diffs)
    else:
        # Fallback to row-hash approach (won't show per-cell diffs, but still flags changes).
        prev["__hash__"] = prev.apply(lambda r: row_hash(r, all_cols), axis=1)
        curr["__hash__"] = curr.apply(lambda r: row_hash(r, all_cols), axis=1)

        prev_hashes = set(prev["__hash__"])
        curr_hashes = set(curr["__hash__"])

        added = curr[~curr["__hash__"].isin(prev_hashes)]
        removed = prev[~prev["__hash__"].isin(curr_hashes)]

        result["added_count"] = len(added)
        result["removed_count"] = len(removed)
        result["added_keys"] = []  # no keys in hash mode
        result["removed_keys"] = []

        # In hash fallback, treat all as changed rows (coarse)
        # We won’t compute changed_cells_count here.
        for _, row in added.head(20).iterrows():
            result["changed_rows"].append({"key": None, "diffs": [("ROW_ADDED", "", "…")]})
        for _, row in removed.head(20).iterrows():
            result["changed_rows"].append({"key": None, "diffs": [("ROW_REMOVED", "…", "")]})

    return result

def build_summary_md(result, first_run=False):
    now_ct = datetime.now(ZoneInfo("America/Chicago"))
    stamp = now_ct.strftime("%Y-%m-%d %H:%M %Z")

    if first_run:
        lines = [
            f"# NERC One Stop Shop – Baseline Established",
            f"**Time:** {stamp}",
            "",
            "Saved the current spreadsheet as the baseline for future comparisons.",
        ]
    else:
        lines = [
            f"# NERC One Stop Shop – Daily Change Summary",
            f"**Time:** {stamp}",
            "",
            f"- Added rows: **{result['added_count']}**",
            f"- Removed rows: **{result['removed_count']}**",
        ]
        if result["key"]:
            lines.append(f"- Key column used: **{result['key']}**")
            lines.append(f"- Changed cells: **{result['changed_cells_count']}**")
        if result["changed_rows"]:
            lines.append("")
            lines.append("## Sample Changes (up to 20)")
            shown = 0
            for row in result["changed_rows"]:
                if shown >= 20:
                    break
                header = f"- **Row key:** {row['key']}" if result["key"] else "- **Row change**"
                lines.append(header)
                for (c, old, new) in row["diffs"][:6]:  # cap per-row detail
                    lines.append(f"    - `{c}`: `{old}` → `{new}`")
                shown += 1
        else:
            lines.append("")
            lines.append("_No changes detected._")

    return "\n".join(lines)

def send_email(subject: str, body: str):
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    to_raw = os.getenv("EMAIL_TO", "")
    from_name = os.getenv("FROM_NAME", "NERC One Stop Bot")

    if not smtp_user or not smtp_pass or not to_raw:
        log("Missing SMTP credentials or recipients. Skipping email.")
        return

    recipients = [e.strip() for e in to_raw.split(",") if e.strip()]
    if not recipients:
        log("No recipients found in EMAIL_TO. Skipping email.")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{smtp_user}>"
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    log(f"Sending email to: {', '.join(recipients)}")
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.ehlo()
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
    log("Email sent.")

def main():
    ensure_dirs()

    # If triggered by schedule twice/day for DST, only run at 8am CT.
    event_name = os.getenv("GITHUB_EVENT_NAME", "")
    if event_name == "schedule" and not is_8am_central_now():
        log("Not 8am Central; skipping this duplicate schedule run.")
        return

    download_excel(NERC_URL, CURRENT_XLSX)

    first_run = not os.path.exists(PREV_XLSX)
    if first_run:
        # Save baseline
        os.replace(CURRENT_XLSX, PREV_XLSX)
        md = build_summary_md({}, first_run=True)
        with open(SUMMARY_MD, "w", encoding="utf-8") as f:
            f.write(md)
        send_email("NERC One Stop Shop: baseline established", md)
        log("Baseline saved.")
        return

    prev_df = load_first_sheet(PREV_XLSX)
    curr_df = load_first_sheet(CURRENT_XLSX)
    result = compare_dataframes(prev_df, curr_df)

    md = build_summary_md(result, first_run=False)
    with open(SUMMARY_MD, "w", encoding="utf-8") as f:
        f.write(md)

    changed = (result["added_count"] > 0) or (result["removed_count"] > 0) or (len(result["changed_rows"]) > 0)
    if changed:
        send_email(
            subject=f"NERC One Stop Shop: changes detected – {datetime.now(ZoneInfo('America/Chicago')).strftime('%Y-%m-%d')}",
            body=md
        )
        # Move current to baseline for next run
        os.replace(CURRENT_XLSX, PREV_XLSX)
        log("Baseline updated after changes.")
    else:
        log("No changes detected; keeping existing baseline.")
        # Remove unused download
        try:
            os.remove(CURRENT_XLSX)
        except FileNotFoundError:
            pass

if __name__ == "__main__":
    main()
