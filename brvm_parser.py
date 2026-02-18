"""
BRVM Daily Bulletin Parser
--------------------------
Downloads the daily BOC (Bulletin Officiel de la Cote) PDF from brvm.org,
extracts stock data, and upserts to Supabase.

URL pattern: https://www.brvm.org/sites/default/files/boc_YYYYMMDD_2.pdf
Published: Monday-Friday (excluding UEMOA public holidays)

Usage:
  python brvm_parser.py                    # process today / last trading day
  python brvm_parser.py --date 2026-02-13  # process a specific date
  python brvm_parser.py --backfill 30      # backfill last N trading days
"""

import re
import sys
import time
import logging
import argparse
import io
import os
from datetime import date, timedelta

import requests
import pdfplumber

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

BOC_URL = "https://www.brvm.org/sites/default/files/boc_{date}_2.pdf"

# UEMOA public holidays (add more as needed)
HOLIDAYS = {
    date(2026, 1, 1),   # Jour de l'An
    date(2026, 4, 6),   # Pâques
    date(2026, 5, 1),   # Fête du Travail
    date(2026, 11, 1),  # Toussaint
    date(2026, 12, 25), # Noël
    date(2025, 1, 1),
    date(2025, 4, 21),
    date(2025, 5, 1),
    date(2025, 11, 1),
    date(2025, 12, 25),
}


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d not in HOLIDAYS  # Mon-Fri, not a holiday


def last_trading_day() -> date:
    d = date.today()
    while not is_trading_day(d):
        d -= timedelta(1)
    return d


def trading_days_back(n: int):
    """Return the last n trading days (most recent first)."""
    days = []
    d = date.today()
    while len(days) < n:
        if is_trading_day(d):
            days.append(d)
        d -= timedelta(1)
    return days


# ---------------------------------------------------------------------------
# PDF download with retry
# ---------------------------------------------------------------------------

def download_pdf(bulletin_date: date, retries: int = 3) -> bytes | None:
    url = BOC_URL.format(date=bulletin_date.strftime("%Y%m%d"))
    log.info(f"Downloading: {url}")

    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, verify=False, timeout=30)
            if r.status_code == 200:
                log.info(f"Downloaded {len(r.content) // 1024} KB")
                return r.content
            elif r.status_code == 404:
                log.info(f"No bulletin for {bulletin_date} (404 — holiday or weekend)")
                return None
            else:
                log.warning(f"HTTP {r.status_code} on attempt {attempt}/{retries}")
                if attempt < retries:
                    time.sleep(5 * attempt)  # backoff: 5s, 10s
        except requests.RequestException as e:
            log.warning(f"Request error attempt {attempt}/{retries}: {e}")
            if attempt < retries:
                time.sleep(5 * attempt)

    log.error(f"Failed to download bulletin for {bulletin_date} after {retries} attempts")
    return None


# ---------------------------------------------------------------------------
# PDF parsing
# ---------------------------------------------------------------------------

SECTOR_CODES = {"CB", "CD", "TEL", "FIN", "IND", "ENE", "SPU"}
TICKER_RE = re.compile(r"^[A-Z]{3,6}$")


def clean_num(s) -> float | None:
    if s is None:
        return None
    cleaned = re.sub(r"[^\d,.\-]", "", str(s)).replace(",", ".")
    # Handle numbers like "1 226,\n13" → take before newline
    cleaned = cleaned.split("\n")[0].split("\\")[0]
    try:
        return float(cleaned) if cleaned not in ("", "-") else None
    except ValueError:
        return None


def parse_stock_row(row: list, bulletin_date: date) -> dict | None:
    """Parse one row from a stocks table. Returns None if not a valid stock row."""
    if not row or len(row) < 12:
        return None
    if str(row[0]).strip() not in SECTOR_CODES:
        return None
    ticker = str(row[1]).strip() if row[1] else ""
    if not TICKER_RE.match(ticker):
        return None

    def pct(col_idx):
        if len(row) <= col_idx or not row[col_idx]:
            return None
        return clean_num(str(row[col_idx]).replace("%", ""))

    return {
        "date":             bulletin_date.isoformat(),
        "secteur":          str(row[0]).strip(),
        "ticker":           ticker,
        "compagnie":        str(row[2]).strip().replace("\n", " ") if row[2] else None,
        "cours_precedent":  clean_num(row[4]),
        "cours_ouv":        clean_num(row[5]),
        "cours_cloture":    clean_num(row[6]),
        "variation_jour":   pct(7),
        "volume":           int(clean_num(str(row[8]).replace(" ", "")) or 0),
        "valeur_transigee": int(clean_num(str(row[9]).replace(" ", "")) or 0),
        "cours_reference":  clean_num(row[10]),
        "variation_ytd":    pct(11),
        "dernier_div":      clean_num(row[12]) if len(row) > 12 else None,
        "date_div":         str(row[13]).strip() if len(row) > 13 and row[13] else None,
        "rendement_net":    pct(14) if len(row) > 14 else None,
        "per":              clean_num(str(row[15]).replace("\n", " ").split(",")[0]) if len(row) > 15 and row[15] else None,
    }


def parse_bulletin(pdf_bytes: bytes, bulletin_date: date) -> list[dict]:
    stocks = []
    seen_tickers = set()

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        # Stocks appear on pages 2-5 (0-indexed: 1-4)
        for page_idx in range(1, 6):
            if page_idx >= len(pdf.pages):
                break
            page = pdf.pages[page_idx]
            for table in page.extract_tables():
                for row in table:
                    record = parse_stock_row(row, bulletin_date)
                    if record and record["ticker"] not in seen_tickers:
                        stocks.append(record)
                        seen_tickers.add(record["ticker"])

    log.info(f"Parsed {len(stocks)} stocks for {bulletin_date}")
    return stocks


# ---------------------------------------------------------------------------
# Supabase upsert
# ---------------------------------------------------------------------------

def upsert_to_supabase(records: list[dict]) -> bool:
    if not records:
        log.warning("No records to upsert")
        return True
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL and SUPABASE_SERVICE_KEY env vars required")
        return False

    url = f"{SUPABASE_URL}/rest/v1/brvm_cotation_journaliere"
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates",  # upsert on conflict
    }

    # Batch upsert (Supabase REST handles arrays)
    r = requests.post(url, json=records, headers=headers, timeout=30)
    if r.status_code in (200, 201):
        log.info(f"✅ Upserted {len(records)} records to Supabase")
        return True
    else:
        log.error(f"Supabase error {r.status_code}: {r.text[:500]}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_date(d: date) -> bool:
    pdf = download_pdf(d)
    if pdf is None:
        return True  # Not an error — no bulletin for this day

    stocks = parse_bulletin(pdf, d)
    if not stocks:
        log.error(f"Parser returned 0 stocks for {d} — check PDF structure")
        return False

    return upsert_to_supabase(stocks)


def main():
    parser = argparse.ArgumentParser(description="BRVM daily bulletin parser")
    parser.add_argument("--date",     help="Process specific date (YYYY-MM-DD)")
    parser.add_argument("--backfill", type=int, metavar="N", help="Backfill last N trading days")
    parser.add_argument("--dry-run",  action="store_true", help="Parse but don't write to Supabase")
    args = parser.parse_args()

    if args.date:
        dates = [date.fromisoformat(args.date)]
    elif args.backfill:
        dates = trading_days_back(args.backfill)
        log.info(f"Backfilling {len(dates)} trading days: {dates[-1]} → {dates[0]}")
        dates = list(reversed(dates))  # oldest first
    else:
        dates = [last_trading_day()]

    success = 0
    for d in dates:
        log.info(f"--- Processing {d} ---")
        if args.dry_run:
            pdf = download_pdf(d)
            if pdf:
                stocks = parse_bulletin(pdf, d)
                for s in stocks:
                    print(f"  {s['ticker']:8} | {s['cours_cloture']:>10} | {s['variation_jour']:>6}%")
            success += 1
        else:
            if process_date(d):
                success += 1
            else:
                log.error(f"Failed for {d}")
            if len(dates) > 1:
                time.sleep(1)  # Be polite to brvm.org

    log.info(f"Done: {success}/{len(dates)} dates processed successfully")
    sys.exit(0 if success == len(dates) else 1)


if __name__ == "__main__":
    main()
