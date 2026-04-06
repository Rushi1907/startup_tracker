"""
Startup News Tracker — Google Sheets + Google News RSS
========================================================
Reads startup names from a source Google Sheet, fetches the latest news
for each startup via Google News RSS, and writes de-duplicated results
to a destination Google Sheet.

SETUP (run once before first use):
    pip install google-auth google-auth-oauthlib google-api-python-client feedparser python-dateutil

CREDENTIALS:
    Place your Google service-account JSON at the path set in SERVICE_ACCOUNT_FILE,
    OR use OAuth2 by following the commented-out section at the bottom.

SHEET LAYOUT EXPECTED:
    Source Sheet  → Column A: "Startup Name" (header row 1, data from row 2)
    Output Sheet  → Headers auto-created: Startup | Title | Link | Published | Source | Fetched At
"""

import feedparser
import gspread
import hashlib
import time
import logging
from datetime import datetime, timezone
from dateutil import parser as dateutil_parser
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────
# CONFIG — edit these before running
# ─────────────────────────────────────────────

SERVICE_ACCOUNT_FILE = "service_account.json"   # Path to your Google service account key

SOURCE_SPREADSHEET_ID  = "Startup Tracker"    # Sheet containing startup names
SOURCE_SHEET_NAME      = "Sheet1"                         # Tab name in source sheet
SOURCE_STARTUP_COLUMN  = "Startup Name"                              # Column holding startup names
SOURCE_HEADER_ROW      = 1                                # Row number of the header

OUTPUT_SPREADSHEET_ID  = "Starup Tracker"                 # Sheet to write news into
OUTPUT_SHEET_NAME      = "News_Log"                   # Tab name in output sheet

MAX_ARTICLES_PER_STARTUP = 10   # Latest N articles per startup per run
SLEEP_BETWEEN_STARTUPS   = 1.5  # Seconds to wait between RSS calls (rate-limit safety)

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# GOOGLE SHEETS CLIENT
# ─────────────────────────────────────────────

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

def get_gspread_client() -> gspread.Client:
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


# ─────────────────────────────────────────────
# FETCH STARTUP NAMES FROM SOURCE SHEET
# ─────────────────────────────────────────────

def fetch_startup_names(client: gspread.Client) -> list[str]:
    """
    Returns a list of startup names from the source Google Sheet.
    Skips blank/header cells automatically.
    """
    sh = client.open_by_key(SOURCE_SPREADSHEET_ID)
    ws = sh.worksheet(SOURCE_SHEET_NAME)

    col_index = ord(SOURCE_STARTUP_COLUMN.upper()) - ord("A") + 1  # 'A' → 1
    all_values = ws.col_values(col_index)                           # list of strings

    # Drop the header row and blank cells
    names = [
        v.strip() for v in all_values[SOURCE_HEADER_ROW:]
        if v.strip()
    ]
    log.info(f"Found {len(names)} startups in source sheet.")
    return names


# ─────────────────────────────────────────────
# FETCH NEWS VIA GOOGLE NEWS RSS
# ─────────────────────────────────────────────

def build_rss_url(startup_name: str) -> str:
    """Google News RSS URL for a given query term."""
    from urllib.parse import quote_plus
    query = quote_plus(f'"{startup_name}"')   # Exact-phrase search
    return (
        f"https://news.google.com/rss/search"
        f"?q={query}&hl=en-IN&gl=IN&ceid=IN:en"
    )


def parse_published(entry) -> str:
    """Return ISO-8601 UTC timestamp from an RSS entry, or empty string."""
    raw = getattr(entry, "published", None) or getattr(entry, "updated", None)
    if not raw:
        return ""
    try:
        dt = dateutil_parser.parse(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return raw


def fetch_news(startup_name: str) -> list[dict]:
    """Fetch latest news articles for a startup from Google News RSS."""
    url = build_rss_url(startup_name)
    feed = feedparser.parse(url)

    articles = []
    for entry in feed.entries[:MAX_ARTICLES_PER_STARTUP]:
        articles.append({
            "startup":   startup_name,
            "title":     entry.get("title", "").strip(),
            "link":      entry.get("link", "").strip(),
            "published": parse_published(entry),
            "source":    entry.get("source", {}).get("title", "Google News").strip(),
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        })

    log.info(f"  [{startup_name}] → {len(articles)} articles fetched.")
    return articles


# ─────────────────────────────────────────────
# DEDUPLICATION HELPERS
# ─────────────────────────────────────────────

HEADERS = ["Startup", "Title", "Link", "Published", "Source", "Fetched At"]

def article_fingerprint(article: dict) -> str:
    """
    A stable hash based on startup name + article URL.
    If two runs fetch the same URL for the same startup, fingerprints match → skip.
    """
    key = f"{article['startup'].lower()}||{article['link'].lower()}"
    return hashlib.md5(key.encode()).hexdigest()


def load_existing_fingerprints(ws: gspread.Worksheet) -> set[str]:
    """
    Build a set of fingerprints from rows already in the output sheet.
    Uses only 'Startup' and 'Link' columns (cols 1 and 3) for efficiency.
    """
    existing = ws.get_all_values()
    fingerprints = set()

    if len(existing) <= 1:          # Empty or header-only
        return fingerprints

    try:
        header = [h.lower() for h in existing[0]]
        startup_col = header.index("startup")
        link_col    = header.index("link")
    except ValueError:
        log.warning("Output sheet headers not found; treating all rows as new.")
        return fingerprints

    for row in existing[1:]:
        if len(row) > max(startup_col, link_col):
            key = f"{row[startup_col].lower()}||{row[link_col].lower()}"
            fingerprints.add(hashlib.md5(key.encode()).hexdigest())

    log.info(f"Loaded {len(fingerprints)} existing article fingerprints from output sheet.")
    return fingerprints


# ─────────────────────────────────────────────
# WRITE TO OUTPUT SHEET
# ─────────────────────────────────────────────

def ensure_headers(ws: gspread.Worksheet) -> None:
    """Write header row if the sheet is empty."""
    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(HEADERS, value_input_option="RAW")
        log.info("Header row written to output sheet.")


def write_articles(
    ws: gspread.Worksheet,
    articles: list[dict],
    existing_fps: set[str],
) -> int:
    """
    Append only new (non-duplicate) articles to the output sheet.
    Returns the count of rows actually written.
    """
    new_rows = []
    for article in articles:
        fp = article_fingerprint(article)
        if fp in existing_fps:
            continue
        new_rows.append([
            article["startup"],
            article["title"],
            article["link"],
            article["published"],
            article["source"],
            article["fetched_at"],
        ])
        existing_fps.add(fp)   # Prevent intra-batch duplicates too

    if new_rows:
        ws.append_rows(new_rows, value_input_option="RAW")
        log.info(f"  ✓ {len(new_rows)} new rows written.")
    else:
        log.info("  – No new articles to add.")

    return len(new_rows)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main() -> None:
    log.info("=== Startup News Tracker starting ===")

    client = get_gspread_client()

    # ── Source: read startup names ──────────────────────────────────────
    startup_names = fetch_startup_names(client)
    if not startup_names:
        log.error("No startup names found in source sheet. Exiting.")
        return

    # ── Output: open/create sheet, load existing data ───────────────────
    out_sh = client.open_by_key(OUTPUT_SPREADSHEET_ID)
    try:
        out_ws = out_sh.worksheet(OUTPUT_SHEET_NAME)
        log.info(f"Output worksheet '{OUTPUT_SHEET_NAME}' found.")
    except gspread.WorksheetNotFound:
        out_ws = out_sh.add_worksheet(title=OUTPUT_SHEET_NAME, rows=5000, cols=10)
        log.info(f"Output worksheet '{OUTPUT_SHEET_NAME}' created.")

    ensure_headers(out_ws)
    existing_fingerprints = load_existing_fingerprints(out_ws)

    # ── Fetch & write loop ───────────────────────────────────────────────
    total_new = 0
    for startup in startup_names:
        log.info(f"Processing: {startup}")
        try:
            articles = fetch_news(startup)
            total_new += write_articles(out_ws, articles, existing_fingerprints)
        except Exception as exc:
            log.error(f"  Failed for '{startup}': {exc}")
        time.sleep(SLEEP_BETWEEN_STARTUPS)

    log.info(f"=== Done. {total_new} new articles added across {len(startup_names)} startups. ===")


if __name__ == "__main__":
    main()
