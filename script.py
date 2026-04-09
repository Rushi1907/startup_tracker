# =========================================================
# IMPORTS
# =========================================================
import gspread
import feedparser
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime
from urllib.parse import quote_plus
import os
import json
import re

# =========================================================
# DATE FILTER
# =========================================================
Q1_START = datetime(2026, 1, 1)

# =========================================================
# RSS FEEDS
# =========================================================
RSS_FEEDS = [
    "https://techcrunch.com/feed/",
    "https://news.crunchbase.com/feed/",
    "https://venturebeat.com/feed/",
    "https://sportstechx.com/feed/",
    "https://www.sportbusiness.com/feed/",
    "https://www.sportsbusinessjournal.com/Feeds/All-News.aspx",
    "https://www.theverge.com/rss/index.xml",
    "https://www.technologyreview.com/feed/",
    "https://www.streamingmedia.com/RSS.aspx",
    "https://www.roadtovr.com/feed/"
]

# =========================================================
# AUTH
# =========================================================
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
client = gspread.authorize(creds)

# =========================================================
# HELPERS
# =========================================================
def clean_title(title):
    return re.sub(r'[^a-zA-Z0-9 ]', '', title.lower())

def get_feed_name(feed, feed_url):
    try:
        if hasattr(feed, "feed") and "title" in feed.feed:
            return feed.feed.title.strip()
    except:
        pass

    domain = feed_url.replace("https://", "").replace("http://", "").split("/")[0]

    domain_map = {
        "techcrunch.com": "TechCrunch",
        "venturebeat.com": "VentureBeat",
        "sportstechx.com": "SportsTechX",
        "sportbusiness.com": "SportBusiness",
        "sportsbusinessjournal.com": "SBJ",
        "theverge.com": "The Verge",
        "technologyreview.com": "MIT Tech Review",
        "streamingmedia.com": "Streaming Media",
        "roadtovr.com": "Road to VR",
        "news.google.com": "Google News"
    }

    return domain_map.get(domain, domain)

def generate_insight(title):
    title = title.lower()

    if "funding" in title or "raises" in title:
        return "Startup secured funding → growth & investor confidence"
    elif "acquire" in title:
        return "Acquisition → expansion or consolidation"
    elif "launch" in title:
        return "Product launch → innovation signal"
    elif "partnership" in title:
        return "Strategic partnership → scaling opportunity"
    else:
        return "General update → monitor"

def is_relevant(title):
    keywords = [
        "funding", "raises", "raised", "acquire", "acquired",
        "launch", "launches", "partnership", "investment",
        "expansion", "deal", "merger"
    ]
    return any(k in title.lower() for k in keywords)

def extract_event_signature(title):
    title = clean_title(title)

    if "raise" in title or "funding" in title:
        event_type = "funding"
    elif "acquire" in title:
        event_type = "acquisition"
    elif "launch" in title:
        event_type = "launch"
    else:
        event_type = "other"

    amount_match = re.search(r'\b\d+\s?(m|b)\b', title)
    amount = amount_match.group(0) if amount_match else ""

    valuation_match = re.search(r'\b\d+\s?b valuation\b', title)
    valuation = valuation_match.group(0) if valuation_match else ""

    return event_type, amount, valuation

def generate_event_key(startup, title):
    event_type, amount, valuation = extract_event_signature(title)

    if amount or valuation:
        return f"{startup.lower()}_{event_type}_{amount}_{valuation}"

    # fallback key
    return f"{startup.lower()}_{event_type}_{clean_title(title)[:50]}"

# =========================================================
# LOAD STARTUPS
# =========================================================
sheet = client.open("Startup Tracker").sheet1
data = sheet.get_all_records()

startup_map = {
    row["Startup Name"].strip().lower(): row["Startup Name"].strip()
    for row in data if row["Startup Name"]
}

startups = list(startup_map.keys())
print("Startups loaded:", startups)

# =========================================================
# FETCH NEWS
# =========================================================
all_articles = []

# -------- GOOGLE NEWS --------
for startup in startups:
    query = f"{startup} (funding OR acquisition OR launch) after:2026-01-01"
    google_url = f"https://news.google.com/rss/search?q={quote_plus(query)}"

    feed = feedparser.parse(google_url)
    source_name = get_feed_name(feed, google_url)

    for entry in feed.entries:
        if not entry.get("published_parsed"):
            continue

        published_time = datetime(*entry.published_parsed[:6])

        if published_time < Q1_START:
            continue

        title = entry.get("title", "")
        if not is_relevant(title):
            continue

        if not re.search(rf'\b{re.escape(startup)}\b', title.lower()):
            continue

        all_articles.append([
            startup_map[startup],
            title,
            entry.get("link", ""),
            entry.get("published", ""),
            source_name,
            generate_insight(title),
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            generate_event_key(startup, title)
        ])

# -------- CUSTOM RSS --------
for feed_url in RSS_FEEDS:
    feed = feedparser.parse(feed_url)
    source_name = get_feed_name(feed, feed_url)

    for entry in feed.entries:
        title = entry.get("title", "")
        if not is_relevant(title):
            continue

        for startup in startups:
            if not re.search(rf'\b{re.escape(startup)}\b', title.lower()):
                continue

            if entry.get("published_parsed"):
                published_time = datetime(*entry.published_parsed[:6])
            else:
                published_time = datetime.utcnow()

            if published_time < Q1_START:
                continue

            all_articles.append([
                startup_map[startup],
                title,
                entry.get("link", ""),
                entry.get("published", ""),
                source_name,
                generate_insight(title),
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                generate_event_key(startup, title)
            ])

# =========================================================
# DATAFRAME
# =========================================================
df = pd.DataFrame(all_articles, columns=[
    "Startup Name",
    "Title",
    "Link",
    "Published",
    "Source",
    "Insights",
    "Fetched At",
    "Event Key"
])

if df.empty:
    print("No data fetched")
    exit()

# =========================================================
# REMOVE DUPLICATES WITHIN RUN
# =========================================================
df = df.drop_duplicates(subset=["Event Key"])

# =========================================================
# LOAD EXISTING KEYS
# =========================================================
output_sheet = client.open("Startup Tracker").worksheet("News_Log_V2")

def get_existing_event_keys(sheet):
    data = sheet.get_all_values()

    if len(data) <= 1:
        return set()

    headers = data[0]
    headers = [h.strip() for h in headers]

    if "Event Key" not in headers:
        raise ValueError("❌ 'Event Key' column missing in sheet")

    idx = headers.index("Event Key")

    return set(row[idx] for row in data[1:] if len(row) > idx)

existing_keys = get_existing_event_keys(output_sheet)

# =========================================================
# FILTER NEW DATA
# =========================================================
df_new = df[~df["Event Key"].isin(existing_keys)]

print(f"\nNew rows: {len(df_new)}")

# =========================================================
# WRITE TO SHEET
# =========================================================
status_sheet = client.open("Startup Tracker").worksheet("System_Status")
current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

if df_new.empty:
    print("✅ No new updates")
    status_sheet.update("A2", [[current_time, "No New Updates", 0]])
else:
    output_sheet.append_rows(df_new.values.tolist(), value_input_option='RAW')
    print("✅ Only NEW data added")
    status_sheet.update("A2", [[current_time, "Updated", len(df_new)]])
