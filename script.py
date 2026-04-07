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
# DATE FILTER (Q1 2026)
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
# AUTHENTICATION
# =========================================================
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
client = gspread.authorize(creds)

# =========================================================
# SOURCE NAME
# =========================================================
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

# =========================================================
# STRONG EVENT FILTER
# =========================================================
def is_strong_event(title):
    strong_keywords = [
        "raises", "raised", "funding", "acquires", "acquisition",
        "launches", "merger", "deal", "secures"
    ]
    return any(k in title.lower() for k in strong_keywords)

# =========================================================
# EVENT SIGNATURE
# =========================================================
def extract_event_signature(title):
    title = title.lower()

    if "raise" in title or "funding" in title:
        event_type = "funding"
    elif "acquire" in title:
        event_type = "acquisition"
    else:
        event_type = "other"

    amount_match = re.search(r'\$(\d+)\s?(m|b)', title)
    amount = amount_match.group(0) if amount_match else "unknown"

    valuation_match = re.search(r'\$?\d+\s?b', title)
    valuation = valuation_match.group(0) if valuation_match else "unknown"

    return event_type, amount, valuation

# =========================================================
# INSIGHT FUNCTION
# =========================================================
def generate_insight(title):
    title = title.lower()

    if "funding" in title or "raises" in title or "raised" in title:
        return "Startup secured funding → growth & investor confidence"
    elif "acquire" in title:
        return "Acquisition → expansion or market consolidation"
    elif "launch" in title:
        return "Product launch → innovation signal"
    else:
        return "General update → monitor"

# =========================================================
# READ STARTUPS
# =========================================================
sheet = client.open("Startup Tracker").sheet1
data = sheet.get_all_records()

startup_map = {
    row["Startup Name"].strip().lower(): row["Startup Name"].strip()
    for row in data
    if row["Startup Name"]
}

startups = list(startup_map.keys())

print("Startups loaded:", startups)

# =========================================================
# FETCH NEWS
# =========================================================
all_articles = []

# ---------------- GOOGLE NEWS ----------------
for startup in startups:
    query = f"{startup} (funding OR acquisition OR launch) after:2026-01-01"
    encoded_query = quote_plus(query)

    google_url = f"https://news.google.com/rss/search?q={encoded_query}"
    feed = feedparser.parse(google_url)

    source_name = get_feed_name(feed, google_url)

    for entry in feed.entries:
        if not hasattr(entry, "published_parsed") or entry.published_parsed is None:
            continue

        published_time = datetime(*entry.published_parsed[:6])
        if published_time < Q1_START:
            continue

        title_raw = entry.get("title", "")
        title = title_raw.lower()

        # STRICT ENTITY MATCH
        if not re.search(rf'\b{re.escape(startup)}\b', title):
            continue

        # REMOVE WEAK CONTEXT
        if any(w in title for w in ["rival", "backed", "related"]):
            continue

        # STRONG EVENT ONLY
        if not is_strong_event(title):
            continue

        all_articles.append([
            startup_map[startup],
            title_raw,
            entry.get("link", ""),
            entry.get("published", ""),
            source_name,
            generate_insight(title_raw),
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            published_time
        ])

# ---------------- CUSTOM RSS ----------------
for feed_url in RSS_FEEDS:
    feed = feedparser.parse(feed_url)
    source_name = get_feed_name(feed, feed_url)

    for entry in feed.entries:
        title_raw = entry.get("title", "")
        title = title_raw.lower()

        if not is_strong_event(title):
            continue

        for startup in startups:
            if not re.search(rf'\b{re.escape(startup)}\b', title):
                continue

            if any(w in title for w in ["rival", "backed", "related"]):
                continue

            if hasattr(entry, "published_parsed") and entry.published_parsed:
                published_time = datetime(*entry.published_parsed[:6])
            else:
                published_time = datetime.utcnow()

            if published_time < Q1_START:
                continue

            all_articles.append([
                startup_map[startup],
                title_raw,
                entry.get("link", ""),
                entry.get("published", ""),
                source_name,
                generate_insight(title_raw),
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                published_time
            ])

# =========================================================
# 🔥 DECISION-LEVEL DEDUP
# =========================================================
event_groups = {}

for row in all_articles:
    startup, title, link, pub, source, insight, fetched, published_time = row

    event_type, amount, valuation = extract_event_signature(title)

    if event_type == "other":
        continue

    if amount == "unknown" and valuation == "unknown":
        continue

    date_key = published_time.strftime("%Y-%m-%d")
    key = f"{startup}_{event_type}_{amount}_{valuation}_{date_key}"

    if key not in event_groups or len(title) > len(event_groups[key][1]):
        event_groups[key] = row

all_articles = list(event_groups.values())

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
    "Published_dt"
])

df.drop(columns=["Published_dt"], inplace=True)
df.drop_duplicates(subset=["Title", "Link"], inplace=True)

print(f"\nFiltered rows after refinement: {len(df)}")

# =========================================================
# WRITE TO GOOGLE SHEET
# =========================================================
output_sheet = client.open("Startup Tracker").worksheet("News_Log_V2")

if df.empty:
    print("✅ No new relevant data")
else:
    output_sheet.append_rows(df.values.tolist(), value_input_option='RAW')
    print("✅ High-quality decision-level insights added")
