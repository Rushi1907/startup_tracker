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
from difflib import SequenceMatcher

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
# CLEAN TITLE (FOR DEDUP)
# =========================================================
def clean_title(title):
    title = title.lower()
    title = re.sub(r'[^a-z0-9\s]', '', title)
    title = re.sub(r'\s+', ' ', title).strip()
    return title

# =========================================================
# SIMILARITY FUNCTION
# =========================================================
def is_similar(a, b, threshold=0.7):
    return SequenceMatcher(None, a, b).ratio() > threshold

# =========================================================
# GET SOURCE NAME
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
# AI INSIGHT FUNCTION
# =========================================================
def generate_insight(title):
    title = title.lower()

    if "funding" in title or "raises" in title or "raised" in title:
        return "Startup secured funding → growth & investor confidence"
    elif "acquire" in title:
        return "Acquisition → expansion or market consolidation"
    elif "launch" in title:
        return "Product launch → innovation signal"
    elif "partnership" in title:
        return "Strategic partnership → scaling opportunity"
    else:
        return "General update → monitor"

# =========================================================
# RELEVANCE FILTER
# =========================================================
def is_relevant(title):
    keywords = [
        "funding", "raises", "raised", "acquire", "acquired",
        "launch", "launches", "partnership", "investment",
        "expansion", "deal", "merger"
    ]
    return any(k in title.lower() for k in keywords)

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

        if not is_relevant(title):
            continue

        pattern = r'\b' + re.escape(startup) + r'\b'

        if not re.search(pattern, title):
            continue

        all_articles.append([
            startup_map[startup],
            title_raw,
            entry.get("link", ""),
            entry.get("published", ""),
            source_name,
            generate_insight(title_raw),
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        ])

# ---------------- CUSTOM RSS ----------------
for feed_url in RSS_FEEDS:
    feed = feedparser.parse(feed_url)
    source_name = get_feed_name(feed, feed_url)

    for entry in feed.entries:
        title_raw = entry.get("title", "")
        title = title_raw.lower()

        if not is_relevant(title):
            continue

        for startup in startups:
            pattern = r'\b' + re.escape(startup) + r'\b'

            if not re.search(pattern, title):
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
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            ])

# =========================================================
# 🔥 SEMANTIC DEDUPLICATION
# =========================================================
unique_articles = []
seen_titles = []

for row in all_articles:
    cleaned = clean_title(row[1])

    duplicate = False
    for seen in seen_titles:
        if is_similar(cleaned, seen):
            duplicate = True
            break

    if not duplicate:
        seen_titles.append(cleaned)
        unique_articles.append(row)

all_articles = unique_articles

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
    "Fetched At"
])

df.drop_duplicates(subset=["Title", "Link"], inplace=True)

print(f"\nFiltered rows after dedup: {len(df)}")

# =========================================================
# WRITE TO GOOGLE SHEET
# =========================================================
output_sheet = client.open("Startup Tracker").worksheet("News_Log_V2")

if df.empty:
    print("✅ No new relevant data")
else:
    output_sheet.append_rows(df.values.tolist(), value_input_option='RAW')
    print("✅ Clean deduplicated insights added")
