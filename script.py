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
import hashlib   # ✅ NEW

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
# 🔥 DEDUP HELPERS (NEW)
# =========================================================
def normalize_title(title):
    title = title.lower()
    title = re.sub(r'[^a-z0-9 ]', '', title)

    stopwords = ["the", "a", "an", "in", "at", "to", "for", "of", "and"]
    words = [w for w in title.split() if w not in stopwords]

    return " ".join(words)

def generate_unique_key(title, link):
    return hashlib.md5((title + link).lower().encode()).hexdigest()

def generate_similarity_key(startup, title):
    normalized = normalize_title(title)
    return f"{startup.lower()}_{normalized[:60]}"

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
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
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
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
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
    "Fetched At"
])

if df.empty:
    print("No data fetched")
    exit()

# =========================================================
# 🔥 DEDUP LAYERS
# =========================================================

# 1. Exact duplicates
df["Unique Key"] = df.apply(
    lambda x: generate_unique_key(x["Title"], x["Link"]), axis=1
)

# 2. Similar titles (same event)
df["Similarity Key"] = df.apply(
    lambda x: generate_similarity_key(x["Startup Name"], x["Title"]), axis=1
)

# Remove duplicates inside current run
df = df.drop_duplicates(subset=["Similarity Key"])

# =========================================================
# 🔥 CHECK EXISTING
# =========================================================
output_sheet = client.open("Startup Tracker").worksheet("News_Log_V2")

def get_existing_keys(sheet):
    data = sheet.get_all_values()

    if len(data) <= 1:
        return set()

    headers = [h.strip() for h in data[0]]

    if "Similarity Key" not in headers:
        raise ValueError("❌ 'Similarity Key' column missing")

    idx = headers.index("Similarity Key")

    return set(row[idx] for row in data[1:] if len(row) > idx)

existing_keys = get_existing_keys(output_sheet)

df_new = df[~df["Similarity Key"].isin(existing_keys)]

print(f"\nNew rows: {len(df_new)}")

# =========================================================
# WRITE
# =========================================================
status_sheet = client.open("Startup Tracker").worksheet("System_Status")
current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

if df_new.empty:
    print("✅ No new updates")
    status_sheet.update("A2", [[current_time, "No New Updates", 0]])
else:
    output_sheet.append_rows(df_new.values.tolist(), value_input_option='RAW')
    print("✅ Clean data added")
    status_sheet.update("A2", [[current_time, "Updated", len(df_new)]])
