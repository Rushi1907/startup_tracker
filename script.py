# =========================================================
# IMPORTS
# =========================================================
import gspread
import feedparser
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from urllib.parse import quote_plus
import os
import json

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

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=scope
)

client = gspread.authorize(creds)

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
# READ STARTUPS (CLEANED)
# =========================================================
Q1_START = datetime(2026, 1, 1)
sheet = client.open("Startup Tracker").sheet1
data = sheet.get_all_records()

startups = list(set([
    row["Startup Name"].strip()
    for row in data
    if row["Startup Name"]
]))

print("Startups loaded:", startups)

# =========================================================
# FETCH NEWS (UNIFIED PIPELINE)
# =========================================================
all_articles = []

# ---------------- GOOGLE NEWS (NOW LIKE RSS) ----------------
for startup in startups:
    query = f"{startup} startup funding OR acquisition OR launch"
    encoded_query = quote_plus(query)

    google_url = f"https://news.google.com/rss/search?q={encoded_query}"
    feed = feedparser.parse(google_url)

    source_name = get_feed_name(feed, google_url)

    print(f"\n[Google News] {startup}")

    for entry in feed.entries:

        if not hasattr(entry, "published_parsed") or entry.published_parsed is None:
            continue

        published_time = datetime(*entry.published_parsed[:6])

        if published_time < datetime.utcnow() - timedelta(days=Q1_START):
            continue

        title = entry.get("title", "")

        if not is_relevant(title):
            continue

        all_articles.append([
            startup,
            title,
            entry.get("link", ""),
            entry.get("published", ""),
            source_name,
            generate_insight(title),
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        ])

# ---------------- CUSTOM RSS ----------------
for feed_url in RSS_FEEDS:
    print(f"\n[RSS] Fetching: {feed_url}")

    feed = feedparser.parse(feed_url)
    source_name = get_feed_name(feed, feed_url)

    for entry in feed.entries:

        title_raw = entry.get("title", "")
        title = title_raw.lower()

        if not is_relevant(title):
            continue

        for startup in startups:
            if startup.lower() in title:

                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    published_time = datetime(*entry.published_parsed[:6])
                else:
                    published_time = datetime.utcnow()

                if published_time < datetime.utcnow() - timedelta(days=Q1_START):
                    continue

                all_articles.append([
                    startup,
                    title_raw,
                    entry.get("link", ""),
                    entry.get("published", ""),
                    source_name,
                    generate_insight(title_raw),
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

df.drop_duplicates(subset=["Title", "Link"], inplace=True)

print(f"\nFiltered rows: {len(df)}")

# =========================================================
# REMOVE EXISTING DUPLICATES
# =========================================================
def get_existing_keys(sheet):
    data = sheet.get_all_values()

    if len(data) <= 1:
        return set()

    return set((row[1], row[2]) for row in data[1:] if len(row) > 2)

output_sheet = client.open("Startup Tracker").worksheet("News_Log_V2")

existing_keys = get_existing_keys(output_sheet)

df_new = df[~df.apply(lambda x: (x["Title"], x["Link"]) in existing_keys, axis=1)]

print(f"New rows to insert: {len(df_new)}")

# =========================================================
# WRITE TO GOOGLE SHEET
# =========================================================
if df_new.empty:
    print("✅ No new relevant data")
else:
    output_sheet.append_rows(df_new.values.tolist(), value_input_option='RAW')
    print("✅ Insights + clean data added")
