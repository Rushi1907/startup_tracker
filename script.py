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
# RSS FEEDS (NEW ADDITION)
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

startups = [row["Startup Name"] for row in data if row["Startup Name"]]

print("Startups loaded:", startups)

# =========================================================
# FETCH NEWS
# =========================================================
all_articles = []

# ---------------- GOOGLE NEWS ----------------
for startup in startups:
    print(f"\n[Google RSS] Fetching: {startup}")

    query = f"{startup} startup funding OR acquisition OR launch"
    encoded_query = quote_plus(query)

    url = f"https://news.google.com/rss/search?q={encoded_query}"
    feed = feedparser.parse(url)

    for entry in feed.entries:
        if not hasattr(entry, "published_parsed") or entry.published_parsed is None:
            continue

        published_time = datetime(*entry.published_parsed[:6])

        if published_time < datetime.utcnow() - timedelta(days=2):
            continue

        title = entry.get("title", "")

        if not is_relevant(title):
            continue

        all_articles.append([
            startup,
            title,
            entry.get("link", ""),
            entry.get("published", ""),
            "Google News",
            generate_insight(title),
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        ])

# ---------------- CUSTOM RSS FEEDS ----------------
for feed_url in RSS_FEEDS:
    print(f"\n[Custom RSS] Fetching from: {feed_url}")

    feed = feedparser.parse(feed_url)

    for entry in feed.entries:

        title = entry.get("title", "").lower()

        if not is_relevant(title):
            continue

        # Match startup name in title
        for startup in startups:
            if startup.lower() in title:

                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    published_time = datetime(*entry.published_parsed[:6])
                else:
                    published_time = datetime.utcnow()

                if published_time < datetime.utcnow() - timedelta(days=2):
                    continue

                all_articles.append([
                    startup,
                    entry.get("title", ""),
                    entry.get("link", ""),
                    entry.get("published", ""),
                    feed_url,   # 🔥 Source = actual RSS
                    generate_insight(entry.get("title", "")),
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
