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

startups = startups[:5]  # remove later

# =========================================================
# FETCH NEWS
# =========================================================
all_articles = []

for startup in startups:
    print(f"\nFetching: {startup}")

    query = f"{startup} startup funding OR acquisition OR launch"
    encoded_query = quote_plus(query)

    url = f"https://news.google.com/rss/search?q={encoded_query}"

    feed = feedparser.parse(url)

    articles_temp = []

    for entry in feed.entries:

        if not hasattr(entry, "published_parsed") or entry.published_parsed is None:
            continue

        published_time = datetime(*entry.published_parsed[:6])

        # 🔥 FILTER LAST 24 HOURS
        if published_time < datetime.utcnow() - timedelta(days=7):
            continue

        title = entry.get("title", "")

        if not is_relevant(title):
            continue

        articles_temp.append((
            published_time,
            [
                startup,
                title,
                entry.get("link", ""),
                entry.get("published", ""),
                "Google RSS",
                generate_insight(title),   # 🔥 AI INSIGHT
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            ]
        ))

    # SORT LATEST
    articles_temp.sort(reverse=True, key=lambda x: x[0])

    # TAKE TOP 3
    for _, article in articles_temp[:3]:
        all_articles.append(article)

# =========================================================
# DATAFRAME (CORRECT ORDER)
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
