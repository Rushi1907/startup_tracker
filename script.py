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
# READ STARTUPS
# =========================================================
sheet = client.open("Startup Tracker").sheet1
data = sheet.get_all_records()

startups = [row["Startup Name"] for row in data if row["Startup Name"]]

print("Startups loaded:", startups)

# 🔥 LIMIT FOR TESTING
startups = startups[:5]

# =========================================================
# FETCH NEWS (STRICT LATEST ONLY)
# =========================================================
all_articles = []

for startup in startups:
    print(f"\nFetching: {startup}")

    query = f"{startup} startup funding OR acquisition OR launch"
    encoded_query = quote_plus(query)

    # 🔥 USE ONLY GOOGLE (BEST FRESHNESS)
    rss_sources = [
        ("Google RSS", f"https://news.google.com/rss/search?q={encoded_query}")
    ]

    for source, url in rss_sources:
        print(f"  → {source}")

        try:
            feed = feedparser.parse(url)

            if not feed.entries:
                print("    ❌ No data")
                continue

            articles_temp = []

            for entry in feed.entries:

                # Skip if no valid timestamp
                if not hasattr(entry, "published_parsed") or entry.published_parsed is None:
                    continue

                published_time = datetime(*entry.published_parsed[:6])

                # 🔥 STRICT FILTER: last 48 hrs
                if published_time < datetime.utcnow() - timedelta(hours=48):
                    continue

                articles_temp.append((
                    published_time,
                    [
                        startup,
                        entry.get("title", ""),
                        entry.get("published", ""),
                        source,
                        entry.get("link", ""),
                        entry.get("summary", ""),
                        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    ]
                ))

            # 🔥 SORT BY LATEST
            articles_temp.sort(reverse=True, key=lambda x: x[0])

            # 🔥 TAKE TOP 3 ONLY
            for _, article in articles_temp[:3]:
                all_articles.append(article)

        except Exception as e:
            print(f"    ❌ Error: {e}")

# =========================================================
# CREATE DATAFRAME
# =========================================================
df = pd.DataFrame(all_articles, columns=[
    "Startup Name",
    "Title",
    "Published",
    "Source",
    "Link",
    "Summary",
    "Fetched At"
])

# Remove duplicates in batch
df.drop_duplicates(subset=["Title", "Link"], inplace=True)

print(f"\nFiltered rows: {len(df)}")

# =========================================================
# REMOVE EXISTING DUPLICATES FROM SHEET
# =========================================================
def get_existing_keys(sheet):
    data = sheet.get_all_values()

    if len(data) <= 1:
        return set()

    return set(
        (row[1], row[4])
        for row in data[1:]
        if len(row) > 4
    )

output_sheet = client.open("Startup Tracker").worksheet("News_Log")

existing_keys = get_existing_keys(output_sheet)

df_new = df[~df.apply(lambda x: (x["Title"], x["Link"]) in existing_keys, axis=1)]

print(f"New rows to insert: {len(df_new)}")

# =========================================================
# WRITE TO GOOGLE SHEET
# =========================================================
if df_new.empty:
    print("✅ No new recent data")
else:
    output_sheet.append_rows(df_new.values.tolist(), value_input_option='RAW')
    print("✅ Only latest fresh data added")
