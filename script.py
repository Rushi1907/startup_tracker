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

# =========================================================
# AUTHENTICATION (GITHUB READY)
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

# 🔥 LIMIT (REMOVE AFTER TESTING)
startups = startups[:5]

# =========================================================
# FETCH NEWS
# =========================================================
all_articles = []

for startup in startups:
    print(f"\nFetching: {startup}")

    query = f"{startup} startup funding OR acquisition OR launch"
    encoded_query = quote_plus(query)

    rss_sources = [
        ("Google RSS", f"https://news.google.com/rss/search?q={encoded_query}"),
        ("Bing RSS", f"https://www.bing.com/news/search?q={encoded_query}&format=rss"),
        ("Yahoo RSS", f"https://news.search.yahoo.com/rss?p={encoded_query}")
    ]

    for source, url in rss_sources:
        print(f"  → {source}")

        try:
            feed = feedparser.parse(url)

            if not feed.entries:
                print(f"    ❌ No data from {source}")
                continue

            for entry in feed.entries[:2]:
                all_articles.append([
                    startup,
                    entry.get("title", ""),
                    entry.get("published", ""),
                    source,
                    entry.get("link", ""),
                    entry.get("summary", ""),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ])

        except Exception as e:
            print(f"    ❌ Error in {source}: {e}")

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

# Remove duplicates within batch
df.drop_duplicates(subset=["Title", "Link"], inplace=True)

print(f"\nFetched rows: {len(df)}")

# =========================================================
# REMOVE EXISTING DUPLICATES FROM SHEET
# =========================================================
def get_existing_keys(sheet):
    data = sheet.get_all_values()

    if len(data) <= 1:
        return set()

    return set(
        (row[1], row[4])  # Title + Link
        for row in data[1:]
        if len(row) > 4
    )

output_sheet = client.open("Startup Tracker").worksheet("News_Log")

existing_keys = get_existing_keys(output_sheet)

# Keep only NEW rows
df_new = df[~df.apply(lambda x: (x["Title"], x["Link"]) in existing_keys, axis=1)]

print(f"New unique rows to insert: {len(df_new)}")

# =========================================================
# WRITE TO GOOGLE SHEET
# =========================================================
if df_new.empty:
    print("✅ No new data (no duplicates added)")
else:
    output_sheet.append_rows(df_new.values.tolist(), value_input_option='RAW')
    print("✅ Only new data added to sheet")
