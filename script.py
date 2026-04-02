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
# AUTHENTICATION (UPDATED FOR GITHUB)
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
# READ STARTUPS FROM SHEET
# =========================================================
sheet = client.open("Startup Tracker").sheet1
data = sheet.get_all_records()

startups = [row["Startup Name"] for row in data if row["Startup Name"]]

print("Startups:", startups)

# 🔥 LIMIT FOR TESTING
startups = startups[:3]

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
# DATAFRAME
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

df.drop_duplicates(subset=["Title", "Link"], inplace=True)

print(f"\nTotal rows fetched: {len(df)}")

# =========================================================
# WRITE TO GOOGLE SHEET
# =========================================================
if df.empty:
    print("❌ No data to write")
else:
    output_sheet = client.open("Startup Tracker").worksheet("News_Log")
    output_sheet.append_rows(df.values.tolist(), value_input_option='RAW')
    print("✅ SUCCESS — Data written")