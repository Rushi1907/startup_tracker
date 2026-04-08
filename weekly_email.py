# =========================================================
# IMPORTS
# =========================================================
import gspread
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# =========================================================
# CONFIG
# =========================================================
SPREADSHEET_NAME = "Startup Tracker"
SHEET_NAME = "News_Log_V2"

EMAIL_USER = os.environ["EMAIL_USER"]
EMAIL_PASS = os.environ["EMAIL_PASS"]

TO_EMAIL = "rushikeshd1907@gmail.com"
GOOGLE_SHEET_LINK = "https://docs.google.com/spreadsheets/d/1xtQQ1eQuvcYgO6c54c7g65l7cGqZN_YSSgStHFvARho/edit?pli=1&gid=1374023458#gid=1374023458"

# =========================================================
# AUTHENTICATION
# =========================================================
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_info(
    json.loads(os.environ["GOOGLE_CREDENTIALS"]),
    scopes=scope
)

client = gspread.authorize(creds)

# =========================================================
# FETCH WEEKLY DATA
# =========================================================
def get_weekly_data():
    sheet = client.open("Startup Tracker").worksheet("Sheet1")
    data = sheet.get_all_records()

    df = pd.DataFrame(data)

    if df.empty:
        return df

    df["Published"] = pd.to_datetime(df["Published"], errors="coerce")

    last_7_days = datetime.utcnow() - timedelta(days=7)
    df_week = df[df["Published"] >= last_7_days]

    return df_week

# =========================================================
# EMAIL TEMPLATE (HTML)
# =========================================================
def generate_email(df):

    total_updates = len(df)

    funding = df[df["Insights"].str.contains("funding", case=False, na=False)]
    acquisitions = df[df["Insights"].str.contains("Acquisition", case=False, na=False)]
    launches = df[df["Insights"].str.contains("launch", case=False, na=False)]

    html = f"""
    <html>
    <body style="font-family: Arial; background:#f5f7fa; padding:20px;">
    <div style="max-width:700px; margin:auto; background:white; padding:20px; border-radius:10px;">

    <h2 style="color:#1a73e8;">📊 Weekly Startup Intelligence Report</h2>

    <p><b>Week Ending:</b> {datetime.utcnow().strftime("%d %b %Y")}</p>

    <div style="background:#eef3ff; padding:15px; border-radius:8px;">
        <p><b>Total Updates:</b> {total_updates}</p>
        <p><b>Funding Events:</b> {len(funding)}</p>
        <p><b>Acquisitions:</b> {len(acquisitions)}</p>
        <p><b>Launches:</b> {len(launches)}</p>
    </div>

    <hr>
    <h3>🔥 Top Funding Events</h3>
    """

    if funding.empty:
        html += "<p>No major funding events this week.</p>"
    else:
        for _, row in funding.head(5).iterrows():
            html += f"""
            <p>
            <b>{row['Startup Name']}</b><br>
            {row['Title']}<br>
            <a href="{row['Link']}">Read More</a>
            </p>
            """

    html += "<hr><h3>🤝 Key Acquisitions</h3>"

    if acquisitions.empty:
        html += "<p>No acquisitions this week.</p>"
    else:
        for _, row in acquisitions.head(3).iterrows():
            html += f"""
            <p>
            <b>{row['Startup Name']}</b><br>
            {row['Title']}<br>
            <a href="{row['Link']}">Read More</a>
            </p>
            """

    html += f"""
    <hr>

    <p style="text-align:center;">
    📊 <a href="{GOOGLE_SHEET_LINK}">View Full Dashboard</a>
    </p>

    <p style="font-size:12px; color:gray; text-align:center;">
    Automated weekly report
    </p>

    </div>
    </body>
    </html>
    """

    return html

# =========================================================
# SEND EMAIL USING GMAIL
# =========================================================
def send_email(html_content):

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "📊 Weekly Startup Intelligence Report"
    msg["From"] = EMAIL_USER
    msg["To"] = TO_EMAIL

    part = MIMEText(html_content, "html")
    msg.attach(part)

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, TO_EMAIL, msg.as_string())
        server.quit()

        print("✅ Email sent successfully")

    except Exception as e:
        print("❌ Email failed:", str(e))

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":

    print("Fetching weekly data...")

    df_week = get_weekly_data()

    print(f"Weekly records: {len(df_week)}")

    html_content = generate_email(df_week)

    send_email(html_content)
