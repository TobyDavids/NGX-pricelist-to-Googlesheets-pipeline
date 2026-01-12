import os
import time
import json
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import gspread
from google.oauth2.service_account import Credentials

# === Constants ===
URL = "https://ngxgroup.com/exchange/data/equities-price-list/"
NOW = datetime.now().strftime("%Y-%m-%d")

# === Directories ===
BASE_DIR = os.getcwd()
LOG_DIR = os.path.join(BASE_DIR, "logs")
DATA_DIR = os.path.join(BASE_DIR, "data")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "web_scrap_log.txt")
CSV_FILE = os.path.join(DATA_DIR, f"data_{NOW}.csv")

def log_message(message):
    print(message) # This prints to the GitHub Actions Console
    with open(LOG_FILE, "a") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

def handle_cookie_consent(driver, wait):
    try:
        cookie_button = wait.until(
            EC.element_to_be_clickable((By.ID, "cookie_action_close_header"))
        )
        driver.execute_script("arguments[0].click();", cookie_button)
        log_message("✅ Cookie popup closed")
    except Exception:
        log_message("ℹ️ No cookie popup found (skipping)")

def scrape_and_push():
    log_message(f"🚀 Job started at {NOW}")

    # --- Chrome setup ---
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080") 

    driver = webdriver.Chrome(service=Service(), options=options)
    wait = WebDriverWait(driver, 20)

    try:
        log_message(f"🌐 Opening URL: {URL}")
        driver.get(URL)
        handle_cookie_consent(driver, wait)
        time.sleep(3)

        log_message("⏳ Setting table to show all rows...")
        # Show all rows
        option = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//*[@id='latestdiclosuresEquities_length']/label/select/option[4]")
            )
        )
        option.click()
        time.sleep(3)

        log_message("📸 Capturing table data...")
        table = wait.until(
            EC.presence_of_element_located((By.ID, "latestdiclosuresEquities"))
        )

        soup = BeautifulSoup(table.get_attribute("outerHTML"), "html.parser")

        headers = [th.text.strip() for th in soup.find("thead").find_all("th")]
        rows = [
            [td.text.strip() for td in tr.find_all("td")]
            for tr in soup.find("tbody").find_all("tr")
        ]

        df = pd.DataFrame(rows, columns=headers)
        
        # Checkpoint: Did we actually get data?
        if df.empty:
            raise ValueError("The scraped dataframe is empty! The website might not have loaded correctly.")
        
        log_message(f"📊 Successfully scraped {len(df)} rows.")

        # Data Cleaning
        df["Company"] = df["Company"].str.split(r"\s|\[", n=1).str[0]
        df.to_csv(CSV_FILE, index=False)
        log_message(f"💾 CSV saved locally to {CSV_FILE}")

        # --- Google Sheets ---
        log_message("🔐 Connecting to Google Sheets...")
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        if "GOOGLE_CREDS_JSON" not in os.environ:
            raise EnvironmentError("GOOGLE_CREDS_JSON secret not found in environment variables!")

        creds_dict = json.loads(os.environ["GOOGLE_CREDS_JSON"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)

        log_message("📂 Opening spreadsheet 'NGX Daily Equity Prices'...")
        sheet = client.open("NGX Daily Equity Prices").sheet1
        
        log_message("🧹 Clearing old data and updating sheet...")
        sheet.clear()
        
        # Format data for gspread (headers + data rows)
        data_to_upload = [df.columns.tolist()] + df.values.tolist()
        sheet.update(data_to_upload)

        log_message("✅ Google Sheet updated successfully!")

    except Exception as e:
        log_message(f"❌ CRITICAL ERROR: {e}")
        # THIS IS THE KEY: We re-raise the error so GitHub Actions sees the failure
        raise e

    finally:
        driver.quit()
        log_message("🔌 Browser closed. Job finished.")

if __name__ == "__main__":
    scrape_and_push()
