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
    with open(LOG_FILE, "a") as f:
        f.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n"
        )
def handle_cookie_consent(driver, wait):
    try:
        cookie_button = wait.until(
            EC.element_to_be_clickable((By.ID, "cookie_action_close_header"))
        )
        driver.execute_script("arguments[0].click();", cookie_button)
        log_message("Cookie popup closed")
    except Exception:
        log_message("No cookie popup found")


def scrape_and_push():
    log_message("Job started")

    # --- Chrome setup ---
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(service=Service(), options=options)
    wait = WebDriverWait(driver, 20)

    try:
        driver.get(URL)
        handle_cookie_consent(driver, wait)
        time.sleep(2)

        # Show all rows
        option = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//*[@id='latestdiclosuresEquities_length']/label/select/option[4]")
            )
        )
        option.click()
        time.sleep(2)

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
        df["Company"] = df["Company"].str.split(r"\s|\[", n=1).str[0]
        df.to_csv(CSV_FILE, index=False)
        log_message("CSV saved")

        # --- Google Sheets ---
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        creds_dict = json.loads(os.environ["GOOGLE_CREDS_JSON"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)

        sheet = client.open("NGX Daily Equity Prices").sheet1
        sheet.clear()
        sheet.update([df.columns.tolist()] + df.values.tolist())

        log_message("Google Sheet updated")

    except Exception as e:
        log_message(f"ERROR: {e}")

    finally:
        driver.quit()
        log_message("Browser closed")

if __name__ == "__main__":
    with open(LOG_FILE, "w") as f:
        f.write(f"{NOW} - Log started\n")

    scrape_and_push()


