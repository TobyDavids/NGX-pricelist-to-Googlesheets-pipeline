import os
import time
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

# === Set up directories ===
home_dir = os.getcwd()
log_dir = os.path.join(home_dir, "logs")
data_dir = os.path.join(home_dir, "data")
os.makedirs(data_dir, exist_ok=True)
os.makedirs(log_dir, exist_ok=True)

# === Define constants ===
log_file = os.path.join(log_dir, "web_scrap_log.txt")
now = datetime.now()
time_str = now.strftime("%Y-%m-%d")
url = "https://ngxgroup.com/exchange/data/equities-price-list/"
filename = os.path.join(data_dir, f"data_{time_str}.csv")
driver_path = "C:\\Users\\user\\WebDriver\\chromedriver.exe" 

# === Logging ===
def log_message(message):
    with open(log_file, "a") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

# === Handle Cookie ===
def handle_cookie_consent(driver, wait):
    try:
        cookie_button = wait.until(
            EC.element_to_be_clickable((By.ID, "cookie_action_close_header"))
        )
        driver.execute_script("arguments[0].click();", cookie_button)
        log_message("Closed cookie popup")
    except Exception as e:
        log_message(f"Cookie popup not found or error: {e}")

# === Main Function ===
def scrape_and_push():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--incognito")

    driver = webdriver.Chrome(service=Service(driver_path), options=options)
    wait = WebDriverWait(driver, 20)

    try:
        driver.get(url)
        handle_cookie_consent(driver, wait)
        time.sleep(2)

        # Expand table rows
        filter_option = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//*[@id='latestdiclosuresEquities_length']/label/select/option[4]"
        )))
        filter_option.click()
        time.sleep(2)

        table = wait.until(EC.presence_of_element_located((By.ID, "latestdiclosuresEquities")))
        table_html = table.get_attribute("outerHTML")
        soup = BeautifulSoup(table_html, "html.parser")

        headers = [th.get_text(strip=True) for th in soup.find("thead").find_all("th")]
        rows = []
        for row in soup.find("tbody").find_all("tr"):
            cells = row.find_all("td")
            rows.append([cell.get_text(strip=True) for cell in cells])

        if headers and rows:
            df = pd.DataFrame(rows, columns=headers)
        if "Company" in df.columns:
            df["Company"] = df["Company"].str.strip().str.split(r"\s|\[", n=1).str[0]
            df.to_csv(filename, index=False)
            log_message("Data saved locally.")


            # === Push to Google Sheets ===
            try:
                scope = [
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive.file",
                    "https://www.googleapis.com/auth/drive",
                ]
                creds = Credentials.from_service_account_file("creds.json", scopes=scope)
                client = gspread.authorize(creds)
                sheet = client.open("NGX Daily Equity Prices").sheet1
                sheet.clear()
                sheet.update("A1", [df.columns.tolist()] + df.values.tolist())
                log_message("Data pushed to Google Sheets.")
            except Exception as e:
                log_message(f"Google Sheets error: {e}")

        else:
            log_message("No data found in table.")
    except Exception as e:
        log_message(f"Scraping failed: {e}")
    finally:
        driver.quit()
        log_message("Browser closed.")

if __name__ == "__main__":
    with open(log_file, "w") as f:
        f.write(f"{time_str} - Log started\n")
    scrape_and_push()
