import requests
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import time 
from dateutil import parser
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Constants
UNIQUEID = 126
MACHINE_NO = int(os.getenv("MACHINE_NO", 1))
STOCKS_MANAGER_BASE_URL = os.getenv("STOCKS_MANAGER_BASE_URL", "https://google.com")
STOCK_HOLDED_STOCK_BASE_URL = os.getenv("STOCK_HOLDED_STOCK_BASE_URL", "https://google.com")
FIRST_TIME = 1
STOCKDATA_API = f"{STOCKS_MANAGER_BASE_URL}/api/stocksbatch/{UNIQUEID}/stocksbatchfromids"
HOLDED_STOCK_API = "https://www.google.com/finance/quote/"
SEND_DATA_API = f"{STOCK_HOLDED_STOCK_BASE_URL}/api/holdedstock/128/Machine{MACHINE_NO}"

# Globals
TIME_TO_SEND_PAYLOAD = None
holded_stock_payloads = []
stocklist = []

class HoldedStockPayload:
    def __init__(self, stock_id, time, price):
        self.stock_id = stock_id
        self.time = time
        self.price = price

    def to_dict(self):
        return {
            "stockId": self.stock_id,
            "time": self.time.isoformat(),
            "price": self.price
        }

    def __repr__(self):
        return f"HoldedStockPayload(stockId={self.stock_id}, time={self.time}, price={self.price})"

def log(message):
    logging.info(f"[{datetime.now()}] {message}")

def fetch_api_data(api_url, stockIds):
    log("Fetching holded stock data to send.")
    global stocklist
    try:
        response = requests.post(api_url, data=json.dumps(stockIds), headers={'Content-Type': 'application/json'})
        response.raise_for_status()
        stocklist = [[key, value] for key, value in response.json().items()]
        log(f"Fetched data successfully: {len(stocklist)} stocks.")
        return True
    except requests.exceptions.RequestException as e:
        log(f"Error fetching API data: {e}")
    except ValueError:
        log("Failed to decode JSON response.")
    return False

def fetch_live_stock_info(key, value):
    stock_info = value.split(" ")
    url = f"{HOLDED_STOCK_API}{stock_info[0]}:{stock_info[1]}"
    log(f"Fetching live stock info for {key} from {url}.")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            class1 = "YMlKec fxKbKc"
            price_element = soup.find(class_=class1)
            if price_element:
                price_text = price_element.text.strip()
                try:
                    price = float(price_text[1:].replace(",", ""))
                    payload = HoldedStockPayload(stock_id=key, time=datetime.now(), price=price)
                    holded_stock_payloads.append(payload)
                    log(f"Fetched price for {key}: {price}")
                except ValueError:
                    log(f"Could not parse price for {key}: {price_text}")
            else:
                log(f"Price element not found for {key}")
        else:
            log(f"Failed to fetch price for {key}. Status code: {response.status_code}")
    except requests.RequestException as e:
        log(f"Error fetching price for {key}: {e}")

def create_time_to_send_payload():
    global TIME_TO_SEND_PAYLOAD
    current_time = datetime.now(timezone.utc)
    next_minute = (current_time + timedelta(minutes=1)).replace(second=0, microsecond=0)
    TIME_TO_SEND_PAYLOAD = next_minute + timedelta(seconds=MACHINE_NO-1)
    log(f"Updated TIME_TO_SEND_PAYLOAD: {TIME_TO_SEND_PAYLOAD}")

def update_time_to_send_payload(next_time):
    global TIME_TO_SEND_PAYLOAD
    parsed_time = parser.isoparse(next_time)
    adjusted_time = parsed_time - timedelta(hours=5, minutes=30)
    TIME_TO_SEND_PAYLOAD = adjusted_time
    log(f"Updated TIME_TO_SEND_PAYLOAD: {TIME_TO_SEND_PAYLOAD}")

def send_live_market_data():
    log("KARTIKEY")
    global TIME_TO_SEND_PAYLOAD
    global holded_stock_payloads
    global FIRST_TIME

    if TIME_TO_SEND_PAYLOAD is None:
        create_time_to_send_payload()

    while datetime.now(timezone.utc) < TIME_TO_SEND_PAYLOAD:
        remaining_time = (TIME_TO_SEND_PAYLOAD - datetime.now(timezone.utc)).total_seconds()
        if remaining_time > 0:
            log(f"Waiting to send data. Remaining Time: {remaining_time:.2f} seconds")
            time.sleep(min(remaining_time, 60))  # Sleep for the minimum of remaining_time or 60 seconds
        else:
            log("Time to send data has arrived.")

    payload_data = [payload.to_dict() for payload in holded_stock_payloads]
    # if not payload_data:
    #     log("No live stock data to send.")
    #     return

    try:
        response = requests.post(f"{SEND_DATA_API}/{FIRST_TIME}", json=payload_data)
        response.raise_for_status()
        log(f"Successfully sent live stock data. Response: {response.json()}")
        check = response.json().get('updateData')
        next_time = response.json().get('nextIterationTime')
        holded_stock_payloads = []
        if next_time:
            update_time_to_send_payload(next_time)
        if check:
            stockIds = response.json().get('stockId')
            fetch_api_data(STOCKDATA_API, stockIds)
    except requests.exceptions.RequestException as e:
        log(f"Error sending live stock data: {e}")

if __name__ == "__main__":
    log("Starting holded stock data processing.")
    send_live_market_data()

    while datetime.now(timezone.utc) < TIME_TO_SEND_PAYLOAD:
        remaining_time = (TIME_TO_SEND_PAYLOAD - datetime.now(timezone.utc)).total_seconds()
        if remaining_time > 0:
            log(f"Waiting to process stock list. Remaining Time: {remaining_time:.2f} seconds")
            time.sleep(min(remaining_time, 60))  # Sleep for the minimum of remaining_time or 60 seconds
        else:
            log("Time to process stock list has arrived.")

    FIRST_TIME = 0
    if stocklist:
        attempt = 0
        max_attempts = 20
        while attempt < max_attempts:
            log(f"Processing stock list. Attempt {attempt + 1}/{max_attempts}")
            num_workers = len(stocklist)
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_stock = {executor.submit(fetch_live_stock_info, key, value): (key, value) for key, value in stocklist}
                for future in as_completed(future_to_stock):
                    key, value = future_to_stock[future]
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Error processing {key} - {value}: {e}")

            send_live_market_data()
            attempt += 1
    else:
        log("No data fetched due to an error.")

    log("Holded market data processing completed.")
