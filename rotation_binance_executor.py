# rotation_binance_executor.py

import os
import time
from datetime import datetime
from binance.client import Client
from binance.exceptions import BinanceAPIException
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Binance Auth
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

# Buy Settings
BUY_ALLOCATION_PCT = 10  # % of USDT to spend per token

def run_rotation_binance_executor():
    print("📈 Checking Rotation_Log for tokens to buy...")

    try:
        # Google Sheets Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        gclient = gspread.authorize(creds)
        sheet = gclient.open_by_url(os.getenv("SHEET_URL"))

        log_ws = sheet.worksheet("Rotation_Log")
        trade_ws = sheet.worksheet("Trade_Log")

        log_rows = log_ws.get_all_values()
        headers = log_rows[0]
        token_idx = headers.index("Token")
        bought_idx = headers.index("Bought") if "Bought" in headers else len(headers)
        if "Bought" not in headers:
            log_ws.update_cell(1, bought_idx + 1, "Bought")

        # Iterate over rows and execute trades
        for i, row in enumerate(log_rows[1:], start=2):
            token = row[token_idx].strip().upper()
            bought_flag = row[bought_idx].strip().upper() if len(row) > bought_idx else ""

            if not token or bought_flag == "YES":
                continue

            symbol = f"{token}USDT"
            try:
                usdt_balance = float(client.get_asset_balance(asset="USDT")["free"])
                allocation = round(usdt_balance * (BUY_ALLOCATION_PCT / 100), 2)
                if allocation < 5:
                    print(f"⚠️ Not enough USDT to buy {token}. Skipping.")
                    continue

                price = float(client.get_symbol_ticker(symbol=symbol)["price"])
                qty = round(allocation / price, 5)

                order = client.order_market_buy(symbol=symbol, quantity=qty)
                now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

                trade_ws.append_row([
                    now, token, "BUY", qty, price, allocation,
                    BUY_ALLOCATION_PCT, "✅ Executed"
                ], value_input_option="USER_ENTERED")

                log_ws.update_cell(i, bought_idx + 1, "YES")
                print(f"✅ {token} bought and marked as purchased.")

            except BinanceAPIException as e:
                print(f"❌ Binance API error for {token}: {e.message}")
                trade_ws.append_row([
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    token, "BUY", "-", "-", "-", BUY_ALLOCATION_PCT,
                    f"❌ API Error: {e.message}"
                ], value_input_option="USER_ENTERED")
            except Exception as e:
                print(f"❌ General error buying {token}: {e}")
                trade_ws.append_row([
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    token, "BUY", "-", "-", "-", BUY_ALLOCATION_PCT,
                    f"❌ Error: {str(e)}"
                ], value_input_option="USER_ENTERED")

        print("✅ Rotation Binance execution complete.")

    except Exception as e:
        print(f"❌ Error in run_rotation_binance_executor: {e}")

