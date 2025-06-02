# binance_executor.py
import os
import time
from binance.client import Client
from binance.exceptions import BinanceAPIException
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Load API keys
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

# Buy/Sell Settings
BUY_ALLOCATION_PCT = 10  # % of USDT to spend
SELL_ON_STALL = True

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
gclient = gspread.authorize(creds)
sheet = gclient.open_by_url(os.getenv("SHEET_URL"))
trade_log = sheet.worksheet("Trade_Log")

def log_trade(token, action, qty, price, usdt_value, alloc_pct, status):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = [now, token, action, qty, price, usdt_value, alloc_pct, status]
    trade_log.append_row(row, value_input_option="USER_ENTERED")
    print(f"✅ Trade logged: {action} {token}")

def execute_buy(token):
    try:
        symbol = f"{token.upper()}USDT"
        balance = float(client.get_asset_balance(asset="USDT")["free"])
        spend = round(balance * (BUY_ALLOCATION_PCT / 100), 2)
        if spend < 5:
            print(f"⚠️ Not enough USDT to buy {token}. Skipping.")
            return

        ticker = client.get_symbol_ticker(symbol=symbol)
        price = float(ticker["price"])
        qty = round(spend / price, 5)

        order = client.order_market_buy(symbol=symbol, quantity=qty)
        log_trade(token, "BUY", qty, price, spend, BUY_ALLOCATION_PCT, "✅ Executed")
    except BinanceAPIException as e:
        log_trade(token, "BUY", "-", "-", "-", BUY_ALLOCATION_PCT, f"❌ API Error: {e.message}")
    except Exception as e:
        log_trade(token, "BUY", "-", "-", "-", BUY_ALLOCATION_PCT, f"❌ Error: {str(e)}")

def execute_sell(token):
    try:
        symbol = f"{token.upper()}USDT"
        balance = float(client.get_asset_balance(asset=token.upper())["free"])
        if balance == 0:
            print(f"⚠️ No {token} to sell.")
            return

        ticker = client.get_symbol_ticker(symbol=symbol)
        price = float(ticker["price"])
        order = client.order_market_sell(symbol=symbol, quantity=round(balance, 5))

        usdt_value = round(balance * price, 2)
        log_trade(token, "SELL", balance, price, usdt_value, 100, "✅ Executed")
    except BinanceAPIException as e:
        log_trade(token, "SELL", "-", "-", "-", 100, f"❌ API Error: {e.message}")
    except Exception as e:
        log_trade(token, "SELL", "-", "-", "-", 100, f"❌ Error: {str(e)}")
