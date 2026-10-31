"""Historical data ingestion script for database seed population."""
import os
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# 1. API & Target Core Parameters
URL = "https://alphavantage.co/query"
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST", "database_service")

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

UPSERT_HISTORY_QUERY = """
    INSERT INTO price_history (asset_id, price, price_date)
    SELECT 
        a.id, 
        t.price::numeric, 
        t.p_date::date
    FROM (VALUES %s) AS t(ticker, p_date, price)
    JOIN assets a ON a.ticker = t.ticker
    ON CONFLICT (asset_id, price_date) 
    DO UPDATE SET price = EXCLUDED.price;
"""

def create_secure_session():
    """Build a resilient HTTP network connection session with retry rules."""
    session = requests.Session()
    
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json"
    })
    
    retries = Retry(
        total=5,
        backoff_factor=3,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False
    )
    
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def get_historical_data():
    """Extract full historical daily pricing matrices from the AlphaVantage API."""
    bulk_data = []
    
    client = create_secure_session()

    crypto_params = {
        "function": "DIGITAL_CURRENCY_DAILY",
        "symbol": "BTC",
        "market": "USD",
        "apikey": API_KEY
    }
    print("Fetching historical data for BTC via secure session...")
    try:
        response = client.get(URL, params=crypto_params, timeout=30)
        response.raise_for_status()
        crypto_res = response.json()
        
        if "Error Message" in crypto_res or "Note" in crypto_res:
            print(f"⚠️ Alpha Vantage API Warning for BTC: {crypto_res.get('Note', crypto_res.get('Error Message'))}")
        
        crypto_series = crypto_res.get("Time Series (Digital Currency Daily)", {})
        for date_str, metrics in crypto_series.items():
            price_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            close_price = float(metrics["4. close"])
            bulk_data.append(('BTC/USD', price_date, close_price))
            
        print(f"-> Successfully extracted {len(crypto_series)} daily entries for BTC.")
    except Exception as e:
        print(f"❌ Failed connection handshake for BTC: {e}")
    
    # Mandated throttling rest block to respect Alpha Vantage's free rate limits
    print("Sleeping 15 seconds to respect endpoint throttle constraints...")
    time.sleep(15)

    fiat_pairs = [("USD", "RUB", "USD/RUB"), ("EUR", "RUB", "EUR/RUB")]
    
    for from_sym, to_sym, ticker_name in fiat_pairs:
        print(f"Fetching historical data for {ticker_name} via secure session...")
        forex_params = {
            "function": "FX_DAILY",
            "from_symbol": from_sym,
            "to_symbol": to_sym,
            "outputsize": "full",
            "apikey": API_KEY
        }
        try:
            response = client.get(URL, params=forex_params, timeout=30)
            response.raise_for_status()
            forex_res = response.json()
            
            if "Error Message" in forex_res or "Note" in forex_res:
                print(f"⚠️ Alpha Vantage API Warning for {ticker_name}: {forex_res.get('Note', forex_res.get('Error Message'))}")

            forex_series = forex_res.get("Time Series FX (Daily)", {})
            for date_str, metrics in forex_series.items():
                price_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                close_price = float(metrics["4. close"])
                bulk_data.append((ticker_name, price_date, close_price))
                
            print(f"-> Successfully extracted {len(forex_series)} daily entries for {ticker_name}.")
        except Exception as e:
            print(f"❌ Failed connection handshake for {ticker_name}: {e}")
        
        print("Sleeping 15 seconds to respect endpoint throttle constraints...")
        time.sleep(15)

    client.close()
    return bulk_data

# 3. Structural Execution Hook
if __name__ == "__main__":
    print("Checking if historical database records already exist...")
    
    # 1. Quick pre-flight check to see if the table already contains data
    has_data = False
    try:
        with psycopg2.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT EXISTS (SELECT 1 FROM price_history LIMIT 1);")
                has_data = cur.fetchone()[0]
    except Exception as db_err:
        print(f"Database pre-flight check failed (Assuming empty): {db_err}")

    # 2. True run - once guarantee condition
    if has_data:
        print("🏁 Historical rows already exist inside price_history. Skipping redundant API download!")
        exit(0)
        
    print("Database empty! Initializing 10-year historical batch migration sequence...")
    data_to_insert = get_historical_data()
    
    if not data_to_insert:
        print("❌ Error: Bulk dataset matrix returned empty. Database save abandoned.")
        exit(1)
        
    print(f"Collected {len(data_to_insert)} total pricing rows. Connecting to target container database...")

    try:
        with psycopg2.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                print("Streaming data to database in optimized chunks...")
                execute_values(
                    cur, 
                    UPSERT_HISTORY_QUERY, 
                    data_to_insert, 
                    template="(%s, %s, %s)",
                    page_size=1000
                )
            conn.commit()
        print("🏁 Success! 10-year historical data has been completely loaded into price_history.")
    except Exception as e:
        print(f"❌ Structural database insertion failed: {e}")
