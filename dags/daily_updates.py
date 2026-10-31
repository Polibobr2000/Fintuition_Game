import pendulum
from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.decorators import task

msk_tz = pendulum.timezone("Europe/Moscow")

logger = logging.getLogger("airflow.task")

# Token for Telegram Bot API integration
BOT_TOKEN = os.getenv("TG_BOT_TOKEN")

default_args = {
    'owner': 'aleksandr',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2026, 1, 1, tz=msk_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='daily_market_forecaster_pipeline',
    default_args=default_args,
    description='Automated pipeline for collecting prices, evaluating forecasts, and distributing notifications.',
    schedule='0 18 * * *',
    catchup=False,
    tags=['production', 'forecaster']
) as dag:

    # =========================================================================
    # TASK 0: Database Pre-flight Connection Verification Check
    # =========================================================================
    @task(task_id="check_db_connection")
    def check_postgres_connection():
        """Verify database health and availability before initiating data ingestion."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        logger.info("Initializing connection via PostgresHook...")
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        # get_first automatically manages conn/cursor and returns the first row
        result = pg_hook.get_first("SELECT 1;")

        logger.info(f"Database health check passed. Received response: {result}")

    # ---------------------------------------------------------------------------
    # TASK 1: EXTRACT & TRANSFORM (API Layer)
    # ---------------------------------------------------------------------------
    @task(task_id="collect_current_prices", retries=2, retry_delay=60)
    def collect_current_prices(**context) -> list:
        """Fetch live currency metrics sequentially, respecting strict API rate limits."""
        from airflow.providers.http.hooks.http import HttpHook
        import time
        import pytz

        msk_tz = pytz.timezone("Europe/Moscow")
        current_weekday = datetime.now(msk_tz).weekday()

        currency_pairs = [
            ("USD", "RUB", "USD/RUB"),
            ("EUR", "RUB", "EUR/RUB"),
            ("BTC", "USD", "BTC"),
        ]

        bulk_data = []

        http_hook = HttpHook(method="GET", http_conn_id="alphavantage_api")
        api_key = http_hook.get_connection("alphavantage_api").password

        with http_hook.get_conn() as session:
            for from_sym, to_sym, ticker_name in currency_pairs:
                is_fx = "RUB" in ticker_name

                if is_fx and current_weekday in (5, 6):
                    logger.info(
                        f"Skipping FX asset retrieval loop for {ticker_name} (Weekend context)."
                    )
                    continue

                params = {
                    "function": "CURRENCY_EXCHANGE_RATE",
                    "from_currency": from_sym,
                    "to_currency": to_sym,
                    "apikey": api_key,
                }

                try:
                    response = session.get(
                        f"{http_hook.base_url}/query", params=params, timeout=15
                    )
                    response.raise_for_status()
                    curr_res = response.json()

                    if "Note" in curr_res:
                        logger.warning(
                            f"AlphaVantage Rate Warning for {ticker_name}: {curr_res['Note']}"
                        )
                        time.sleep(20)
                        continue

                    curr_series = curr_res.get("Realtime Currency Exchange Rate", {})
                    if not curr_series:
                        logger.error(
                            f"Invalid structural payload returned for {ticker_name}: {curr_res}"
                        )
                        continue

                    price_date_str = datetime.strptime(
                        curr_series["6. Last Refreshed"], "%Y-%m-%d %H:%M:%S"
                    ).strftime("%Y-%m-%d")
                    close_price = float(curr_series["5. Exchange Rate"])

                    if ticker_name == 'BTC':
                        ticker_name = 'BTC/USD'

                    bulk_data.append([ticker_name, price_date_str, close_price])
                    logger.info(
                        f"Fetched {ticker_name}: {close_price} at {price_date_str}"
                    )
                    
                except Exception as e:
                    logger.error(
                        f"Failed to fetch market data loop step for token {ticker_name}: {e}"
                    )
                    raise

                time.sleep(15)

        if not bulk_data:
            raise ValueError("All expected market data network connection attempts failed. Triggering Airflow Task Retry engine.")
        
        return bulk_data

    # ---------------------------------------------------------------------------
    # TASK 2: LOAD (Database Persistence Layer)
    # ---------------------------------------------------------------------------
    @task(task_id="insert_prices_into_db")
    def insert_prices_into_db(prices_matrix: list) -> None:
        """Accept processed price matrix values and persist data rows into history using batch upserts."""
        if not prices_matrix:
            logger.warning(
                "Received an empty prices matrix list parameter. Cancelling execution."
            )
            return

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from psycopg2.extras import execute_values

        upsert_history_query = """
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

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg_hook.get_conn()

        try:
            with conn, conn.cursor() as cur:
                execute_values(
                    cur,
                    upsert_history_query,
                    prices_matrix,
                    template="(%s, %s, %s)",
                )
            logger.info(
                f"Successfully pushed {len(prices_matrix)} matrix metrics to price_history table."
            )
        finally:
            conn.close()
    
    # =========================================================================
    # TASK 3: Calculate Forecast Successes
    # =========================================================================
    @task(task_id="calculate_forecasts")
    def calculate_forecasts() -> None:
        """Evaluate and update accuracy marks for
        historical forecasts matching the current date."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        update_query = """
            update
                forecasts f
            set
                is_correct = case
                    when f.forecast_direction = 1
                    and ph_target.price > ph_base.price then true
                    when f.forecast_direction = 0
                    and ph_target.price < ph_base.price then true
                    else false
                end
            from
                price_history ph_target
            join price_history ph_base on
                ph_base.asset_id = ph_target.asset_id
                and ph_base.price_date = (
                select
                    max(price_date)
                from
                    price_history
                where
                    asset_id = ph_target.asset_id
                    and price_date < ph_target.price_date
                )
            where
                f.asset_id = ph_target.asset_id
                and f.target_date = ph_target.price_date
                and f.target_date = CURRENT_DATE
                and f.is_correct is null;
        """

        logger.info("Executing daily live forecast updates using CURRENT_DATE...")
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        pg_hook.run(update_query)

        logger.info("Successfully processed and locked in live forecast scores.")

    # ---------------------------------------------------------------------------
    # TASK 4: NOTIFICATIONS (Communication Layer)
    # ---------------------------------------------------------------------------
    @task(task_id="send_general_announcement")
    def send_general_announcement():
        """Distribute interactive product reports and new survey loops to active users."""
        from urllib3.util import Retry
        import requests
        from requests.adapters import HTTPAdapter
        from psycopg2.extras import RealDictCursor
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # Setup retry strategy rules for network resiliency against Telegram API timeouts
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        tg_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

        subscribers_query = "SELECT telegram_id FROM users WHERE is_subscribed = TRUE;"
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        
        try:
            with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(subscribers_query)
                subscribers = cur.fetchall()
                
            initial_text = (
                "<b>Результаты прогнозов и новые опросы готовы!</b> 💹\n\n"
                "🔎 Вы можете посмотреть результаты с помощью следующих команд:\n\n"
                "🔹 <b>Только результаты:</b>\n"
                "/brief\n"
                "🔸 <b>Подробный отчет:</b>\n"
                "/detailed\n\n"
                "📊 <b>Опросы:</b> "
                "/polls\n"
                "<i>_________________________________</i>" # 33
            )
            
            reply_markup = {
                "inline_keyboard": [
                    [{"text": "Удалить сообщение ❌", "callback_data": "delete_general_msg"}]
                ]
            }

            for sub in subscribers:
                chat_id = sub['telegram_id']
                payload = {
                    "chat_id": chat_id, 
                    "text": initial_text, 
                    "parse_mode": "HTML",
                    "reply_markup": reply_markup
                }
                try:
                    response = session.post(tg_url, json=payload, timeout=10)
                    if not response.ok:
                        print(f"Failed delivery to {chat_id}: {response.text}")
                except Exception as send_err:
                    print(f"Failed outbound delivery to context {chat_id}: {send_err}")
                        
        except Exception as e:
            print(f"General announcement dispatcher crashed: {e}")
            raise
        finally:
            conn.close()

    # =========================================================================
    # TASK 5*: Refresh Materialized Views Layer (BI Analytics Data Marts)
    # =========================================================================
    @task(task_id="refresh_materialized_views")
    def refresh_materialized_views() -> None:
        """Executes chronological baseline refreshes across all analytical data marts."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        
        views_to_refresh = [
            "mv_datalens_prices",
            "mv_bi_daily_turnout",
            "mv_bi_feature_adoption",
            "mv_bi_response_velocity",
            "mv_bi_interface_spam",
            "mv_datalens_forecasts_accuracy"
        ]
        
        
        for view_name in views_to_refresh:
            logger.info(f"Triggering storage engine refresh calculation for: {view_name}...")
            pg_hook.run(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name};")
            logger.info(f"Finished compiling updates for view: {view_name}.")

    # =========================================================================
    # PIPELINE DEPENDENCY ROUTING GRAPH
    # =========================================================================
    task_db_check = check_postgres_connection()
    task_collect_prices = collect_current_prices()
    task_insert_into_db = insert_prices_into_db(task_collect_prices)
    task_run_evaluations = calculate_forecasts()
    task_general_announcement = send_general_announcement()
    task_refresh_views = refresh_materialized_views()

    task_db_check >> task_collect_prices >> task_insert_into_db >> task_run_evaluations >> task_general_announcement >> task_refresh_views 
