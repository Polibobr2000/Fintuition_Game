import pendulum
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

msk_tz = pendulum.timezone("Europe/Moscow")

logger = logging.getLogger("airflow.task")

BOT_TOKEN = os.getenv("TG_BOT_TOKEN")

default_args = {
    'owner': 'aleksandr',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2026, 1, 1, tz=msk_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='weekly_leaderboard_pipeline',
    default_args=default_args,
    description='Pipeline for calculating weekly prediction accuracy statistics and broadcasting rankings.',
    schedule='5 18 * * 0',
    catchup=False,
    tags=['production', 'leaderboard']
) as dag:

    # =========================================================================
    # TASK 0: Database Pre-flight Connection Verification Check
    # =========================================================================
    @task(task_id="check_db_connection")
    def check_postgres_connection():
        """Verify database availability before starting the pipeline."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        logger.info("Initializing connection via PostgresHook...")
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        # get_first automatically manages conn/cursor and returns the first row
        result = pg_hook.get_first("SELECT 1;")
        logger.info(f"Database health check passed. Received response: {result}")

    # =========================================================================
    # TASK 1: Aggregate and Extract Leaderboard Statistics (Optimized)
    # =========================================================================
    @task(task_id="calculate_weekly_leaderboard")
    def calculate_weekly_leaderboard():
        """Aggregate forecast accuracy metrics for the current calendar week."""
        from psycopg2.extras import RealDictCursor
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        query = """
            SELECT 
                COALESCE(u.username, 'Anonymous') as username,
                COUNT(f.id) FILTER (WHERE f.is_correct = TRUE) as points
            FROM users u
            LEFT JOIN forecasts f ON f.user_id = u.id
            WHERE f.target_date >= DATE_TRUNC('week', CURRENT_DATE)::DATE
              AND f.target_date <= CURRENT_DATE
            GROUP BY u.id, u.username
            ORDER BY points DESC, username ASC;
        """
        
        logger.info("Executing weekly statistics aggregation query using RealDictCursor...")
        
        conn = pg_hook.get_conn()
        try:
            with conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                leaderboard_rows = cursor.fetchall()
        finally:
            conn.close()
            
        logger.info(f"Successfully aggregated {len(leaderboard_rows)} leaderboard user rows directly to dictionaries.")
        return leaderboard_rows

    # =========================================================================
    # TASK 2: Format Leaderboard Card and Broadcast to Subscribed Users
    # =========================================================================
    @task(task_id="broadcast_leaderboard_notification")
    def broadcast_leaderboard_notification(leaderboard_rows: list):
        """Format the weekly user rankings and broadcast them via Telegram."""
        import requests
        import time
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not leaderboard_rows:
            logger.warning("Leaderboard contains no data rows for the active tracking window.")
            leaderboard_text = "📊 <b>Недельный топ</b>\n\nНа этой неделе прогнозов пока что не было!"
        else:
            lines = ["🏆 <b>ИТОГИ НЕДЕЛИ</b> 🏆\n"]
            medals = {1: "🥇 ", 2: "🥈 ", 3: "🥉 "}
            
            for index, row in enumerate(leaderboard_rows, start=1):
                medal = medals.get(index, f"{index}. ")
                lines.append(f"{medal}{row['username']} — {row['points']} points")
                
            leaderboard_text = "\n".join(lines)

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        subscriber_query = "SELECT telegram_id FROM users WHERE is_subscribed = TRUE;"
        
        conn = pg_hook.get_conn()
        try:
            with conn, conn.cursor() as cursor:
                cursor.execute(subscriber_query)
                subscribers = [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
        
        logger.info(f"Targeting broadcast to {len(subscribers)} active subscribed users.")

        telegram_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        success_count = 0
        
        with requests.Session() as session:
            for tg_id in subscribers:
                payload = {
                    "chat_id": tg_id,
                    "text": leaderboard_text,
                    "parse_mode": "HTML"
                }
                try:
                    response = session.post(telegram_url, json=payload, timeout=10)
                    if response.status_code == 200:
                        success_count += 1
                    else:
                        logger.warning(f"Telegram returned error code {response.status_code} for user {tg_id}: {response.text}")
                except Exception as e:
                    logger.error(f"Network drop while transmitting scorecard to user {tg_id}: {e}")
                
                time.sleep(0.04)
                    
        logger.info(f"Leaderboard broadcast complete. Delivered messages to {success_count}/{len(subscribers)} chats.")

    # =========================================================================
    # Pipeline Task Sequence Topology Declaration (Perfect TaskFlow Chain)
    # =========================================================================
    db_check = check_postgres_connection()
    stats_data = calculate_weekly_leaderboard()
    broadcast_job = broadcast_leaderboard_notification(stats_data)

    db_check >> stats_data >> broadcast_job

