# ----------------------------
# 0. IMPORTS
# ----------------------------
import logging
import asyncpg
import asyncio
from typing import Dict, List, Optional, Tuple, Any
import os
from dotenv import load_dotenv
import signal
from datetime import date, datetime, time, timedelta
import pytz
import json

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import (
    Application, 
    CommandHandler, 
    CallbackQueryHandler, 
    ContextTypes,
    MessageHandler,
    filters,
)

# ----------------------------
# 1. CONFIGURATION & SETUP
# ----------------------------
load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("ProductPipelineBot")

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST", "database_service")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"


# ----------------------------
# 2. DATABASE FUNCS and CACHE
# ----------------------------
# ----------------------------
# 2.1 SUBSCRIPTION
# ----------------------------
async def create_db_pool() -> asyncpg.Pool:
    """Create database connection pool."""
    return await asyncpg.create_pool(DATABASE_URL)

async def is_user_subscribed(user_id: int, pool: asyncpg.Pool) -> bool:
    """Check if user is subscribed."""
    async with pool.acquire() as conn:
        result = await conn.fetchrow(
            "SELECT is_subscribed FROM users WHERE telegram_id = $1",
            user_id
        )
        return result['is_subscribed'] if result else False

async def toggle_subscription(user_id: int, first_name: str, pool: asyncpg.Pool) -> Tuple[bool, str]:
    """Toggle subscription using unified INSERT...ON CONFLICT pattern."""
    async with pool.acquire() as conn:

        current = await conn.fetchrow(
            "SELECT is_subscribed FROM users WHERE telegram_id = $1", 
            user_id
        )

        new_status = not current['is_subscribed'] if current else True
        
        await conn.execute("""
            INSERT INTO users (telegram_id, username, is_subscribed)
            VALUES ($1, $2, $3)
            ON CONFLICT (telegram_id) DO UPDATE -- previously was user_id
            SET is_subscribed = EXCLUDED.is_subscribed
        """, user_id, first_name, new_status)
        
        action = "subscribed" if new_status else "unsubscribed"
        return new_status, action
    
async def get_subscription_message(user_id: int, pool: asyncpg.Pool) -> tuple:
    """
    Generate subscription status message and appropriate button.
    Returns (message_text, button_text, callback_data)
    """
    is_subscribed = await is_user_subscribed(user_id, pool)
    status_text = "подписаны" if is_subscribed else "не подписаны"
    
    message = (
        "📋 <b>Статус подписки</b>\n\n"
        f"Вы <b>[{status_text}]</b> на симулятор бинарных опционов."
        # "Каждый день в 18:00 вы будете получать уведомление о том, что результаты ваших предыдущих прогнозов и новые опросы готовы.\n\n"
        # "📊 <b>Расписание ежедневных опросов:</b>\n"
        # "• 3 опроса по будням\n"
        # "• 1 опрос по выходным"
    )
    
    if is_subscribed:
        button_text = "❌ Отписаться"
        callback_data = "toggle_subscription"
    else:
        button_text = "✅ Подписаться"
        callback_data = "toggle_subscription"
    
    return message, button_text, callback_data
# ----------------------------
# 2.2 Prediction results
# ----------------------------
    
async def fetch_brief_results(user_id: int, target_date, pool: asyncpg.Pool) -> list:
    """Fetch today's prediction outcomes for a user."""
    async with pool.acquire() as conn:
        query_str = """
            SELECT 
                a.ticker AS ticker,
                f.is_correct
            FROM forecasts f
            JOIN assets a ON f.asset_id = a.id
            WHERE f.user_id = (SELECT id FROM users WHERE telegram_id = $1)
              AND f.target_date = $2;
        """
        return await conn.fetch(query_str, user_id, target_date)

async def fetch_detailed_results(user_id: int, target_date, pool: asyncpg.Pool) -> list:
    """Fetch detailed pricing and forecast outcomes for a user and date."""
    async with pool.acquire() as conn:
        query_str = """
            SELECT
                a.name AS asset_name,
                a.ticker AS asset_ticker,
                f.forecast_direction,
                f.is_correct,
                ph_target.price AS today_price,
                ph_base.price AS yesterday_price
            FROM users u
            CROSS JOIN assets a
            JOIN price_history ph_target ON a.id = ph_target.asset_id 
                AND ph_target.price_date = $2
            JOIN price_history ph_base ON a.id = ph_base.asset_id 
                AND ph_base.price_date = (
                    SELECT MAX(price_date)
                    FROM price_history
                    WHERE asset_id = ph_target.asset_id
                      AND price_date < ph_target.price_date
                )
            LEFT JOIN forecasts f ON u.id = f.user_id 
                AND a.id = f.asset_id 
                AND f.target_date = $2
            WHERE u.telegram_id = $1 AND u.is_subscribed = TRUE;
        """
        return await conn.fetch(query_str, user_id, target_date)
    
async def user_has_yesterday_predictions(user_id: int, current_date: datetime.date, pool) -> bool:
    """Check if the user has any logged predictions from yesterday using a fast index scan."""

    query = """
        SELECT EXISTS (
            SELECT 1 
            FROM forecasts f
            JOIN users u ON f.user_id = u.id
            WHERE u.telegram_id = $1 AND f.target_date = $2
        );
    """
    async with pool.acquire() as conn:
        return await conn.fetchval(query, user_id, current_date)
# ----------------------------
# 2.3 Action logger
# ----------------------------
async def global_action_logger(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Unified interceptor middleware. 

    Dynamically registers unknown/unsubscribed users and captures all incoming 
    interactions (messages, commands, button clicks) in a single step.
    """
    if not update.effective_user:
        return

    user_id: int = update.effective_user.id
    # Safely handle missing usernames by falling back to first name
    username: str = update.effective_user.username or update.effective_user.first_name or "Unknown"
    action_value: str = ""

    # Parse what the user actually did
    if update.message and update.message.text:
        action_value = update.message.text
    elif update.callback_query:
        action_value = f"Button: {update.callback_query.data}"

    if action_value:
        pool: Optional[asyncpg.Pool] = context.bot_data.get("db_pool")
        if pool:
            # Atomic CTE: Ensures user exists in users first, then logs action
            unified_query = """
            WITH targeted_user AS (
                INSERT INTO users (telegram_id, username, is_subscribed)
                VALUES ($1, $2, FALSE)
                ON CONFLICT (telegram_id) DO UPDATE 
                SET username = EXCLUDED.username -- Keeps their telegram handle updated
                RETURNING id
            )
            INSERT INTO action_log (user_id, action_type)
            SELECT id, $3 FROM targeted_user;
            """
            try:
                async with pool.acquire() as conn:
                    await conn.execute(unified_query, user_id, username, action_value[:100])
                logger.info(f"🎉 Auto-registered & logged action for user {user_id}: {action_value[:30]}...")
            except Exception as e:
                logger.error(f"❌ Unified global logger failed to persist to DB: {e}")
# ----------------------------
# 2.3 Active prices
# ----------------------------
async def fetch_active_assets_and_prices(pool) -> list:
    """Fetch active asset profiles and their reference prices for today."""
    async with pool.acquire() as conn:
        query_str = """
            SELECT a.id, a.ticker, p.price
            FROM assets a
            LEFT JOIN price_history p ON a.id = p.asset_id AND p.price_date = CURRENT_DATE
            ORDER BY a.id;
        """
        return await conn.fetch(query_str)

# ----------------------------
# 2.4 Clear Cache
# ----------------------------
async def clear_expired_caches(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Wipes the entire cache at midnight since the viewing window has closed."""
    cache = context.bot_data.get("results_cache", {})
    cache.clear()
    logger.info("🧹 [CACHE CLEANUP] Midnight reached. Results cache has been completely cleared.")
# ----------------------------
# 3. Main Commands
# ----------------------------
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command with welcome and subscription management option."""
    user = update.effective_user
    
    welcome_text =  (
            f"👋 <b>Привет</b>, <b>{user.first_name}!</b>\n\n"
            f"Добро пожаловать в <b>FinTuition Challenge</b>\n\n"
            f"🔮 <b>Прогнозируй движение рынка:</b>\nУгадай, куда пойдет цена активов через 24 часа —\nВВЕРХ 📈 или ВНИЗ 📉\n\n"
            f"<b>Поднимайся в рейтинге:</b>\nСоревнуйся с друзьями 👥 и доминируй в недельном лидерборде!\n\n"
            f"<b>Как играть:</b>\n\n"
            f"🗳 <b>Ежедневные опросы:</b>\nТы будешь получать опросы для прогнозов каждый день в 18:00 по МСК.\n\n"
            f"⏳ <b>3-часовое окно:</b>\nУ тебя есть ровно 3 часа\n(до 21:00 по МСК), чтобы проголосовать.\n\n"
            f"📈 <b>Отслеживаемые активы:</b>\n• Биткоин (BTC/USD),\n• Доллар США (USD/RUB)\n• Евро (EUR/RUB).\n\n"
            f"🏆 <b>Ежедневные результаты:</b>\nНа следующий день в 18:00 ты получишь доступ к кратким/подробным результатам своих прогнозов.\n\n"
            f"🔢 <b>Недельные баллы:</b>\nТы будешь получать по одному баллу за каждый успешный прогноз.\n\n"
            f"🔘 Посмотреть все доступные команды и их описание можно с помощью\n/commands\n\n"
            f"Подписаться можно нажав на кнопку ниже:"
        )
    
    keyboard = [
        [InlineKeyboardButton("⚙️ Управление подпиской", callback_data="check_status")]
        ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode="HTML")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command to list all available operations."""
    if not update.message:
        return

    help_text = (
        "📖 <b>Доступные команды</b>\n\n"
        "🚀 <b>/start</b>\n"
        "└ Начальные инструкции\n\n"
        "🔔 <b>/subscribe</b>\n"
        "└ Управление подпиской\n\n"
        "🔮 <b>/brief</b>\n"
        "└ Краткие результаты прогнозов\n\n"
        "🔎 <b>/detailed</b>\n"
        "└ Подробные результаты прогнозов\n\n"
        "📊 <b>/polls</b>\n"
        "└ Получить опросы\n\n"
        "🏅 <b>/points</b>\n"
        "└ Баллы за неделю\n\n"
        "📈 <b>/graphs</b>\n"
        "└ Графики цен/прогонозов/рейтинга\n"
    )

    await update.message.reply_text(text=help_text, parse_mode="HTML")

async def manage_subscription_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /manage_subscription command - shows current status."""
    user_id = update.effective_user.id
    pool = context.bot_data['db_pool']
    
    message_text, button_text, callback_data = await get_subscription_message(user_id, pool)
    keyboard = [[InlineKeyboardButton(button_text, callback_data=callback_data)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(message_text, reply_markup=reply_markup, parse_mode="HTML")

async def get_points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /points command synced to fixed Monday-to-Sunday calendar weeks."""
    if not update.effective_user or not update.message:
        return
        
    user_telegram_id = update.effective_user.id
    
    msk_tz = pytz.timezone("Europe/Moscow")
    click_date = datetime.now(msk_tz).date()

    
    pool = context.bot_data.get('db_pool')
    if not pool:
        return await update.message.reply_text("⚠️ Ошибка подключения к базе данных.")

    async with pool.acquire() as conn:
        query_str = """
            SELECT COUNT(*) FILTER (WHERE is_correct)
            FROM forecasts
            WHERE user_id = (SELECT id FROM users WHERE telegram_id = $1)
              AND target_date::date >= DATE_TRUNC('week', $2::date)::DATE
              AND target_date::date <= (DATE_TRUNC('week', $2::date) + INTERVAL '6 days')::DATE;
        """
        
        points = await conn.fetchval(query_str, user_telegram_id, click_date) or 0

        return await update.message.reply_text(
            text=f"Набрано баллов на этой неделе: <b>{points}</b>",
            parse_mode="HTML"
        )

async def brief_results_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Expose daily prediction results subject to timeframe restrictions with local memory caching."""
    if not update.effective_user or not update.message:
        return

    msk_tz = pytz.timezone("Europe/Moscow")
    now_msk = datetime.now(msk_tz)
    current_hour = now_msk.hour
    one_day = timedelta(days=1)
    current_date = now_msk.date()
    yest_date = current_date - one_day

    # =========================================================================
    # OUTCOME 1: Request made between 00:00 and 17:59 MSK
    # =========================================================================
    if current_hour < 18:
        await update.message.reply_text(
            text="⏳ Проверить результаты предыдущих прогнозов можно только в период с <b>18:00</b> до <b>23:59 МСК</b>.",
            parse_mode="HTML",
        )
        return

    # =========================================================================
    # OUTCOME 2 & 3: Request made between 18:00 and 23:59 MSK
    # =========================================================================
    user_id = update.effective_user.id
    target_date_str = current_date.strftime("%Y-%m-%d")
    
    cache_key = f"brief_{user_id}_{target_date_str}"
    cache = context.bot_data.setdefault("results_cache", {})

    if cache_key in cache:
        logger.info(f"🚀 [CACHE HIT] Serving brief results for user {user_id} directly from RAM.")
        await update.message.reply_text(text=cache[cache_key], parse_mode="HTML")
        return

    logger.info(f"💾 [CACHE MISS] Querying PostgreSQL database for brief results of user {user_id}.")
    pool = context.bot_data.get("db_pool")
    if not pool:
        await update.message.reply_text("⚠️ Ошибка подключения к базе данных.")
        return

    try:
        rows = await fetch_brief_results(user_id, current_date, pool)
        if not rows:
            response_text = f"ℹ️ Вы не голосовали вчера ({yest_date})."
        else:
            lines = [f"📊 <b>Краткие результаты прогнозов на {current_date}:</b>\n"]
            for row in rows:
                is_correct = row["is_correct"]
                emoji = "🟢 " if is_correct else "🔴 "
                lines.append(f"{emoji} <b>{row['ticker']}</b>")
            response_text = "\n".join(lines)
        
        cache[cache_key] = response_text
        
        await update.message.reply_text(text=response_text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Brief results command failed: {e}")
        await update.message.reply_text(text="⚠️ Не удалось загрузить результаты. Пожалуйста, попробуйте еще раз.")

async def detailed_results_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /detailed_results by displaying all active asset boards for the current market cycle."""
    if not update.effective_user or not update.message:
        return

    msk_tz = pytz.timezone("Europe/Moscow")
    now_msk = datetime.now(msk_tz)
    current_date = now_msk.date()
    weekday = now_msk.weekday()
    one_day = timedelta(days=1)
    yest_date = current_date - one_day

    if now_msk.hour < 18:
        await update.message.reply_text(
            text="⏳ Проверить результаты предыдущих прогнозов можно только в период с <b>18:00</b> до <b>23:59 МСК</b>.",
            parse_mode="HTML"
        )
        return

    user_id = update.effective_user.id
    target_date_str = current_date.strftime("%Y-%m-%d")

    menu_cache_key = f"detail_menu_{user_id}_{target_date_str}"
    cache = context.bot_data.setdefault("results_cache", {})

    if menu_cache_key in cache:
        logger.info(f"🚀 [CACHE HIT] Serving detailed menu layout for user {user_id} straight from RAM.")
        cached_text, cached_markup = cache[menu_cache_key]
        await update.message.reply_text(text=cached_text, reply_markup=cached_markup, parse_mode="HTML")
        return

    pool = context.bot_data.get("db_pool")
    if not pool:
        await update.message.reply_text("⚠️ Ошибка подключения к базе данных.")
        return

    try:
        # STEP A: Use your fast index checker function first!
        has_voted = await user_has_yesterday_predictions(user_id, current_date, pool)
        if not has_voted:
            await update.message.reply_text(
                text=f"ℹ️ Вы не голосовали вчера ({yest_date}).",
                parse_mode="HTML"
            )
            return

        # 2. Query data to ensure market prices are populated for today
        rows = await fetch_detailed_results(user_id, current_date, pool)
        
        # Safe Guard: If rows are completely empty, the system hasn't loaded asset history records yet
        if not rows:
            await update.message.reply_text(
                text="⚠️ Показатели на сегодня еще не готовы. Пожалуйста, свяжитесь с администратором.",
                parse_mode="HTML"
            )
            return

        all_assets = {
            "BTC/USD": "🪙 BTC/USD",
            "USD/RUB": "💵 USD/RUB",
            "EUR/RUB": "💶 EUR/RUB",
        }

        keyboard = [
            [InlineKeyboardButton(label, callback_data=f"detail_asset_{ticker}")]
            for ticker, label in all_assets.items()
            if not ("RUB" in ticker and weekday in (5, 6))
        ]

        response_text = f"📊 <b>Подробные результаты прогнозов на {current_date}:</b>"
        reply_markup = InlineKeyboardMarkup(keyboard)

        cache[menu_cache_key] = (response_text, reply_markup)

        await update.message.reply_text(
            text=response_text,
            parse_mode="HTML",
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Detailed command layout processing failed: {e}")
        await update.message.reply_text("⚠️ Не удалось загрузить детали по этому активу.")

async def get_polls_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /get_polls with strict window, market rules, and network-safe idempotency."""
    if not update.effective_user or not update.message:
        return

    msk_tz = pytz.timezone("Europe/Moscow")
    now_msk = datetime.now(msk_tz)
    
    # Active polling window gate limits
    if not (18 <= now_msk.hour < 21): # 18 <=, < 21 # to_change_time
        await update.message.reply_text(
            text="⏳ Опросы доступны только с <b>18:00 до 21:00 МСК</b>.",
            parse_mode="HTML"
        )
        return

    user_telegram_id = update.effective_user.id
    current_date = now_msk.date()
    weekday = current_date.weekday()
    
    pool = context.bot_data.get('db_pool')
    if not pool:
        await update.message.reply_text("⚠️ Ошибка подключения к базе данных.")
        return

    try:
        async with pool.acquire() as conn:
            user_row = await conn.fetchrow(
                "SELECT id, is_subscribed, last_poll_sent_date FROM users WHERE telegram_id = $1", 
                user_telegram_id
            )
        
        if not user_row:
            await update.message.reply_text("⚠️ Профиль пользователя не найден. Пожалуйста, сначала нажмите на /start и подпишитесь.")
            return
        
        if not user_row['is_subscribed']:
            await update.message.reply_text(
                text=(
                    "🔒 Только подписанные пользователи могут запрашивать ежедневные опросы\n\n"
                    "Вы можете изменить статус подписки в любое время с помощью команды /subscribe."
                ),
                parse_mode="HTML"
            )
            return

        if user_row['last_poll_sent_date'] == current_date:
            await update.message.reply_text(
                text="ℹ️ Сегодня вы уже запрашивали опросы! Проверьте историю чата выше.",
                parse_mode="HTML"
            )
            return

        assets_list = await fetch_active_assets_and_prices(pool)
        if not assets_list:
            await update.message.reply_text("⚠️ Данные о ценах активов еще не готовы. Пожалуйста, попробуйте позже.")
            return

        for asset in assets_list:
            ticker = asset['ticker']
            price = asset['price']
            
            if price is None:
                logger.warning(f"Skipping poll rendering for {ticker}: Reference price missing.")
                continue

            is_fx = "RUB" in ticker

            if is_fx and weekday in (5, 6):
                continue

            if is_fx and weekday == 4:
                poll_target_date = current_date + timedelta(days=3)
            else:
                poll_target_date = current_date + timedelta(days=1)

            target_date_str = poll_target_date.strftime("%Y-%m-%d")
            price_formatted = f"${float(price):,.2f}" if not is_fx else f"{float(price):,.2f} RUB"

            callback_up = f"voteup_{ticker}_{target_date_str}"
            callback_down = f"votedown_{ticker}_{target_date_str}"
            
            keyboard = [[
                InlineKeyboardButton("🚀 Вверх (UP)", callback_data=callback_up),
                InlineKeyboardButton("📉 Вниз (DOWN)", callback_data=callback_down)
            ]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            poll_text = (
                f"📊 <b>Прогноз по {ticker}</b>\n"
                f"🎯 Целевая дата: <code>{target_date_str}</code>\n"
                f"💰 Текущая цена: <code>{price_formatted}</code>\n\n"
                f"Голосуйте до 21:00 MSK!"
            )
        
            await update.message.reply_text(
                text=poll_text,
                parse_mode="HTML",
                reply_markup=reply_markup
            )

        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET last_poll_sent_date = $1 WHERE id = $2",
                current_date, user_row['id']
            )
            
    except Exception as e:
        logger.error(f"Failed to issue on-demand user polls securely: {e}")
        await update.message.reply_text("⚠️ Ошибка соединения. Пожалуйста, свяжитесь с администратором.")

async def graphs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Attach a secure Telegram WebApp button pointing to the public BI analytics dashboard."""
    DATALENS_PUBLIC_URL = "https://datalens.yandex/28vro5kxng5il?_share_link=public"
    
    keyboard = [
        [
            InlineKeyboardButton(
                text="📊 Открыть графики", 
                web_app=WebAppInfo(url=DATALENS_PUBLIC_URL)
            )
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        text="Нажмите кнопку ниже, чтобы посмотреть тренды активов и лидерборд не выходя из Telegram:",
        reply_markup=reply_markup
    )

# ----------------------------
# 4. Callbacks
# ----------------------------
async def subscription_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle all inline keyboard button presses."""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    first_name = update.effective_user.first_name
    pool = context.bot_data['db_pool']
    
    if query.data == "check_status":
        message_text, button_text, callback_data = await get_subscription_message(user_id, pool)
        keyboard = [[InlineKeyboardButton(button_text, callback_data=callback_data)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode="HTML")
    
    elif query.data == "toggle_subscription":
        new_status, action = await toggle_subscription(user_id, first_name, pool)
        
        if action == "subscribed":
            confirmation_text = (
                "✅ <b>Вы успешно подписались!</b>\n\n"
                "Каждый день в 18:00 вы будете получать уведомление о том, что результаты ваших предыдущих прогнозов и новые опросы готовы.\n\n"
                "📊 <b>Расписание ежедневных опросов:</b>\n"
                "• 3 опроса по будням\n"
                "• 1 опрос по выходным\n\n"
                "Вы можете изменить статус подписки в любое время с помощью команды /subscribe."
            )

            await query.edit_message_text(confirmation_text, parse_mode="HTML")
        
        else:
            confirmation_text = (
                "❌ <b>Вы отписались</b>\n\n"
                "Вы больше не будете получать опросы.\n\n"
                "Вы можете повторно подписаться в любой момент с помощью:\n"
                "• Команды /subscribe\n"
                "• Меню /start\n"
                "• Или нажав кнопку ниже:"
            )
            button_text = "✅ Subscribe"
            keyboard = [[InlineKeyboardButton(button_text, callback_data="toggle_subscription")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(confirmation_text, reply_markup=reply_markup, parse_mode="HTML")
            
async def detailed_asset_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Refreshes asset dashboards, removing active targets and handling weekend exclusions safely."""
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = update.effective_user.id
    
    all_assets = {
        "BTC/USD": "🪙 BTC/USD",
        "USD/RUB": "💵 USD/RUB",
        "EUR/RUB": "💶 EUR/RUB",
    }
    
    msk_tz = pytz.timezone("Europe/Moscow")
    now_msk = datetime.now(msk_tz)
    current_date = now_msk.date()
    yest_date = current_date - timedelta(days=1)
    current_date_str = current_date.strftime("%Y-%m-%d")
    weekday = current_date.weekday()
    
    cache = context.bot_data.setdefault("results_cache", {})

    # -------------------------------------------------------------------------
    # 1. SUB-MENU ROUTING LAYER (Clicking "Back to all assets")
    # -------------------------------------------------------------------------
    if data == "detail_back_menu":
        menu_cache_key = f"detail_menu_{user_id}_{current_date_str}"

        if menu_cache_key in cache:
            logger.info(f"🚀 [CACHE HIT] Routing user {user_id} back to their menu layout via RAM.")
            response_text, reply_markup = cache[menu_cache_key]
            await query.edit_message_text(
                text=response_text,
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
            return
            
        keyboard = [
            [InlineKeyboardButton(label, callback_data=f"detail_asset_{ticker}")]
            for ticker, label in all_assets.items()
            if not ("RUB" in ticker and weekday in (5, 6))
        ]
        await query.edit_message_text(
            text=f"📊 <b>Подробные результаты прогнозов на {current_date_str}:</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    # -------------------------------------------------------------------------
    # 2. INDIVIDUAL ASSET DETAILS CARD LAYER
    # -------------------------------------------------------------------------
    asset_ticker_target = data.replace("detail_asset_", "")
    cache_key = f"detail_{user_id}_{asset_ticker_target}_{current_date_str}"
    
    if cache_key in cache:
        logger.info(f"🚀 [CACHE HIT] Serving detailed {asset_ticker_target} results from RAM.")
        response_text, reply_markup = cache[cache_key]
        await query.edit_message_text(text=response_text, parse_mode="HTML", reply_markup=reply_markup)
        return
        
    pool = context.bot_data.get("db_pool")
    if not pool:
        await query.edit_message_text("⚠️ Ошибка подключения к базе данных.")
        return
        
    try:
        rows = await fetch_detailed_results(user_id, current_date, pool)
        selected_row = next((r for r in rows if r["asset_ticker"] == asset_ticker_target), None)
        
        if not selected_row:
            response_text = f"ℹ️ Прогноз по <b>{asset_ticker_target}</b> за вчера ({yest_date}) отсутствует."
        else:
            ticker = selected_row["asset_ticker"]
            forecast_direction = selected_row["forecast_direction"]
            is_correct = selected_row["is_correct"]
            today_price = round(float(selected_row["today_price"]), 2)
            yesterday_price = round(float(selected_row["yesterday_price"]), 2)
            price_diff = today_price - yesterday_price
            pct_change = (price_diff / yesterday_price) * 100 if yesterday_price != 0 else 0
            
            yest_fmt = f"{yesterday_price:,.2f} Руб" if "RUB" in ticker else f"${yesterday_price:,.2f}"
            today_fmt = f"{today_price:,.2f} Руб" if "RUB" in ticker else f"${today_price:,.2f}"
            diff_fmt = f"{abs(price_diff):,.2f} Руб" if "RUB" in ticker else f"${abs(price_diff):,.2f}"
            market_outcome_str = "Вверх 📈 " if price_diff >= 0 else "Вниз 📉 "
            sign = "+" if price_diff >= 0 else "-"
            
            if forecast_direction is None:
                status_msg = f"💬 Вы не голосовали вчера ({yest_date})."
                forecast_line = "ℹ️ Ваш прогноз:\nОтсутствует"
            else:
                user_prediction_str = "Вверх 📈 " if forecast_direction == 1 else "Вниз 📉 "
                forecast_line = f"ℹ️ Ваш прогноз: {user_prediction_str}"
                status_msg = "🎯 В точку!\nВаш прогноз подтвердился!" if is_correct else "🔄 Не в этот раз!\nЦена актива ушла в противоположную сторону"
                
            response_text = (
                f"📊 <b>{ticker} на {current_date_str}</b>\n\n"
                f"{status_msg}\n"
                f"{forecast_line}\n"
                f"📊 Рынок: пошёл {market_outcome_str}\n\n"
                f"🔹 Вчера: {yest_fmt}\n"
                f"🔹 Сегодня: {today_fmt}\n"
                f"🔹 Разница: {sign}{diff_fmt} ({pct_change:+.2f}%)\n"
            )
            
        keyboard = []
        for ticker, label in all_assets.items():
            if "RUB" in ticker and weekday in (5, 6):
                continue
            if ticker != asset_ticker_target:
                keyboard.append([InlineKeyboardButton(label, callback_data=f"detail_asset_{ticker}")])
                
        keyboard.append([InlineKeyboardButton("⬅️ Обратно к списку активов", callback_data="detail_back_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        cache[cache_key] = (response_text, reply_markup)
        await query.edit_message_text(text=response_text, parse_mode="HTML", reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Detailed asset callback failed: {e}")
        await query.edit_message_text("⚠️ Не удалось загрузить детали по этому активу.")

async def handle_poll_vote_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[Any]:
    """Process vote clicks with clean server-side date, time, and asyncpg type formatting."""
    query = update.callback_query
    await query.answer()

    data: str = query.data
    try:
        action, ticker, target_date_str = data.split("_")
    except ValueError:
        return await query.answer("⚠️ Неверный формат запроса (кнопки).", show_alert=True)

    msk_tz = pytz.timezone("Europe/Moscow")
    now_msk = datetime.now(msk_tz)
    current_date = now_msk.date()
    target_date_dt = date.fromisoformat(target_date_str)
    weekday = current_date.weekday()

    is_fx = "RUB" in ticker
    if is_fx and weekday == 4: # 4
        expected_target_date = current_date + timedelta(days=3)
    else:
        expected_target_date = current_date + timedelta(days=1)
        
    expected_target_str = expected_target_date.strftime("%Y-%m-%d")

    if target_date_str != expected_target_str or now_msk.hour >= 21:
        try:
            await query.edit_message_text(
                text="⏰ <b>Срок действия опроса истек!</b>", 
                parse_mode="HTML", 
                reply_markup=None
            )
        except Exception as e:
            logger.warning(f"Could not overwrite expired poll text window: {e}")
            
        return None

    pool = context.application.bot_data.get("db_pool")
    if not pool:
        await query.edit_message_text(
            "❌ База данных недоступна. Прогноз не сохранен. Пожалуйста, свяжитесь с администратором."
        )
        return None

    direction_numeric = 1 if action == "voteup" else 0
    direction_word = "выше" if direction_numeric == 1 else "ниже"
    telegram_user_id = query.from_user.id

    try:
        insert_query = """
        INSERT INTO forecasts (user_id, asset_id, forecast_direction, target_date)
        SELECT u.id, a.id, $3, $4::DATE
        FROM users u
        CROSS JOIN assets a
        WHERE u.telegram_id = $1 AND a.ticker = $2
        ON CONFLICT (user_id, asset_id, target_date)
        DO UPDATE SET forecast_direction = EXCLUDED.forecast_direction,
                      forecast_ts = CURRENT_TIMESTAMP
        RETURNING id;
        """
        async with pool.acquire() as conn:
            prediction_id = await conn.fetchval(
                insert_query, telegram_user_id, ticker, direction_numeric, target_date_dt
            )
            
        if not prediction_id:
            await query.edit_message_text(
                "⚠️ Профиль пользователя не найден. Пожалуйста, подпишитесь с помощью команды /subscribe."
            )
            return None

        when = 'завтра' if target_date_dt == current_date + timedelta(days=1) else 'в понедельник'
        confirmation_text = (
            f"Вы считаете, что {when} ({target_date_str}) цена {ticker} "
            f"будет {direction_word}, чем сегодня."
        )
        
        await query.edit_message_text(text=confirmation_text)

    except Exception as e:
        logger.error(f"Error handling button click database transaction: {e}")
        await query.edit_message_text(
            "❌ Произошла ошибка при сохранении прогноза. Пожалуйста, попробуйте нажать еще раз."
        )

async def delete_message_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"Could not delete message: {e}")

# ----------------------------
# 5. MAIN APPLICATION
# ----------------------------
async def main():
    """Main async function to run the bot."""
    TOKEN = TG_BOT_TOKEN
    
    try:
        db_pool = await create_db_pool()
        logger.info("Database connection established")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return
    
    application = Application.builder().token(TOKEN).build()
    
    application.bot_data['db_pool'] = db_pool

    application.bot_data["results_cache"] = {}

    midnight_msk = time(hour=0, minute=0, second=0, tzinfo=pytz.timezone("Europe/Moscow"))

    application.job_queue.run_daily(clear_expired_caches, time=midnight_msk)

    application.add_handler(MessageHandler(filters.ALL, global_action_logger), group=-1)
    application.add_handler(CallbackQueryHandler(global_action_logger), group=-1)

    # regular handlers (group=0 by default)
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("commands", help_command))
    application.add_handler(CommandHandler("subscribe", manage_subscription_command))
    application.add_handler(CommandHandler("brief", brief_results_command))
    application.add_handler(CommandHandler("detailed", detailed_results_command))
    application.add_handler(CommandHandler("polls", get_polls_command))
    application.add_handler(CommandHandler("points", get_points_command))
    application.add_handler(CommandHandler("graphs", graphs_command))

    
    # 1. Register poll extraction and dynamic callback matching
    application.add_handler(CallbackQueryHandler(handle_poll_vote_callback, pattern="^vote"))
    # 2. Route subscription clicks matching your exact standalone strings
    application.add_handler(CallbackQueryHandler(subscription_callback, pattern="^(check_status|toggle_subscription)$"))
    # 3. Route detailed prediction results callback
    application.add_handler(CallbackQueryHandler(detailed_asset_callback, pattern="^detail_"))
    # 4. Route general message deletion button
    application.add_handler(CallbackQueryHandler(delete_message_callback, pattern="^delete_general_msg$"))

    logger.info("Bot starting with PostgreSQL database...")
    
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    stop_event = asyncio.Event()
    
    def signal_handler():
        stop_event.set()
    
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        
        await db_pool.close()
        logger.info("Database connection closed")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
