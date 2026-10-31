# 💹 FinTuition Game

[](https://github.com/Polibobr2000/Fintuition_Game#-fintuition-game)

An interactive 24/7 **Telegram binary options market simulator bot** that challenges users to predict daily financial assets trends. Built with Python and an enterprise-grade async architecture, the platform gamifies financial intuition by allowing users to vote on whether market prices will close **UP 📈 or DOWN 📉**.

The backend runs data ingestion jobs via **Apache Airflow**, tracks user accuracy, generates live weekly leaderboards, and provides modern metrics analytics via Datalens dashboards (and an embedded **Apache Superset** business intelligence engine).

---

## 📦 Technologies

[](https://github.com/Polibobr2000/Fintuition_Game#-technologies)

- **Database Engine:** PostgreSQL 15 (Alpine Cluster)
- **BI & Analytics Dashboarding:** Datalens (Apache Superset, Redis Cache)
- **Asynchronous Processing:** Python, `asyncio`, `asyncpg`
- **Data Pipelines & Scheduling:** Apache Airflow
- **Application Interface:** `python-telegram-bot`
- **Data Providers:** AlphaVantage Financial API
- **Containerization & Environment Infrastructure:** Docker Compose

---

## 🎮 Features

[](https://github.com/Polibobr2000/Fintuition_Game#-features)

### ❇️ The Core Experience: Telegram Integration

[](https://github.com/Polibobr2000/Fintuition_Game#%EF%B8%8F-the-core-experience-telegram-integration)

- **Interactive Menu Interface (`/start`):** A welcome portal introducing game configurations, structural rules, and quick actions.
- **Automated Forecasting Windows (`/polls`):** Triggers interactive market voting panels every single weekday at 18:00 MSK with an immutable, strict 3-hour lock-in window expiring at 21:00 MSK.
- **Tiered Result Tracking (`/brief` & `/detailed`):** Instantly review predictive outcomes. `/brief` exposes quick green/red binary signals, while `/detailed` expands complete price deltas, relative asset metrics, and net percent margins with zero database penalty via transactional RAM memory-caching layers.
- **Gamified Scoreboards (`/points`):** Instantly aggregates calendar week success factors synced to hard Monday-to-Sunday boundaries.
- **Embedded Trend Metrics (`/graphs`):** Opens a beautiful external visual dashboard tracking systems (Datalens) and leaderboard rankings using secure Telegram WebApp containers.

### ⚙️ The Data Engine: Airflow Orchestration

[](https://github.com/Polibobr2000/Fintuition_Game#%EF%B8%8F-the-data-engine-airflow-orchestration)

- **Daily Analytics Engine (`daily_market_forecaster_pipeline`):** Runs every day at 18:00 MSK. Handles live currency exchange extraction, stores incoming metric snapshots, processes binary options wins/losses against yesterday's closes, issues push notifications, and refreshes analytical viewports.
- **Weekly Broadcasts (`weekly_leaderboard_pipeline`):** Runs every Sunday at 18:05 MSK. Sums correct forecasting counts, transforms logs into formatted scoreboard text featuring victory podium medals, and sequentially broadcasts payloads across the entire subscriber registry.

## 👩🏽‍🍳 The Process

[](https://github.com/Polibobr2000/Fintuition_Game#%E2%80%8D-the-process)

The application architecture was developed in iterative phases, transitioning from a simple standalone script to a production-ready distributed network system:

```
[ Telegram Users ] <---> [ Asynchronous Bot Worker ] <---> [ PostgreSQL Database ]
                                                                   ^
[ AlphaVantage API ] <---> [ Apache Airflow Engines ] ------------/
                                                                   v
                          [ Apache Superset Web Portal ] <---> [ Redis Cache ]
```

1. **Database Strategy & Storage Architecture:** Designed a normalized schema in PostgreSQL separating core asset attributes (dimension), historical price snapshots (fact), active user attributes, transactional forecasts, and raw action tracking arrays.
2. **Building the Asynchronous Core:** Built the Telegram handler loop utilizing non-blocking asynchronous drivers (`asyncpg`). Implemented unified `INSERT...ON CONFLICT DO UPDATE` CTE structural chains to atomically register user touchpoints and log event telemetry in a single runtime operation.
3. **Data Ingestion Integration & Airflow Workflows:** Set up separate DAG data workflows. Transformed raw runtime scripts into decoupled Airflow Tasks utilizing local module imports and connection pooling hooks. Added strict verification blocks that explicitly enforce worker tasks to retry if financial endpoints throw rate-limiting errors.
4. **Implementing Cache Management & Performance Tuning:** Created dedicated RAM caching engines inside the bot state to eliminate redundant read queries on the PostgreSQL server. Built automated midnight cleanup hooks to wipe the transient memory once daily checking windows expire.
5. **Analytics Layer Integration:** Compiled heavy window functions, spam tracking filters, and velocity calculations into performance-tuned Database Materialized Views to allow instant analytical updates without exhausting resource channels. Integrated an independent Redis layer to fuel Apache Superset caching.

---

## 📚 What I Learned

[](https://github.com/Polibobr2000/Fintuition_Game#-what-i-learned)

---

## 💭 How Can It Be Improved?

[](https://github.com/Polibobr2000/Fintuition_Game#-how-can-it-be-improved)

---

## 🍿 Video
