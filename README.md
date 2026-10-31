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

### ❇️ The Core Experience: Telegram UI

[](https://github.com/Polibobr2000/Fintuition_Game#%EF%B8%8F-the-core-experience-telegram-integration)

- **/start** guides users through an automated onboarding portal covering structural setup rules, time-zone constraints, and native language preferences to minimize early funnel drop-off rates.
- **/polls** triggers predictable time-gated engagement loops at 18:00 MSK with a strict 3-hour expiration window designed to build daily active usage habits.
- **/brief** and **/detailed** provide dual-layer performance reviews, allowing users to instantly switch between viewing low-friction binary signals or analyzing deep relative pricing deltas and percent margins.
- **/points** aggregates gamified retention scoreboards by summarizing calendar-week user performance indicators to drive long-term user behavior cycles.
- **/graphs** embeds live interactive product BI metrics into secure Telegram WebApp containers, letting users explore asset trends and leaderboard data via Yandex DataLens in real time.

### ⚙️ The Data Engine: Airflow Orchestration

[](https://github.com/Polibobr2000/Fintuition_Game#%EF%B8%8F-the-data-engine-airflow-orchestration)

- **Daily Analytics Engine (`daily_market_forecaster_pipeline`):** Runs every day at 18:00 MSK. Handles live currency exchange extraction, stores incoming metric snapshots, processes binary options wins/losses against yesterday's closes, issues push notifications, and refreshes analytical datamarts.
- **Weekly Broadcasts (`weekly_leaderboard_pipeline`):** Runs every Sunday at 18:05 MSK. Sums correct forecasting counts, transforms logs into formatted scoreboard text featuring victory podium medals, and sequentially broadcasts payloads across the entire subscriber registry.

## 🔗 The Process

[](https://github.com/Polibobr2000/Fintuition_Game#%E2%80%8D-the-process)

The application architecture was developed in iterative phases, transitioning from a simple standalone script to a production-ready distributed network system:

``` mermaid
graph TD
    %% Define System Actors and Layers
    User([📱 Telegram Users])
    
    subgraph Telegram_Interface [Interface & Caching Layer]
        Bot[🤖 Telegram Bot Script]
        Cache[(RAM Cache)]
    end

    subgraph Data_Orchestration [Data Pipeline Layer]
        API[🌐 AlphaVantage API]
        Airflow{🌬️ Apache Airflow Engine}
    end

    subgraph Storage_Analytics [Storage & Reporting Layer]
        DB[(🐘 PostgreSQL Database)]
        DataLens[[📊 Yandex DataLens BI]]
    end

    %% Flow Connections
    User <--> |Interaction & Polls| Bot
    Bot <--> |Fast RAM Lookups| Cache
    Bot <--> |Log Telemetry & Reads| DB
    
    API <--> |Throttled Extraction| Airflow
    Airflow --> |Daily Batch Ingestion| DB
    
    DB ---> |Direct Analytical Read| DataLens

    %% Visual Themes and Branding
    classDef interface fill:#fff4fc,stroke:#d4a5c9,stroke-width:2px;
    classDef pipeline fill:#fff7ed,stroke:#ea580c,stroke-width:2px;
    classDef storage fill:#f0fdf4,stroke:#16a34a,stroke-width:2px;
    classDef actor fill:#f8fafc,stroke:#475569,stroke-width:2px;

    class Bot,Cache interface;
    class API,Airflow pipeline;
    class DB,DataLens storage;
    class User actor;
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
