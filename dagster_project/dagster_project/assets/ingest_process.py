from dagster import asset
from dagster_project.dagster_project.helpers.scrape_sector import SectorScraper
from helpers.ticker_enricher import TickerEnricher
from helpers.yahoo_stats_scraper import YahooStatsScraper
from config import config
from datetime import datetime
import pandas as pd
import numpy as np
import duckdb
import os

DB_PATH = config.db_path
TABLE_NAME = "sector_company_daily"

@asset
def ingest_stock_data():
    """
    Main ingestion asset: scrapes Yahoo Finance data, enriches priority tickers,
    saves CSV, and upserts to DuckDB.
    """
    print("Starting sector scrape...")
    scraper = SectorScraper()
    rows = scraper.scrape_sectors()

    print("\nEnriching priority tickers...")
    enricher = TickerEnricher()
    yahoo_scraper = YahooStatsScraper()

    for row in rows:
        symbol = row['symbol']

        if symbol not in config.watch_list and "strong buy" not in str(row.get('rating', '')).lower():
            continue

        enriched = enricher.enrich_ticker(symbol)
        row.update(enriched)

        price_over_median = enriched.get('price_targets_overMedian', np.nan)

        if (price_over_median > 15) or (symbol in config.watch_list):
            yahoo_stats, yahoo_summary = yahoo_scraper.fetch_with_rate_limit(symbol)
            row.update(yahoo_stats)
            row["yahoo_business_summary"] = yahoo_summary

    # Step 3: Save to CSV
    df = pd.DataFrame(rows)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"sector_company_daily_{timestamp}.csv"
    filepath = os.path.join(config.downloads_path, filename)
    df.to_csv(filepath, index=False)
    print(f"Saved CSV to {filepath}")

    # Step 4: Upsert to DuckDB
    con = duckdb.connect(DB_PATH)
    try:
        # Align with existing table schema to prevent column mismatch
        table_info = con.execute(f"PRAGMA table_info({TABLE_NAME})").fetchall()
        if table_info:
            table_cols = [col[1] for col in table_info]
            # Add missing columns as NaN, reorder to match table
            for col in table_cols:
                if col not in df.columns:
                    df[col] = np.nan
            df = df[table_cols]
    except Exception:
        pass  # Table doesn't exist yet, will be created

    try:
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} AS
        SELECT * FROM df WHERE 1=0
        """)

        # Add primary key constraint for dedup if not exists
        try:
            con.execute(f"ALTER TABLE {TABLE_NAME} ADD PRIMARY KEY (date, symbol)")
        except Exception:
            pass  # Already exists

        con.execute(f"""INSERT OR REPLACE INTO {TABLE_NAME} SELECT * FROM df""")
    finally:
        con.close()

    print(f"Upserted {len(df)} rows to {TABLE_NAME}")

    return datetime.today().date()