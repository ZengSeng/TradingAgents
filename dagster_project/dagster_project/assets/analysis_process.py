"""
Analysis asset - orchestrates the technical analysis and AI briefing pipeline.

This asset:
1. Fetches top stocks from ingested data
2. Updates price history for selected tickers
3. Calculates technical indicators
4. Generates AI-powered executive briefing
5. Emails report with CSV attachment
"""
from dagster import asset
import duckdb
import pandas as pd
import os
from datetime import datetime

from config import config
from dagster_project.dagster_project.helpers.stock_data_updater import StockDataUpdater
from helpers.technical_analyzer import run_analysis
from helpers.report_generator import fetch_top_signals_with_sector, generate_briefing
from helpers.email_sender import send_email
from helpers.ollama_client import MyStockBot

DB_PATH = config.db_path

@asset(deps=["ingest_stock_data"])
def analyze_stock_data():
    """
    Main analysis asset: computes signals, generates AI briefing, emails report.
    """
    with duckdb.connect(DB_PATH) as conn:  
        # Step 1: Determine top tickers from fundamentals and update their price data
        top_tickers = conn.execute("""
            SELECT symbol
            FROM sector_company_daily
            WHERE date = (SELECT MAX(date) FROM sector_company_daily)
                AND is_priority = 1
            ORDER BY price_targets_overMedian DESC
        """).fetchdf()

        tickers = top_tickers["symbol"].tolist()
        updater = StockDataUpdater(conn)
        updater.update_tickers(tickers)
        summary = updater.get_summary()
        print("\nDatabase Summary:")
        print(summary)

        # Step 2: Fetch price history for all tickers with data
        tickers_df = conn.execute("""
            WITH latest AS (
                SELECT MAX(date) AS max_date
                FROM stock_prices
            )
            SELECT DISTINCT ticker
            FROM stock_prices, latest
            GROUP BY ticker, latest.max_date
            HAVING MAX(date) = latest.max_date
        """).fetchdf()
        
        
    
    try:
        available_tickers = tickers_df['ticker'].tolist()

        df = con.execute("""
            SELECT *
            FROM stock_prices
            WHERE ticker IN ({})
            AND date >= CURRENT_DATE - INTERVAL 200 DAY
            ORDER BY ticker, date
        """.format(",".join([f"'{t}'" for t in available_tickers]))).fetchdf()

        # Step 3: Calculate technical indicators
        df_analysis = df.groupby("ticker", group_keys=False).apply(run_analysis).reset_index(drop=True)

        # Step 4: Store latest signals
        max_date = con.execute("SELECT MAX(date) FROM stock_prices").fetchone()[0]
        df_latest = df_analysis[
            pd.to_datetime(df_analysis['date']).dt.date == max_date
        ]

        signals_to_store = df_latest[[
            'date',
            'ticker',
            'close',
            'RSI_14',
            'MACD',
            'MACD_Signal',
            'SMA_20',
            'Combined_Signal',
            'Trade_Signal'
        ]]

        con.execute("""
        INSERT OR REPLACE INTO technical_signals
        SELECT
            date,
            ticker,
            close,
            RSI_14,
            MACD,
            MACD_Signal,
            SMA_20,
            Combined_Signal,
            Trade_Signal
        FROM signals_to_store
        """)

        # Step 5: Fetch top signals with sector fundamentals for AI briefing
        top_stocks_df = fetch_top_signals_with_sector(con, limit=15)

        # Save CSV for attachment
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        downloads_path = config.downloads_path
        filename = f"top10_{timestamp}.csv"
        filepath = os.path.join(downloads_path, filename)

        top_stocks_df.to_csv(filepath, index=False)
        print(f"Saved top signals to {filepath}")

        # Step 6: Generate AI briefing and subject
        my_bot = MyStockBot()
        briefing, subject = generate_briefing(my_bot, top_stocks_df)

        # Step 7: Send email
        send_email(
            subject=subject,
            body=briefing,
            sender=config.email_sender,
            receiver=config.email_receiver,
            password=config.email_password,
            attachment_path=filepath
        )

    finally:
        con.close()