"""
Configuration management for the stock analysis pipeline.
"""
import os
from typing import Set, Optional
from dataclasses import dataclass

@dataclass
class PipelineConfig:
    """Configuration settings for the stock analysis pipeline."""

    # Database settings
    db_path: str = os.getenv("DB_PATH", "data/duckdb/yfinance.duckdb")
    db_path_test: str = os.getenv("DB_PATH", "../../../data/duckdb/yfinance.duckdb")

    # Watch list settings
    watch_list: Set[str] = None
    max_watch_list_size: int = int(os.getenv("MAX_WATCH_LIST_SIZE", "10"))

    # File paths
    downloads_path: str = os.getenv("DOWNLOADS_PATH", "C:/Users/Zengseng/Downloads/")

    # Analysis settings
    analysis_lookback_days: int = int(os.getenv("ANALYSIS_LOOKBACK_DAYS", "200"))
    top_stocks_limit: int = int(os.getenv("TOP_STOCKS_LIMIT", "20"))
    final_stocks_limit: int = int(os.getenv("FINAL_STOCKS_LIMIT", "15"))

    # Signal thresholds
    price_targets_over_median_threshold: float = float(os.getenv("PRICE_TARGETS_OVER_MEDIAN_THRESHOLD", "15"))
    price_targets_over_median_field: str = os.getenv("PRICE_TARGETS_OVER_MEDIAN_FIELD", "price_targets_overMedian")
    recommendations_strong_buy_threshold: int = int(os.getenv("RECOMMENDATIONS_STRONG_BUY_THRESHOLD", "5"))
    combined_signal_buy_threshold: float = float(os.getenv("COMBINED_SIGNAL_BUY_THRESHOLD", "0.3"))
    combined_signal_sell_threshold: float = float(os.getenv("COMBINED_SIGNAL_SELL_THRESHOLD", "-0.3"))

    # Ollama settings
    ollama_url: str = os.getenv("OLLAMA_URL", "http://localhost:11434")
    ollama_model: str = os.getenv("OLLAMA_MODEL", "deepseek-r1:7b")
    ollama_timeout: int = int(os.getenv("OLLAMA_TIMEOUT", "480"))
    ollama_max_retries: int = int(os.getenv("OLLAMA_MAX_RETRIES", "3"))

    # Email settings
    email_sender: str = os.getenv("EMAIL", "")
    email_receiver: str = os.getenv("EMAIL", "")
    email_password: str = os.getenv("PASSWORD", "")

    # Monitoring settings
    enable_monitoring: bool = os.getenv("ENABLE_MONITORING", "false").lower() == "true"
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    def __post_init__(self):
        """Initialize default watch list if not provided."""
        if self.watch_list is None:
            # Default to empty set - can be populated via environment or code
            default_watch = os.getenv("WATCH_LIST_TICKERS", "")
            if default_watch:
                self.watch_list = set(ticker.strip().upper() for ticker in default_watch.split(",") if ticker.strip())
            else:
                # Default watch list from original code
                self.watch_list = {"RKLB", "GE"}

        # Ensure watch list doesn't exceed max size (for phased rollout)
        if len(self.watch_list) > self.max_watch_list_size:
            # Keep only the first N stocks for phased approach
            self.watch_list = set(list(self.watch_list)[:self.max_watch_list_size])

# Global config instance
config = PipelineConfig()