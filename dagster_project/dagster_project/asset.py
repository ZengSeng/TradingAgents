from dagster import asset
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.default_config import DEFAULT_CONFIG

@asset
def nvda_decision():
    ta = TradingAgentsGraph(debug=True, config=DEFAULT_CONFIG.copy())
    _, decision = ta.propagate("NVDA", "2026-01-15")
    return decision