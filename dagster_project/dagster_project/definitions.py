from dagster import Definitions, load_assets_from_modules
from .assets import trading_agents, ingest_process

defs = Definitions(
    assets=load_assets_from_modules([
        trading_agents, 
        ingest_process,
    ])
)