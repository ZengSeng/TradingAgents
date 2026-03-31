from dagster import Definitions
from .assets import nvda_decision

defs = Definitions(assets=[nvda_decision])