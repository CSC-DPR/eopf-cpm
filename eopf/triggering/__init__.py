"""eopf.triggering module simplify the integration of processing units
with the most widespread processing orchestration systems
(Spring Cloud Data Flow, Apache Airflow, Zeebee, Apache Beam ...).
"""
from .cli import EOCLITrigger
from .event import EOEventTrigger
from .web import EOWebTrigger

__all__ = ["EOCLITrigger", "EOEventTrigger", "EOWebTrigger"]
