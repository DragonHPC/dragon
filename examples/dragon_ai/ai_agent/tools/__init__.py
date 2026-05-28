"""Parallel Experiment tools package.

Separates tool implementations from the pipeline wiring so the main
pipeline script stays short and easy to understand.

Modules
-------
- ``worker``    — Monte Carlo worker function (launched by ProcessGroup)
- ``planner``   — propose_experiment tool (HITL feedback loop)
- ``runner``    — launch_experiment / check_progress / collect_results
- ``analyzer``  — analyze_convergence tool
- ``reporter``  — format_results_table tool
"""

from .planner import propose_experiment
from .runner import launch_experiment, check_progress, collect_results
from .analyzer import analyze_convergence
from .reporter import format_results_table
from .worker import monte_carlo_worker

__all__ = [
    "propose_experiment",
    "launch_experiment",
    "check_progress",
    "collect_results",
    "analyze_convergence",
    "format_results_table",
    "monte_carlo_worker",
]
