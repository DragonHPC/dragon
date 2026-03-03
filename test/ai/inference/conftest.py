"""
Pytest configuration and fixtures for unit tests.

This module sets up Dragon multiprocessing for tests.
"""

import dragon
import multiprocessing as mp


def pytest_configure(config):
    """Set the multiprocessing start method to dragon at pytest startup."""
    try:
        mp.set_start_method("dragon", force=True)
    except RuntimeError:
        # Start method may already be set
        pass
