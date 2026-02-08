"""
Public API for the `other_utils.weekly` package.

This module defines the *stable* entry points used by the rest of the project.
Internal modules (`engine`, `forecast`, `format`, etc.) are considered private
implementation details and may change without notice.

Public (stable):
- generate_weekly
- format_weekly
- WeeklyResult (and selected domain types)

Compat (temporary aliases, will be deprecated):
- compute_weekly_apuestas
- format_weekly_result
"""

from __future__ import annotations

# Import only from leaf modules to avoid circular imports.
from .engine import compute_weekly_apuestas as generate_weekly
from .format import format_weekly_result as format_weekly
from .types import WeeklyResult

# --- Backward-compatible aliases (temporary) ---
# Keep old names available for callers that haven't migrated yet.
compute_weekly_apuestas = generate_weekly
format_weekly_result = format_weekly

__all__ = [
    # stable
    "generate_weekly",
    "format_weekly",
    "WeeklyResult",
    # compat
    "compute_weekly_apuestas",
    "format_weekly_result",
]
