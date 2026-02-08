"""
LEGACY compatibility module.

Do not add new code here. Use `other_utils.weekly` instead.
"""

from __future__ import annotations
import warnings

from other_utils.weekly import compute_weekly_apuestas as _compute, format_weekly_result as _format

def compute_weekly_apuestas(*args, **kwargs):
    warnings.warn(
        "other_utils.ranking_semanal is legacy; use other_utils.weekly.generate_weekly instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _compute(*args, **kwargs)

def format_weekly_result(*args, **kwargs):
    warnings.warn(
        "other_utils.ranking_semanal is legacy; use other_utils.weekly.format_weekly instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _format(*args, **kwargs)

__all__ = ["compute_weekly_apuestas", "format_weekly_result"]
