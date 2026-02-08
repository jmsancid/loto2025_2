"""
API pública para la clasificación semanal y la generación de apuestas.
La implementación interna se realiza en other_utils.weekly.*
"""

from __future__ import annotations

from other_utils.weekly.types import WeeklyResult
from other_utils.weekly.format import format_weekly_result  # Se importa aquí, pero se usa en main
from other_utils.weekly.engine import compute_weekly_apuestas  # Se importa aquí, pero se usa en main
