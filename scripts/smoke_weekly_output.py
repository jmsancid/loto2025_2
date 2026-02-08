"""
Comandos bash para probar los cambios en la aplicación (ejecutar desde el repo)

export SANTILOTO_TODAY=2026-02-07
export SANTILOTO_FORECAST_FIXTURE_DIR=tests/fixtures/forecast
uv run python scripts/smoke_weekly_output.py

"""

from __future__ import annotations
import os
import hashlib
import subprocess
import sys

BEGIN = "===WEEKLY_RESULT_BEGIN==="
END = "===WEEKLY_RESULT_END==="

def h16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def extract_block(stdout: str) -> str:
    s = stdout.replace("\r\n", "\n")
    if BEGIN not in s or END not in s:
        raise RuntimeError("Weekly markers not found in stdout")
    block = s.split(BEGIN, 1)[1].split(END, 1)[0]
    return block.strip("\n").strip()

def main() -> int:
    os.environ["SANTILOTO_TODAY"] = "2026-02-07"
    os.environ["SANTILOTO_FREEZE_FORECAST"] = "1"
    p = subprocess.run([sys.executable, "main.py"], capture_output=True, text=True)
    out = p.stdout or ""
    err = p.stderr or ""

    if p.returncode != 0:
        print("exit:", p.returncode)
        print("stderr_head:\n", "\n".join(err.splitlines()[:40]))
        return p.returncode

    weekly = extract_block(out)
    print("weekly_chars:", len(weekly))
    print("weekly_hash:", h16(weekly))
    print("weekly_head:")
    for line in weekly.splitlines()[:12]:
        print(line)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
