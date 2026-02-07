from __future__ import annotations

import hashlib
import importlib.util
import subprocess
import sys
from pathlib import Path

def h16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def run_as_script() -> tuple[int, str, str]:
    p = subprocess.run(
        [sys.executable, "main.py"],
        capture_output=True,
        text=True,
    )
    out = (p.stdout or "").strip().replace("\r\n", "\n")
    err = (p.stderr or "").strip().replace("\r\n", "\n")
    return p.returncode, out, err

def run_main_func_if_exists() -> tuple[int, str, str]:
    """
    Intenta cargar main.py como módulo y llamar a main() si existe.
    Si no existe, devuelve (None) y el caller cae al modo script.
    """
    path = Path("main.py").resolve()
    spec = importlib.util.spec_from_file_location("_santiloto_main", path)
    if spec is None or spec.loader is None:
        return 999, "", "Could not load main.py as module"

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]

    fn = getattr(mod, "main", None)
    if callable(fn):
        # Captura stdout si main imprime; si main devuelve texto, lo recogemos.
        # Para no complicarlo, preferimos el modo script como fuente de verdad.
        try:
            ret = fn()
            return 0, str(ret) if isinstance(ret, str) else "", ""
        except SystemExit as e:
            code = int(e.code) if isinstance(e.code, int) else 0
            return code, "", ""
    return 998, "", "No main() callable found"

def main() -> int:
    # Fuente de verdad: ejecución como script (respeta __main__)
    code, out, err = run_as_script()

    print("mode: script")
    print("exit:", code)
    print("stdout_chars:", len(out))
    print("stdout_hash:", h16(out))
    if out:
        print("stdout_head:")
        for line in out.splitlines()[:12]:
            print(line)

    if err:
        print("stderr_chars:", len(err))
        print("stderr_head:")
        for line in err.splitlines()[:20]:
            print(line)

    return 0 if code == 0 else code

if __name__ == "__main__":
    raise SystemExit(main())

