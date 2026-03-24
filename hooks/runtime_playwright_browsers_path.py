# Script Control:
# - Role: Runtime hook to resolve Playwright browser binaries.
# - Track file: docs/SCRIPT_CONTROL.md
# Runtime hook: resolve Playwright browser path in frozen/dev modes.
import os
import sys
from pathlib import Path

try:
    if getattr(sys, "frozen", False):
        base = Path(getattr(sys, "_MEIPASS", Path.cwd()))
        existing = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
        if existing and Path(existing).exists():
            pass
        else:
            candidates = [
                base / "_internal" / "ms-playwright",
                base / "ms-playwright",
                base / "_internal" / "_internal" / "ms-playwright",
                base / "_internal" / ".local-browsers",
                base / "_internal" / "playwright" / "driver" / "package" / ".local-browsers",
                base / "_internal" / "_internal" / ".local-browsers",
            ]
            for p in candidates:
                if p.exists():
                    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(p)
                    break
            else:
                os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")
    else:
        local = os.environ.get("LOCALAPPDATA")
        if not local:
            local = str(Path.home() / "AppData" / "Local")
        ms_pw = Path(local) / "ms-playwright"
        if ms_pw.exists():
            os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(ms_pw))
        else:
            os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")
except Exception:
    pass

