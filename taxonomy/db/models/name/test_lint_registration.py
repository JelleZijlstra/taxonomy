"""Check registration in isolation from tests that import individual lint modules."""

import os
import subprocess
import sys


def test_all_lint_modules_are_registered() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib
import pkgutil

from taxonomy.db.models.name import lint

registered = {wrapper.label for wrapper in (*lint.LINT.linters, *lint.LINT.disabled_linters)}
for module in pkgutil.iter_modules(lint.__path__, prefix=lint.__name__ + "."):
    importlib.import_module(module.name)
all_labels = {wrapper.label for wrapper in (*lint.LINT.linters, *lint.LINT.disabled_linters)}
assert registered == all_labels, f"Unregistered lint checks: {sorted(all_labels - registered)}"
""",
        ],
        env={**os.environ, "CLIRM_READONLY": "1"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
