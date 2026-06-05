#!/usr/bin/env python
"""Run project-owned regression tests without shell glob or PS execution policy."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

DEFAULT_DBURI = "yashandb+yaspy://MY_TEST001:123456@172.16.90.87:1688/test"

PROJECT_TEST_FILES = (
    "test/project_test_sqlalchemy20_compat.py",
    "test/project_test_compile.py",
    "test/project_test_reflection.py",
    "test/project_test_types.py",
    "test/project_test_returning.py",
    "test/project_test_update_delete_returning.py",
    "test/project_test_known_limits.py",
    "test/project_test_orm_smoke.py",
)


def _parse_dburi(argv: list[str]) -> tuple[str, list[str]]:
    dburi = os.environ.get("YASHANDB_URL", DEFAULT_DBURI)
    rest: list[str] = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in ("--dburi", "-DbUri") and i + 1 < len(argv):
            dburi = argv[i + 1]
            i += 2
            continue
        if arg.startswith("--dburi="):
            dburi = arg.split("=", 1)[1]
            i += 1
            continue
        rest.append(arg)
        i += 1
    return dburi, rest


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    root = Path(__file__).resolve().parent.parent
    os.chdir(root)

    dburi, pytest_extra = _parse_dburi(argv)
    test_paths = [str(root / name) for name in PROJECT_TEST_FILES]
    command = [
        sys.executable,
        "-m",
        "pytest",
        *test_paths,
        "--dburi",
        dburi,
        *pytest_extra,
    ]
    return subprocess.call(command)


if __name__ == "__main__":
    raise SystemExit(main())
