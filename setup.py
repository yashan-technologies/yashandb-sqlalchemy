from setuptools import setup
import os
import re
import subprocess


def get_project_path():
    return os.path.dirname(os.path.abspath(__file__))


def get_version():
    """
    Return a PEP 440 compliant version derived from `git describe`, using .postN.

    Examples:
      - v1.1.2              -> 1.1.2
      - v1.1.2-0-gabcd123   -> 1.1.2
      - v1.1.2-3-gabcd123   -> 1.1.2.post3
    """
    repo_dir = get_project_path()

    def _run_git(args):
        try:
            return subprocess.check_output(
                ["git"] + args,
                cwd=repo_dir,
                stderr=subprocess.DEVNULL,
                text=True,
            ).strip()
        except Exception:
            return ""

    # Use a stable format when available
    desc = _run_git(["describe", "--tags", "--long", "--always"])
    if not desc:
        return "0.0.0"

    # Accept: v1.1.2-3-gxxxx / v1.1.2 / 1.1.2-3-gxxxx / 1.1.2
    m = re.match(r"^v?(\d+(?:\.\d+)*)(?:-(\d+)-g([0-9a-f]+))?$", desc)
    if not m:
        return "0.0.0"

    base = m.group(1)
    distance = m.group(2)

    # Exactly on tag (or tag-like)
    if distance is None or distance == "0":
        return base

    # Post-release version (no dev)
    return f"{base}.post{distance}"


setup(
    name="yashandb-sqlalchemy",
    version=get_version(),
    description="YashanDB Dialect for SQLAlchemy",
    author="CoD",
    author_email="cod@sics.ac.cn",
    url="https://www.yashandb.com/",
    license="MulanPSL-2.0",
    packages=["yashandb_sqlalchemy"],
    include_package_data=True,
    entry_points={
        "sqlalchemy.dialects": [
            "yashandb = yashandb_sqlalchemy.yaspy:YasDialect_yaspy",
            "yashandb.yaspy = yashandb_sqlalchemy.yaspy:YasDialect_yaspy",
            "yashandb.yasdb = yashandb_sqlalchemy.yasdb:YasDialect_yasdb",
        ]
    },
)
