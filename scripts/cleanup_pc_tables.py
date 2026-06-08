#!/usr/bin/env python
"""Drop leftover project-test tables (PC_% prefix) from the connected schema."""

from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine, text

DEFAULT_URL = "yashandb+yaspy://MY_TEST001:123456@172.16.90.87:1688/test"


def main() -> int:
    url = os.environ.get("YASHANDB_URL", DEFAULT_URL)
    if len(sys.argv) > 1:
        url = sys.argv[1]
    engine = create_engine(url)
    with engine.connect() as conn:
        names = conn.execute(
            text(
                "select table_name from user_tables "
                "where table_name like 'PC_%'"
            )
        ).scalars()
        for name in names:
            try:
                conn.exec_driver_sql(f"drop table {name} purge")
                conn.commit()
                print(f"dropped {name}")
            except Exception as exc:
                conn.rollback()
                print(f"skip {name}: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
