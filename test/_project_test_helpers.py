from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.testing import config


DEFAULT_URL = "yashandb+yaspy://MY_TEST001:123456@172.16.90.87:1688/test"


def db_url() -> str:
    return os.environ.get("YASHANDB_URL", DEFAULT_URL)


@pytest.fixture()
def engine():
    if config.db is not None:
        yield config.db
        return

    engine = create_engine(db_url())
    try:
        yield engine
    finally:
        engine.dispose()


def exec_ignore(connection, statement: str) -> None:
    try:
        connection.exec_driver_sql(statement)
        connection.commit()
    except Exception:
        connection.rollback()


def cleanup_objects(connection, *names: str) -> None:
    for name in names:
        exec_ignore(connection, f"drop view {name}")
    for name in names:
        exec_ignore(connection, f"drop table {name} cascade constraints")
    for name in names:
        exec_ignore(connection, f"drop sequence {name}")


def assert_no_project_objects(connection, prefix: str = "PC_") -> None:
    table_count = connection.execute(
        text("select count(*) from user_tables where table_name like :prefix"),
        {"prefix": f"{prefix}%"},
    ).scalar()
    view_count = connection.execute(
        text("select count(*) from user_views where view_name like :prefix"),
        {"prefix": f"{prefix}%"},
    ).scalar()
    sequence_count = connection.execute(
        text("select count(*) from user_sequences where sequence_name like :prefix"),
        {"prefix": f"{prefix}%"},
    ).scalar()
    assert table_count == 0
    assert view_count == 0
    assert sequence_count == 0
