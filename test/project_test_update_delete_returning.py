from __future__ import annotations

import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import delete
from sqlalchemy import insert
from sqlalchemy import update
from sqlalchemy.exc import DatabaseError
from sqlalchemy.testing import fixtures

from test._project_test_helpers import cleanup_objects


class TestProjectUpdateDeleteReturning(fixtures.TestBase):
    """YashanDB 23.4.12.1+: UPDATE RETURNING via yaspy; DELETE RETURNING still rejected."""

    __backend__ = True

    _TABLE_PREFIX = "pc_ret_"

    def _define_table(self, metadata: MetaData, table_name: str) -> Table:
        return Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
        )

    def test_update_returning_round_trip(self, engine):
        table_name = f"{self._TABLE_PREFIX}upd_rt"
        metadata = MetaData()
        table = self._define_table(metadata, table_name)

        with engine.connect() as connection:
            cleanup_objects(connection, table_name)
            metadata.create_all(connection)
            connection.execute(insert(table), {"id": 1, "data": "d1"})

            result = connection.execute(
                update(table)
                .where(table.c.id == 1)
                .values(data="d1_new")
                .returning(table.c.id, table.c.data)
            )
            assert result.all() == [(1, "d1_new")]

            row = connection.execute(
                table.select().where(table.c.id == 1)
            ).one()
            assert row == (1, "d1_new")

            metadata.drop_all(connection)
            cleanup_objects(connection, table_name)

    def test_delete_returning_rejected_by_database(self, engine):
        table_name = f"{self._TABLE_PREFIX}del_reject"
        metadata = MetaData()
        table = self._define_table(metadata, table_name)

        with engine.connect() as connection:
            cleanup_objects(connection, table_name)
            metadata.create_all(connection)
            connection.execute(insert(table), {"id": 1, "data": "d1"})

            with pytest.raises(DatabaseError, match="YAS-04209|RETURNING"):
                connection.execute(
                    delete(table)
                    .where(table.c.id == 1)
                    .returning(table.c.id, table.c.data)
                )

            row = connection.execute(
                table.select().where(table.c.id == 1)
            ).one()
            assert row == (1, "d1")

            metadata.drop_all(connection)
            cleanup_objects(connection, table_name)

    def test_update_returning_no_matching_rows(self, engine):
        table_name = f"{self._TABLE_PREFIX}upd_nomatch"
        metadata = MetaData()
        table = self._define_table(metadata, table_name)

        with engine.connect() as connection:
            cleanup_objects(connection, table_name)
            metadata.create_all(connection)
            connection.execute(insert(table), {"id": 1, "data": "d1"})

            result = connection.execute(
                update(table)
                .where(table.c.id == 99)
                .values(data="ignored")
                .returning(table.c.id, table.c.data)
            )
            assert result.all() == []

            row = connection.execute(
                table.select().where(table.c.id == 1)
            ).one()
            assert row == (1, "d1")

            metadata.drop_all(connection)
            cleanup_objects(connection, table_name)

    def test_update_without_returning_still_works(self, engine):
        table_name = f"{self._TABLE_PREFIX}upd_ok"
        metadata = MetaData()
        table = self._define_table(metadata, table_name)

        with engine.connect() as connection:
            cleanup_objects(connection, table_name)
            metadata.create_all(connection)
            connection.execute(insert(table), {"id": 1, "data": "d1"})

            result = connection.execute(
                update(table).where(table.c.id == 1).values(data="d1_new")
            )
            assert result.rowcount == 1

            row = connection.execute(
                table.select().where(table.c.id == 1)
            ).one()
            assert row == (1, "d1_new")

            metadata.drop_all(connection)
            cleanup_objects(connection, table_name)
