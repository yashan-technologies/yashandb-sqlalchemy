from __future__ import annotations

import uuid

import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import insert
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.testing import fixtures
from sqlalchemy.types import Uuid

from test._project_test_helpers import cleanup_objects


class TestProjectReturning(fixtures.TestBase):
    __backend__ = True

    def test_insert_returning_round_trip(self, engine):
        table_name = "pc_returning"
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(30)),
        )

        with engine.connect() as connection:
            cleanup_objects(connection, table_name)
            metadata.create_all(connection)

            row = connection.execute(
                insert(table).returning(table.c.id, table.c.name),
                {"id": 1, "name": "returned"},
            ).one()

            assert row == (1, "returned")

            metadata.drop_all(connection)
            cleanup_objects(connection, table_name)

    def test_uuid_returning_reports_unsupported_out_parameter(self, engine):
        table_name = "pc_uuid_returning"
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("uuid_value", Uuid),
        )

        with engine.connect() as connection:
            cleanup_objects(connection, table_name)
            metadata.create_all(connection)

            with pytest.raises(InvalidRequestError, match="Uuid"):
                connection.execute(
                    insert(table).returning(table.c.uuid_value),
                    {"id": 1, "uuid_value": uuid.uuid4()},
                ).one()

            metadata.drop_all(connection)
            cleanup_objects(connection, table_name)
