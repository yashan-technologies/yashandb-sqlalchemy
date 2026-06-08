from __future__ import annotations

import datetime as dt

from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Time
from sqlalchemy import insert
from sqlalchemy import literal
from sqlalchemy import select
from sqlalchemy.testing import fixtures

from test._project_test_helpers import cleanup_objects


class TestProjectTypes(fixtures.TestBase):
    __backend__ = True

    def test_date_datetime_time_literal_processors(self, engine):
        with engine.connect() as connection:
            date_value = dt.date(2026, 6, 3)
            datetime_value = dt.datetime(2026, 6, 3, 10, 30, 45)
            time_value = dt.time(10, 30, 45, 123)

            assert connection.scalar(select(literal(date_value))) == date_value
            assert connection.scalar(select(literal(datetime_value))) == datetime_value
            assert connection.scalar(select(literal(time_value))) == time_value

    def test_basic_type_round_trips(self, engine):
        table_name = "pc_type_roundtrip"
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("date_value", Date),
            Column("datetime_value", DateTime),
            Column("time_value", Time),
            Column("float_value", Float),
        )

        with engine.connect() as connection:
            cleanup_objects(connection, table_name)
            metadata.create_all(connection)

            values = {
                "id": 1,
                "date_value": dt.date(2026, 6, 3),
                "datetime_value": dt.datetime(2026, 6, 3, 10, 30, 45),
                "time_value": dt.time(10, 30, 45, 123),
                "float_value": 12.5,
            }
            connection.execute(insert(table), values)

            row = connection.execute(select(table)).one()
            assert row.date_value == values["date_value"]
            assert row.datetime_value == values["datetime_value"]
            assert row.time_value == values["time_value"]
            assert abs(row.float_value - values["float_value"]) < 0.000001

            metadata.drop_all(connection)
            cleanup_objects(connection, table_name)
