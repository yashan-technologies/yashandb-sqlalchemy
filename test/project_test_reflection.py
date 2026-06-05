from __future__ import annotations

import pytest
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import UniqueConstraint
from sqlalchemy import inspect
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.testing import fixtures

from test._project_test_helpers import cleanup_objects


class TestProjectReflection(fixtures.TestBase):
    __backend__ = True

    def test_single_object_reflection_contracts(self, engine):
        metadata = MetaData()
        users = Table(
            "pc_ref_users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(30), nullable=False),
            Column("email", String(50)),
            UniqueConstraint("email", name="pc_ref_users_email_uq"),
        )
        orders = Table(
            "pc_ref_orders",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer, ForeignKey("pc_ref_users.id")),
            Column("code", String(30)),
        )
        Index("pc_ref_orders_code_idx", orders.c.code, unique=True)

        with engine.connect() as connection:
            cleanup_objects(
                connection,
                "pc_ref_orders_v",
                "pc_ref_orders",
                "pc_ref_users",
            )
            metadata.create_all(connection)
            connection.exec_driver_sql(
                "create view pc_ref_orders_v as "
                "select id, user_id, code from pc_ref_orders"
            )

            inspector = inspect(connection)

            columns = inspector.get_columns("pc_ref_users")
            assert [column["name"] for column in columns] == ["id", "name", "email"]

            pk = inspector.get_pk_constraint("pc_ref_users")
            assert pk["constrained_columns"] == ["id"]

            fks = inspector.get_foreign_keys("pc_ref_orders")
            assert len(fks) == 1
            assert fks[0]["constrained_columns"] == ["user_id"]
            assert fks[0]["referred_table"] == "pc_ref_users"
            assert fks[0]["referred_columns"] == ["id"]

            indexes = inspector.get_indexes("pc_ref_orders")
            code_index = next(
                index for index in indexes if index["name"] == "pc_ref_orders_code_idx"
            )
            assert code_index["column_names"] == ["code"]
            assert code_index["unique"] is True

            unique_constraints = inspector.get_unique_constraints("pc_ref_users")
            assert any(
                constraint["name"] == "pc_ref_users_email_uq"
                and constraint["column_names"] == ["email"]
                for constraint in unique_constraints
            )

            assert "pc_ref_orders_v" in inspector.get_view_names()
            assert "pc_ref_orders" in inspector.get_view_definition("pc_ref_orders_v")

            with pytest.raises(NoSuchTableError):
                inspector.get_columns("pc_ref_missing")
            with pytest.raises(NoSuchTableError):
                inspector.get_view_definition("pc_ref_missing_v")

            connection.exec_driver_sql("drop view pc_ref_orders_v")
            metadata.drop_all(connection)
            cleanup_objects(
                connection,
                "pc_ref_orders_v",
                "pc_ref_orders",
                "pc_ref_users",
            )

    def test_get_multi_columns_and_table_options(self, engine):
        table_name = "pc_multi_cols"
        metadata = MetaData()
        Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(30)),
        )

        with engine.connect() as connection:
            cleanup_objects(connection, table_name)
            metadata.create_all(connection)
            inspector = inspect(connection)

            single = inspector.get_columns(table_name)
            multi = inspector.get_multi_columns(filter_names=[table_name])
            assert (None, table_name) in multi
            assert [c["name"] for c in multi[(None, table_name)]] == [
                c["name"] for c in single
            ]

            options = inspector.get_table_options(table_name)
            assert isinstance(options, dict)

            metadata.drop_all(connection)
            cleanup_objects(connection, table_name)
