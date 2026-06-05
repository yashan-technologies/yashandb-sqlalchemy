from __future__ import annotations

from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import inspect
from sqlalchemy.engine import interfaces
from sqlalchemy.testing import fixtures

from test._project_test_helpers import cleanup_objects


class TestSQLAlchemy20Compat(fixtures.TestBase):
    __backend__ = True

    def _assert_sqlalchemy20_entrypoints(self, dialect_cls):
        assert callable(dialect_cls.import_dbapi)
        assert dialect_cls.bind_typing is interfaces.BindTyping.SETINPUTSIZES
        assert getattr(dialect_cls, "type_compiler_cls").__name__ == "YasTypeCompiler"

    def test_yaspy_dialect_exposes_sqlalchemy20_entrypoints(self):
        from yashandb_sqlalchemy.yaspy import YasDialect_yaspy

        self._assert_sqlalchemy20_entrypoints(YasDialect_yaspy)

    def test_inspector_has_table_uses_info_cache(self, engine):
        table_name = "pc_sa20_cache"
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(20)),
        )

        with engine.connect() as connection:
            cleanup_objects(connection, table_name)
            inspector = inspect(connection)

            assert inspector.has_table(table_name) is False
            table.create(connection)

            assert inspector.has_table(table_name) is False
            inspector.clear_cache()
            assert inspector.has_table(table_name) is True

            table.drop(connection)
            inspector.clear_cache()
            assert inspector.has_table(table_name) is False
            cleanup_objects(connection, table_name)

    def test_inspector_has_sequence_accepts_info_cache(self, engine):
        sequence_name = "pc_sa20_seq"
        sequence = Sequence(sequence_name)

        with engine.connect() as connection:
            cleanup_objects(connection, sequence_name)
            inspector = inspect(connection)

            assert inspector.has_sequence(sequence_name) is False
            sequence.create(connection)

            assert inspector.has_sequence(sequence_name) is False
            inspector.clear_cache()
            assert inspector.has_sequence(sequence_name) is True

            sequence.drop(connection)
            inspector.clear_cache()
            assert inspector.has_sequence(sequence_name) is False
            cleanup_objects(connection, sequence_name)

    def test_inspector_has_table_detects_view(self, engine):
        view_name = "pc_sa20_view"
        table_name = "pc_sa20_view_base"
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(20)),
        )

        with engine.connect() as connection:
            cleanup_objects(connection, view_name, table_name)
            inspector = inspect(connection)
            assert inspector.has_table(view_name) is False

            table.create(connection)
            connection.exec_driver_sql(
                f"create view {view_name} as select id, data from {table_name}"
            )
            inspector.clear_cache()
            assert inspector.has_table(view_name) is True
            assert inspector.has_table(table_name) is True

            connection.exec_driver_sql(f"drop view {view_name}")
            table.drop(connection)
            inspector.clear_cache()
            assert inspector.has_table(view_name) is False
            cleanup_objects(connection, view_name, table_name)

    def test_inspector_has_index_uses_info_cache(self, engine):
        table_name = "pc_sa20_idx"
        index_name = "pc_sa20_idx_name"
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("code", String(20)),
        )
        Index(index_name, table.c.code, unique=True)

        with engine.connect() as connection:
            cleanup_objects(connection, table_name)
            inspector = inspect(connection)

            assert inspector.has_index(table_name, index_name) is False
            metadata.create_all(connection)

            assert inspector.has_index(table_name, index_name) is False
            inspector.clear_cache()
            assert inspector.has_index(table_name, index_name) is True

            metadata.drop_all(connection)
            inspector.clear_cache()
            assert inspector.has_index(table_name, index_name) is False
            cleanup_objects(connection, table_name)
