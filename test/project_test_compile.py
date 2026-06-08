from __future__ import annotations

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import delete
from sqlalchemy import insert
from sqlalchemy import update
from sqlalchemy.schema import CreateTable
from sqlalchemy.testing import fixtures


class TestSQLAlchemy20CompileSmoke(fixtures.TestBase):
    """Non-connected compilation checks for SQLAlchemy 2.0 dialect hooks."""

    __backend__ = True

    def _assert_dialect_compiles(self, dialect_cls):
        metadata = MetaData()
        table = Table(
            "pc_compile_smoke",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(30)),
        )
        dialect = dialect_cls()

        ddl = str(CreateTable(table).compile(dialect=dialect))
        assert "CREATE TABLE" in ddl.upper()
        assert "PC_COMPILE_SMOKE" in ddl.upper()

        returning_sql = str(
            insert(table)
            .values(id=1, name="smoke")
            .returning(table.c.id, table.c.name)
            .compile(dialect=dialect)
        )
        assert "RETURNING" in returning_sql.upper()
        assert "INTO" in returning_sql.upper()

        update_sql = str(
            update(table)
            .where(table.c.id == 1)
            .values(name="upd")
            .returning(table.c.id, table.c.name)
            .compile(dialect=dialect)
        )
        assert "UPDATE" in update_sql.upper()
        assert "RETURNING" in update_sql.upper()
        assert "INTO" in update_sql.upper()

        delete_sql = str(
            delete(table)
            .where(table.c.id == 1)
            .returning(table.c.id, table.c.name)
            .compile(dialect=dialect)
        )
        assert "DELETE" in delete_sql.upper()
        assert "RETURNING" in delete_sql.upper()
        assert "INTO" in delete_sql.upper()

    def test_yaspy_dialect_compiles_ddl_and_insert_returning(self):
        from yashandb_sqlalchemy.yaspy import YasDialect_yaspy

        self._assert_dialect_compiles(YasDialect_yaspy)
