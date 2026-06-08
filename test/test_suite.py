from sqlalchemy.testing.suite import *
from sqlalchemy.testing.suite.test_ddl import *
from sqlalchemy.testing.suite.test_select import *
from sqlalchemy.testing.suite.test_insert import *


from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import LastrowidTest as _LastrowidTest
from sqlalchemy.testing.suite import ReturningTest as _ReturningTest
from sqlalchemy.testing.suite.test_rowcount import RowCountTest as _RowCountTest
from sqlalchemy.testing.suite.test_types import BinaryTest as _SuiteBinaryTest
from sqlalchemy.testing.suite.test_types import TrueDivTest as _SuiteTrueDivTest
from sqlalchemy.testing.suite.test_types import UuidTest as _SuiteUuidTest
from sqlalchemy.testing.suite.test_reflection import BizarroCharacterTest as _SuiteBizarroCharacterTest
from sqlalchemy.testing.suite.test_reflection import ComponentReflectionTest as _SuiteComponentReflectionTest
from sqlalchemy.testing.suite.test_select import ExistsTest as _SuiteExistsTest
from sqlalchemy.testing.suite.test_select import IdentityAutoincrementTest as _SuiteIdentityAutoincrementTest
from sqlalchemy.testing.suite.test_select import PostCompileParamsTest as _SuitePostCompileParamsTest

import pytest

from sqlalchemy import testing

from sqlalchemy.testing.schema import Column as _Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import _truncate_name
from sqlalchemy.testing import config
from sqlalchemy.testing.config import requirements

from sqlalchemy.schema import ForeignKey
from sqlalchemy.schema import Sequence
from sqlalchemy import event
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy.testing import eq_
from sqlalchemy import literal
from sqlalchemy import literal_column


def Column(*args, **kw):
    """A schema.Column wrapper/hook for yashandb-specific tweaks."""

    test_opts = {k: kw.pop(k) for k in list(kw) if k.startswith("test_")}

    if not config.requirements.foreign_key_ddl.enabled_for_config(config):
        args = [arg for arg in args if not isinstance(arg, ForeignKey)]

    col = _Column(*args, **kw)
    if test_opts.get("test_needs_autoincrement", False) and kw.get(
        "primary_key", False
    ):

        if col.default is None and col.server_default is None:
            col.autoincrement = True

        # allow any test suite to pick up on this
        col.info["test_needs_autoincrement"] = True

        def add_seq(c, tbl):
            c._init_items(
                Sequence(
                    _truncate_name(config.db.dialect, tbl.name + "_" + c.name + "_seq"),
                    optional=True,
                )
            )

        event.listen(col, "after_parent_attach", add_seq, propagate=True)

    return col


class InsertBehaviorTest(_InsertBehaviorTest):

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "autoinc_pk",
            metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("data", String(50)),
        )
        Table(
            "manual_pk",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(50)),
        )
        Table(
            "no_implicit_returning",
            metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("data", String(50)),
            implicit_returning=False,
        )
        Table(
            "includes_defaults",
            metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("data", String(50)),
            Column("x", Integer, default=5),
            Column(
                "y",
                Integer,
                default=literal_column("2", type_=Integer) + literal(2),
            ),
        )


class LastrowidTest(_LastrowidTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "autoinc_pk",
            metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("data", String(50)),
        )

        Table(
            "manual_pk",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(50)),
        )


class RowCountTest(_RowCountTest):
    @testing.variation("implicit_returning", [True, False])
    @testing.variation(
        "dml",
        [
            ("update", testing.requires.update_returning),
            ("delete", testing.requires.delete_returning),
        ],
    )
    def test_update_delete_rowcount_return_defaults(
        self, connection, implicit_returning, dml
    ):
        if dml.update:
            config.skip_test(
                "YashanDB supports single-row UPDATE RETURNING only (YAS-05205)"
            )

        if implicit_returning:
            employees_table = self.tables.employees
        else:
            employees_table = Table(
                "employees",
                MetaData(),
                Column(
                    "employee_id",
                    Integer,
                    autoincrement=False,
                    primary_key=True,
                ),
                Column("name", String(50)),
                Column("department", String(1)),
                implicit_returning=False,
            )

        department = employees_table.c.department

        if dml.update:
            stmt = (
                employees_table.update()
                .where(department == "C")
                .values(name=employees_table.c.department + "Z")
                .return_defaults()
            )
        elif dml.delete:
            stmt = (
                employees_table.delete()
                .where(department == "C")
                .return_defaults()
            )
        else:
            dml.fail()

        r = connection.execute(stmt)
        eq_(r.rowcount, 3)


class ReturningTest(_ReturningTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "autoinc_pk",
            metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("data", String(50)),
        )

    @pytest.mark.skip(
        reason=(
            "SQLAlchemy 2.0 suite variant uses Identity(); current YashanDB "
            "mode uses sequence/trigger autoincrement instead"
        )
    )
    def test_insert_w_floats(self, *args, **kw):
        return super().test_insert_w_floats(*args, **kw)

    @pytest.mark.skip(
        reason=(
            "SQLAlchemy 2.0 suite variant uses Identity(); current YashanDB "
            "mode uses sequence/trigger autoincrement instead"
        )
    )
    def test_imv_returning_datatypes(self, *args, **kw):
        return super().test_imv_returning_datatypes(*args, **kw)


class BinaryTest(_SuiteBinaryTest):
    def test_pickle_roundtrip(self, connection):
        return super().test_pickle_roundtrip(connection)


class BizarroCharacterTest(_SuiteBizarroCharacterTest):
    @pytest.mark.skip(
        reason=(
            "Current YashanDB/yaspy combination can hang while reflecting "
            "foreign keys for special-character table/column names"
        )
    )
    def test_fk_ref(self, *args, **kw):
        return super().test_fk_ref(*args, **kw)


class ComponentReflectionTest(_SuiteComponentReflectionTest):
    @testing.combinations(
        (True, testing.requires.schemas),
        False,
        argnames="use_schema",
    )
    @testing.combinations(
        (True, testing.requires.views), False, argnames="views"
    )
    def test_metadata(self, connection, use_schema, views):
        m = MetaData()
        schema = config.test_schema if use_schema else None
        m.reflect(connection, schema=schema, views=views, resolve_fks=False)

        insp = inspect(connection)
        tables = insp.get_table_names(schema)
        if views:
            tables += insp.get_view_names(schema)
            try:
                tables += insp.get_materialized_view_names(schema)
            except NotImplementedError:
                pass
        if schema:
            tables = [f"{schema}.{t}" for t in tables]
        eq_(sorted(m.tables), sorted(tables))


class IdentityAutoincrementTest(_SuiteIdentityAutoincrementTest):
    @pytest.mark.skip(
        reason=(
            "current YashanDB mode does not support GENERATED AS IDENTITY; "
            "autoincrement is emulated with sequence/trigger for plain integer PKs"
        )
    )
    def test_autoincrement_with_identity(self, connection):
        return super().test_autoincrement_with_identity(connection)


class TrueDivTest(_SuiteTrueDivTest):
    @pytest.mark.skip(
        reason=(
            "YashanDB numeric division/floor division result coercion differs "
            "from SQLAlchemy suite's exact Python numeric expectations"
        )
    )
    def test_floordiv_integer(self, *args, **kw):
        return super().test_floordiv_integer(*args, **kw)

    @pytest.mark.skip(
        reason=(
            "YashanDB numeric division/floor division result coercion differs "
            "from SQLAlchemy suite's exact Python numeric expectations"
        )
    )
    def test_floordiv_integer_bound(self, *args, **kw):
        return super().test_floordiv_integer_bound(*args, **kw)

    @pytest.mark.skip(
        reason=(
            "yaspy returns floating point division with DB-native binary "
            "precision, so exact equality to 2.3 is not stable"
        )
    )
    def test_truediv_float(self, *args, **kw):
        return super().test_truediv_float(*args, **kw)


class UuidTest(_SuiteUuidTest):
    @pytest.mark.skip(
        reason=(
            "yaspy does not expose a DBAPI OUT parameter type for SQLAlchemy "
            "Uuid(), so UUID RETURNING is not supported"
        )
    )
    def test_uuid_returning(self, connection):
        return super().test_uuid_returning(connection)


class DifficultParametersTest(DifficultParametersTest):
    __requires__ = ("difficult_parameters",)


class ExistsTest(_SuiteExistsTest):
    @testing.requires.select_literal_binds
    def test_select_exists(self, connection):
        return super(ExistsTest, self).test_select_exists(connection)


class PostCompileParamsTest(_SuitePostCompileParamsTest):
    @testing.requires.assertsql_empty_parameters_tuple
    def test_execute(self):
        return super(PostCompileParamsTest, self).test_execute()

    @testing.requires.assertsql_empty_parameters_tuple
    def test_execute_expanding_plus_literal_execute(self):
        return super(PostCompileParamsTest, self).test_execute_expanding_plus_literal_execute()
