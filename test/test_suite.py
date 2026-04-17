# from sqlalchemy.testing.suite import *
# from sqlalchemy.testing.suite.test_ddl import *
# from sqlalchemy.testing.suite.test_select import *
from sqlalchemy.testing.suite.test_insert import *


from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import LastrowidTest as _LastrowidTest
from sqlalchemy.testing.suite import ReturningTest as _ReturningTest

from sqlalchemy.testing.schema import Column as _Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import _truncate_name
from sqlalchemy.testing import config
from sqlalchemy.testing.config import requirements

from sqlalchemy.schema import ForeignKey
from sqlalchemy.schema import Sequence
from sqlalchemy import event
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import literal
from sqlalchemy import literal_column


def Column(*args, **kw):
    """A schema.Column wrapper/hook for yasdb-specific tweaks."""

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


class ReturningTest(_ReturningTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "autoinc_pk",
            metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("data", String(50)),
        )
