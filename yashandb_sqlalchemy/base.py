# base.py
# Copyright (C) 2005-2023 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This project (yashandb-sqlalchemy/yashandb_sqlalchemy) is licensed under
# Mulan PSL v2. See the repository root LICENSE file.
#
# This file contains and/or is derived from portions of SQLAlchemy, which is
# licensed under the MIT License. Upstream attribution is retained. See NOTICE.


from itertools import groupby
import re

from sqlalchemy import Computed
from sqlalchemy import exc
from sqlalchemy import schema as sa_schema
from sqlalchemy import sql
from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy.engine import reflection
from sqlalchemy import event
from sqlalchemy.sql import compiler
from sqlalchemy.sql import expression
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import visitors
from sqlalchemy.types import BLOB
from sqlalchemy.types import CHAR
from sqlalchemy.types import CLOB
from sqlalchemy.types import FLOAT
from sqlalchemy.types import INTEGER
from sqlalchemy.types import NCHAR
from sqlalchemy.types import NVARCHAR
from sqlalchemy.types import TIMESTAMP
from sqlalchemy.types import VARCHAR
from sqlalchemy.types import BOOLEAN
from sqlalchemy.types import SMALLINT
from sqlalchemy.types import BIGINT
from sqlalchemy.types import TIME
from sqlalchemy.util import compat

RESERVED_WORDS = set(
    "SHARE RAW DROP BETWEEN FROM DESC OPTION PRIOR LONG THEN "
    "DEFAULT ALTER IS INTO MINUS INTEGER NUMBER GRANT IDENTIFIED "
    "ALL TO ORDER ON FLOAT DATE HAVING CLUSTER NOWAIT RESOURCE "
    "ANY TABLE INDEX FOR UPDATE WHERE CHECK SMALLINT WITH DELETE "
    "BY ASC REVOKE LIKE SIZE RENAME NOCOMPRESS NULL GROUP VALUES "
    "AS IN VIEW EXCLUSIVE COMPRESS SYNONYM SELECT INSERT EXISTS "
    "NOT TRIGGER ELSE CREATE INTERSECT PCTFREE DISTINCT USER "
    "CONNECT SET MODE OF UNIQUE VARCHAR2 VARCHAR LOCK OR CHAR "
    "DECIMAL UNION PUBLIC AND START UID COMMENT CURRENT LEVEL".split()
)

NO_ARG_FNS = set(
    "UID CURRENT_DATE SYSDATE USER " "CURRENT_TIME CURRENT_TIMESTAMP".split()
)


class RAW(sqltypes._Binary):
    __visit_name__ = "RAW"


YasRaw = RAW


class NCLOB(sqltypes.Text):
    __visit_name__ = "NCLOB"


class VARCHAR2(VARCHAR):
    __visit_name__ = "VARCHAR2"


NVARCHAR2 = NVARCHAR


class NUMBER(sqltypes.Numeric, sqltypes.Integer):
    __visit_name__ = "NUMBER"

    def __init__(self, precision=None, scale=None, asdecimal=None):
        if asdecimal is None:
            asdecimal = bool(scale and scale > 0)

        super(NUMBER, self).__init__(
            precision=precision, scale=scale, asdecimal=asdecimal
        )

    def adapt(self, impltype):
        ret = super(NUMBER, self).adapt(impltype)
        return ret

    @property
    def _type_affinity(self):
        if bool(self.scale and self.scale > 0):
            return sqltypes.Numeric
        else:
            return sqltypes.Integer


class DOUBLE_PRECISION(sqltypes.Float):
    __visit_name__ = "DOUBLE_PRECISION"


class BINARY_DOUBLE(sqltypes.Float):
    __visit_name__ = "BINARY_DOUBLE"


class BINARY_FLOAT(sqltypes.Float):
    __visit_name__ = "BINARY_FLOAT"


class BFILE(sqltypes.LargeBinary):
    __visit_name__ = "BFILE"


class LONG(sqltypes.Text):
    __visit_name__ = "LONG"


class DATE(sqltypes.DateTime):

    __visit_name__ = "DATE"

    def _compare_type_affinity(self, other):
        return other._type_affinity in (sqltypes.DateTime, sqltypes.Date)


class INTERVAL(sqltypes.NativeForEmulated, sqltypes._AbstractInterval):
    __visit_name__ = "INTERVAL"

    def __init__(self, day_precision=None, second_precision=None):
        """Construct an INTERVAL.

        Note that only DAY TO SECOND intervals are currently supported.
        This is due to a lack of support for YEAR TO MONTH intervals
        within available DBAPIs.

        :param day_precision: the day precision value.  this is the number of
          digits to store for the day field.  Defaults to "2"
        :param second_precision: the second precision value.  this is the
          number of digits to store for the fractional seconds field.
          Defaults to "6".

        """
        self.day_precision = day_precision
        self.second_precision = second_precision

    @classmethod
    def _adapt_from_generic_interval(cls, interval):
        return INTERVAL(
            day_precision=interval.day_precision,
            second_precision=interval.second_precision,
        )

    @property
    def _type_affinity(self):
        return sqltypes.Interval

    def as_generic(self, allow_nulltype=False):
        return sqltypes.Interval(
            native=True,
            second_precision=self.second_precision,
            day_precision=self.day_precision,
        )

    def coerce_compared_value(self, op, value):
        return self


class ROWID(sqltypes.TypeEngine):

    __visit_name__ = "ROWID"


class TINYINT(sqltypes.TypeEngine):
    __visit_name__ = "TINYINT"


class _YasBoolean(sqltypes.Boolean):
    def get_dbapi_type(self, dbapi):
        return dbapi.NUMBER

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            # SQLAlchemy suite BooleanTest expects Python bool values.
            # Drivers may return NUMBER-ish values as strings ('0'/'1').
            if isinstance(value, bool):
                return value
            if isinstance(value, (int,)):
                return bool(value)
            if isinstance(value, (util.string_types, bytes)):
                s = value.decode("ascii") if isinstance(value, bytes) else value
                s = s.strip()
                if s in ("0", "N", "n", "false", "FALSE", "f", "F"):
                    return False
                if s in ("1", "Y", "y", "true", "TRUE", "t", "T"):
                    return True
            try:
                return bool(int(value))
            except Exception:
                return bool(value)

        return process


class _YasBigInteger(sqltypes.BigInteger):
    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            if isinstance(value, int):
                return value
            # yaspy / database may return NUMBER-ish values as Decimal / str.
            try:
                return int(value)
            except Exception:
                try:
                    return int(str(value).strip())
                except Exception:
                    return value

        return process


colspecs = {
    sqltypes.Boolean: _YasBoolean,
    sqltypes.Interval: INTERVAL,
    # Use TIMESTAMP for DateTime so time components round-trip correctly.
    sqltypes.DateTime: TIMESTAMP,
    sqltypes.BigInteger: _YasBigInteger,
}

ischema_names = {
    "VARCHAR": VARCHAR,
    "INTEGER": INTEGER,
    "VARCHAR2": VARCHAR,
    "NVARCHAR2": NVARCHAR,
    "CHAR": CHAR,
    "NCHAR": NCHAR,
    "DATE": DATE,
    "NUMBER": NUMBER,
    "BLOB": BLOB,
    "BFILE": BFILE,
    "CLOB": CLOB,
    "NCLOB": NCLOB,
    "TIMESTAMP": TIMESTAMP,
    "TIMESTAMP WITH TIME ZONE": TIMESTAMP,
    "INTERVAL DAY TO SECOND": INTERVAL,
    "RAW": RAW,
    "FLOAT": FLOAT,
    "DOUBLE PRECISION": DOUBLE_PRECISION,
    "LONG": LONG,
    "BINARY_DOUBLE": BINARY_DOUBLE,
    "BINARY_FLOAT": BINARY_FLOAT,
    "ROWID": ROWID,
    "BOOLEAN": BOOLEAN,
    "SMALLINT": SMALLINT,
    "TINYINT": TINYINT,
    "BIGINT": BIGINT,
    "DOUBLE": BINARY_DOUBLE,
    "TIME": TIME,
}


class YasTypeCompiler(compiler.GenericTypeCompiler):

    def visit_datetime(self, type_, **kw):
        # SQLAlchemy DateTime should preserve time (and microseconds). Map to
        # TIMESTAMP rather than DATE which would truncate the time portion.
        if getattr(type_, "timezone", False):
            return "TIMESTAMP WITH TIME ZONE"
        return "TIMESTAMP"

    def visit_float(self, type_, **kw):
        # When SQLAlchemy Float is used with asdecimal=True (e.g. the suite's
        # Float(decimal_return_scale=N, asdecimal=True)), store as NUMBER so
        # values round-trip with exact decimal scale rather than binary float
        # rounding artifacts.
        if getattr(type_, "asdecimal", False):
            scale = getattr(type_, "decimal_return_scale", None)
            if scale is None:
                scale = getattr(type_, "_effective_decimal_return_scale", None)
            if scale is not None:
                return "NUMBER(38, %d)" % int(scale)
            return "NUMBER"
        return self.visit_FLOAT(type_, **kw)

    def visit_unicode(self, type_, **kw):
        if self.dialect._use_nchar_for_unicode:
            return self.visit_NVARCHAR2(type_, **kw)
        else:
            return self.visit_VARCHAR2(type_, **kw)

    def visit_INTERVAL(self, type_, **kw):
        return "INTERVAL DAY%s TO SECOND%s" % (
            type_.day_precision is not None and "(%d)" % type_.day_precision or "",
            type_.second_precision is not None
            and "(%d)" % type_.second_precision
            or "",
        )

    def visit_LONG(self, type_, **kw):
        return "LONG"

    def visit_TIMESTAMP(self, type_, **kw):
        if type_.timezone:
            return "TIMESTAMP WITH TIME ZONE"
        else:
            return "TIMESTAMP"

    def visit_DOUBLE_PRECISION(self, type_, **kw):
        return self._generate_numeric(type_, "DOUBLE PRECISION", **kw)

    def visit_BINARY_DOUBLE(self, type_, **kw):
        return self._generate_numeric(type_, "BINARY_DOUBLE", **kw)

    def visit_BINARY_FLOAT(self, type_, **kw):
        return self._generate_numeric(type_, "BINARY_FLOAT", **kw)

    def visit_FLOAT(self, type_, **kw):
        # don't support conversion between decimal/binary
        # precision yet
        kw["no_precision"] = True
        return self._generate_numeric(type_, "FLOAT", **kw)

    def visit_NUMBER(self, type_, **kw):
        return self._generate_numeric(type_, "NUMBER", **kw)

    def _generate_numeric(
        self, type_, name, precision=None, scale=None, no_precision=False, **kw
    ):
        if precision is None:
            precision = type_.precision

        if scale is None:
            scale = getattr(type_, "scale", None)

        if no_precision or precision is None:
            return name
        elif scale is None:
            n = "%(name)s(%(precision)s)"
            return n % {"name": name, "precision": precision}
        else:
            n = "%(name)s(%(precision)s, %(scale)s)"
            return n % {"name": name, "precision": precision, "scale": scale}

    def visit_string(self, type_, **kw):
        return self.visit_VARCHAR2(type_, **kw)

    def visit_VARCHAR2(self, type_, **kw):
        return self._visit_varchar(type_, "", "2")

    def visit_NVARCHAR2(self, type_, **kw):
        return self._visit_varchar(type_, "N", "2")

    visit_NVARCHAR = visit_NVARCHAR2

    def visit_VARCHAR(self, type_, **kw):
        return self._visit_varchar(type_, "", "")

    def _visit_varchar(self, type_, n, num):
        if not type_.length:
            # YashanDB expects explicit length for VARCHAR2/NVARCHAR2.
            # SQLAlchemy's String(length=None) appears in suite tests.
            default_len = 255
            varchar = "%(n)sVARCHAR%(two)s(%(length)s)"
            return varchar % {"length": default_len, "two": num, "n": n}
        elif not n and self.dialect._supports_char_length:
            varchar = "VARCHAR%(two)s(%(length)s CHAR)"
            return varchar % {"length": type_.length, "two": num}
        else:
            varchar = "%(n)sVARCHAR%(two)s(%(length)s)"
            return varchar % {"length": type_.length, "two": num, "n": n}

    def visit_text(self, type_, **kw):
        return self.visit_CLOB(type_, **kw)

    def visit_unicode_text(self, type_, **kw):
        if self.dialect._use_nchar_for_unicode:
            return self.visit_NCLOB(type_, **kw)
        else:
            return self.visit_CLOB(type_, **kw)

    def visit_large_binary(self, type_, **kw):
        return self.visit_BLOB(type_, **kw)

    def visit_big_integer(self, type_, **kw):
        return self.visit_NUMBER(type_, precision=19, **kw)

    def visit_boolean(self, type_, **kw):
        return self.visit_SMALLINT(type_, **kw)

    def visit_RAW(self, type_, **kw):
        if type_.length:
            return "RAW(%(length)s)" % {"length": type_.length}
        else:
            return "RAW"

    def visit_ROWID(self, type_, **kw):
        return "ROWID"


class YasCompiler(compiler.SQLCompiler):

    compound_keywords = util.update_copy(
        compiler.SQLCompiler.compound_keywords,
        {expression.CompoundSelect.EXCEPT: "MINUS"},
    )

    def __init__(self, *args, **kwargs):
        self.__wheres = {}
        super(YasCompiler, self).__init__(*args, **kwargs)

    def visit_mod_binary(self, binary, operator, **kw):
        return "mod(%s, %s)" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw),
        )

    def visit_now_func(self, fn, **kw):
        return "CURRENT_TIMESTAMP"

    def visit_char_length_func(self, fn, **kw):
        return "LENGTH" + self.function_argspec(fn, **kw)

    def visit_match_op_binary(self, binary, operator, **kw):
        return "CONTAINS (%s, %s)" % (
            self.process(binary.left),
            self.process(binary.right),
        )

    def visit_true(self, expr, **kw):
        return "1"

    def visit_false(self, expr, **kw):
        return "0"


    def get_cte_preamble(self, recursive):
        return "WITH"

    def get_select_hint_text(self, byfroms):
        return " ".join("/*+ %s */" % text for table, text in byfroms.items())

    def function_argspec(self, fn, **kw):
        if len(fn.clauses) > 0 or fn.name.upper() not in NO_ARG_FNS:
            return compiler.SQLCompiler.function_argspec(self, fn, **kw)
        else:
            return ""

    def visit_function(self, func, **kw):
        text = super(YasCompiler, self).visit_function(func, **kw)
        if kw.get("asfrom", False):
            text = "TABLE (%s)" % text
        return text

    def visit_table_valued_column(self, element, **kw):
        text = super(YasCompiler, self).visit_table_valued_column(element, **kw)
        text = text + ".COLUMN_VALUE"
        return text

    def default_from(self):
        """Called when a ``SELECT`` statement has no froms,
        and no ``FROM`` clause is to be appended.
        """

        return " FROM DUAL"

    def visit_join(self, join, from_linter=None, **kwargs):
        if self.dialect.use_ansi:
            return compiler.SQLCompiler.visit_join(
                self, join, from_linter=from_linter, **kwargs
            )
        else:
            if from_linter:
                from_linter.edges.add((join.left, join.right))

            kwargs["asfrom"] = True
            if isinstance(join.right, expression.FromGrouping):
                right = join.right.element
            else:
                right = join.right
            return (
                self.process(join.left, from_linter=from_linter, **kwargs)
                + ", "
                + self.process(right, from_linter=from_linter, **kwargs)
            )

    def _get_nonansi_join_whereclause(self, froms):
        clauses = []

        def visit_join(join):
            if join.isouter:
                # "apply the outer join operator (+) to all columns of B in
                # the join condition in the WHERE clause" - that is,
                # unconditionally regardless of operator or the other side
                def visit_binary(binary):
                    if isinstance(
                        binary.left, expression.ColumnClause
                    ) and join.right.is_derived_from(binary.left.table):
                        binary.left = _OuterJoinColumn(binary.left)
                    elif isinstance(
                        binary.right, expression.ColumnClause
                    ) and join.right.is_derived_from(binary.right.table):
                        binary.right = _OuterJoinColumn(binary.right)

                clauses.append(
                    visitors.cloned_traverse(
                        join.onclause, {}, {"binary": visit_binary}
                    )
                )
            else:
                clauses.append(join.onclause)

            for j in join.left, join.right:
                if isinstance(j, expression.Join):
                    visit_join(j)
                elif isinstance(j, expression.FromGrouping):
                    visit_join(j.element)

        for f in froms:
            if isinstance(f, expression.Join):
                visit_join(f)

        if not clauses:
            return None
        else:
            return sql.and_(*clauses)

    def visit_outer_join_column(self, vc, **kw):
        return self.process(vc.column, **kw) + "(+)"

    def visit_sequence(self, seq, **kw):
        return self.preparer.format_sequence(seq) + ".nextval"

    def get_render_as_alias_suffix(self, alias_name_text):

        return " " + alias_name_text

    def returning_clause(self, stmt, returning_cols):
        columns = []
        binds = []

        for i, column in enumerate(expression._select_iterables(returning_cols)):
            if column.type._has_column_expression:
                col_expr = column.type.column_expression(column)
            else:
                col_expr = column

            outparam = sql.outparam("ret_%d" % i, type_=column.type)
            self.binds[outparam.key] = outparam
            binds.append(self.bindparam_string(self._truncate_bindparam(outparam)))

            self.has_out_parameters = False

            columns.append(self.process(col_expr, within_columns_clause=False))

            # SQLAlchemy 1.4.5 compatibility:
            # - getattr(col, "name", fallback) does NOT fallback when name exists but is None
            # - _anon_name_label may not exist on 1.4.5
            name = getattr(col_expr, "name", None)
            if not name:
                name = (
                    getattr(col_expr, "_anon_name_label", None)
                    or getattr(col_expr, "_anon_name", None)
                    or ("ret_%d" % i)
                )

            self._add_to_result_map(
                name,
                name,
                (
                    column,
                    getattr(column, "name", None),
                    getattr(column, "key", None),
                ),
                column.type,
            )

        return "RETURNING " + ", ".join(columns) + " INTO " + ", ".join(binds)

    def translate_select_structure(self, select_stmt, **kwargs):
        select = select_stmt

        if not getattr(select, "_yashandb_visit", None):
            if not self.dialect.use_ansi:
                froms = self._display_froms_for_select(
                    select, kwargs.get("asfrom", False)
                )
                whereclause = self._get_nonansi_join_whereclause(froms)
                if whereclause is not None:
                    select = select.where(whereclause)
                    select._yashandb_visit = True

            # if fetch is used this is not needed
            if select._has_row_limiting_clause and select._fetch_clause is None:
                limit_clause = select._limit_clause
                offset_clause = select._offset_clause

                # NOTE: Don't use render_literal_execute() for LIMIT/OFFSET.
                # In SQLAlchemy 1.4.5 this produces [POSTCOMPILE_*] tokens; for
                # the yaspy driver these can leak into the final SQL and/or be
                # consumed multiple times when the same parameter is reused in
                # the ROWNUM rewrite, causing errors.

                orig_select = select
                select = select._generate()
                select._yashandb_visit = True

                # add expressions to accommodate FOR UPDATE OF
                for_update = select._for_update_arg
                if for_update is not None and for_update.of:
                    for_update = for_update._clone()
                    for_update._copy_internals()

                    for elem in for_update.of:
                        if not select.selected_columns.contains_column(elem):
                            select = select.add_columns(elem)

                # Wrap the middle select and add the hint
                inner_subquery = select.alias()
                limitselect = sql.select(
                    *[
                        c
                        for c in inner_subquery.c
                        if orig_select.selected_columns.corresponding_column(c)
                        is not None
                    ]
                )

                if (
                    limit_clause is not None
                    and self.dialect.optimize_limits
                    and select._simple_int_clause(limit_clause)
                ):
                    limitselect = limitselect.prefix_with(
                        expression.text(
                            "/*+ FIRST_ROWS(%s) */"
                            % self.process(limit_clause, **kwargs)
                        )
                    )

                limitselect._yashandb_visit = True
                limitselect._is_wrapper = True

                # add expressions to accommodate FOR UPDATE OF
                if for_update is not None and for_update.of:

                    adapter = sql_util.ClauseAdapter(inner_subquery)
                    for_update.of = [adapter.traverse(elem) for elem in for_update.of]

                # If needed, add the limiting clause
                if limit_clause is not None:
                    if select._simple_int_clause(limit_clause) and (
                        offset_clause is None
                        or select._simple_int_clause(offset_clause)
                    ):
                        max_row = limit_clause

                        if offset_clause is not None:
                            max_row = max_row + offset_clause

                    else:
                        max_row = limit_clause

                        if offset_clause is not None:
                            max_row = max_row + offset_clause
                    limitselect = limitselect.where(
                        sql.literal_column("ROWNUM") <= max_row
                    )

                # If needed, add the ora_rn, and wrap again with offset.
                if offset_clause is None:
                    limitselect._for_update_arg = for_update
                    select = limitselect
                else:
                    limitselect = limitselect.add_columns(
                        sql.literal_column("ROWNUM").label("ora_rn")
                    )
                    limitselect._yashandb_visit = True
                    limitselect._is_wrapper = True

                    if for_update is not None and for_update.of:
                        limitselect_cols = limitselect.selected_columns
                        for elem in for_update.of:
                            if limitselect_cols.corresponding_column(elem) is None:
                                limitselect = limitselect.add_columns(elem)

                    limit_subquery = limitselect.alias()
                    origselect_cols = orig_select.selected_columns
                    offsetselect = sql.select(
                        *[
                            c
                            for c in limit_subquery.c
                            if origselect_cols.corresponding_column(c) is not None
                        ]
                    )

                    offsetselect._yashandb_visit = True
                    offsetselect._is_wrapper = True

                    if for_update is not None and for_update.of:
                        adapter = sql_util.ClauseAdapter(limit_subquery)
                        for_update.of = [
                            adapter.traverse(elem) for elem in for_update.of
                        ]

                    offsetselect = offsetselect.where(
                        sql.literal_column("ora_rn") > offset_clause
                    )

                    offsetselect._for_update_arg = for_update
                    select = offsetselect

        return select

    def limit_clause(self, select, **kw):
        return ""

    def visit_empty_set_expr(self, type_):
        return "SELECT 1 FROM DUAL WHERE 1!=1"

    def for_update_clause(self, select, **kw):
        if self.is_subquery():
            return ""

        tmp = " FOR UPDATE"

        if select._for_update_arg.of:
            tmp += " OF " + ", ".join(
                self.process(elem, **kw) for elem in select._for_update_arg.of
            )

        if select._for_update_arg.nowait:
            tmp += " NOWAIT"
        if select._for_update_arg.skip_locked:
            tmp += " SKIP LOCKED"

        return tmp

    def visit_is_distinct_from_binary(self, binary, operator, **kw):
        return "DECODE(%s, %s, 0, 1) = 1" % (
            self.process(binary.left),
            self.process(binary.right),
        )

    def visit_is_not_distinct_from_binary(self, binary, operator, **kw):
        return "DECODE(%s, %s, 0, 1) = 0" % (
            self.process(binary.left),
            self.process(binary.right),
        )

    def visit_regexp_match_op_binary(self, binary, operator, **kw):
        string = self.process(binary.left, **kw)
        pattern = self.process(binary.right, **kw)
        flags = binary.modifiers["flags"]
        if flags is None:
            return "REGEXP_LIKE(%s, %s)" % (string, pattern)
        else:
            return "REGEXP_LIKE(%s, %s, %s)" % (
                string,
                pattern,
                self.render_literal_value(flags, sqltypes.STRINGTYPE),
            )

    def visit_not_regexp_match_op_binary(self, binary, operator, **kw):
        return "NOT %s" % self.visit_regexp_match_op_binary(binary, operator, **kw)

    def visit_regexp_replace_op_binary(self, binary, operator, **kw):
        string = self.process(binary.left, **kw)
        pattern_replace = self.process(binary.right, **kw)
        flags = binary.modifiers["flags"]
        if flags is None:
            return "REGEXP_REPLACE(%s, %s)" % (
                string,
                pattern_replace,
            )
        else:
            return "REGEXP_REPLACE(%s, %s, %s)" % (
                string,
                pattern_replace,
                self.render_literal_value(flags, sqltypes.STRINGTYPE),
            )


class YasDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kwargs):
        text = super(YasDDLCompiler, self).get_column_specification(
            column, **kwargs
        )

        # SQLAlchemy suite uses plain Integer PK autoincrement columns (no
        # explicit Identity()). Ensure YashanDB generates values so INSERT ..
        # RETURNING doesn't attempt to insert NULL into the PK.
        try:
            has_identity = bool(getattr(column, "identity", None))
        except Exception:
            has_identity = False

        # NOTE: YashanDB deployments may not support GENERATED .. AS IDENTITY
        # nor AUTO_INCREMENT syntax in CREATE TABLE. Autoincrement behavior is
        # instead emulated via sequence + trigger installed at the dialect
        # level (see YasDialect.initialize()).

        return text

    def define_constraint_cascades(self, constraint):
        text = ""
        if constraint.ondelete is not None:
            text += " ON DELETE %s" % constraint.ondelete

        if constraint.onupdate is not None:
            util.warn(
                "YashanDB does not contain native UPDATE CASCADE "
                "functionality - onupdates will not be rendered for foreign "
                "keys.  Consider using deferrable=True, initially='deferred' "
                "or triggers."
            )

        return text

    def visit_drop_table_comment(self, drop):
        return "COMMENT ON TABLE %s IS ''" % self.preparer.format_table(drop.element)

    def visit_create_index(self, create):
        index = create.element
        self._verify_index_table(index)
        preparer = self.preparer
        text = "CREATE "
        if index.unique:
            text += "UNIQUE "
        if index.dialect_options["yashandb"]["bitmap"]:
            text += "BITMAP "
        text += "INDEX %s ON %s (%s)" % (
            self._prepared_index_name(index, include_schema=True),
            preparer.format_table(index.table, use_schema=True),
            ", ".join(
                self.sql_compiler.process(expr, include_table=False, literal_binds=True)
                for expr in index.expressions
            ),
        )
        if index.dialect_options["yashandb"]["compress"] is not False:
            if index.dialect_options["yashandb"]["compress"] is True:
                text += " COMPRESS"
            else:
                text += " COMPRESS %d" % (index.dialect_options["yashandb"]["compress"])
        return text

    def post_create_table(self, table):
        table_opts = []
        opts = table.dialect_options["yashandb"]

        if opts["on_commit"]:
            on_commit_options = opts["on_commit"].replace("_", " ").upper()
            table_opts.append("\n ON COMMIT %s" % on_commit_options)

        if opts["compress"]:
            if opts["compress"] is True:
                table_opts.append("\n COMPRESS")
            else:
                table_opts.append("\n COMPRESS FOR %s" % (opts["compress"]))

        return "".join(table_opts)

    def get_identity_options(self, identity_options):
        text = super(YasDDLCompiler, self).get_identity_options(identity_options)
        text = text.replace("NO MINVALUE", "NOMINVALUE")
        text = text.replace("NO MAXVALUE", "NOMAXVALUE")
        text = text.replace("NO CYCLE", "NOCYCLE")
        if identity_options.order is not None:
            text += " ORDER" if identity_options.order else " NOORDER"
        return text.strip()

    def visit_computed_column(self, generated):
        text = "GENERATED ALWAYS AS (%s)" % self.sql_compiler.process(
            generated.sqltext, include_table=False, literal_binds=True
        )
        if generated.persisted is True:
            raise exc.CompileError(
                "YashanDB computed columns do not support 'stored' persistence; "
                "set the 'persisted' flag to None or False for YashanDB support."
            )
        elif generated.persisted is False:
            text += " VIRTUAL"
        return text

    def visit_identity_column(self, identity, **kw):
        if identity.always is None:
            kind = ""
        else:
            kind = "ALWAYS" if identity.always else "BY DEFAULT"
        text = "GENERATED %s" % kind
        if identity.on_null:
            text += " ON NULL"
        text += " AS IDENTITY"
        options = self.get_identity_options(identity)
        if options:
            text += " (%s)" % options
        return text


class YasIdentifierPreparer(compiler.IdentifierPreparer):

    reserved_words = {x.lower() for x in RESERVED_WORDS}
    illegal_initial_characters = {str(dig) for dig in range(0, 10)}.union(["_", "$"])

    def _bindparam_requires_quotes(self, value):
        """Return True if the given identifier requires quoting."""
        lc_value = value.lower()
        return (
            lc_value in self.reserved_words
            or value[0] in self.illegal_initial_characters
            or not self.legal_characters.match(util.text_type(value))
        )

    def format_savepoint(self, savepoint):
        name = savepoint.ident.lstrip("_")
        return super(YasIdentifierPreparer, self).format_savepoint(savepoint, name)


class YasExecutionContext(default.DefaultExecutionContext):
    def fire_sequence(self, seq, type_):
        return self._execute_scalar(
            "SELECT "
            + self.identifier_preparer.format_sequence(seq)
            + ".nextval FROM DUAL",
            type_,
        )


class YasDialect(default.DefaultDialect):
    name = "yashandb"
    supports_statement_cache = True
    supports_alter = True
    supports_unicode_statements = False
    supports_unicode_binds = False
    # YashanDB object names are limited to 64 characters. This impacts naming
    # conventions for constraints and indexes; SQLAlchemy will truncate
    # `_truncated_label` names at format time based on this value.
    max_identifier_length = 64
    max_constraint_name_length = 64
    max_index_name_length = 64

    supports_simple_order_by_label = False
    cte_follows_insert = True

    supports_sequences = True
    sequences_optional = False
    postfetch_lastrowid = False

    default_paramstyle = "named"
    colspecs = colspecs
    ischema_names = ischema_names
    requires_name_normalize = True

    supports_comments = True

    supports_default_values = False
    supports_default_metavalue = True
    supports_empty_insert = False
    supports_identity_columns = True
    # UPDATE .. RETURNING is not supported in current YashanDB mode / yaspy.
    update_returning = False

    statement_compiler = YasCompiler
    ddl_compiler = YasDDLCompiler
    type_compiler = YasTypeCompiler
    preparer = YasIdentifierPreparer
    execution_ctx_cls = YasExecutionContext

    _use_nchar_for_unicode = False

    construct_arguments = [
        (
            sa_schema.Table,
            {"resolve_synonyms": False, "on_commit": None, "compress": False},
        ),
        (sa_schema.Index, {"bitmap": False, "compress": False}),
    ]

    @util.deprecated_params(
        use_binds_for_limits=(
            "1.4",
            "The ``use_binds_for_limits`` YashanDB dialect parameter is "
            "deprecated. The dialect now renders LIMIT /OFFSET integers "
            "inline in all cases using a post-compilation hook, so that the "
            "value is still represented by a 'bound parameter' on the Core "
            "Expression side.",
        )
    )
    def __init__(
        self,
        use_ansi=True,
        optimize_limits=False,
        use_binds_for_limits=None,
        use_nchar_for_unicode=False,
        exclude_tablespaces=("SYSAUX",),
        **kwargs
    ):
        default.DefaultDialect.__init__(self, **kwargs)
        self._use_nchar_for_unicode = use_nchar_for_unicode
        self.use_ansi = use_ansi
        self.optimize_limits = optimize_limits
        self.exclude_tablespaces = exclude_tablespaces

        # fore use numeric bind style, TODO: fix yasdb driver's paramstype or suppport named style
        self.paramstyle = self.default_paramstyle
        self.positional = True

    def initialize(self, connection):
        super(YasDialect, self).initialize(connection)

        self.implicit_returning = self.__dict__.get("implicit_returning", True)

        self.supports_identity_columns = True

        # YashanDB may not support inline self-referential foreign keys within
        # CREATE TABLE. Emit these constraints via ALTER TABLE after the table
        # is created to avoid "table or view does not exist" errors during
        # metadata.create_all().
        if not getattr(self, "_selfref_fk_event_installed", False):
            self._selfref_fk_event_installed = True

            @event.listens_for(sa_schema.Table, "before_create")
            def _yashandb_selfref_fk_use_alter(target, connection, **kw):
                if getattr(connection.dialect, "name", None) != "yashandb":
                    return
                for fkc in list(target.foreign_key_constraints):
                    try:
                        if fkc.referred_table is target:
                            fkc.use_alter = True
                    except Exception:
                        continue

        # Emulate autoincrement integer primary keys using SEQUENCE + TRIGGER.
        # This avoids relying on CREATE TABLE syntax like GENERATED/IDENTITY or
        # AUTO_INCREMENT which may not be available in all YashanDB modes.
        if not getattr(self, "_autoinc_trigger_event_installed", False):
            self._autoinc_trigger_event_installed = True

            def _autoinc_pk_column(table):
                try:
                    pkcols = list(table.primary_key.columns)
                except Exception:
                    return None
                if len(pkcols) != 1:
                    return None
                col = pkcols[0]
                try:
                    if not col.primary_key:
                        return None
                    if getattr(col, "identity", None) is not None:
                        return None
                    if getattr(col, "default", None) is not None:
                        return None
                    if getattr(col, "server_default", None) is not None:
                        return None
                    if getattr(col, "autoincrement", "auto") not in ("auto", True):
                        return None
                    aff = getattr(getattr(col, "type", None), "_type_affinity", None)
                    if aff is not sqltypes.Integer:
                        return None
                except Exception:
                    return None
                return col

            def _truncated_name(raw_name):
                # apply dialect identifier length limit; database will enforce
                # 64 chars; SQLAlchemy's identifier preparer will quote as needed.
                maxlen = getattr(self, "max_identifier_length", 64) or 64
                if len(raw_name) <= maxlen:
                    return raw_name
                # keep suffix to reduce collision risk
                return raw_name[: maxlen - 8] + "_" + raw_name[-7:]

            @event.listens_for(sa_schema.Table, "after_create")
            def _yashandb_autoinc_after_create(target, connection, **kw):
                if getattr(connection.dialect, "name", None) != "yashandb":
                    return
                col = _autoinc_pk_column(target)
                if col is None:
                    return

                seq_name = _truncated_name(f"{target.name}_{col.name}_seq")
                trg_name = _truncated_name(f"{target.name}_{col.name}_trg")

                tbl = self.identifier_preparer.format_table(target)
                seq = self.identifier_preparer.quote(seq_name)
                trg = self.identifier_preparer.quote(trg_name)
                colname = self.identifier_preparer.quote(col.name)

                # best-effort create; ignore if already exists
                try:
                    connection.exec_driver_sql(f"CREATE SEQUENCE {seq} START WITH 1 INCREMENT BY 1")
                except Exception:
                    pass
                try:
                    connection.exec_driver_sql(
                        "CREATE OR REPLACE TRIGGER {trg}\n"
                        "BEFORE INSERT ON {tbl}\n"
                        "FOR EACH ROW\n"
                        "WHEN (new.{colname} IS NULL)\n"
                        "BEGIN\n"
                        "  SELECT {seq}.NEXTVAL INTO :new.{colname} FROM DUAL;\n"
                        "END;".format(trg=trg, tbl=tbl, colname=colname, seq=seq)
                    )
                except Exception:
                    # if triggers aren't supported, we'll fall back to user-provided ids
                    pass

            @event.listens_for(sa_schema.Table, "before_drop")
            def _yashandb_autoinc_before_drop(target, connection, **kw):
                if getattr(connection.dialect, "name", None) != "yashandb":
                    return
                col = _autoinc_pk_column(target)
                if col is None:
                    return
                seq_name = _truncated_name(f"{target.name}_{col.name}_seq")
                trg_name = _truncated_name(f"{target.name}_{col.name}_trg")
                seq = self.identifier_preparer.quote(seq_name)
                trg = self.identifier_preparer.quote(trg_name)
                try:
                    connection.exec_driver_sql(f"DROP TRIGGER {trg}")
                except Exception:
                    pass
                try:
                    connection.exec_driver_sql(f"DROP SEQUENCE {seq}")
                except Exception:
                    pass

    def _get_effective_compat_server_version_info(self, connection):

        if self.server_version_info < (12, 2):
            return self.server_version_info
        try:
            compat = connection.exec_driver_sql(
                "SELECT value FROM v$parameter WHERE name = 'compatible'"
            ).scalar()
        except exc.DBAPIError:
            compat = None

        if compat:
            try:
                return tuple(int(x) for x in compat.split("."))
            except:
                return self.server_version_info
        else:
            return self.server_version_info

    @property
    def _supports_table_compression(self):
        return True

    @property
    def _supports_table_compress_for(self):
        return False

    @property
    def _supports_char_length(self):
        return True

    @property
    def _supports_update_returning_computed_cols(self):
        return True

    def do_release_savepoint(self, connection, name):
        # YashanDB does not support RELEASE SAVEPOINT
        pass

    def _ensure_has_table_connection(self, connection):
        if not (hasattr(connection, "execute") or hasattr(connection, "exec_driver_sql")):
            raise exc.ArgumentError(
                "Connection passed to has_table() is not executable"
            )

    def _check_max_identifier_length(self, connection):
        if self._get_effective_compat_server_version_info(connection) < (
            12,
            2,
        ):
            return 30
        else:
            # use the default
            return None

    def _check_unicode_returns(self, connection):
        additional_tests = [
            expression.cast(
                expression.literal_column("'test nvarchar2 returns'"),
                sqltypes.NVARCHAR(60),
            )
        ]
        return super(YasDialect, self)._check_unicode_returns(
            connection, additional_tests
        )

    _isolation_lookup = ["READ COMMITTED", "SERIALIZABLE"]

    def get_isolation_level(self, connection):
        raise NotImplementedError("implemented by yasdb dialect")

    def get_default_isolation_level(self, dbapi_conn):
        try:
            return self.get_isolation_level(dbapi_conn)
        except NotImplementedError:
            raise
        except:
            return "READ COMMITTED"

    def set_isolation_level(self, connection, level):
        raise NotImplementedError("implemented by yasdb dialect")

    def has_table(self, connection, table_name, schema=None):
        self._ensure_has_table_connection(connection)

        if not schema:
            schema = self.default_schema_name

        cursor = connection.execute(
            sql.text(
                "SELECT table_name FROM all_tables "
                "WHERE table_name = CAST(:name AS VARCHAR2(128)) "
                "AND owner = CAST(:schema_name AS VARCHAR2(128))"
            ),
            dict(
                name=self.denormalize_name(table_name),
                schema_name=self.denormalize_name(schema),
            ),
        )
        return cursor.first() is not None

    def has_sequence(self, connection, sequence_name, schema=None):
        if not schema:
            schema = self.default_schema_name
        cursor = connection.execute(
            sql.text(
                "SELECT sequence_name FROM all_sequences "
                "WHERE sequence_name = :name AND "
                "sequence_owner = :schema_name"
            ),
            dict(
                name=self.denormalize_name(sequence_name),
                schema_name=self.denormalize_name(schema),
            ),
        )
        return cursor.first() is not None

    def _get_default_schema_name(self, connection):
        return self.normalize_name(
            connection.exec_driver_sql(
                "select sys_context( 'userenv', 'current_schema' ) from dual"
            ).scalar()
        )

    def _resolve_synonym(
        self,
        connection,
        desired_owner=None,
        desired_synonym=None,
        desired_table=None,
    ):
        """search for a local synonym matching the given desired owner/name.

        if desired_owner is None, attempts to locate a distinct owner.

        returns the actual name, owner, dblink name, and synonym name if
        found.
        """

        q = (
            "SELECT owner, table_owner, table_name, db_link, "
            "synonym_name FROM all_synonyms WHERE "
        )
        clauses = []
        params = {}
        if desired_synonym:
            clauses.append("synonym_name = CAST(:synonym_name AS VARCHAR2(128))")
            params["synonym_name"] = desired_synonym
        if desired_owner:
            clauses.append("owner = CAST(:desired_owner AS VARCHAR2(128))")
            params["desired_owner"] = desired_owner
        if desired_table:
            clauses.append("table_name = CAST(:tname AS VARCHAR2(128))")
            params["tname"] = desired_table

        q += " AND ".join(clauses)

        result = connection.execution_options(future_result=True).execute(
            sql.text(q), params
        )
        if desired_owner:
            row = result.mappings().first()
            if row:
                return (
                    row["table_name"],
                    row["table_owner"],
                    row["db_link"],
                    row["synonym_name"],
                )
            else:
                return None, None, None, None
        else:
            rows = result.mappings().all()
            if len(rows) > 1:
                raise AssertionError(
                    "There are multiple tables visible to the schema, you "
                    "must specify owner"
                )
            elif len(rows) == 1:
                row = rows[0]
                return (
                    row["table_name"],
                    row["table_owner"],
                    row["db_link"],
                    row["synonym_name"],
                )
            else:
                return None, None, None, None

    @reflection.cache
    def _prepare_reflection_args(
        self,
        connection,
        table_name,
        schema=None,
        resolve_synonyms=False,
        dblink="",
        **kw
    ):

        if resolve_synonyms:
            actual_name, owner, dblink, synonym = self._resolve_synonym(
                connection,
                desired_owner=self.denormalize_name(schema),
                desired_synonym=self.denormalize_name(table_name),
            )
        else:
            actual_name, owner, dblink, synonym = None, None, None, None
        if not actual_name:
            actual_name = self.denormalize_name(table_name)

        if dblink:
            owner = connection.scalar(
                sql.text("SELECT username FROM user_db_links " "WHERE db_link=:link"),
                dict(link=dblink),
            )
            dblink = "@" + dblink
        elif not owner:
            owner = self.denormalize_name(schema or self.default_schema_name)

        return (actual_name, owner, dblink or "", synonym)

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        s = "SELECT username FROM all_users ORDER BY username"
        cursor = connection.exec_driver_sql(s)
        return [self.normalize_name(row[0]) for row in cursor]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        schema = self.denormalize_name(schema or self.default_schema_name)

        # note that table_names() isn't loading DBLINKed or synonym'ed tables
        if schema is None:
            schema = self.default_schema_name

        sql_str = "SELECT table_name FROM all_tables WHERE "
        if self.exclude_tablespaces:
            sql_str += "nvl(tablespace_name, 'no tablespace') " "NOT IN (%s) AND " % (
                ", ".join(["'%s'" % ts for ts in self.exclude_tablespaces])
            )
        sql_str += "OWNER = :owner " "AND DURATION IS NULL"

        cursor = connection.execute(sql.text(sql_str), dict(owner=schema))
        table_names = [self.normalize_name(row[0]) for row in cursor]

        # SQLAlchemy's reflection suite may request table names that include
        # views via Inspector.get_table_names(include_views=True).
        if kw.get("include_views", False):
            try:
                table_names.extend(self.get_view_names(connection, schema=schema, **kw))
            except Exception:
                pass

        return table_names

    @reflection.cache
    def get_temp_table_names(self, connection, **kw):
        schema = self.denormalize_name(self.default_schema_name)

        sql_str = "SELECT table_name FROM all_tables WHERE "
        if self.exclude_tablespaces:
            sql_str += "nvl(tablespace_name, 'no tablespace') " "NOT IN (%s) AND " % (
                ", ".join(["'%s'" % ts for ts in self.exclude_tablespaces])
            )
        sql_str += "OWNER = :owner " "AND DURATION IS NOT NULL"

        cursor = connection.execute(sql.text(sql_str), dict(owner=schema))
        return [self.normalize_name(row[0]) for row in cursor]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        schema = self.denormalize_name(schema or self.default_schema_name)

        # Primary: ALL_VIEWS
        s = sql.text("SELECT view_name FROM all_views WHERE owner = :owner")
        cursor = connection.execute(s, dict(owner=self.denormalize_name(schema)))
        names = [self.normalize_name(row[0]) for row in cursor]
        if names:
            return names

        # Fallback: USER_VIEWS (more widely available)
        try:
            s = sql.text("SELECT view_name FROM user_views")
            cursor = connection.execute(s)
            names = [self.normalize_name(row[0]) for row in cursor]
            if names:
                return names
        except Exception:
            pass

        # Fallback: ALL_OBJECTS (some deployments may not expose ALL_VIEWS)
        s = sql.text(
            "SELECT object_name FROM all_objects "
            "WHERE owner = :owner AND object_type = 'VIEW'"
        )
        cursor = connection.execute(s, dict(owner=self.denormalize_name(schema)))
        return [self.normalize_name(row[0]) for row in cursor]

    @reflection.cache
    def get_sequence_names(self, connection, schema=None, **kw):
        if not schema:
            schema = self.default_schema_name
        cursor = connection.execute(
            sql.text(
                "SELECT sequence_name FROM all_sequences "
                "WHERE sequence_owner = :schema_name"
            ),
            dict(schema_name=self.denormalize_name(schema)),
        )
        return [self.normalize_name(row[0]) for row in cursor]

    @reflection.cache
    def get_table_options(self, connection, table_name, schema=None, **kw):
        options = {}
        return options

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):

        resolve_synonyms = kw.get("yashandb_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )
        columns = []
        if self._supports_char_length:
            char_length_col = "char_length"
        else:
            char_length_col = "data_length"

        identity_cols = "NULL as default_on_null, NULL as identity_options"

        params = {"table_name": table_name}

        text = """
            SELECT
                col.column_name,
                col.data_type,
                col.%(char_length_col)s,
                col.data_precision,
                col.data_scale,
                col.nullable,
                col.data_default,
                com.comments,
                %(identity_cols)s
            FROM all_tab_cols%(dblink)s col
            LEFT JOIN all_col_comments%(dblink)s com
            ON col.table_name = com.table_name
            AND col.column_name = com.column_name
            AND col.owner = com.owner
            WHERE col.table_name = CAST(:table_name AS VARCHAR2(128))
        """
        if schema is not None:
            params["owner"] = schema
            text += " AND col.owner = :owner "
        text += " ORDER BY col.column_id"
        text = text % {
            "dblink": dblink,
            "char_length_col": char_length_col,
            "identity_cols": identity_cols,
        }

        c = connection.execute(sql.text(text), params)
        for row in c:
            colname = self.normalize_name(row[0])
            orig_colname = row[0]
            coltype = row[1]
            length = row[2]
            precision = row[3]
            scale = row[4]
            nullable = row[5] == "Y"
            default = row[6]
            comment = row[7]
            generated = "NO"
            default_on_nul = row[8]
            identity_options = row[9]

            if coltype == "NUMBER":
                if precision is None and scale == 0:
                    coltype = INTEGER()
                else:
                    coltype = NUMBER(precision, scale)
            elif coltype == "FLOAT":
                # TODO: support "precision" here as "binary_precision"
                coltype = FLOAT()
            elif coltype in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "VARCHAR"):
                coltype = self.ischema_names.get(coltype)(length)
            elif "WITH TIME ZONE" in coltype:
                coltype = TIMESTAMP(timezone=True)
            else:
                coltype = re.sub(r"\(\d+\)", "", coltype)
                try:
                    coltype = self.ischema_names[coltype]
                except KeyError:
                    util.warn(
                        "Did not recognize type '%s' of column '%s'"
                        % (coltype, colname)
                    )
                    coltype = sqltypes.NULLTYPE

            if generated == "YES":
                computed = dict(sqltext=default)
                default = None
            else:
                computed = None

            if identity_options is not None:
                identity = self._parse_identity_options(
                    identity_options, default_on_nul
                )
                default = None
            else:
                identity = None

            cdict = {
                "name": colname,
                "type": coltype,
                "nullable": nullable,
                "default": default,
                "autoincrement": "auto",
                "comment": comment,
            }
            if orig_colname.lower() == orig_colname:
                cdict["quote"] = True
            if computed is not None:
                cdict["computed"] = computed
            if identity is not None:
                cdict["identity"] = identity

            columns.append(cdict)
        return columns

    def _parse_identity_options(self, identity_options, default_on_nul):
        # identity_options is a string that starts with 'ALWAYS,' or
        # 'BY DEFAULT,' and continues with
        # START WITH: 1, INCREMENT BY: 1, MAX_VALUE: 123, MIN_VALUE: 1,
        # CYCLE_FLAG: N, CACHE_SIZE: 1, ORDER_FLAG: N, SCALE_FLAG: N,
        # EXTEND_FLAG: N, SESSION_FLAG: N, KEEP_VALUE: N
        parts = [p.strip() for p in identity_options.split(",")]
        identity = {
            "always": parts[0] == "ALWAYS",
            "on_null": default_on_nul == "YES",
        }

        for part in parts[1:]:
            option, value = part.split(":")
            value = value.strip()

            if "START WITH" in option:
                identity["start"] = compat.long_type(value)
            elif "INCREMENT BY" in option:
                identity["increment"] = compat.long_type(value)
            elif "MAX_VALUE" in option:
                identity["maxvalue"] = compat.long_type(value)
            elif "MIN_VALUE" in option:
                identity["minvalue"] = compat.long_type(value)
            elif "CYCLE_FLAG" in option:
                identity["cycle"] = value == "Y"
            elif "CACHE_SIZE" in option:
                identity["cache"] = compat.long_type(value)
            elif "ORDER_FLAG" in option:
                identity["order"] = value == "Y"
        return identity

    @reflection.cache
    def get_table_comment(
        self,
        connection,
        table_name,
        schema=None,
        resolve_synonyms=False,
        dblink="",
        **kw
    ):

        info_cache = kw.get("info_cache")
        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        if not schema:
            schema = self.default_schema_name

        COMMENT_SQL = """
            SELECT comments
            FROM all_tab_comments
            WHERE table_name = CAST(:table_name AS VARCHAR(128))
            AND owner = CAST(:schema_name AS VARCHAR(128))
        """

        c = connection.execute(
            sql.text(COMMENT_SQL),
            dict(table_name=table_name, schema_name=schema),
        )
        return {"text": c.scalar()}

    @reflection.cache
    def get_indexes(
        self,
        connection,
        table_name,
        schema=None,
        resolve_synonyms=False,
        dblink="",
        **kw
    ):

        info_cache = kw.get("info_cache")
        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )
        indexes = []

        params = {"table_name": table_name}
        text = (
            "SELECT a.index_name, a.column_name, "
            "\nb.index_type, b.uniqueness, b.compression, b.prefix_length "
            "\nFROM ALL_IND_COLUMNS%(dblink)s a, "
            "\nALL_INDEXES%(dblink)s b "
            "\nWHERE "
            "\na.index_name = b.index_name "
            "\nAND a.table_owner = b.table_owner "
            "\nAND a.table_name = b.table_name "
            "\nAND a.table_name = CAST(:table_name AS VARCHAR(128))"
        )

        if schema is not None:
            params["schema"] = schema
            text += "AND a.table_owner = :schema "

        text += "ORDER BY a.index_name, a.column_position"

        text = text % {"dblink": dblink}

        q = sql.text(text)
        rp = connection.execute(q, params)
        indexes = []
        last_index_name = None
        pk_constraint = self.get_pk_constraint(
            connection,
            table_name,
            schema,
            resolve_synonyms=resolve_synonyms,
            dblink=dblink,
            info_cache=kw.get("info_cache"),
        )

        uniqueness = dict(NONUNIQUE=False, UNIQUE=True)
        enabled = dict(DISABLED=False, ENABLED=True)

        yashandb_sys_col = re.compile(r"SYS_NC\d+\$", re.IGNORECASE)

        index = None
        for rset in rp:
            index_name_normalized = self.normalize_name(rset.index_name)

            if pk_constraint and index_name_normalized == pk_constraint["name"]:
                continue

            if rset.index_name != last_index_name:
                index = dict(
                    name=index_name_normalized,
                    column_names=[],
                    dialect_options={},
                )
                indexes.append(index)
            index["unique"] = uniqueness.get(rset.uniqueness, False)

            if rset.index_type in ("BITMAP", "FUNCTION-BASED BITMAP"):
                index["dialect_options"]["yashandb_bitmap"] = True
            if enabled.get(rset.compression, False):
                index["dialect_options"]["yashandb_compress"] = rset.prefix_length

            if not yashandb_sys_col.match(rset.column_name):
                index["column_names"].append(self.normalize_name(rset.column_name))
            last_index_name = rset.index_name

        return indexes

    @reflection.cache
    def _get_constraint_data(
        self, connection, table_name, schema=None, dblink="", **kw
    ):

        params = {"table_name": table_name}

        text = (
            "SELECT"
            "\nac.constraint_name,"  # 0
            "\nac.constraint_type,"  # 1
            "\nloc.column_name AS local_column,"  # 2
            "\nrem.table_name AS remote_table,"  # 3
            "\nrem.column_name AS remote_column,"  # 4
            "\nrem.owner AS remote_owner,"  # 5
            "\nloc.position as loc_pos,"  # 6
            "\nrem.position as rem_pos,"  # 7
            "\nac.search_condition,"  # 8
            "\nac.delete_rule"  # 9
            "\nFROM all_constraints%(dblink)s ac,"
            "\nall_cons_columns%(dblink)s loc,"
            "\nall_cons_columns%(dblink)s rem"
            "\nWHERE ac.table_name = CAST(:table_name AS VARCHAR2(128))"
            "\nAND ac.constraint_type IN ('R','P', 'U', 'C')"
        )

        if schema is not None:
            params["owner"] = schema
            text += "\nAND ac.owner = CAST(:owner AS VARCHAR2(128))"

        text += (
            "\nAND ac.owner = loc.owner"
            "\nAND ac.constraint_name = loc.constraint_name"
            "\nAND ac.r_owner = rem.owner(+)"
            "\nAND ac.r_constraint_name = rem.constraint_name(+)"
            "\nAND (rem.position IS NULL or loc.position=rem.position)"
            "\nORDER BY ac.constraint_name, loc.position"
        )

        text = text % {"dblink": dblink}
        rp = connection.execute(sql.text(text), params)
        constraint_data = rp.fetchall()
        return constraint_data

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        resolve_synonyms = kw.get("yashandb_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )
        pkeys = []
        constraint_name = None
        constraint_data = self._get_constraint_data(
            connection,
            table_name,
            schema,
            dblink,
            info_cache=kw.get("info_cache"),
        )

        for row in constraint_data:
            (
                cons_name,
                cons_type,
                local_column,
                remote_table,
                remote_column,
                remote_owner,
            ) = row[0:2] + tuple([self.normalize_name(x) for x in row[2:6]])
            if cons_type == "P":
                if constraint_name is None:
                    constraint_name = self.normalize_name(cons_name)
                pkeys.append(local_column)
        return {"constrained_columns": pkeys, "name": constraint_name}

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):

        requested_schema = schema  # to check later on
        resolve_synonyms = kw.get("yashandb_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        # The SQLAlchemy reflection suite calls get_foreign_keys() with
        # schema=None for the "current schema" case. On some YashanDB
        # deployments, ALL_* dictionary views may be restricted or may not
        # include current-user objects as expected; USER_* is the most reliable
        # source in that scenario.
        if requested_schema is None and not dblink:
            rp_user = []

            def _detect_user_constraints_rcol():
                # Different YashanDB dictionary view variants may name the
                # referenced-constraint column differently (e.g. R_CONSTRAINT_NAME
                # vs R_CONS_NAME). Detect at runtime to avoid silent empty FKs.
                cache_key = "_yashandb_user_constraints_rcol"
                cached = getattr(self, cache_key, None)
                if cached is not None:
                    return cached
                try:
                    keys = connection.execute(
                        sql.text("SELECT * FROM user_constraints WHERE 1=0")
                    ).keys()
                    keys_l = {k.lower() for k in keys}
                except Exception:
                    keys_l = set()
                for cand in ("r_constraint_name", "r_cons_name"):
                    if cand in keys_l:
                        setattr(self, cache_key, cand)
                        return cand
                setattr(self, cache_key, None)
                return None

            rcol = _detect_user_constraints_rcol()
            if rcol:
                try:
                    rp_user = connection.execute(
                        sql.text(
                            "SELECT "
                            "uc.constraint_name, "
                            "ucc.column_name AS local_column, "
                            "rcon.table_name AS remote_table, "
                            "rcc.column_name AS remote_column, "
                            "NULL AS remote_owner, "
                            "uc.delete_rule "
                            "FROM user_constraints uc "
                            "JOIN user_cons_columns ucc "
                            "  ON uc.constraint_name = ucc.constraint_name "
                            "JOIN user_constraints rcon "
                            f"  ON uc.{rcol} = rcon.constraint_name "
                            "JOIN user_cons_columns rcc "
                            "  ON rcon.constraint_name = rcc.constraint_name "
                            " AND ucc.position = rcc.position "
                            "WHERE UPPER(uc.table_name) = UPPER(:table_name) "
                            f"AND uc.{rcol} IS NOT NULL "
                            "ORDER BY uc.constraint_name, ucc.position"
                        ),
                        {"table_name": table_name},
                    ).fetchall()
                except Exception:
                    rp_user = []
        else:
            rp_user = []

        params = {"table_name": table_name}
        text = (
            "SELECT "
            "ac.constraint_name, "
            "acc.column_name AS local_column, "
            "rcon.table_name AS remote_table, "
            "rcc.column_name AS remote_column, "
            "rcon.owner AS remote_owner, "
            "ac.delete_rule "
            "FROM all_constraints%(dblink)s ac "
            "JOIN all_cons_columns%(dblink)s acc "
            "  ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name "
            "JOIN all_constraints%(dblink)s rcon "
            "  ON ac.r_owner = rcon.owner AND ac.r_constraint_name = rcon.constraint_name "
            "JOIN all_cons_columns%(dblink)s rcc "
            "  ON rcon.owner = rcc.owner "
            " AND rcon.constraint_name = rcc.constraint_name "
            " AND acc.position = rcc.position "
            "WHERE UPPER(ac.table_name) = UPPER(:table_name) "
            "AND ac.r_constraint_name IS NOT NULL "
        )

        if schema is not None:
            params["owner"] = schema
            text += "AND ac.owner = CAST(:owner AS VARCHAR2(128)) "

        text += "ORDER BY ac.constraint_name, acc.position"
        text = text % {"dblink": dblink}
        rp = connection.execute(sql.text(text), params).fetchall()

        def fkey_rec():
            return {
                "name": None,
                "constrained_columns": [],
                "referred_schema": None,
                "referred_table": None,
                "referred_columns": [],
                "options": {},
            }

        fkeys = util.defaultdict(fkey_rec)

        def _populate_from_rows(rows):
            for (
                cons_name,
                local_col,
                remote_table,
                remote_col,
                remote_owner,
                delete_rule,
            ) in rows:
                cons_name = self.normalize_name(cons_name)
                local_col = self.normalize_name(local_col)
                remote_table_n = self.normalize_name(remote_table)
                remote_col = self.normalize_name(remote_col)
                remote_owner_n = self.normalize_name(remote_owner)

                rec = fkeys[cons_name]
                rec["name"] = cons_name
                # Dictionary view joins may return duplicate rows for the same
                # FK/position depending on the deployment. Keep ordering stable
                # but avoid duplicating column pairs.
                existing_pairs = set(
                    zip(rec["constrained_columns"], rec["referred_columns"])
                )
                if (local_col, remote_col) not in existing_pairs:
                    rec["constrained_columns"].append(local_col)
                    rec["referred_columns"].append(remote_col)

                if not rec["referred_table"]:
                    if resolve_synonyms:
                        (
                            ref_remote_name,
                            ref_remote_owner,
                            ref_dblink,
                            ref_synonym,
                        ) = self._resolve_synonym(
                            connection,
                            desired_owner=self.denormalize_name(remote_owner_n),
                            desired_table=self.denormalize_name(remote_table_n),
                        )
                        if ref_synonym:
                            remote_table_n = self.normalize_name(ref_synonym)
                            remote_owner_n = self.normalize_name(ref_remote_owner)

                    rec["referred_table"] = remote_table_n

                    if (
                        requested_schema is not None
                        or self.denormalize_name(remote_owner_n) != schema
                    ):
                        rec["referred_schema"] = remote_owner_n

                    if delete_rule and delete_rule != "NO ACTION":
                        rec["options"]["ondelete"] = delete_rule

        if rp_user:
            _populate_from_rows(rp_user)

        _populate_from_rows(rp)

        if not fkeys:
            # Fallback approach: fetch constraint + local/remote columns in
            # separate steps. This is more compatible with some dictionary
            # view variants and self-referential FKs.
            cons_text = (
                "SELECT constraint_name, r_owner, r_constraint_name, delete_rule "
                "FROM all_constraints%(dblink)s "
                "WHERE UPPER(table_name) = UPPER(:table_name) "
                "AND r_constraint_name IS NOT NULL "
            )
            cons_params = {"table_name": table_name}
            if schema is not None:
                cons_text += "AND owner = CAST(:owner AS VARCHAR2(128)) "
                cons_params["owner"] = schema

            cons_text = cons_text % {"dblink": dblink}
            cons_rows = connection.execute(sql.text(cons_text), cons_params).fetchall()
            for cons_name, r_owner, r_cons_name, delete_rule in cons_rows:
                local_cols = connection.execute(
                    sql.text(
                        "SELECT column_name FROM all_cons_columns%(dblink)s "
                        "WHERE owner = :owner AND constraint_name = :cname "
                        "ORDER BY position"
                        % {"dblink": dblink}
                    ),
                    dict(owner=schema, cname=cons_name),
                ).fetchall()
                remote_cols = connection.execute(
                    sql.text(
                        "SELECT column_name FROM all_cons_columns%(dblink)s "
                        "WHERE owner = :owner AND constraint_name = :cname "
                        "ORDER BY position"
                        % {"dblink": dblink}
                    ),
                    dict(owner=r_owner, cname=r_cons_name),
                ).fetchall()
                remote_table = connection.execute(
                    sql.text(
                        "SELECT table_name FROM all_constraints%(dblink)s "
                        "WHERE owner = :owner AND constraint_name = :cname"
                        % {"dblink": dblink}
                    ),
                    dict(owner=r_owner, cname=r_cons_name),
                ).scalar()

                rows = []
                for (lc,), (rc,) in zip(local_cols, remote_cols):
                    rows.append(
                        (cons_name, lc, remote_table, rc, r_owner, delete_rule)
                    )
                _populate_from_rows(rows)

        if not fkeys:
            # Final fallback: use USER_* views (common when ALL_* is restricted)
            try:
                cache_key = "_yashandb_user_constraints_rcol"
                rcol = getattr(self, cache_key, None)
                if rcol is None:
                    try:
                        keys = connection.execute(
                            sql.text("SELECT * FROM user_constraints WHERE 1=0")
                        ).keys()
                        keys_l = {k.lower() for k in keys}
                    except Exception:
                        keys_l = set()
                    for cand in ("r_constraint_name", "r_cons_name"):
                        if cand in keys_l:
                            rcol = cand
                            break
                    setattr(self, cache_key, rcol)

                cons_rows = []
                if rcol:
                    cons_rows = connection.execute(
                        sql.text(
                            f"SELECT constraint_name, {rcol}, delete_rule "
                            "FROM user_constraints "
                            "WHERE UPPER(table_name) = UPPER(:table_name) "
                            f"AND {rcol} IS NOT NULL"
                        ),
                        dict(table_name=table_name),
                    ).fetchall()
                for cons_name, r_cons_name, delete_rule in cons_rows:
                    local_cols = connection.execute(
                        sql.text(
                            "SELECT column_name FROM user_cons_columns "
                            "WHERE constraint_name = :cname ORDER BY position"
                        ),
                        dict(cname=cons_name),
                    ).fetchall()
                    remote_cols = connection.execute(
                        sql.text(
                            "SELECT column_name FROM user_cons_columns "
                            "WHERE constraint_name = :cname ORDER BY position"
                        ),
                        dict(cname=r_cons_name),
                    ).fetchall()
                    remote_table = connection.execute(
                        sql.text(
                            "SELECT table_name FROM user_constraints "
                            "WHERE constraint_name = :cname"
                        ),
                        dict(cname=r_cons_name),
                    ).scalar()

                    rows = []
                    for (lc,), (rc,) in zip(local_cols, remote_cols):
                        rows.append(
                            (cons_name, lc, remote_table, rc, schema, delete_rule)
                        )
                    _populate_from_rows(rows)
            except Exception:
                pass

        return list(fkeys.values())

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        resolve_synonyms = kw.get("yashandb_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        constraint_data = self._get_constraint_data(
            connection,
            table_name,
            schema,
            dblink,
            info_cache=kw.get("info_cache"),
        )

        unique_keys = filter(lambda x: x[1] == "U", constraint_data)
        uniques_group = groupby(unique_keys, lambda x: x[0])

        index_names = {
            ix["name"] for ix in self.get_indexes(connection, table_name, schema=schema)
        }
        return [
            {
                "name": name,
                "column_names": cols,
                "duplicates_index": name if name in index_names else None,
            }
            for name, cols in [
                [
                    self.normalize_name(i[0]),
                    [self.normalize_name(x[2]) for x in i[1]],
                ]
                for i in uniques_group
            ]
        ]

    @reflection.cache
    def get_view_definition(
        self,
        connection,
        view_name,
        schema=None,
        resolve_synonyms=False,
        dblink="",
        **kw
    ):
        info_cache = kw.get("info_cache")
        (view_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            view_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        params = {"view_name": view_name}
        text = "SELECT text FROM all_views WHERE view_name=:view_name"

        if schema is not None:
            text += " AND owner = :schema"
            params["schema"] = schema

        rp = connection.execute(sql.text(text), params).scalar()
        if rp:
            if util.py2k:
                rp = rp.decode(self.encoding)
            return rp
        else:
            return None

    @reflection.cache
    def get_check_constraints(
        self, connection, table_name, schema=None, include_all=False, **kw
    ):
        resolve_synonyms = kw.get("yashandb_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        constraint_data = self._get_constraint_data(
            connection,
            table_name,
            schema,
            dblink,
            info_cache=kw.get("info_cache"),
        )

        check_constraints = filter(lambda x: x[1] == "C", constraint_data)

        return [
            {"name": self.normalize_name(cons[0]), "sqltext": cons[8]}
            for cons in check_constraints
            if include_all or not re.match(r"..+?. IS NOT NULL$", cons[8])
        ]


class _OuterJoinColumn(sql.ClauseElement):
    __visit_name__ = "outer_join_column"

    def __init__(self, column):
        self.column = column
