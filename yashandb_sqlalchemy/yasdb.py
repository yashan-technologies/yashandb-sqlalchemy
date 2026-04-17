# Copyright (C) 2005-2023 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This project (yashandb-sqlalchemy/yashandb_sqlalchemy) is licensed under
# Mulan PSL v2. See the repository root LICENSE file.
#
# This file contains and/or is derived from portions of SQLAlchemy, which is
# licensed under the MIT License. Upstream attribution is retained. See NOTICE.

from __future__ import absolute_import

import decimal
import random
import re

from . import base as yashandb
from .base import YasCompiler
from .base import YasDialect
from .base import YasExecutionContext
from sqlalchemy import exc
from sqlalchemy import processors
from sqlalchemy import types as sqltypes
from sqlalchemy import util
from sqlalchemy.engine import cursor as _cursor
from sqlalchemy.sql import expression
from sqlalchemy.util import compat


class _YasInteger(sqltypes.Integer):
    def get_dbapi_type(self, dbapi):
        return dbapi.INTEGER


class _YasNumeric(sqltypes.Numeric):
    is_number = False

    def bind_processor(self, dialect):
        if self.scale == 0:
            return None
        elif self.asdecimal:
            processor = processors.to_decimal_processor_factory(
                decimal.Decimal, self._effective_decimal_return_scale
            )

            def process(value):
                if isinstance(value, (int, float)):
                    return processor(value)
                elif value is not None and value.is_infinite():
                    return float(value)
                else:
                    return value

            return process
        else:
            return processors.to_float

    def result_processor(self, dialect, coltype):
        return None


class _YasBinaryFloat(_YasNumeric):
    def get_dbapi_type(self, dbapi):
        return dbapi.FLOAT


class _YasBINARY_FLOAT(_YasBinaryFloat, yashandb.BINARY_FLOAT):
    pass


class _YasBINARY_DOUBLE(_YasBinaryFloat, yashandb.BINARY_DOUBLE):
    pass


class _YasNUMBER(_YasNumeric):
    is_number = True


class _YasDate(sqltypes.Date):
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        def process(value):
            return value

        return process


# TODO: the names used across CHAR / VARCHAR / NCHAR / NVARCHAR
# here are inconsistent and not very good
class _YasChar(sqltypes.CHAR):
    def get_dbapi_type(self, dbapi):
        return dbapi.CHAR


class _YasNChar(sqltypes.NCHAR):
    def get_dbapi_type(self, dbapi):
        return dbapi.NCHAR


class _YasUnicodeStringNCHAR(yashandb.NVARCHAR2):
    def get_dbapi_type(self, dbapi):
        return dbapi.NCHAR


class _YasUnicodeStringCHAR(sqltypes.Unicode):
    def get_dbapi_type(self, dbapi):
        return dbapi.VARCHAR


class _YasUnicodeTextNCLOB(yashandb.NCLOB):
    def get_dbapi_type(self, dbapi):
        return dbapi.NCLOB


class _YasUnicodeTextCLOB(sqltypes.UnicodeText):
    def get_dbapi_type(self, dbapi):
        return dbapi.CLOB


class _YasText(sqltypes.Text):
    def get_dbapi_type(self, dbapi):
        return dbapi.CLOB


class _YasLong(yashandb.LONG):
    def get_dbapi_type(self, dbapi):
        return dbapi.VARCHAR


class _YasString(sqltypes.String):
    def get_dbapi_type(self, dbapi):
        return dbapi.VARCHAR


class _YasEnum(sqltypes.Enum):
    def bind_processor(self, dialect):
        enum_proc = sqltypes.Enum.bind_processor(self, dialect)

        def process(value):
            raw_str = enum_proc(value)
            return raw_str

        return process

    def get_dbapi_type(self, dbapi):
        return dbapi.VARCHAR


class _YasBinary(sqltypes.LargeBinary):
    def get_dbapi_type(self, dbapi):
        return dbapi.BLOB

    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        if not dialect.auto_convert_lobs:
            return None
        else:
            return super(_YasBinary, self).result_processor(dialect, coltype)


class _YasInterval(yashandb.INTERVAL):
    def get_dbapi_type(self, dbapi):
        return dbapi.TIMEDELTA


class _YasRaw(yashandb.RAW):
    pass


class _YasRowid(yashandb.ROWID):
    def get_dbapi_type(self, dbapi):
        return dbapi.ROWID


class YasCompiler_yasdb(YasCompiler):
    # yasdb has not this attr
    _yasdb_sql_compiler = True


class YasExecutionContext_yasdb(YasExecutionContext):
    out_parameters = None

    def _generate_out_parameter_vars(self):
        paramIndex = 0
        if self.compiled.returning or self.compiled.has_out_parameters:
            preParamValue = None
            for bindparam in self.compiled.binds.values():
                if bindparam.isoutparam:
                    name = self.compiled.bind_names[bindparam]
                    type_impl = bindparam.type.dialect_impl(self.dialect)

                    dbtype = type_impl.get_dbapi_type(self.dialect.dbapi)

                    yasdbApi = self.dialect.dbapi

                    if dbtype is None:
                        raise exc.InvalidRequestError(
                            "Cannot create out parameter for "
                            "parameter "
                            "%r - its type %r is not supported by"
                            " yasdb" % (bindparam.key, bindparam.type)
                        )

                    if compat.py2k and dbtype in (
                        yasdbApi.CLOB,
                        yasdbApi.NCLOB,
                    ):
                        outconverter = processors.to_unicode_processor_factory(
                            self.dialect.encoding,
                            errors=self.dialect.encoding_errors,
                        )
                        self.out_parameters[name] = self.cursor.var(
                            dbtype,
                            outconverter=lambda value: outconverter(value.read()),
                        )
                    # elif dbtype in (
                    #     yasdbApi.BLOB,
                    #     yasdbApi.CLOB,
                    #     yasdbApi.NCLOB,
                    # ):
                    #     self.out_parameters[name] = self.cursor.var(
                    #         dbtype, outconverter=lambda value: value.read()
                    #     )
                    elif compat.py2k and isinstance(type_impl, sqltypes.Unicode):
                        outconverter = processors.to_unicode_processor_factory(
                            self.dialect.encoding,
                            errors=self.dialect.encoding_errors,
                        )
                        self.out_parameters[name] = self.cursor.var(
                            dbtype, outconverter=outconverter
                        )
                    else:
                        self.out_parameters[name] = self.cursor.var(dbtype)

                    self.parameters[0][paramIndex] = self.out_parameters[name]
                if preParamValue is None or preParamValue != bindparam:
                    paramIndex += 1
                preParamValue = bindparam

    def pre_exec(self):
        if not getattr(self.compiled, "_yasdb_sql_compiler", False):
            return

        self.out_parameters = {}

        self._generate_out_parameter_vars()

        self.include_set_input_sizes = self.dialect._include_setinputsizes

    def post_exec(self):
        if self.compiled and self.out_parameters and self.compiled.returning:
            # create a fake cursor result from the out parameters. unlike
            # get_out_parameter_values(), the result-row handlers here will be
            # applied at the Result level
            returning_params = [
                self.dialect._returningval(self.out_parameters["ret_%d" % i])
                for i in range(len(self.out_parameters))
            ]

            def _returning_col_name(col, i):
                name = getattr(col, "name", None)
                if not name:
                    name = (
                        getattr(col, "_anon_name_label", None)
                        or getattr(col, "_anon_name", None)
                        or ("ret_%d" % i)
                    )
                return name

            fetch_strategy = _cursor.FullyBufferedCursorFetchStrategy(
                self.cursor,
                [
                    (_returning_col_name(col, i), None)
                    for i, col in enumerate(
                        expression._select_iterables(self.compiled.returning)
                    )
                ],
                initial_buffer=[tuple(returning_params)],
            )
            self.cursor_fetch_strategy = fetch_strategy

    def create_cursor(self):
        c = self._dbapi_connection.cursor()
        if self.dialect.arraysize:
            c.arraysize = self.dialect.arraysize

        return c

    def get_out_parameter_values(self, out_param_names):
        # this method should not be called when the compiler has
        # RETURNING as we've turned the has_out_parameters flag set to
        # False.
        assert not self.compiled.returning

        return [
            self.dialect._paramval(self.out_parameters[name])
            for name in out_param_names
        ]


class YasDialect_yasdb(YasDialect):
    supports_statement_cache = True
    execution_ctx_cls = YasExecutionContext_yasdb
    statement_compiler = YasCompiler_yasdb

    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    supports_unicode_statements = True
    supports_unicode_binds = True

    # yasdb set false, todo: ensure require or not ?
    use_setinputsizes = True
    # use_setinputsizes = False

    driver = "yasdb"

    colspecs = {
        sqltypes.Numeric: _YasNumeric,
        sqltypes.Float: _YasNumeric,
        yashandb.BINARY_FLOAT: _YasBINARY_FLOAT,
        yashandb.BINARY_DOUBLE: _YasBINARY_DOUBLE,
        sqltypes.Integer: _YasInteger,
        yashandb.NUMBER: _YasNUMBER,
        sqltypes.Date: _YasDate,
        sqltypes.LargeBinary: _YasBinary,
        sqltypes.Boolean: yashandb._YasBoolean,
        sqltypes.Interval: _YasInterval,
        yashandb.INTERVAL: _YasInterval,
        sqltypes.Text: _YasText,
        sqltypes.String: _YasString,
        sqltypes.UnicodeText: _YasUnicodeTextCLOB,
        sqltypes.CHAR: _YasChar,
        sqltypes.NCHAR: _YasNChar,
        sqltypes.Enum: _YasEnum,
        yashandb.LONG: _YasLong,
        yashandb.RAW: _YasRaw,
        sqltypes.Unicode: _YasUnicodeStringCHAR,
        sqltypes.NVARCHAR: _YasUnicodeStringNCHAR,
        yashandb.NCLOB: _YasUnicodeTextNCLOB,
        yashandb.ROWID: _YasRowid,
    }

    execute_sequence_format = list

    @util.deprecated_params(
        threaded=(
            "1.3",
            "The 'threaded' parameter to the yasdb dialect "
            "is deprecated as a dialect-level argument, and will be removed "
            "in a future release.  As of version 1.3, it defaults to False "
            "rather than True.  The 'threaded' option can be passed to "
            "yasdb directly in the URL query string passed to "
            ":func:`_sa.create_engine`.",
        )
    )
    def __init__(
        self,
        auto_convert_lobs=True,
        coerce_to_unicode=True,
        coerce_to_decimal=True,
        arraysize=50,
        encoding_errors=None,
        threaded=None,
        **kwargs
    ):

        YasDialect.__init__(self, **kwargs)
        self.arraysize = arraysize
        self.encoding_errors = encoding_errors
        self.auto_convert_lobs = auto_convert_lobs
        self.coerce_to_unicode = coerce_to_unicode
        self.coerce_to_decimal = coerce_to_decimal
        if self._use_nchar_for_unicode:
            self.colspecs = self.colspecs.copy()
            self.colspecs[sqltypes.Unicode] = _YasUnicodeStringNCHAR
            self.colspecs[sqltypes.UnicodeText] = _YasUnicodeTextNCLOB

        yasdbDbapi = self.dbapi

        if yasdbDbapi is None:
            self._include_setinputsizes = {}
        else:
            self._include_setinputsizes = {
                yasdbDbapi.CHAR,
                yasdbDbapi.NCHAR,
                _YasChar,
                _YasNChar
            }

            self._paramval = lambda value: value.getvalue()

            # adapt for yashan, temporarily only suppport single value for out parameter
            def _returningval(value):
                try:
                    return value.values[0]
                except IndexError:
                    return None

            self._returningval = _returningval

    def _parse_yasdb_ver(self, version):
        m = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?", version)
        if m:
            return tuple(int(x) for x in m.group(1, 2, 3) if x is not None)
        else:
            return (0, 0, 0)

    @classmethod
    def dbapi(cls):
        import yasdb

        return yasdb

    def initialize(self, connection):
        super(YasDialect_yasdb, self).initialize(connection)
        # yashandb has error if call _detect_decimal_char
        # self._detect_decimal_char(connection)

    def get_isolation_level(self, connection):
        with connection.cursor() as cursor:
            # this is the only way to ensure a transaction is started without
            # actually running DML.   There's no way to see the configured
            # isolation level without getting it from v$transaction which
            # means transaction has to be started.
            outval = cursor.var(str)
            cursor.execute(
                """
                begin
                   :trans_id := dbms_transaction.local_transaction_id( TRUE );
                end;
                """,
                {"trans_id": outval},
            )
            trans_id = outval.getvalue()
            xidusn, xidslot, xidsqn = trans_id.split(".", 2)

            cursor.execute(
                "SELECT CASE BITAND(t.flag, POWER(2, 28)) "
                "WHEN 0 THEN 'READ COMMITTED' "
                "ELSE 'SERIALIZABLE' END AS isolation_level "
                "FROM v$transaction t WHERE "
                "(t.xidusn, t.xidslot, t.xidsqn) = "
                "((:xidusn, :xidslot, :xidsqn))",
                {"xidusn": xidusn, "xidslot": xidslot, "xidsqn": xidsqn},
            )
            row = cursor.fetchone()
            if row is None:
                raise exc.InvalidRequestError("could not retrieve isolation level")
            result = row[0]

        return result

    def set_isolation_level(self, connection, level):
        if hasattr(connection, "dbapi_connection"):
            dbapi_connection = connection.dbapi_connection
        else:
            dbapi_connection = connection
        if level == "AUTOCOMMIT":
            dbapi_connection.autocommit = True
        else:
            dbapi_connection.autocommit = False
            connection.rollback()
            with connection.cursor() as cursor:
                cursor.execute("ALTER SESSION SET ISOLATION_LEVEL=%s" % level)

    def _detect_decimal_char(self, connection):
        # we have the option to change this setting upon connect,
        # or just look at what it is upon connect and convert.
        # to minimize the chance of interference with changes to
        # NLS_TERRITORY or formatting behavior of the DB, we opt
        # to just look at it

        dbapi_connection = connection.connection

        with dbapi_connection.cursor() as cursor:

            def output_type_handler(cursor, name, defaultType, size, precision, scale):
                return cursor.var(self.dbapi.VARCHAR, 255, arraysize=cursor.arraysize)

            cursor.outputtypehandler = output_type_handler
            cursor.execute("SELECT 1.1 FROM DUAL")
            value = cursor.fetchone()[0]

            decimal_char = value.lstrip("0")[1]
            assert not decimal_char[0].isdigit()

        self._decimal_char = decimal_char

        if self._decimal_char != ".":
            _detect_decimal = self._detect_decimal
            _to_decimal = self._to_decimal

            self._detect_decimal = lambda value: _detect_decimal(
                value.replace(self._decimal_char, ".")
            )
            self._to_decimal = lambda value: _to_decimal(
                value.replace(self._decimal_char, ".")
            )

    def _detect_decimal(self, value):
        if "." in value:
            return self._to_decimal(value)
        else:
            return int(value)

    _to_decimal = decimal.Decimal

    def create_connect_args(self, url):
        opts = dict(url.query)

        database = url.database
        port = url.port
        if port:
            port = int(port)
        else:
            port = 1688
        dsn = url.host + ":" + str(port)

        if dsn is not None:
            opts["dsn"] = dsn
        if url.password is not None:
            opts["password"] = url.password
        if url.username is not None:
            opts["user"] = url.username

        def convert_yasdb_constant(value):
            if isinstance(value, util.string_types):
                try:
                    int_val = int(value)
                except ValueError:
                    value = value.upper()
                    return getattr(self.dbapi, value)
                else:
                    return int_val
            else:
                return value

        util.coerce_kw_type(opts, "mode", convert_yasdb_constant)
        util.coerce_kw_type(opts, "threaded", bool)
        util.coerce_kw_type(opts, "events", bool)
        util.coerce_kw_type(opts, "purity", convert_yasdb_constant)
        return ([], opts)

    def _get_server_version_info(self, connection):
        # return tuple(int(x) for x in connection.connection.version.split("."))
        # yashdb yasdb driver has no connection.version
        return tuple(int(x) for x in "23.1.1".split("."))

    def is_disconnect(self, e, connection, cursor):
        (error,) = e.args
        if isinstance(
            e, (self.dbapi.InterfaceError, self.dbapi.DatabaseError)
        ) and "not connected" in str(e):
            return True

        if hasattr(error, "code") and error.code in {
            28,
            3114,
            3113,
            3135,
            1033,
            2396,
        }:
            return True

        if re.match(r"^(?:DPI-1010|DPI-1080|DPY-1001|DPY-4011)", str(e)):
            # DPI-1010: not connected
            # DPI-1080: connection was closed by ORA-3113
            # connection
            # TODO: others?
            return True

        return False

    def create_xid(self):
        """create a two-phase transaction ID.

        this id will be passed to do_begin_twophase(), do_rollback_twophase(),
        do_commit_twophase().  its format is unspecified.

        """

        id_ = random.randint(0, 2**128)
        return (0x1234, "%032x" % id_, "%032x" % 9)

    def do_executemany(self, cursor, statement, parameters, context=None):
        if isinstance(parameters, tuple):
            parameters = list(parameters)
        cursor.executemany(statement, parameters)

    def do_begin_twophase(self, connection, xid):
        connection.connection.begin(*xid)
        connection.connection.info["yasdb_xid"] = xid

    def do_prepare_twophase(self, connection, xid):
        result = connection.connection.prepare()
        connection.info["yasdb_prepared"] = result

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        self.do_rollback(connection.connection)
        # TODO: need to end XA state here

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):

        if not is_prepared:
            self.do_commit(connection.connection)
        else:
            if recover:
                raise NotImplementedError("2pc recovery not implemented for yasdb")
            yac_prepared = connection.info["yasdb_prepared"]
            if yac_prepared:
                self.do_commit(connection.connection)
        # TODO: need to end XA state here

    def do_set_input_sizes(self, cursor, list_of_tuples, context):
        if self.positional:
            # not usually used, here to support if someone is modifying
            # the dialect to use positional style
            # cursor.setinputsizes(
            #     *[dbtype for key, dbtype, sqltype in list_of_tuples]
            # )

            # if dbtype is fixed char/nchar, we should bind parameter by fixed char type so that
            # it can match column value by blank padding strategy.
            replaced_parameters = []
            for parameter_row in context.parameters:
                replaced_parameter_row = []
                for (key, dbtype, sqltypes), pval in zip(list_of_tuples, parameter_row):
                    if dbtype in [self.dbapi.CHAR, self.dbapi.NCHAR]:
                        replaced_parameter_row.append(cursor.var(self.dbapi.CHAR).setvalue(pval))
                    else:
                        replaced_parameter_row.append(pval)
                replaced_parameters.append(replaced_parameter_row)
            context.parameters = replaced_parameters
        else:
            collection = (
                (key, dbtype) for key, dbtype, sqltype in list_of_tuples if dbtype
            )

            if not self.supports_unicode_binds:
                collection = (
                    (self.dialect._encoder(key)[0], dbtype)
                    for key, dbtype in collection
                )

            cursor.setinputsizes(**{key: dbtype for key, dbtype in collection})

    def do_recover_twophase(self, connection):
        raise NotImplementedError("recover two phase query for yasdb not implemented")


dialect = YasDialect_yasdb
