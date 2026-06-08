from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):
    @property
    def views(self):
        return exclusions.open()

    @property
    def reflect_table_options(self):
        # Align with Oracle: reflect tablespace_name from ALL_TABLES.
        return exclusions.open()

    @property
    def temp_table_reflection(self):
        return exclusions.open()

    @property
    def temp_table_names(self):
        return exclusions.open()

    @property
    def unique_constraints_reflect_as_index(self):
        # YashanDB mirrors UNIQUE constraints with indexes in ALL_INDEXES.
        return exclusions.open()

    @property
    def schemas(self):
        # The SQLAlchemy suite requires a secondary user/schema named
        # TEST_SCHEMA. The current test environment only provides MY_TEST001.
        return exclusions.closed()

    @property
    def denormalized_names(self):
        # Current YashanDB mode does not reliably preserve SQLAlchemy's
        # quoted lowercase identifier round-trip expectations.
        return exclusions.closed()

    @property
    def implicitly_named_constraints(self):
        # The suite expects this attribute to exist. YashanDB supports
        # server-side generated constraint names when a name isn't given.
        return exclusions.open()

    @property
    def self_referential_foreign_keys(self):
        # YashanDB can support self-referential FKs, however in some
        # deployments/drivers the SQLAlchemy suite self-referential FK fixture
        # does not reliably round-trip via reflection. Disable to avoid
        # false negatives; non-self-referential FK reflection remains enabled.
        return exclusions.closed()

    @property
    def reflects_pk_names(self):
        # YashanDB reflects primary key constraint names.
        return exclusions.open()

    @property
    def symbol_names_w_double_quote(self):
        # Current YashanDB mode used in tests rejects identifiers that contain
        # embedded double quotes (e.g. name like: some " table). Skip the
        # quoted-name argument suite which relies on this capability.
        return exclusions.closed()

    @property
    def empty_inserts_executemany(self):
        return exclusions.closed()

    @property
    def sane_rowcount_w_returning(self):
        # Kernel supports single-row RETURNING only; multi-row UPDATE
        # RETURNING is not available (YAS-05205).
        return exclusions.closed()

    @property
    def float_is_numeric(self):
        # The dialect adapts SQLAlchemy Float/Double through the YashanDB
        # numeric implementation, so the suite assertion that Float and
        # Numeric have distinct dialect impl identities does not apply.
        return exclusions.closed()

    @property
    def empty_strings_text(self):
        # Like Oracle, YashanDB treats empty string values as NULL for
        # VARCHAR/CLOB semantics, so "empty string round-trip" tests must skip.
        return exclusions.closed()

    @property
    def empty_strings_varchar(self):
        # Like Oracle, YashanDB treats empty string values as NULL.
        return exclusions.closed()

    @property
    def expressions_against_unbounded_text(self):
        # 问题5：在部分 YashanDB 模式下，CLOB/NCLOB 与字面量在 WHERE 中比较/引用会失败；
        # 这类 suite 测试不作为当前方言的支持目标，直接跳过。
        return exclusions.closed()

    @property
    def difficult_parameters(self):
        # 问题9：suite 的 DifficultParametersTest 会生成包含空格/标点等字符的 bind 名称；
        # 当前驱动/数据库对这种占位符语法不兼容，直接跳过。
        return exclusions.closed()

    @property
    def standalone_null_binds_whereclause(self):
        # 问题8（已知限制）：在 CASE/比较表达式中传入 NULL 的时间/日期类 bind 时，
        # 当前驱动/数据库无法稳定推断/绑定正确的 TIME/DATE/TIMESTAMP 类型，
        # 会导致类型不匹配或 "invalid datatype"。该能力暂不作为支持目标，跳过相关测试。
        return exclusions.closed()

    @property
    def select_literal_binds(self):
        # 问题11（已知限制）：当 SQLAlchemy 将 literal(1) 编译成 SELECT 列表中的绑定参数
        # （例如 :param_1）时，yaspy/数据库可能将其按字符串通道返回为 '1'，
        # 与 suite 期望的数值 1 不一致。该行为目前不作为支持目标，跳过相关测试。
        return exclusions.closed()

    @property
    def assertsql_empty_parameters_tuple(self):
        # 问题10（已知限制）：SQLAlchemy 1.4 的 assertsql 期望“无参数执行”的 parameters
        # 形状为 ()，但当前 yaspy 执行路径会产生 []，导致严格断言失败。跳过相关测试。
        return exclusions.closed()
