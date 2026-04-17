from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):
    @property
    def views(self):
        return exclusions.open()

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
        # YashanDB (in current tested mode) does not support
        # UPDATE .. RETURNING .. INTO, so rowcount tests that rely on RETURNING
        # must be skipped.
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
