from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):
    @property
    def empty_inserts_executemany(self):
        return exclusions.closed()
