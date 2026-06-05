# -*- coding: utf-8 -*-
from sqlalchemy.dialects import registry
import pytest

# Only yaspy is registered for tests; yasdb is legacy and not promoted.
registry.register("sqlalchemy.dialects", "yashandb.yaspy", "YasDialect_yaspy")

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")
from sqlalchemy.testing.plugin.pytestplugin import *

from test._project_test_helpers import engine
