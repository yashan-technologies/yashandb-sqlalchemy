from __future__ import annotations

import importlib.util
from pathlib import Path

from sqlalchemy.testing import fixtures


class TestProjectOrmSmoke(fixtures.TestBase):
    __backend__ = True

    def test_orm_smoke_script_main_passes(self):
        smoke_path = Path(__file__).with_name("orm_smoke.py")
        spec = importlib.util.spec_from_file_location("orm_smoke_project", smoke_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

        assert module.main() == 0
