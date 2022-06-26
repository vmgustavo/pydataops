import tempfile
from pathlib import Path

import pytest

from src import DataPath, CreateData

from .. import BaseOperator


@pytest.yield_fixture(scope="session")
def setup_environment():
    tempdir = tempfile.TemporaryDirectory()

    # SETUP FAKE DATA
    path_dir = Path(tempdir.name)

    nrows = 1000
    groups_arg = 0.1
    groups_num = int(groups_arg * nrows)

    path_primary = DataPath(str(path_dir), nrows, groups_num, groups_arg).primary()
    path_secondary = DataPath(str(path_dir), nrows, groups_num, groups_arg).secondary()

    CreateData(rows=nrows, groups=groups_num, datadir=str(path_dir)).gen(
        primary=path_primary, secondary=path_secondary
    )

    assert path_primary and path_secondary
    yield {"primary": path_primary, "secondary": path_secondary}

    tempdir.cleanup()


@pytest.mark.parametrize("operator", BaseOperator.__subclasses__())
def test_operators(setup_environment, operator):
    op = operator(paths=(str(setup_environment["primary"]), str(setup_environment["secondary"])))

    op.groupby("int")
    op.join("int")
    op.aggregate("int")

    assert True
