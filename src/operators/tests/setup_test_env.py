import tempfile
from pathlib import Path

import pytest

from src import DataPath, CreateData


@pytest.fixture()
def setup_environment():
    tempdir = tempfile.TemporaryDirectory()

    # SETUP FAKE DATA
    path_dir = Path(tempdir.name)

    nrows = 100
    groups = 5
    path_primary = DataPath(str(path_dir), nrows, groups, groups).primary()
    path_secondary = DataPath(str(path_dir), nrows, groups, groups).secondary()

    CreateData(rows=nrows, groups=groups, datadir=str(path_dir)).gen(
        primary=path_primary, secondary=path_secondary
    )

    assert path_primary and path_secondary
    yield {"primary": path_primary, "secondary": path_secondary}

    tempdir.cleanup()
