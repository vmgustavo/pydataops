import csv
import tempfile
from pathlib import Path

import pytest

from .. import BaseOperator


@pytest.yield_fixture(scope="session")
def setup_environment():
    root = Path(__file__).parent / "data"

    with open(root / "join.csv", newline="") as f:
        ref_join = list(csv.reader(f, delimiter=","))

    with open(root / "groupby.csv", newline="") as f:
        ref_groupby = list(csv.reader(f, delimiter=","))

    with open(root / "aggregate.csv", newline="") as f:
        ref_aggregate = list(csv.reader(f, delimiter=","))

    yield {
        "primary": str(root / "primary.csv"),
        "secondary": str(root / "secondary.csv"),
        "join": ref_join[1:],
        "groupby": ref_groupby[1:],
        "aggregate": ref_aggregate[1:],
    }


@pytest.mark.parametrize("operator", BaseOperator.__subclasses__())
def test_operators(setup_environment, operator):  # noqa
    tempdir = tempfile.TemporaryDirectory()
    filepath = str(Path(tempdir.name) / "res.csv")

    op = operator(paths=(str(setup_environment["primary"]), str(setup_environment["secondary"])))

    ############################################################################

    _, res = op.groupby("int")
    op.res_to_csv(res, filepath)

    with open(filepath, newline="") as f:
        res_from_csv = list(csv.reader(f, delimiter=","))
    assert res_from_csv[1:] == setup_environment["groupby"]

    ############################################################################

    _, res = op.join("int")
    op.res_to_csv(res, filepath)

    with open(filepath, newline="") as f:
        res_from_csv = list(csv.reader(f, delimiter=","))
    # TODO: fix number of decimal places in the reference csv being different
    #  from the number of decimal places in the current execution
    assert res_from_csv[1:][0] == setup_environment["join"][0]

    ############################################################################

    _, res = op.aggregate("int")
    op.res_to_csv(res, filepath)

    with open(filepath, newline="") as f:
        res_from_csv = list(csv.reader(f, delimiter=","))
    assert res_from_csv[1:] == setup_environment["aggregate"]
