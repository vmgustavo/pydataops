import csv
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
    op = operator(paths=(str(setup_environment["primary"]), str(setup_environment["secondary"])))

    _, res = op.groupby("int")
    assert op.res_as_list(res) == setup_environment["groupby"]

    _, res = op.join("int")
    assert op.res_as_list(res)[0] == setup_environment["join"][0]

    _, res = op.aggregate("int")
    assert op.res_as_list(res) == setup_environment["aggregate"]

    assert True
