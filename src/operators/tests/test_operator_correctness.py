import csv
from pathlib import Path

import pytest

from .. import BaseOperator, PandasOperator


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
        "join": ref_join,
        "groupby": ref_groupby,
        "aggregate": ref_aggregate,
    }


# BaseOperator.__subclasses__()
@pytest.mark.parametrize("operator", [PandasOperator])
def test_operators(setup_environment, operator):
    op = operator(paths=(str(setup_environment["primary"]), str(setup_environment["secondary"])))

    op.groupby("int")
    assert op.last_result_aslist() == setup_environment["groupby"]

    op.join("int")
    assert op.last_result_aslist() == setup_environment["join"]

    op.aggregate("int")
    assert op.last_result_aslist() == setup_environment["aggregate"]

    assert True
