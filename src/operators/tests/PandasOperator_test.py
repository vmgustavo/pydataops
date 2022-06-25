from .. import PandasOperator
from .setup_test_env import setup_environment  # noqa


def test_pyspark_operator(setup_environment):
    op = PandasOperator(
        paths=(str(setup_environment["primary"]), str(setup_environment["secondary"]))
    )
    op.groupby("int")
    op.join("int")
    op.aggregate("int")

    assert True
