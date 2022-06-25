from .. import ModinRayOperator
from .setup_test_env import setup_environment  # noqa


def test_modinray_operator(setup_environment):
    op = ModinRayOperator(
        paths=(str(setup_environment["primary"]), str(setup_environment["secondary"]))
    )
    op.groupby("int")
    op.join("int")
    op.aggregate("int")

    assert True
