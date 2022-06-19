import os
import math
import logging

import click
from tqdm import tqdm

from src.availability import DTYPES, LIBRARIES

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


@click.group()
@click.option("-d", "--directory", type=str, default="data", help="Name of the data directory")
@click.pass_context
def cli(ctx, directory):
    logger = logging.getLogger("cli")

    if not os.path.exists(directory):
        logger.info(f"Create {directory} directory")
        os.makedirs(directory, exist_ok=True)
    else:
        logger.info(f"Directory {directory} already exists")

    ctx.obj["filepaths"] = (
        os.path.join(directory, "data_0.csv"),
        os.path.join(directory, "data_1.csv"),
    )


@cli.command()
@click.option("-r", "--rows", type=int, required=True, default=int(1e3), help="Number of rows")
@click.pass_context
def create_data(ctx, rows):
    import csv

    from faker import Faker

    logger = logging.getLogger("create-data")

    fkr = Faker(["en_US"])

    def fake_row():
        return [
            fkr.name(),
            fkr.safe_color_name(),
            fkr.pyfloat(right_digits=3, min_value=-100, max_value=100),
            fkr.pyint(min_value=0, max_value=100, step=1),
            fkr.android_platform_token(),
            fkr.boolean(),
        ]

    def create_csv(filepath: str):
        # TODO: guarantee existence of index column for each dataset to make it
        #  possible to join both datasets later
        with open(filepath, "w") as f:
            writer = csv.writer(f, dialect="unix", delimiter=",")
            writer.writerow(
                [
                    "name",
                    "color",
                    "value_float",
                    "value_int",
                    "value_bool",
                    "android_platform",
                ]
            )
            for _ in tqdm(range(rows)):
                writer.writerow(fake_row())

    if not os.path.exists(ctx.obj["filepaths"][0]):
        logger.info("Create first CSV data file")
        create_csv(ctx.obj["filepaths"][0])
    else:
        logger.info(f"First CSV data file already exists")

    if not os.path.exists(ctx.obj["filepaths"][1]):
        logger.info("Create second CSV data file")
        create_csv(ctx.obj["filepaths"][1])
    else:
        logger.info("Second CSV data file already exists")

    filesizes = [
        f"{os.path.getsize(filepath) / math.pow(2, 10) / math.pow(2, 10):.01f} MiB"
        for filepath in ctx.obj["filepaths"]
    ]
    logger.info(f"Files size: {filesizes}")


@cli.command()
@click.option(
    "--library",
    required=True,
    multiple=True,
    type=click.Choice(LIBRARIES, case_sensitive=False),
    help="Define which libraries to execute the operations",
)
@click.option(
    "--groupby",
    required=False,
    multiple=True,
    type=click.Choice(DTYPES, case_sensitive=False),
    help="Define which type of data to execute the groupy operation",
)
@click.option(
    "--join",
    required=False,
    multiple=True,
    type=click.Choice(DTYPES, case_sensitive=False),
    help="Define which type of data to execute the join operation",
)
@click.option(
    "--aggregate",
    required=False,
    multiple=True,
    type=click.Choice(DTYPES, case_sensitive=False),
    help="Define which type of data to execute the aggregate operation",
)
@click.pass_context
def eval_library(ctx, library, groupby, join, aggregate):
    from src.operators import BaseOperator

    mapper = {elem.__name__.lower(): elem for elem in BaseOperator.__subclasses__()}
    for curr_lib in library:
        curr_instance = mapper[f"{curr_lib}operator"](paths=ctx.obj["filepaths"])

        for curr_dtype in groupby:
            curr_instance.groupby(curr_dtype)

        for curr_dtype in join:
            curr_instance.join(curr_dtype)

        for curr_dtype in aggregate:
            curr_instance.aggregate(curr_dtype)


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
