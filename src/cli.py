import math
import logging

import click
from tqdm import tqdm


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.option("-r", "--rows", type=int, required=True, default=int(1e6), help="Number of rows")
@click.option("-d", "--directory", type=str, default="data", help="Name of the data directory")
def create_data(rows, directory):
    import os
    import csv

    from faker import Faker

    logging.basicConfig(
        format="%(asctime)s | %(levelname)-8s | %(name)s : %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

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

    if not os.path.exists(directory):
        logger.info(f"Create {directory} directory")
        os.makedirs(directory, exist_ok=True)
    else:
        logger.info(f"Directory {directory} already exists")

    csvfile = os.path.join(directory, "data.csv")
    if not os.path.exists(csvfile):
        logger.info(f"Create CSV data file")
        with open(csvfile, "w") as f:
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
    else:
        logger.info(f"CSV data file already exists")

    filesize = os.path.getsize(csvfile) / math.pow(2, 10) / math.pow(2, 10)
    logger.info(f"File size: {filesize:.01f} MiB")


@cli.command()
@click.pass_context
def eval_pandas(ctx):
    pass


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
