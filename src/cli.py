import os
import logging

import click

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
@click.option("--overwrite", is_flag=True, help="Overwrite existing data files")
@click.pass_context
def create_data(ctx, rows, overwrite):
    from src import CreateData

    logger = logging.getLogger("create-data")
    if (
        overwrite
        or (not os.path.exists(ctx.obj["filepaths"][0]))
        or (not os.path.exists(ctx.obj["filepaths"][0]))
    ):
        logger.info("Generating new random data")
        CreateData(rows=rows, filepaths=ctx.obj["filepaths"]).gen()
    else:
        logger.info("Random data already exists. Skip create-data step")


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
    type=click.Choice(
        list(filter(lambda x: "str" not in x, DTYPES)),
        case_sensitive=False,
    ),
    help="Define which type of data to execute the aggregate operation",
)
@click.option(
    "--samples",
    required=False,
    default=1,
    type=int,
    help="Number of samples to execute for each operation specified",
)
@click.pass_context
def eval_library(ctx, library, groupby, join, aggregate, samples):
    from tqdm import tqdm

    from src import EvalData, Collector
    from src.operators import BaseOperator

    collector = Collector()

    mapper = {elem.__name__.lower(): elem for elem in BaseOperator.__subclasses__()}
    for curr_lib in library:
        curr_instance = mapper[f"{curr_lib}operator"](paths=ctx.obj["filepaths"])

        for curr_dtype in tqdm(groupby, ncols=80, desc="GroupBy"):
            for _ in tqdm(range(samples), ncols=80, desc=f"{curr_dtype}"):
                exec_time = curr_instance.groupby(curr_dtype)
                collector.save(
                    EvalData(
                        library=curr_lib,
                        operation="groupby",
                        col_dtype=curr_dtype,
                        time=exec_time,
                        dataset_0=ctx.obj["filepaths"][0],
                        dataset_1=None,
                    )
                )

        for curr_dtype in tqdm(join, ncols=80, desc="Join"):
            for _ in tqdm(range(samples), ncols=80, desc=f"{curr_dtype}"):
                exec_time = curr_instance.join(curr_dtype)
                collector.save(
                    EvalData(
                        library=curr_lib,
                        operation="join",
                        col_dtype=curr_dtype,
                        time=exec_time,
                        dataset_0=ctx.obj["filepaths"][0],
                        dataset_1=ctx.obj["filepaths"][1],
                    )
                )

        for curr_dtype in tqdm(aggregate, ncols=80, desc="Aggregate"):
            for _ in tqdm(range(samples), ncols=80, desc=f"{curr_dtype}", leave=False):
                exec_time = curr_instance.aggregate(curr_dtype)
                collector.save(
                    EvalData(
                        library=curr_lib,
                        operation="aggregate",
                        col_dtype=curr_dtype,
                        time=exec_time,
                        dataset_0=ctx.obj["filepaths"][0],
                        dataset_1=None,
                    )
                )


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
