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

    ctx.obj["directory"] = directory


@cli.command()
@click.option(
    "--rows",
    type=int,
    required=True,
    multiple=True,
    help="Number of rows",
)
@click.option(
    "--groups",
    required=True,
    multiple=True,
    help="Percentage of the number of rows to use as number of groups or absolute number of groups",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite existing data files",
)
@click.pass_context
def create_data(ctx, rows, groups, overwrite):
    from src import DataPath, CreateData

    logger = logging.getLogger("create-data")

    # TODO: add information in the path about the origin of the calculation of
    #  the number of groups, if it is a result of a float or an integer spec
    for elem in map(float, groups):
        if elem.is_integer():
            curr_groups = [int(elem)] * len(rows)
        else:
            curr_groups = [int(elem * row) for row in rows]

        for nrows, ngroups in zip(rows, curr_groups):
            filepaths = (
                DataPath(ctx.obj["directory"], nrows, ngroups).primary(),
                DataPath(ctx.obj["directory"], nrows, ngroups).secondary(),
            )

            if (
                overwrite
                or (not os.path.exists(filepaths[0]))
                or (not os.path.exists(filepaths[1]))
            ):
                logger.info(f"Generating new random data with {nrows} rows and {ngroups} groups.")
                CreateData(rows=nrows, groups=ngroups, datadir=ctx.obj["directory"]).gen()
            else:
                logger.info(
                    "Skip data creation."
                    + f" Data with {nrows} rows and {ngroups} groups already exists."
                    + f" Paths: {list(map(str, filepaths))}"
                )


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
    "--rows",
    required=True,
    multiple=True,
    type=int,
    help="Number of rows of the datasets",
)
@click.option(
    "--groups",
    required=True,
    multiple=True,
    help="Percentage of the number of rows to use as number of groups or absolute number of groups",
)
@click.option(
    "--samples",
    required=False,
    default=1,
    type=int,
    help="Number of samples to execute for each operation specified",
)
@click.pass_context
def eval_library(ctx, library, groupby, join, aggregate, rows, groups, samples):
    from itertools import product

    from tqdm import tqdm

    from src.operators import BaseOperator
    from src import DataPath, EvalData, Collector

    logger = logging.getLogger(__name__)
    collector = Collector()

    mapper = {elem.__name__.lower(): elem for elem in BaseOperator.__subclasses__()}
    for elem in map(float, groups):
        if elem.is_integer():
            curr_groups = [int(elem)] * len(rows)
        else:
            curr_groups = [int(elem * row) for row in rows]

        for curr_lib, (curr_rows, curr_groups) in product(library, zip(rows, curr_groups)):
            datapath = DataPath(ctx.obj["directory"], curr_rows, curr_groups)

            if (not datapath.primary().exists()) or (not datapath.secondary().exists()):
                logger.warning(
                    f"The combination of library '{curr_lib}', rows '{curr_rows}' and groups '{curr_groups}'"
                    + " has no available dataset. The current processing step will be skipped."
                )
                continue

            dataset_p = str(datapath.primary())
            dataset_s = str(datapath.secondary())
            assert datapath.primary().exists()

            curr_instance = mapper[f"{curr_lib}operator"](
                paths=(datapath.primary(), datapath.secondary())
            )

            for curr_dtype in tqdm(groupby, ncols=80, desc="GroupBy"):
                for _ in tqdm(range(samples), desc=f"{curr_dtype}"):
                    exec_time = curr_instance.groupby(curr_dtype)
                    collector.save(
                        EvalData(
                            library=curr_lib,
                            operation="groupby",
                            col_dtype=curr_dtype,
                            time=exec_time,
                            dataset_p=dataset_p,
                            dataset_s=None,
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
                            dataset_p=dataset_p,
                            dataset_s=dataset_s,
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
                            dataset_p=dataset_p,
                            dataset_s=None,
                        )
                    )


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
