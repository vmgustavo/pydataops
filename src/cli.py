import os
import logging

import click

from src.availability import DTYPES, LIBRARIES

logging.basicConfig(
    filename="logs.log",
    filemode="a",
    format="%(asctime)s | %(levelname)-8s | %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
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

    for groups_arg in map(float, groups):
        if groups_arg.is_integer():
            groups_num = [int(groups_arg)] * len(rows)
        else:
            groups_num = [int(groups_arg * row) for row in rows]

        for nrows, ngroups in zip(rows, groups_num):
            filepaths = (
                DataPath(ctx.obj["directory"], nrows, ngroups, groups_arg).primary(),
                DataPath(ctx.obj["directory"], nrows, ngroups, groups_arg).secondary(),
            )

            if (
                overwrite
                or (not os.path.exists(filepaths[0]))
                or (not os.path.exists(filepaths[1]))
            ):
                logger.info(f"Generating new random data with {nrows} rows and {ngroups} groups.")
                CreateData(rows=nrows, groups=ngroups, datadir=ctx.obj["directory"]).gen(
                    primary=filepaths[0], secondary=filepaths[1]
                )
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
    from src import execute_eval

    execute_eval(ctx.obj["directory"], library, groupby, join, aggregate, rows, groups, samples)


@cli.command()
@click.option(
    "--inpath",
    required=False,
    type=str,
    help="Path of the input directory with the data of the executions",
)
@click.option(
    "--outpath",
    required=True,
    type=str,
    help="Path of the output file that contains all of the executions",
)
@click.pass_context
def union_results(ctx, inpath, outpath):
    import re
    import json
    from glob import glob
    from pathlib import Path

    import pandas as pd
    from tqdm import tqdm

    logger = logging.getLogger("union-results")

    if inpath is None:
        inpath = Path(ctx.obj["directory"]) / "execs__2022-06-29"
    else:
        inpath = Path(inpath)

    outpath = Path(outpath)

    files = glob(str(inpath / "*.json"))

    logger.info(f"Found {len(files)} JSON files in {str(inpath)}")

    jsons = list()
    for file in tqdm(files, desc="Load JSONs"):
        with open(file, "r") as f:
            jsons.append(pd.DataFrame([json.load(f)]))

    df = pd.concat(jsons)

    pattern = re.compile(pattern=r"rows_(\d+)__groups_num_(\d+)__groups_arg_(\d+\w)")

    primary = df["dataset_p"].str.extract(pattern)
    df[["primary.rows", "primary.groups_num", "primary.groups_arg"]] = primary
    secondary = df["dataset_s"].str.extract(pattern)
    df[["secondary.rows", "secondary.groups_num", "secondary.groups_arg"]] = secondary

    df = df.drop(columns=["dataset_p", "dataset_s"])

    df.to_csv(outpath, index=False)


@cli.command()
@click.option(
    "--samples",
    required=True,
    type=int,
    help="Number of samples to generate",
)
@click.pass_context
def run_all(ctx, samples):
    from glob import glob

    from src import DataPath
    from src.availability import LIBRARIES

    logger = logging.getLogger("run-all")

    data_files = glob(ctx.obj["directory"] + "/primary__*.csv")
    rows = list()
    groups = set()
    for file in data_files:
        data_path = DataPath.from_str(file)
        rows.append(data_path.rows)
        groups.add(data_path.groups_arg)

    count = len(LIBRARIES) * len(rows) * len(groups) * samples * (3 + 3 + 2)
    logger.info(f"Number of cases to execute: {count:d}")

    ctx.invoke(
        eval_library,
        library=LIBRARIES,
        groupby=["str", "int", "float"],
        join=["str", "int", "float"],
        aggregate=["int", "float"],
        rows=rows,
        groups=groups,
        samples=samples,
    )


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
