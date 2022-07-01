import os
import logging
from glob import glob
from itertools import product

from tqdm import tqdm

from .DataPath import DataPath
from .operators import BaseOperator
from .Collector import EvalData, Collector

logger = logging.getLogger("eval-library")


def execute_eval(directory, library, groupby, join, aggregate, rows, groups, samples, limit):
    dirpath = os.path.join(directory, "execs")
    collector = Collector(dirpath=dirpath)

    mapper = {elem.__name__.lower(): elem for elem in BaseOperator.__subclasses__()}

    logger.info(
        f"{len(library)} libraries"
        + f" | {len(rows)} rows sets"
        + f" | {len(groups)} groups specs"
        + f" | {samples} samples"
        + " | 8 columns dtypes"
    )
    count = len(library) * len(rows) * len(groups) * samples * (3 + 3 + 2)
    logger.info(f"Total number of evaluations: {count}")

    pbar = tqdm(total=count)
    for groups_arg in map(float, groups):
        if groups_arg.is_integer():
            groups_num = [int(groups_arg)] * len(rows)
        else:
            groups_num = [int(groups_arg * row) for row in rows]

        for curr_lib, (curr_rows, curr_groups) in product(library, zip(rows, groups_num)):
            datapath = DataPath(directory, curr_rows, curr_groups, groups_arg)

            if (not datapath.primary().exists()) or (not datapath.secondary().exists()):
                logger.warning(
                    f"The combination of library '{curr_lib}', rows '{curr_rows}' and groups '{curr_groups}'"
                    + " has no available dataset. The current processing step will be skipped."
                )
                continue

            dataset_p = str(datapath.primary())
            dataset_s = str(datapath.secondary())
            assert datapath.primary().exists()

            curr_instance = mapper[f"{curr_lib}operator"](paths=(dataset_p, dataset_s))

            for curr_dtype in groupby:
                for _ in range(samples):
                    _exec(
                        "groupby",
                        curr_lib,
                        curr_dtype,
                        curr_rows,
                        curr_groups,
                        groups_arg,
                        dataset_p,
                        dataset_s,
                        dirpath,
                        limit,
                        curr_instance,
                        collector,
                    )
                    pbar.update(1)

            for curr_dtype in join:
                for _ in range(samples):
                    _exec(
                        "join",
                        curr_lib,
                        curr_dtype,
                        curr_rows,
                        curr_groups,
                        groups_arg,
                        dataset_p,
                        dataset_s,
                        dirpath,
                        limit,
                        curr_instance,
                        collector,
                    )
                    pbar.update(1)

            for curr_dtype in aggregate:
                for _ in range(samples):
                    _exec(
                        "aggregate",
                        curr_lib,
                        curr_dtype,
                        curr_rows,
                        curr_groups,
                        groups_arg,
                        dataset_p,
                        dataset_s,
                        dirpath,
                        limit,
                        curr_instance,
                        collector,
                    )
                    pbar.update(1)


def _exec(
    operation: str,
    curr_lib: str,
    curr_dtype: str,
    curr_rows: int,
    curr_groups: int,
    groups_arg: float,
    dataset_p: str,
    dataset_s: str,
    dirpath: str,
    limit: int,
    curr_instance,
    collector: Collector,
):
    logger.debug(
        f"library={curr_lib}"
        + f" | operation={operation}"
        + f" | col_dtype={curr_dtype}"
        + f" | dataset_p={dataset_p}"
        + f" | dataset_s={dataset_s}"
    )

    eval_data = EvalData(
        library=curr_lib,
        operation=operation,
        col_dtype=curr_dtype,
        time=None,
        rows=curr_rows,
        groups=curr_groups,
        groups_arg=groups_arg,
        dataset_p=dataset_p,
        dataset_s=dataset_s,
        exception=None,
    )

    fname = eval_data.filename()

    execs = len(glob(os.path.join(dirpath, fname) + "*.json"))
    if (limit is None) or (execs < limit):
        try:
            exec_time, _ = getattr(curr_instance, operation)(curr_dtype)
            exception = None
        except Exception as e:
            exec_time = -1.0
            exception = e.__class__.__name__
            logger.error(str(e))

        eval_data.time = exec_time
        eval_data.exception = exception

        collector.save(eval_data)
    else:
        logger.info(
            f"Current number of samples ({execs}) reached the limit ({limit}),"
            + f" skipping {fname}"
        )
