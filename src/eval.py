import logging
from itertools import product

from tqdm import tqdm

from .DataPath import DataPath
from .operators import BaseOperator
from .Collector import EvalData, Collector


def execute_eval(directory, library, groupby, join, aggregate, rows, groups, samples):
    logger = logging.getLogger("eval-library")
    collector = Collector()

    # TODO: better progress monitoring to indicate how many more executions are
    #  left considering all of the algorithms that are yet to run
    mapper = {elem.__name__.lower(): elem for elem in BaseOperator.__subclasses__()}

    count = len(library) * len(rows) * len(groups) * samples * (3 + 3 + 2)

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
            # TODO: add descriptive logging statements

            for curr_dtype in groupby:
                for _ in range(samples):

                    try:
                        exec_time, _ = curr_instance.groupby(curr_dtype)
                        exception = None
                    except Exception as e:
                        exec_time = -1.0
                        exception = e.__class__.__name__

                    collector.save(
                        EvalData(
                            library=curr_lib,
                            operation="groupby",
                            col_dtype=curr_dtype,
                            time=exec_time,
                            dataset_p=dataset_p,
                            dataset_s=None,
                            exception=exception,
                        )
                    )
                    pbar.update(1)

            for curr_dtype in join:
                for _ in range(samples):

                    try:
                        exec_time, _ = curr_instance.join(curr_dtype)
                        exception = None
                    except Exception as e:
                        exec_time = -1.0
                        exception = e.__class__.__name__

                    collector.save(
                        EvalData(
                            library=curr_lib,
                            operation="join",
                            col_dtype=curr_dtype,
                            time=exec_time,
                            dataset_p=dataset_p,
                            dataset_s=dataset_s,
                            exception=exception,
                        )
                    )
                    pbar.update(1)

            for curr_dtype in aggregate:
                for _ in range(samples):

                    try:
                        exec_time, _ = curr_instance.aggregate(curr_dtype)
                        exception = None
                    except Exception as e:
                        exec_time = -1.0
                        exception = e.__class__.__name__

                    collector.save(
                        EvalData(
                            library=curr_lib,
                            operation="aggregate",
                            col_dtype=curr_dtype,
                            time=exec_time,
                            dataset_p=dataset_p,
                            dataset_s=None,
                            exception=exception,
                        )
                    )
                    pbar.update(1)
