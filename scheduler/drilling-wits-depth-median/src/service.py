import collections
import itertools
import statistics
from typing import Any, Dict, List, Optional, Union

from src import models


def get_stats(data: List[dict]) -> Dict[str, Union[float, List[Optional[float]]]]:
    # find unique keys from all data dicts
    keys = set(itertools.chain(*data))

    stats: Dict[str, Union[float, List[Optional[float]]]] = {}

    for key in keys:
        values = []

        for datum in data:
            if key not in datum:
                continue

            values.append(datum[key])

        if isinstance(values[0], list):
            # get medians for columns
            stat = [get_median(row=list(row)) for row in itertools.zip_longest(*values)]

            if all(item is None for item in stat):
                stat = None
        else:
            stat = get_median(values)

        if stat is None:
            continue

        stats[key] = stat

    return stats


def get_median(row: List[Any]) -> Optional[float]:
    filtered_row = [item for item in row if isinstance(item, (float, int))]

    if not filtered_row:
        return None

    return statistics.median(filtered_row)


def group_records_by_milestone(
    records: List[models.Record], interval: float
) -> Dict[float, List[models.Record]]:
    milestone_to_records = collections.defaultdict(list)

    for record in records:
        milestone = get_depth_milestone(depth=record.measured_depth, interval=interval)
        milestone_to_records[milestone].append(record)

    return dict(milestone_to_records)


def get_depth_milestone(depth: float, interval: float) -> float:
    """Calculates depth milestone.

    Depth milestone is either left or right bound of the range that current depth
    lies in. Each range has length of interval.

    Returns:
        Right bound if current depth is more than halfway through to the right bound.
        Left bound otherwise.

    Examples:
        Depth --->  interval(3)
                    [ --- ]                         [ --- ]
        milestone_1 ↑ 012 ↑ milestone_2 milestone_3 ↑ 345 ↑ milestone_4

        For current depth of 0 or 1 returns milestone_1.
        For current depth of 5 returns milestone_4.
    """

    n_intervals = depth // interval

    if depth % interval > interval / 2:
        # current depth is more than halfway through to the right bound
        n_intervals += 1

    return n_intervals * interval
