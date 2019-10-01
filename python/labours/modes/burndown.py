from argparse import Namespace
import contextlib
import io
import json
import sys
from typing import List, TYPE_CHECKING

import numpy
import tqdm

from labours.burndown import load_burndown
from labours.plotting import apply_plot_style, deploy_plot, get_plot_path, import_pyplot
from labours.utils import default_json, parse_date

if TYPE_CHECKING:
    from pandas.core.indexes.datetimes import DatetimeIndex


def plot_burndown(
    args: Namespace,
    target: str,
    name: str,
    matrix: numpy.ndarray,
    date_range_sampling: 'DatetimeIndex',
    labels: List[int],
    granularity: int,
    sampling: int,
    resample: str,
) -> None:
    if args.output and args.output.endswith(".json"):
        data = locals().copy()
        del data["args"]
        data["type"] = "burndown"
        if args.mode == "project" and target == "project":
            output = args.output
        else:
            if target == "project":
                name = "project"
            output = get_plot_path(args.output, name)
        with open(output, "w") as fout:
            json.dump(data, fout, sort_keys=True, default=default_json)
        return

    matplotlib, pyplot = import_pyplot(args.backend, args.style)

    pyplot.stackplot(date_range_sampling, matrix, labels=labels)
    if args.relative:
        for i in range(matrix.shape[1]):
            matrix[:, i] /= matrix[:, i].sum()
        pyplot.ylim(0, 1)
        legend_loc = 3
    else:
        legend_loc = 2
    legend = pyplot.legend(loc=legend_loc, fontsize=args.font_size)
    pyplot.ylabel("Lines of code")
    pyplot.xlabel("Time")
    apply_plot_style(
        pyplot.gcf(), pyplot.gca(), legend, args.background, args.font_size, args.size
    )
    pyplot.xlim(
        parse_date(args.start_date, date_range_sampling[0]),
        parse_date(args.end_date, date_range_sampling[-1]),
    )
    locator = pyplot.gca().xaxis.get_major_locator()
    # set the optimal xticks locator
    if "M" not in resample:
        pyplot.gca().xaxis.set_major_locator(matplotlib.dates.YearLocator())
    locs = pyplot.gca().get_xticks().tolist()
    if len(locs) >= 16:
        pyplot.gca().xaxis.set_major_locator(matplotlib.dates.YearLocator())
        locs = pyplot.gca().get_xticks().tolist()
        if len(locs) >= 16:
            pyplot.gca().xaxis.set_major_locator(locator)
    if locs[0] < pyplot.xlim()[0]:
        del locs[0]
    endindex = -1
    if len(locs) >= 2 and pyplot.xlim()[1] - locs[-1] > (locs[-1] - locs[-2]) / 2:
        locs.append(pyplot.xlim()[1])
        endindex = len(locs) - 1
    startindex = -1
    if len(locs) >= 2 and locs[0] - pyplot.xlim()[0] > (locs[1] - locs[0]) / 2:
        locs.append(pyplot.xlim()[0])
        startindex = len(locs) - 1
    pyplot.gca().set_xticks(locs)
    # hacking time!
    labels = pyplot.gca().get_xticklabels()
    if startindex >= 0:
        labels[startindex].set_text(date_range_sampling[0].date())
        labels[startindex].set_text = lambda _: None
        labels[startindex].set_rotation(30)
        labels[startindex].set_ha("right")
    if endindex >= 0:
        labels[endindex].set_text(date_range_sampling[-1].date())
        labels[endindex].set_text = lambda _: None
        labels[endindex].set_rotation(30)
        labels[endindex].set_ha("right")
    title = "%s %d x %d (granularity %d, sampling %d)" % (
        (name,) + matrix.shape + (granularity, sampling)
    )
    output = args.output
    if output:
        if args.mode == "project" and target == "project":
            output = args.output
        else:
            if target == "project":
                name = "project"
            output = get_plot_path(args.output, name)
    deploy_plot(title, output, args.background)


def plot_many_burndown(args: Namespace, target: str, header, parts):
    if not args.output:
        print("Warning: output not set, showing %d plots." % len(parts))
    stdout = io.StringIO()
    for name, matrix in tqdm.tqdm(parts):
        with contextlib.redirect_stdout(stdout):
            plot_burndown(
                args, target, *load_burndown(header, name, matrix, args.resample)
            )
    sys.stdout.write(stdout.getvalue())
