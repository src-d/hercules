from argparse import Namespace
import contextlib
from datetime import datetime, timedelta
import io
import json
import sys
from typing import List, Tuple, TYPE_CHECKING
import warnings

import numpy
import tqdm

from labours.plotting import apply_plot_style, deploy_plot, get_plot_path, import_pyplot
from labours.utils import default_json, floor_datetime, import_pandas, parse_date

if TYPE_CHECKING:
    from lifelines import KaplanMeierFitter
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


def fit_kaplan_meier(matrix: numpy.ndarray) -> 'KaplanMeierFitter':
    from lifelines import KaplanMeierFitter

    T = []
    W = []
    indexes = numpy.arange(matrix.shape[0], dtype=int)
    entries = numpy.zeros(matrix.shape[0], int)
    dead = set()
    for i in range(1, matrix.shape[1]):
        diff = matrix[:, i - 1] - matrix[:, i]
        entries[diff < 0] = i
        mask = diff > 0
        deaths = diff[mask]
        T.append(numpy.full(len(deaths), i) - entries[indexes[mask]])
        W.append(deaths)
        entered = entries > 0
        entered[0] = True
        dead = dead.union(set(numpy.where((matrix[:, i] == 0) & entered)[0]))
    # add the survivors as censored
    nnzind = entries != 0
    nnzind[0] = True
    nnzind[sorted(dead)] = False
    T.append(numpy.full(nnzind.sum(), matrix.shape[1]) - entries[nnzind])
    W.append(matrix[nnzind, -1])
    T = numpy.concatenate(T)
    E = numpy.ones(len(T), bool)
    E[-nnzind.sum() :] = 0
    W = numpy.concatenate(W)
    if T.size == 0:
        return None
    kmf = KaplanMeierFitter().fit(T, E, weights=W)
    return kmf


def print_survival_function(kmf: 'KaplanMeierFitter', sampling: int) -> None:
    sf = kmf.survival_function_
    sf.index = [timedelta(days=d) for d in sf.index * sampling]
    sf.columns = ["Ratio of survived lines"]
    try:
        print(sf[len(sf) // 6 :: len(sf) // 6].append(sf.tail(1)))
    except ValueError:
        pass


def interpolate_burndown_matrix(
    matrix: numpy.ndarray, granularity: int, sampling: int, progress: bool = False
) -> numpy.ndarray:
    daily = numpy.zeros(
        (matrix.shape[0] * granularity, matrix.shape[1] * sampling), dtype=numpy.float32
    )
    """
    ----------> samples, x
    |
    |
    |
    ⌄
    bands, y
    """
    for y in tqdm.tqdm(range(matrix.shape[0]), disable=(not progress)):
        for x in range(matrix.shape[1]):
            if y * granularity > (x + 1) * sampling:
                # the future is zeros
                continue

            def decay(start_index: int, start_val: float):
                if start_val == 0:
                    return
                k = matrix[y][x] / start_val  # <= 1
                scale = (x + 1) * sampling - start_index
                for i in range(y * granularity, (y + 1) * granularity):
                    initial = daily[i][start_index - 1]
                    for j in range(start_index, (x + 1) * sampling):
                        daily[i][j] = initial * (
                            1 + (k - 1) * (j - start_index + 1) / scale
                        )

            def grow(finish_index: int, finish_val: float):
                initial = matrix[y][x - 1] if x > 0 else 0
                start_index = x * sampling
                if start_index < y * granularity:
                    start_index = y * granularity
                if finish_index == start_index:
                    return
                avg = (finish_val - initial) / (finish_index - start_index)
                for j in range(x * sampling, finish_index):
                    for i in range(start_index, j + 1):
                        daily[i][j] = avg
                # copy [x*g..y*s)
                for j in range(x * sampling, finish_index):
                    for i in range(y * granularity, x * sampling):
                        daily[i][j] = daily[i][j - 1]

            if (y + 1) * granularity >= (x + 1) * sampling:
                # x*granularity <= (y+1)*sampling
                # 1. x*granularity <= y*sampling
                #    y*sampling..(y+1)sampling
                #
                #       x+1
                #        /
                #       /
                #      / y+1  -|
                #     /        |
                #    / y      -|
                #   /
                #  / x
                #
                # 2. x*granularity > y*sampling
                #    x*granularity..(y+1)sampling
                #
                #       x+1
                #        /
                #       /
                #      / y+1  -|
                #     /        |
                #    / x      -|
                #   /
                #  / y
                if y * granularity <= x * sampling:
                    grow((x + 1) * sampling, matrix[y][x])
                elif (x + 1) * sampling > y * granularity:
                    grow((x + 1) * sampling, matrix[y][x])
                    avg = matrix[y][x] / ((x + 1) * sampling - y * granularity)
                    for j in range(y * granularity, (x + 1) * sampling):
                        for i in range(y * granularity, j + 1):
                            daily[i][j] = avg
            elif (y + 1) * granularity >= x * sampling:
                # y*sampling <= (x+1)*granularity < (y+1)sampling
                # y*sampling..(x+1)*granularity
                # (x+1)*granularity..(y+1)sampling
                #        x+1
                #         /\
                #        /  \
                #       /    \
                #      /    y+1
                #     /
                #    y
                v1 = matrix[y][x - 1]
                v2 = matrix[y][x]
                delta = (y + 1) * granularity - x * sampling
                previous = 0
                if x > 0 and (x - 1) * sampling >= y * granularity:
                    # x*g <= (y-1)*s <= y*s <= (x+1)*g <= (y+1)*s
                    #           |________|.......^
                    if x > 1:
                        previous = matrix[y][x - 2]
                    scale = sampling
                else:
                    # (y-1)*s < x*g <= y*s <= (x+1)*g <= (y+1)*s
                    #            |______|.......^
                    scale = sampling if x == 0 else x * sampling - y * granularity
                peak = v1 + (v1 - previous) / scale * delta
                if v2 > peak:
                    # we need to adjust the peak, it may not be less than the decayed value
                    if x < matrix.shape[1] - 1:
                        # y*s <= (x+1)*g <= (y+1)*s < (y+2)*s
                        #           ^.........|_________|
                        k = (v2 - matrix[y][x + 1]) / sampling  # > 0
                        peak = matrix[y][x] + k * (
                            (x + 1) * sampling - (y + 1) * granularity
                        )
                        # peak > v2 > v1
                    else:
                        peak = v2
                        # not enough data to interpolate; this is at least not restricted
                grow((y + 1) * granularity, peak)
                decay((y + 1) * granularity, peak)
            else:
                # (x+1)*granularity < y*sampling
                # y*sampling..(y+1)sampling
                decay(x * sampling, matrix[y][x - 1])
    return daily


def load_burndown(
    header: Tuple[int, int, int, int, float],
    name: str,
    matrix: numpy.ndarray,
    resample: str,
    report_survival: bool = True,
    interpolation_progress: bool = False,
) -> Tuple[str, numpy.ndarray, 'DatetimeIndex', List[int], int, int, str]:
    pandas = import_pandas()

    start, last, sampling, granularity, tick = header
    assert sampling > 0
    assert granularity > 0
    start = floor_datetime(datetime.fromtimestamp(start), tick)
    last = datetime.fromtimestamp(last)
    if report_survival:
        kmf = fit_kaplan_meier(matrix)
        if kmf is not None:
            print_survival_function(kmf, sampling)
    finish = start + timedelta(seconds=matrix.shape[1] * sampling * tick)
    if resample not in ("no", "raw"):
        print("resampling to %s, please wait..." % resample)
        # Interpolate the day x day matrix.
        # Each day brings equal weight in the granularity.
        # Sampling's interpolation is linear.
        daily = interpolate_burndown_matrix(
            matrix=matrix,
            granularity=granularity,
            sampling=sampling,
            progress=interpolation_progress,
        )
        daily[(last - start).days :] = 0
        # Resample the bands
        aliases = {"year": "A", "month": "M", "day": "D"}
        resample = aliases.get(resample, resample)
        periods = 0
        date_granularity_sampling = [start]
        while date_granularity_sampling[-1] < finish:
            periods += 1
            date_granularity_sampling = pandas.date_range(
                start, periods=periods, freq=resample
            )
        if date_granularity_sampling[0] > finish:
            if resample == "A":
                print("too loose resampling - by year, trying by month")
                return load_burndown(
                    header, name, matrix, "month", report_survival=False
                )
            elif resample == "M":
                print("too loose resampling - by month, trying by day")
                return load_burndown(
                    header, name, matrix, "day", report_survival=False
                )
            else:
                raise ValueError("Too loose resampling: %s. Try finer." % resample)
        date_range_sampling = pandas.date_range(
            date_granularity_sampling[0],
            periods=(finish - date_granularity_sampling[0]).days,
            freq="1D",
        )
        # Fill the new square matrix
        matrix = numpy.zeros(
            (len(date_granularity_sampling), len(date_range_sampling)),
            dtype=numpy.float32,
        )
        for i, gdt in enumerate(date_granularity_sampling):
            istart = (date_granularity_sampling[i - 1] - start).days if i > 0 else 0
            ifinish = (gdt - start).days

            for j, sdt in enumerate(date_range_sampling):
                if (sdt - start).days >= istart:
                    break
            matrix[i, j:] = daily[istart:ifinish, (sdt - start).days :].sum(axis=0)
        # Hardcode some cases to improve labels' readability
        if resample in ("year", "A"):
            labels = [dt.year for dt in date_granularity_sampling]
        elif resample in ("month", "M"):
            labels = [dt.strftime("%Y %B") for dt in date_granularity_sampling]
        else:
            labels = [dt.date() for dt in date_granularity_sampling]
    else:
        labels = [
            "%s - %s"
            % (
                (start + timedelta(seconds=i * granularity * tick)).date(),
                (start + timedelta(seconds=(i + 1) * granularity * tick)).date(),
            )
            for i in range(matrix.shape[0])
        ]
        if len(labels) > 18:
            warnings.warn("Too many labels - consider resampling.")
        resample = "M"  # fake resampling type is checked while plotting
        date_range_sampling = pandas.date_range(
            start + timedelta(seconds=sampling * tick),
            periods=matrix.shape[1],
            freq="%dD" % sampling,
        )
    return name, matrix, date_range_sampling, labels, granularity, sampling, resample
