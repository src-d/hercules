from argparse import Namespace
from collections import defaultdict
from datetime import datetime, timedelta
import sys
from typing import Dict, List, Set, Tuple

import numpy
import tqdm

from labours.objects import DevDay
from labours.plotting import apply_plot_style, deploy_plot, get_plot_path, import_pyplot
from labours.utils import _format_number


def show_devs(
    args: Namespace,
    name: str,
    start_date: int,
    end_date: int,
    people: List[str],
    days: Dict[int, Dict[int, DevDay]],
    max_people: int = 50,
) -> None:
    from scipy.signal import convolve, slepian

    if len(people) > max_people:
        print("Picking top %s developers by commit count" % max_people)
        # pick top N developers by commit count
        commits = defaultdict(int)
        for devs in days.values():
            for dev, stats in devs.items():
                commits[dev] += stats.Commits
        commits = sorted(((v, k) for k, v in commits.items()), reverse=True)
        chosen_people = {people[k] for _, k in commits[:max_people]}
    else:
        chosen_people = set(people)
    dists, devseries, devstats, route = order_commits(chosen_people, days, people)
    route_map = {v: i for i, v in enumerate(route)}
    # determine clusters
    clusters = hdbscan_cluster_routed_series(dists, route)
    keys = list(devseries.keys())
    route = [keys[node] for node in route]
    print("Plotting")
    # smooth time series
    start_date = datetime.fromtimestamp(start_date)
    start_date = datetime(start_date.year, start_date.month, start_date.day)
    end_date = datetime.fromtimestamp(end_date)
    end_date = datetime(end_date.year, end_date.month, end_date.day)
    size = (end_date - start_date).days + 1
    plot_x = [start_date + timedelta(days=i) for i in range(size)]
    resolution = 64
    window = slepian(size // resolution, 0.5)
    final = numpy.zeros((len(devseries), size), dtype=numpy.float32)
    for i, s in enumerate(devseries.values()):
        arr = numpy.array(s).transpose()
        full_history = numpy.zeros(size, dtype=numpy.float32)
        mask = arr[0] < size
        full_history[arr[0][mask]] = arr[1][mask]
        final[route_map[i]] = convolve(full_history, window, "same")

    matplotlib, pyplot = import_pyplot(args.backend, args.style)
    pyplot.rcParams["figure.figsize"] = (32, 16)
    pyplot.rcParams["font.size"] = args.font_size
    prop_cycle = pyplot.rcParams["axes.prop_cycle"]
    colors = prop_cycle.by_key()["color"]
    fig, axes = pyplot.subplots(final.shape[0], 1)
    try:
        axes = tuple(axes)
    except TypeError:
        axes = axes,
    backgrounds = (
        ("#C4FFDB", "#FFD0CD") if args.background == "white" else ("#05401C", "#40110E")
    )
    max_cluster = numpy.max(clusters)
    for ax, series, cluster, dev_i in zip(axes, final, clusters, route):
        if cluster >= 0:
            color = colors[cluster % len(colors)]
            i = 1
            while color == "#777777":
                color = colors[(max_cluster + i) % len(colors)]
                i += 1
        else:
            # outlier
            color = "#777777"
        ax.fill_between(plot_x, series, color=color)
        ax.set_axis_off()
        author = people[dev_i]
        ax.text(
            0.03,
            0.5,
            author[:36] + (author[36:] and "..."),
            horizontalalignment="right",
            verticalalignment="center",
            transform=ax.transAxes,
            fontsize=args.font_size,
            color="black" if args.background == "white" else "white",
        )
        ds = devstats[dev_i]
        stats = "%5d %8s %8s" % (
            ds[0],
            _format_number(ds[1] - ds[2]),
            _format_number(ds[3]),
        )
        ax.text(
            0.97,
            0.5,
            stats,
            horizontalalignment="left",
            verticalalignment="center",
            transform=ax.transAxes,
            fontsize=args.font_size,
            family="monospace",
            backgroundcolor=backgrounds[ds[1] <= ds[2]],
            color="black" if args.background == "white" else "white",
        )
    axes[0].text(
        0.97,
        1.75,
        " cmts    delta  changed",
        horizontalalignment="left",
        verticalalignment="center",
        transform=axes[0].transAxes,
        fontsize=args.font_size,
        family="monospace",
        color="black" if args.background == "white" else "white",
    )
    axes[-1].set_axis_on()
    target_num_labels = 12
    num_months = (
        (end_date.year - start_date.year) * 12 + end_date.month - start_date.month
    )
    interval = int(numpy.ceil(num_months / target_num_labels))
    if interval >= 8:
        interval = int(numpy.ceil(num_months / (12 * target_num_labels)))
        axes[-1].xaxis.set_major_locator(
            matplotlib.dates.YearLocator(base=max(1, interval // 12))
        )
        axes[-1].xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y"))
    else:
        axes[-1].xaxis.set_major_locator(
            matplotlib.dates.MonthLocator(interval=interval)
        )
        axes[-1].xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m"))
    for tick in axes[-1].xaxis.get_major_ticks():
        tick.label.set_fontsize(args.font_size)
    axes[-1].spines["left"].set_visible(False)
    axes[-1].spines["right"].set_visible(False)
    axes[-1].spines["top"].set_visible(False)
    axes[-1].get_yaxis().set_visible(False)
    axes[-1].set_facecolor((1.0,) * 3 + (0.0,))

    title = ("%s commits" % name) if not args.output else ""
    if args.mode == "all" and args.output:
        output = get_plot_path(args.output, "time_series")
    else:
        output = args.output
    deploy_plot(title, output, args.background)


def order_commits(
    chosen_people: Set[str], days: Dict[int, Dict[int, DevDay]], people: List[str]
) -> Tuple[numpy.ndarray, defaultdict, defaultdict, List[int]]:
    from seriate import seriate

    try:
        from fastdtw import fastdtw
    except ImportError as e:
        print(
            "Cannot import fastdtw: %s\nInstall it from https://github.com/slaypni/fastdtw"
            % e
        )
        sys.exit(1)
    # FIXME(vmarkovtsev): remove once https://github.com/slaypni/fastdtw/pull/28 is merged&released
    try:
        sys.modules[
            "fastdtw.fastdtw"
        ].__norm = lambda p: lambda a, b: numpy.linalg.norm(
            numpy.atleast_1d(a) - numpy.atleast_1d(b), p
        )
    except KeyError:
        # the native extension does not have this bug
        pass

    devseries = defaultdict(list)
    devstats = defaultdict(lambda: DevDay(0, 0, 0, 0, {}))
    for day, devs in sorted(days.items()):
        for dev, stats in devs.items():
            if people[dev] in chosen_people:
                devseries[dev].append((day, stats.Commits))
                devstats[dev] = devstats[dev].add(stats)
    print("Calculating the distance matrix")
    # max-normalize the time series using a sliding window
    series = list(devseries.values())
    for i, s in enumerate(series):
        arr = numpy.array(s).transpose().astype(numpy.float32)
        arr[1] /= arr[1].sum()
        series[i] = arr.transpose()
    # calculate the distance matrix using dynamic time warping
    dists = numpy.full((len(series),) * 2, -100500, dtype=numpy.float32)
    # TODO: what's the total for this progress bar?
    with tqdm.tqdm() as pb:
        for x, serx in enumerate(series):
            dists[x, x] = 0
            for y, sery in enumerate(series[x + 1 :], start=x + 1):
                min_day = int(min(serx[0][0], sery[0][0]))
                max_day = int(max(serx[-1][0], sery[-1][0]))
                arrx = numpy.zeros(max_day - min_day + 1, dtype=numpy.float32)
                arry = numpy.zeros_like(arrx)
                arrx[serx[:, 0].astype(int) - min_day] = serx[:, 1]
                arry[sery[:, 0].astype(int) - min_day] = sery[:, 1]
                # L1 norm
                dist, _ = fastdtw(arrx, arry, radius=5, dist=1)
                dists[x, y] = dists[y, x] = dist
                pb.update()
    print("Ordering the series")
    route = seriate(dists)
    return dists, devseries, devstats, route


def hdbscan_cluster_routed_series(
    dists: numpy.ndarray, route: List[int]
) -> numpy.ndarray:
    try:
        from hdbscan import HDBSCAN
    except ImportError as e:
        print("Cannot import hdbscan: %s" % e)
        sys.exit(1)

    opt_dist_chain = numpy.cumsum(
        numpy.array(
            [0] + [dists[route[i], route[i + 1]] for i in range(len(route) - 1)]
        )
    )
    if len(route) < 2:
        clusters = numpy.zeros(len(route), dtype=int)
    else:
        clusters = HDBSCAN(min_cluster_size=2).fit_predict(opt_dist_chain[:, numpy.newaxis])
    return clusters


def show_devs_efforts(
    args: Namespace,
    name: str,
    start_date: int,
    end_date: int,
    people: List[str],
    days: Dict[int, Dict[int, DevDay]],
    max_people: int,
) -> None:
    from scipy.signal import convolve, slepian

    start_date = datetime.fromtimestamp(start_date)
    start_date = datetime(start_date.year, start_date.month, start_date.day)
    end_date = datetime.fromtimestamp(end_date)
    end_date = datetime(end_date.year, end_date.month, end_date.day)

    efforts_by_dev = defaultdict(int)
    for day, devs in days.items():
        for dev, stats in devs.items():
            efforts_by_dev[dev] += stats.Added + stats.Removed + stats.Changed
    if len(efforts_by_dev) > max_people:
        chosen = {
            v
            for k, v in sorted(
                ((v, k) for k, v in efforts_by_dev.items()), reverse=True
            )[:max_people]
        }
        print("Warning: truncated people to the most active %d" % max_people)
    else:
        chosen = set(efforts_by_dev)
    chosen_efforts = sorted(((efforts_by_dev[k], k) for k in chosen), reverse=True)
    chosen_order = {k: i for i, (_, k) in enumerate(chosen_efforts)}

    efforts = numpy.zeros(
        (len(chosen) + 1, (end_date - start_date).days + 1), dtype=numpy.float32
    )
    for day, devs in days.items():
        if day < efforts.shape[1]:
            for dev, stats in devs.items():
                dev = chosen_order.get(dev, len(chosen_order))
                efforts[dev][day] += stats.Added + stats.Removed + stats.Changed
    efforts_cum = numpy.cumsum(efforts, axis=1)
    window = slepian(10, 0.5)
    window /= window.sum()
    for e in (efforts, efforts_cum):
        for i in range(e.shape[0]):
            ending = e[i][-len(window) * 2 :].copy()
            e[i] = convolve(e[i], window, "same")
            e[i][-len(ending) :] = ending
    matplotlib, pyplot = import_pyplot(args.backend, args.style)
    plot_x = [start_date + timedelta(days=i) for i in range(efforts.shape[1])]

    people = [people[k] for _, k in chosen_efforts] + ["others"]
    for i, name in enumerate(people):
        if len(name) > 40:
            people[i] = name[:37] + "..."

    polys = pyplot.stackplot(plot_x, efforts_cum, labels=people)
    if len(polys) == max_people + 1:
        polys[-1].set_hatch("/")
    polys = pyplot.stackplot(plot_x, -efforts * efforts_cum.max() / efforts.max())
    if len(polys) == max_people + 1:
        polys[-1].set_hatch("/")
    yticks = []
    for tick in pyplot.gca().yaxis.iter_ticks():
        if tick[1] >= 0:
            yticks.append(tick[1])
    pyplot.gca().yaxis.set_ticks(yticks)
    legend = pyplot.legend(loc=2, ncol=2, fontsize=args.font_size)
    apply_plot_style(
        pyplot.gcf(),
        pyplot.gca(),
        legend,
        args.background,
        args.font_size,
        args.size or "16,10",
    )
    if args.mode == "all" and args.output:
        output = get_plot_path(args.output, "efforts")
    else:
        output = args.output
    deploy_plot("Efforts through time (changed lines of code)", output, args.background)
