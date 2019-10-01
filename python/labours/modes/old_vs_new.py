from argparse import Namespace
from datetime import datetime, timedelta
from itertools import chain
from typing import Dict, List

import numpy

from labours.objects import DevDay
from labours.plotting import deploy_plot, get_plot_path, import_pyplot


def show_old_vs_new(
    args: Namespace,
    name: str,
    start_date: int,
    end_date: int,
    people: List[str],
    days: Dict[int, Dict[int, DevDay]],
) -> None:
    from scipy.signal import convolve, slepian

    start_date = datetime.fromtimestamp(start_date)
    start_date = datetime(start_date.year, start_date.month, start_date.day)
    end_date = datetime.fromtimestamp(end_date)
    end_date = datetime(end_date.year, end_date.month, end_date.day)
    new_lines = numpy.zeros((end_date - start_date).days + 2)
    old_lines = numpy.zeros_like(new_lines)
    for day, devs in days.items():
        for stats in devs.values():
            new_lines[day] += stats.Added
            old_lines[day] += stats.Removed + stats.Changed
    resolution = 32
    window = slepian(max(len(new_lines) // resolution, 1), 0.5)
    new_lines = convolve(new_lines, window, "same")
    old_lines = convolve(old_lines, window, "same")
    matplotlib, pyplot = import_pyplot(args.backend, args.style)
    plot_x = [start_date + timedelta(days=i) for i in range(len(new_lines))]
    pyplot.fill_between(plot_x, new_lines, color="#8DB843", label="Changed new lines")
    pyplot.fill_between(
        plot_x, old_lines, color="#E14C35", label="Changed existing lines"
    )
    pyplot.legend(loc=2, fontsize=args.font_size)
    for tick in chain(
        pyplot.gca().xaxis.get_major_ticks(), pyplot.gca().yaxis.get_major_ticks()
    ):
        tick.label.set_fontsize(args.font_size)
    if args.mode == "all" and args.output:
        output = get_plot_path(args.output, "old_vs_new")
    else:
        output = args.output
    deploy_plot("Additions vs changes", output, args.background)
