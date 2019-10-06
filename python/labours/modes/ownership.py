from datetime import datetime, timedelta
import json
from typing import Any, Dict, List, Tuple

import numpy

from labours.plotting import apply_plot_style, deploy_plot, get_plot_path, import_pyplot
from labours.utils import default_json, floor_datetime, import_pandas, parse_date


def load_ownership(
    header: Tuple[int, int, int, int, float],
    sequence: List[Any],
    contents: Dict[Any, Any],
    max_people: int,
    order_by_time: bool,
):
    pandas = import_pandas()

    start, last, sampling, _, tick = header
    start = datetime.fromtimestamp(start)
    start = floor_datetime(start, tick)
    last = datetime.fromtimestamp(last)
    people = []
    for name in sequence:
        people.append(contents[name].sum(axis=1))
    people = numpy.array(people)
    date_range_sampling = pandas.date_range(
        start + timedelta(seconds=sampling * tick),
        periods=people[0].shape[0],
        freq="%dD" % sampling,
    )

    if people.shape[0] > max_people:
        chosen = numpy.argpartition(-numpy.sum(people, axis=1), max_people)
        others = people[chosen[max_people:]].sum(axis=0)
        people = people[chosen[: max_people + 1]]
        people[max_people] = others
        sequence = [sequence[i] for i in chosen[:max_people]] + ["others"]
        print("Warning: truncated people to the most owning %d" % max_people)

    if order_by_time:
        appearances = numpy.argmax(people > 0, axis=1)
        if people.shape[0] > max_people:
            appearances[-1] = people.shape[1]
    else:
        appearances = -people.sum(axis=1)
        if people.shape[0] > max_people:
            appearances[-1] = 0
    order = numpy.argsort(appearances)
    people = people[order]
    sequence = [sequence[i] for i in order]

    for i, name in enumerate(sequence):
        if len(name) > 40:
            sequence[i] = name[:37] + "..."
    return sequence, people, date_range_sampling, last


def plot_ownership(args, repo, names, people, date_range, last):
    if args.output and args.output.endswith(".json"):
        data = locals().copy()
        del data["args"]
        data["type"] = "ownership"
        if args.mode == "all" and args.output:
            output = get_plot_path(args.output, "people")
        else:
            output = args.output
        with open(output, "w") as fout:
            json.dump(data, fout, sort_keys=True, default=default_json)
        return

    matplotlib, pyplot = import_pyplot(args.backend, args.style)

    polys = pyplot.stackplot(date_range, people, labels=names)
    if names[-1] == "others":
        polys[-1].set_hatch("/")
    pyplot.xlim(
        parse_date(args.start_date, date_range[0]), parse_date(args.end_date, last)
    )

    if args.relative:
        for i in range(people.shape[1]):
            people[:, i] /= people[:, i].sum()
        pyplot.ylim(0, 1)
        legend_loc = 3
    else:
        legend_loc = 2
    ncol = 1 if len(names) < 15 else 2
    legend = pyplot.legend(loc=legend_loc, fontsize=args.font_size, ncol=ncol)
    apply_plot_style(
        pyplot.gcf(), pyplot.gca(), legend, args.background, args.font_size, args.size
    )
    if args.mode == "all" and args.output:
        output = get_plot_path(args.output, "people")
    else:
        output = args.output
    deploy_plot("%s code ownership through time" % repo, output, args.background)
