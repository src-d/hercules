from collections import defaultdict
import sys
from typing import Any, Dict, List, Tuple

import numpy
from scipy.sparse.csr import csr_matrix

from labours.modes.devs import hdbscan_cluster_routed_series, order_commits
from labours.objects import DevDay, ParallelDevData
from labours.plotting import deploy_plot, import_pyplot


def load_devs_parallel(
    ownership: Tuple[List[Any], Dict[Any, Any]],
    couples: Tuple[List[str], csr_matrix],
    devs: Tuple[List[str], Dict[int, Dict[int, DevDay]]],
    max_people: int,
):
    from seriate import seriate

    try:
        from hdbscan import HDBSCAN
    except ImportError as e:
        print(
            "Cannot import ortools: %s\nInstall it from "
            "https://developers.google.com/optimization/install/python/" % e
        )
        sys.exit(1)

    people, owned = ownership
    _, cmatrix = couples
    _, days = devs

    print("calculating - commits")
    commits = defaultdict(int)
    for day, devs in days.items():
        for dev, stats in devs.items():
            commits[people[dev]] += stats.Commits
    chosen = [
        k
        for v, k in sorted(((v, k) for k, v in commits.items()), reverse=True)[
            :max_people
        ]
    ]
    result = {k: ParallelDevData() for k in chosen}
    for k, v in result.items():
        v.commits_rank = chosen.index(k)
        v.commits = commits[k]

    print("calculating - lines")
    lines = defaultdict(int)
    for day, devs in days.items():
        for dev, stats in devs.items():
            lines[people[dev]] += stats.Added + stats.Removed + stats.Changed
    lines_index = {
        k: i
        for i, (_, k) in enumerate(
            sorted(((v, k) for k, v in lines.items() if k in chosen), reverse=True)
        )
    }
    for k, v in result.items():
        v.lines_rank = lines_index[k]
        v.lines = lines[k]

    print("calculating - ownership")
    owned_index = {
        k: i
        for i, (_, k) in enumerate(
            sorted(((owned[k][-1].sum(), k) for k in chosen), reverse=True)
        )
    }
    for k, v in result.items():
        v.ownership_rank = owned_index[k]
        v.ownership = owned[k][-1].sum()

    print("calculating - couples")
    embeddings = numpy.genfromtxt(fname="couples_people_data.tsv", delimiter="\t")[
        [people.index(k) for k in chosen]
    ]
    embeddings /= numpy.linalg.norm(embeddings, axis=1)[:, None]
    cos = embeddings.dot(embeddings.T)
    cos[cos > 1] = 1  # tiny precision faults
    dists = numpy.arccos(cos)
    clusters = HDBSCAN(min_cluster_size=2, metric="precomputed").fit_predict(dists)
    for k, v in result.items():
        v.couples_cluster = clusters[chosen.index(k)]

    couples_order = seriate(dists)
    roll_options = []
    for i in range(len(couples_order)):
        loss = 0
        for k, v in result.items():
            loss += abs(
                v.ownership_rank
                - (couples_order.index(chosen.index(k)) + i) % len(chosen)
            )
        roll_options.append(loss)
    best_roll = numpy.argmin(roll_options)
    couples_order = list(numpy.roll(couples_order, best_roll))
    for k, v in result.items():
        v.couples_index = couples_order.index(chosen.index(k))

    print("calculating - commit series")
    dists, devseries, _, orig_route = order_commits(chosen, days, people)
    keys = list(devseries.keys())
    route = [keys[node] for node in orig_route]
    for roll in range(len(route)):
        loss = 0
        for k, v in result.items():
            i = route.index(people.index(k))
            loss += abs(v.couples_index - ((i + roll) % len(route)))
        roll_options[roll] = loss
    best_roll = numpy.argmin(roll_options)
    route = list(numpy.roll(route, best_roll))
    orig_route = list(numpy.roll(orig_route, best_roll))
    clusters = hdbscan_cluster_routed_series(dists, orig_route)
    for k, v in result.items():
        v.commit_coocc_index = route.index(people.index(k))
        v.commit_coocc_cluster = clusters[v.commit_coocc_index]

    return result


def show_devs_parallel(args, name, start_date, end_date, devs):
    matplotlib, pyplot = import_pyplot(args.backend, args.style)
    from matplotlib.collections import LineCollection

    def solve_equations(x1, y1, x2, y2):
        xcube = (x1 - x2) ** 3
        a = 2 * (y2 - y1) / xcube
        b = 3 * (y1 - y2) * (x1 + x2) / xcube
        c = 6 * (y2 - y1) * x1 * x2 / xcube
        d = y1 - a * x1 ** 3 - b * x1 ** 2 - c * x1
        return a, b, c, d

    # biggest = {k: max(getattr(d, k) for d in devs.values())
    #            for k in ("commits", "lines", "ownership")}
    for k, dev in devs.items():
        points = numpy.array(
            [
                (1, dev.commits_rank),
                (2, dev.lines_rank),
                (3, dev.ownership_rank),
                (4, dev.couples_index),
                (5, dev.commit_coocc_index),
            ],
            dtype=float,
        )
        points[:, 1] = points[:, 1] / len(devs)
        splines = []
        for i in range(len(points) - 1):
            a, b, c, d = solve_equations(*points[i], *points[i + 1])
            x = numpy.linspace(i + 1, i + 2, 100)
            smooth_points = numpy.array(
                [x, a * x ** 3 + b * x ** 2 + c * x + d]
            ).T.reshape(-1, 1, 2)
            splines.append(smooth_points)
        points = numpy.concatenate(splines)
        segments = numpy.concatenate([points[:-1], points[1:]], axis=1)
        lc = LineCollection(segments)
        lc.set_array(numpy.linspace(0, 0.1, segments.shape[0]))
        pyplot.gca().add_collection(lc)

    pyplot.xlim(0, 6)
    pyplot.ylim(-0.1, 1.1)
    deploy_plot("Developers", args.output, args.background)
