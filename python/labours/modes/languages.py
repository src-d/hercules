from argparse import Namespace
from collections import defaultdict
from typing import Dict, List

import numpy

from labours.objects import DevDay


def show_languages(
    args: Namespace,
    name: str,
    start_date: int,
    end_date: int,
    people: List[str],
    days: Dict[int, Dict[int, DevDay]],
) -> None:
    devlangs = defaultdict(lambda: defaultdict(lambda: numpy.zeros(3, dtype=int)))
    for day, devs in days.items():
        for dev, stats in devs.items():
            for lang, vals in stats.Languages.items():
                devlangs[dev][lang] += vals
    devlangs = sorted(
        devlangs.items(), key=lambda p: -sum(x.sum() for x in p[1].values())
    )
    for dev, ls in devlangs:
        print()
        print("#", people[dev])
        ls = sorted(((vals.sum(), lang) for lang, vals in ls.items()), reverse=True)
        for vals, lang in ls:
            if lang:
                print("%s: %d" % (lang, vals))
