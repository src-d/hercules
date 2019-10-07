import argparse
from argparse import Namespace
import os
import subprocess
import sys
import time
from typing import List

import numpy

from labours.cors_web_server import web_server
from labours.embeddings import train_embeddings, write_embeddings
from labours.modes.burndown import load_burndown, plot_burndown, plot_many_burndown
from labours.modes.devs import show_devs, show_devs_efforts
from labours.modes.devs_parallel import load_devs_parallel, show_devs_parallel
from labours.modes.languages import show_languages
from labours.modes.old_vs_new import show_old_vs_new
from labours.modes.overwrites import load_overwrites_matrix, plot_overwrites_matrix
from labours.modes.ownership import load_ownership, plot_ownership
from labours.modes.sentiment import show_sentiment_stats
from labours.modes.shotness import show_shotness_stats
from labours.readers import read_input
from labours.utils import import_pandas

# NB: this value is modified within the Dockerfile.
DEFAULT_MATPLOTLIB_BACKEND = None


def list_matplotlib_styles() -> List[str]:
    script = (
        "import sys; from matplotlib import pyplot; "
        "sys.stdout.write(repr(pyplot.style.available))"
    )
    styles = eval(subprocess.check_output([sys.executable, "-c", script]))
    styles.remove("classic")
    return ["default", "classic"] + styles


def parse_args() -> Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o",
        "--output",
        default="",
        help="Path to the output file/directory (empty for display). "
        "If the extension is JSON, the data is saved instead of "
        "the real image.",
    )
    parser.add_argument(
        "-i", "--input", default="-", help="Path to the input file (- for stdin)."
    )
    parser.add_argument(
        "-f", "--input-format", default="auto", choices=["yaml", "pb", "auto"]
    )
    parser.add_argument(
        "--font-size", default=12, type=int, help="Size of the labels and legend."
    )
    parser.add_argument(
        "--style",
        default="ggplot",
        choices=list_matplotlib_styles(),
        help="Plot style to use.",
    )
    parser.add_argument(
        "--backend",
        default=DEFAULT_MATPLOTLIB_BACKEND,
        help="Matplotlib backend to use.",
    )
    parser.add_argument(
        "--background",
        choices=["black", "white"],
        default="white",
        help="Plot's general color scheme.",
    )
    parser.add_argument("--size", help="Axes' size in inches, for example \"12,9\"")
    parser.add_argument(
        "--relative",
        action="store_true",
        help="Occupy 100%% height for every measurement.",
    )
    parser.add_argument("--tmpdir", help="Temporary directory for intermediate files.")
    parser.add_argument(
        "-m",
        "--mode",
        dest="modes",
        default=[],
        action="append",
        choices=[
            "burndown-project",
            "burndown-file",
            "burndown-person",
            "overwrites-matrix",
            "ownership",
            "couples-files",
            "couples-people",
            "couples-shotness",
            "shotness",
            "sentiment",
            "devs",
            "devs-efforts",
            "old-vs-new",
            "run-times",
            "languages",
            "devs-parallel",
            "all",
        ],
        help="What to plot. Can be repeated, e.g. " "-m burndown-project -m run-times",
    )
    parser.add_argument(
        "--resample",
        default="year",
        help="The way to resample the time series. Possible values are: "
        "\"month\", \"year\", \"no\", \"raw\" and pandas offset aliases ("
        "http://pandas.pydata.org/pandas-docs/stable/timeseries.html"
        "#offset-aliases).",
    )
    dateutil_url = (
        "https://dateutil.readthedocs.io/en/stable/parser.html#dateutil.parser.parse"
    )
    parser.add_argument(
        "--start-date",
        help="Start date of time-based plots. Any format is accepted which is "
        "supported by %s" % dateutil_url,
    )
    parser.add_argument(
        "--end-date",
        help="End date of time-based plots. Any format is accepted which is "
        "supported by %s" % dateutil_url,
    )
    parser.add_argument(
        "--disable-projector",
        action="store_true",
        help="Do not run Tensorflow Projector on couples.",
    )
    parser.add_argument(
        "--max-people",
        default=20,
        type=int,
        help="Maximum number of developers in overwrites matrix and people plots.",
    )
    parser.add_argument(
        "--order-ownership-by-time",
        action="store_true",
        help="Sort developers in the ownership plot according to their first "
        "appearance in the history. The default is sorting by the number of "
        "commits.",
    )
    args = parser.parse_args()
    return args


def main() -> None:
    args = parse_args()
    reader = read_input(args)
    header = reader.get_header()
    name = reader.get_name()

    burndown_warning = (
        "Burndown stats were not collected. Re-run hercules with --burndown."
    )
    burndown_files_warning = (
        "Burndown stats for files were not collected. Re-run hercules with "
        "--burndown --burndown-files."
    )
    burndown_people_warning = (
        "Burndown stats for people were not collected. Re-run hercules with "
        "--burndown --burndown-people."
    )
    couples_warning = (
        "Coupling stats were not collected. Re-run hercules with --couples."
    )
    shotness_warning = (
        "Structural hotness stats were not collected. Re-run hercules with "
        "--shotness. Also check --languages - the output may be empty."
    )
    sentiment_warning = (
        "Sentiment stats were not collected. Re-run hercules with --sentiment."
    )
    devs_warning = "Devs stats were not collected. Re-run hercules with --devs."

    def run_times():
        rt = reader.get_run_times()
        pandas = import_pandas()
        series = pandas.to_timedelta(
            pandas.Series(rt).sort_values(ascending=False), unit="s"
        )
        df = pandas.concat([series, series / series.sum()], axis=1)
        df.columns = ["time", "ratio"]
        print(df)

    def project_burndown():
        try:
            full_header = header + reader.get_burndown_parameters()
        except KeyError:
            print("project: " + burndown_warning)
            return
        plot_burndown(
            args,
            "project",
            *load_burndown(
                full_header,
                *reader.get_project_burndown(),
                resample=args.resample,
                interpolation_progress=True,
            ),
        )

    def files_burndown():
        try:
            full_header = header + reader.get_burndown_parameters()
        except KeyError:
            print(burndown_warning)
            return
        try:
            plot_many_burndown(args, "file", full_header, reader.get_files_burndown())
        except KeyError:
            print("files: " + burndown_files_warning)

    def people_burndown():
        try:
            full_header = header + reader.get_burndown_parameters()
        except KeyError:
            print(burndown_warning)
            return
        try:
            plot_many_burndown(
                args, "person", full_header, reader.get_people_burndown()
            )
        except KeyError:
            print("people: " + burndown_people_warning)

    def overwrites_matrix():
        try:

            plot_overwrites_matrix(
                args,
                name,
                *load_overwrites_matrix(
                    *reader.get_people_interaction(), max_people=args.max_people
                ),
            )
            people, matrix = load_overwrites_matrix(
                *reader.get_people_interaction(), max_people=1000000, normalize=False
            )
            from scipy.sparse import csr_matrix

            matrix = matrix[:, 1:]
            matrix = numpy.triu(matrix) + numpy.tril(matrix).T
            matrix = matrix + matrix.T
            matrix = csr_matrix(matrix)
            try:
                write_embeddings(
                    "overwrites",
                    args.output,
                    not args.disable_projector,
                    *train_embeddings(people, matrix, tmpdir=args.tmpdir),
                )
            except AttributeError as e:
                print(
                    "Training the embeddings is not possible: %s: %s",
                    type(e).__name__,
                    e,
                )
        except KeyError:
            print("overwrites_matrix: " + burndown_people_warning)

    def ownership_burndown():
        try:
            full_header = header + reader.get_burndown_parameters()
        except KeyError:
            print(burndown_warning)
            return
        try:
            plot_ownership(
                args,
                name,
                *load_ownership(
                    full_header,
                    *reader.get_ownership_burndown(),
                    max_people=args.max_people,
                    order_by_time=args.order_ownership_by_time,
                ),
            )
        except KeyError:
            print("ownership: " + burndown_people_warning)

    def couples_files():
        try:
            write_embeddings(
                "files",
                args.output,
                not args.disable_projector,
                *train_embeddings(*reader.get_files_coocc(), tmpdir=args.tmpdir),
            )
        except KeyError:
            print(couples_warning)

    def couples_people():
        try:
            write_embeddings(
                "people",
                args.output,
                not args.disable_projector,
                *train_embeddings(*reader.get_people_coocc(), tmpdir=args.tmpdir),
            )
        except KeyError:
            print(couples_warning)

    def couples_shotness():
        try:
            write_embeddings(
                "shotness",
                args.output,
                not args.disable_projector,
                *train_embeddings(*reader.get_shotness_coocc(), tmpdir=args.tmpdir),
            )
        except KeyError:
            print(shotness_warning)

    def shotness():
        try:
            data = reader.get_shotness()
        except KeyError:
            print(shotness_warning)
            return
        show_shotness_stats(data)

    def sentiment():
        try:
            data = reader.get_sentiment()
        except KeyError:
            print(sentiment_warning)
            return
        show_sentiment_stats(
            args, reader.get_name(), args.resample, reader.get_header()[0], data
        )

    def devs():
        try:
            data = reader.get_devs()
        except KeyError:
            print(devs_warning)
            return
        show_devs(
            args,
            reader.get_name(),
            *reader.get_header(),
            *data,
            max_people=args.max_people,
        )

    def devs_efforts():
        try:
            data = reader.get_devs()
        except KeyError:
            print(devs_warning)
            return
        show_devs_efforts(
            args,
            reader.get_name(),
            *reader.get_header(),
            *data,
            max_people=args.max_people,
        )

    def old_vs_new():
        try:
            data = reader.get_devs()
        except KeyError:
            print(devs_warning)
            return
        show_old_vs_new(args, reader.get_name(), *reader.get_header(), *data)

    def languages():
        try:
            data = reader.get_devs()
        except KeyError:
            print(devs_warning)
            return
        show_languages(args, reader.get_name(), *reader.get_header(), *data)

    def devs_parallel():
        try:
            ownership = reader.get_ownership_burndown()
        except KeyError:
            print(burndown_people_warning)
            return
        try:
            couples = reader.get_people_coocc()
        except KeyError:
            print(couples_warning)
            return
        try:
            devs = reader.get_devs()
        except KeyError:
            print(devs_warning)
            return
        show_devs_parallel(
            args,
            reader.get_name(),
            *reader.get_header(),
            load_devs_parallel(ownership, couples, devs, args.max_people),
        )

    modes = {
        "run-times": run_times,
        "burndown-project": project_burndown,
        "burndown-file": files_burndown,
        "burndown-person": people_burndown,
        "overwrites-matrix": overwrites_matrix,
        "ownership": ownership_burndown,
        "couples-files": couples_files,
        "couples-people": couples_people,
        "couples-shotness": couples_shotness,
        "shotness": shotness,
        "sentiment": sentiment,
        "devs": devs,
        "devs-efforts": devs_efforts,
        "old-vs-new": old_vs_new,
        "languages": languages,
        "devs-parallel": devs_parallel,
    }

    if "all" in args.modes:
        all_mode = True
        args.modes = [
            "burndown-project",
            "overwrites-matrix",
            "ownership",
            "couples-files",
            "couples-people",
            "couples-shotness",
            "shotness",
            "devs",
            "devs-efforts",
        ]
    else:
        all_mode = False

    for mode in args.modes:
        if mode not in modes:
            print("Unknown mode: %s" % mode)
            continue

        print("Running: %s" % mode)
        # `args.mode` is required for path determination in the mode functions
        args.mode = "all" if all_mode else mode
        try:
            modes[mode]()
        except ImportError as ie:
            print("A module required by the %s mode was not found: %s" % (mode, ie))
            if not all_mode:
                raise

    if web_server.running:
        secs = int(os.getenv("COUPLES_SERVER_TIME", "60"))
        print("Sleeping for %d seconds, safe to Ctrl-C" % secs)
        sys.stdout.flush()
        try:
            time.sleep(secs)
        except KeyboardInterrupt:
            pass
        web_server.stop()
