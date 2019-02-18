#!/usr/bin/env python3
import argparse
from collections import defaultdict, namedtuple
from datetime import datetime, timedelta
from importlib import import_module
import io
from itertools import chain
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import warnings


try:
    from clint.textui import progress
except ImportError:
    print("Warning: clint is not installed, no fancy progressbars in the terminal for you.")
    progress = None
import numpy
import yaml


if sys.version_info[0] < 3:
    # OK, ancients, I will support Python 2, but you owe me a beer
    input = raw_input  # noqa: F821


def list_matplotlib_styles():
    script = "import sys; from matplotlib import pyplot; " \
             "sys.stdout.write(repr(pyplot.style.available))"
    styles = eval(subprocess.check_output([sys.executable, "-c", script]))
    styles.remove("classic")
    return ["default", "classic"] + styles


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", default="",
                        help="Path to the output file/directory (empty for display). "
                             "If the extension is JSON, the data is saved instead of "
                             "the real image.")
    parser.add_argument("-i", "--input", default="-",
                        help="Path to the input file (- for stdin).")
    parser.add_argument("-f", "--input-format", default="auto", choices=["yaml", "pb", "auto"])
    parser.add_argument("--font-size", default=12, type=int,
                        help="Size of the labels and legend.")
    parser.add_argument("--style", default="ggplot", choices=list_matplotlib_styles(),
                        help="Plot style to use.")
    parser.add_argument("--backend", help="Matplotlib backend to use.")
    parser.add_argument("--background", choices=["black", "white"], default="white",
                        help="Plot's general color scheme.")
    parser.add_argument("--size", help="Axes' size in inches, for example \"12,9\"")
    parser.add_argument("--relative", action="store_true",
                        help="Occupy 100%% height for every measurement.")
    parser.add_argument("--couples-tmp-dir", help="Temporary directory to work with couples.")
    parser.add_argument("-m", "--mode",
                        choices=["burndown-project", "burndown-file", "burndown-person",
                                 "churn-matrix", "ownership", "couples", "shotness", "sentiment",
                                 "devs", "old-vs-new", "all", "run-times", "languages"],
                        help="What to plot.")
    parser.add_argument(
        "--resample", default="year",
        help="The way to resample the time series. Possible values are: "
             "\"month\", \"year\", \"no\", \"raw\" and pandas offset aliases ("
             "http://pandas.pydata.org/pandas-docs/stable/timeseries.html"
             "#offset-aliases).")
    parser.add_argument("--disable-projector", action="store_true",
                        help="Do not run Tensorflow Projector on couples.")
    parser.add_argument("--max-people", default=20, type=int,
                        help="Maximum number of developers in churn matrix and people plots.")
    args = parser.parse_args()
    return args


class Reader(object):
    def read(self, file):
        raise NotImplementedError

    def get_name(self):
        raise NotImplementedError

    def get_header(self):
        raise NotImplementedError

    def get_burndown_parameters(self):
        raise NotImplementedError

    def get_project_burndown(self):
        raise NotImplementedError

    def get_files_burndown(self):
        raise NotImplementedError

    def get_people_burndown(self):
        raise NotImplementedError

    def get_ownership_burndown(self):
        raise NotImplementedError

    def get_people_interaction(self):
        raise NotImplementedError

    def get_files_coocc(self):
        raise NotImplementedError

    def get_people_coocc(self):
        raise NotImplementedError

    def get_shotness_coocc(self):
        raise NotImplementedError

    def get_shotness(self):
        raise NotImplementedError

    def get_sentiment(self):
        raise NotImplementedError

    def get_devs(self):
        raise NotImplementedError


class YamlReader(Reader):
    def read(self, file):
        yaml.reader.Reader.NON_PRINTABLE = re.compile(r"(?!x)x")
        try:
            loader = yaml.CLoader
        except AttributeError:
            print("Warning: failed to import yaml.CLoader, falling back to slow yaml.Loader")
            loader = yaml.Loader
        try:
            if file != "-":
                with open(file) as fin:
                    data = yaml.load(fin, Loader=loader)
            else:
                data = yaml.load(sys.stdin, Loader=loader)
        except (UnicodeEncodeError, yaml.reader.ReaderError) as e:
            print("\nInvalid unicode in the input: %s\nPlease filter it through "
                  "fix_yaml_unicode.py" % e)
            sys.exit(1)
        if data is None:
            print("\nNo data has been read - has Hercules crashed?")
            sys.exit(1)
        self.data = data

    def get_run_times(self):
        return {}

    def get_name(self):
        return self.data["hercules"]["repository"]

    def get_header(self):
        header = self.data["hercules"]
        return header["begin_unix_time"], header["end_unix_time"]

    def get_burndown_parameters(self):
        header = self.data["Burndown"]
        return header["sampling"], header["granularity"]

    def get_project_burndown(self):
        return self.data["hercules"]["repository"], \
            self._parse_burndown_matrix(self.data["Burndown"]["project"]).T

    def get_files_burndown(self):
        return [(p[0], self._parse_burndown_matrix(p[1]).T)
                for p in self.data["Burndown"]["files"].items()]

    def get_people_burndown(self):
        return [(p[0], self._parse_burndown_matrix(p[1]).T)
                for p in self.data["Burndown"]["people"].items()]

    def get_ownership_burndown(self):
        return self.data["Burndown"]["people_sequence"].copy(), \
            {p[0]: self._parse_burndown_matrix(p[1])
             for p in self.data["Burndown"]["people"].items()}

    def get_people_interaction(self):
        return self.data["Burndown"]["people_sequence"].copy(), \
            self._parse_burndown_matrix(self.data["Burndown"]["people_interaction"])

    def get_files_coocc(self):
        coocc = self.data["Couples"]["files_coocc"]
        return coocc["index"], self._parse_coocc_matrix(coocc["matrix"])

    def get_people_coocc(self):
        coocc = self.data["Couples"]["people_coocc"]
        return coocc["index"], self._parse_coocc_matrix(coocc["matrix"])

    def get_shotness_coocc(self):
        shotness = self.data["Shotness"]
        index = ["%s:%s" % (i["file"], i["name"]) for i in shotness]
        indptr = numpy.zeros(len(shotness) + 1, dtype=numpy.int64)
        indices = []
        data = []
        for i, record in enumerate(shotness):
            pairs = [(int(k), v) for k, v in record["counters"].items()]
            pairs.sort()
            indptr[i + 1] = indptr[i] + len(pairs)
            for k, v in pairs:
                indices.append(k)
                data.append(v)
        indices = numpy.array(indices, dtype=numpy.int32)
        data = numpy.array(data, dtype=numpy.int32)
        from scipy.sparse import csr_matrix
        return index, csr_matrix((data, indices, indptr), shape=(len(shotness),) * 2)

    def get_shotness(self):
        from munch import munchify
        obj = munchify(self.data["Shotness"])
        # turn strings into ints
        for item in obj:
            item.counters = {int(k): v for k, v in item.counters.items()}
        if len(obj) == 0:
            raise KeyError
        return obj

    def get_sentiment(self):
        from munch import munchify
        return munchify({int(key): {
            "Comments": vals[2].split("|"),
            "Commits": vals[1],
            "Value": float(vals[0])
        } for key, vals in self.data["Sentiment"].items()})

    def get_devs(self):
        people = self.data["Devs"]["people"]
        days = {int(d): {int(dev): DevDay(*(int(x) for x in day[:-1]), day[-1])
                         for dev, day in devs.items()}
                for d, devs in self.data["Devs"]["days"].items()}
        return days, people

    def _parse_burndown_matrix(self, matrix):
        return numpy.array([numpy.fromstring(line, dtype=int, sep=" ")
                            for line in matrix.split("\n")])

    def _parse_coocc_matrix(self, matrix):
        from scipy.sparse import csr_matrix
        data = []
        indices = []
        indptr = [0]
        for row in matrix:
            for k, v in sorted(row.items()):
                data.append(v)
                indices.append(k)
            indptr.append(indptr[-1] + len(row))
        return csr_matrix((data, indices, indptr), shape=(len(matrix),) * 2)


class ProtobufReader(Reader):
    def read(self, file):
        try:
            from internal.pb.pb_pb2 import AnalysisResults
        except ImportError as e:
            print("\n\n>>> You need to generate internal/pb/pb_pb2.py - run \"make\"\n",
                  file=sys.stderr)
            raise e from None
        self.data = AnalysisResults()
        if file != "-":
            with open(file, "rb") as fin:
                bytes = fin.read()
        else:
            bytes = sys.stdin.buffer.read()
        if not bytes:
            raise ValueError("empty input")
        self.data.ParseFromString(bytes)
        self.contents = {}
        for key, val in self.data.contents.items():
            try:
                mod, name = PB_MESSAGES[key].rsplit(".", 1)
            except KeyError:
                sys.stderr.write("Warning: there is no registered PB decoder for %s\n" % key)
                continue
            cls = getattr(import_module(mod), name)
            self.contents[key] = msg = cls()
            msg.ParseFromString(val)

    def get_run_times(self):
        return {key: val for key, val in self.data.header.run_time_per_item.items()}

    def get_name(self):
        return self.data.header.repository

    def get_header(self):
        header = self.data.header
        return header.begin_unix_time, header.end_unix_time

    def get_burndown_parameters(self):
        burndown = self.contents["Burndown"]
        return burndown.sampling, burndown.granularity

    def get_project_burndown(self):
        return self._parse_burndown_matrix(self.contents["Burndown"].project)

    def get_files_burndown(self):
        return [self._parse_burndown_matrix(i) for i in self.contents["Burndown"].files]

    def get_people_burndown(self):
        return [self._parse_burndown_matrix(i) for i in self.contents["Burndown"].people]

    def get_ownership_burndown(self):
        people = self.get_people_burndown()
        return [p[0] for p in people], {p[0]: p[1].T for p in people}

    def get_people_interaction(self):
        burndown = self.contents["Burndown"]
        return [i.name for i in burndown.people], \
            self._parse_sparse_matrix(burndown.people_interaction).toarray()

    def get_files_coocc(self):
        node = self.contents["Couples"].file_couples
        return list(node.index), self._parse_sparse_matrix(node.matrix)

    def get_people_coocc(self):
        node = self.contents["Couples"].people_couples
        return list(node.index), self._parse_sparse_matrix(node.matrix)

    def get_shotness_coocc(self):
        shotness = self.get_shotness()
        index = ["%s:%s" % (i.file, i.name) for i in shotness]
        indptr = numpy.zeros(len(shotness) + 1, dtype=numpy.int32)
        indices = []
        data = []
        for i, record in enumerate(shotness):
            pairs = list(record.counters.items())
            pairs.sort()
            indptr[i + 1] = indptr[i] + len(pairs)
            for k, v in pairs:
                indices.append(k)
                data.append(v)
        indices = numpy.array(indices, dtype=numpy.int32)
        data = numpy.array(data, dtype=numpy.int32)
        from scipy.sparse import csr_matrix
        return index, csr_matrix((data, indices, indptr), shape=(len(shotness),) * 2)

    def get_shotness(self):
        records = self.contents["Shotness"].records
        if len(records) == 0:
            raise KeyError
        return records

    def get_sentiment(self):
        byday = self.contents["Sentiment"].SentimentByDay
        if len(byday) == 0:
            raise KeyError
        return byday

    def get_devs(self):
        people = list(self.contents["Devs"].dev_index)
        days = {d: {dev: DevDay(stats.commits, stats.stats.added, stats.stats.removed,
                                stats.stats.changed, {k: [v.added, v.removed, v.changed]
                                                      for k, v in stats.languages.items()})
                    for dev, stats in day.devs.items()}
                for d, day in self.contents["Devs"].days.items()}
        return days, people

    def _parse_burndown_matrix(self, matrix):
        dense = numpy.zeros((matrix.number_of_rows, matrix.number_of_columns), dtype=int)
        for y, row in enumerate(matrix.rows):
            for x, col in enumerate(row.columns):
                dense[y, x] = col
        return matrix.name, dense.T

    def _parse_sparse_matrix(self, matrix):
        from scipy.sparse import csr_matrix
        return csr_matrix((list(matrix.data), list(matrix.indices), list(matrix.indptr)),
                          shape=(matrix.number_of_rows, matrix.number_of_columns))


READERS = {"yaml": YamlReader, "yml": YamlReader, "pb": ProtobufReader}
PB_MESSAGES = {
    "Burndown": "internal.pb.pb_pb2.BurndownAnalysisResults",
    "Couples": "internal.pb.pb_pb2.CouplesAnalysisResults",
    "Shotness": "internal.pb.pb_pb2.ShotnessAnalysisResults",
    "Devs": "internal.pb.pb_pb2.DevsAnalysisResults",
}


def read_input(args):
    sys.stdout.write("Reading the input... ")
    sys.stdout.flush()
    if args.input != "-":
        if args.input_format == "auto":
            args.input_format = args.input.rsplit(".", 1)[1]
    elif args.input_format == "auto":
        args.input_format = "yaml"
    reader = READERS[args.input_format]()
    reader.read(args.input)
    print("done")
    return reader


class DevDay(namedtuple("DevDay", ("Commits", "Added", "Removed", "Changed", "Languages"))):
    def add(self, dd):
        langs = defaultdict(lambda: [0] * 3)
        for key, val in self.Languages.items():
            for i in range(3):
                langs[key][i] += val[i]
        for key, val in dd.Languages.items():
            for i in range(3):
                langs[key][i] += val[i]
        return DevDay(Commits=self.Commits + dd.Commits,
                      Added=self.Added + dd.Added,
                      Removed=self.Removed + dd.Removed,
                      Changed=self.Changed + dd.Changed,
                      Languages=dict(langs))


def calculate_average_lifetime(matrix):
    lifetimes = numpy.zeros(matrix.shape[1] - 1)
    for band in matrix:
        start = 0
        for i, line in enumerate(band):
            if i == 0 or band[i - 1] == 0:
                start += 1
                continue
            lifetimes[i - start] = band[i - 1] - line
        lifetimes[i - start] = band[i - 1]
    lsum = lifetimes.sum()
    if lsum != 0:
        total = lifetimes.dot(numpy.arange(1, matrix.shape[1], 1))
        return total / (lsum * matrix.shape[1])
    return numpy.nan


def interpolate_burndown_matrix(matrix, granularity, sampling):
    daily = numpy.zeros(
        (matrix.shape[0] * granularity, matrix.shape[1] * sampling),
        dtype=numpy.float32)
    """
    ----------> samples, x
    |
    |
    |
    ⌄
    bands, y
    """
    for y in range(matrix.shape[0]):
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
                            1 + (k - 1) * (j - start_index + 1) / scale)

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
                        peak = matrix[y][x] + k * ((x + 1) * sampling - (y + 1) * granularity)
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


def import_pandas():
    import pandas
    try:
        from pandas.plotting import register_matplotlib_converters
        register_matplotlib_converters()
    except ImportError:
        pass
    return pandas


def load_burndown(header, name, matrix, resample):
    pandas = import_pandas()

    start, last, sampling, granularity = header
    assert sampling > 0
    assert granularity >= sampling
    start = datetime.fromtimestamp(start)
    last = datetime.fromtimestamp(last)
    print(name, "lifetime index:", calculate_average_lifetime(matrix))
    finish = start + timedelta(days=matrix.shape[1] * sampling)
    if resample not in ("no", "raw"):
        print("resampling to %s, please wait..." % resample)
        # Interpolate the day x day matrix.
        # Each day brings equal weight in the granularity.
        # Sampling's interpolation is linear.
        daily = interpolate_burndown_matrix(matrix, granularity, sampling)
        daily[(last - start).days:] = 0
        # Resample the bands
        aliases = {
            "year": "A",
            "month": "M"
        }
        resample = aliases.get(resample, resample)
        periods = 0
        date_granularity_sampling = [start]
        while date_granularity_sampling[-1] < finish:
            periods += 1
            date_granularity_sampling = pandas.date_range(
                start, periods=periods, freq=resample)
        date_range_sampling = pandas.date_range(
            date_granularity_sampling[0],
            periods=(finish - date_granularity_sampling[0]).days,
            freq="1D")
        # Fill the new square matrix
        matrix = numpy.zeros(
            (len(date_granularity_sampling), len(date_range_sampling)),
            dtype=numpy.float32)
        for i, gdt in enumerate(date_granularity_sampling):
            istart = (date_granularity_sampling[i - 1] - start).days \
                if i > 0 else 0
            ifinish = (gdt - start).days

            for j, sdt in enumerate(date_range_sampling):
                if (sdt - start).days >= istart:
                    break
            matrix[i, j:] = \
                daily[istart:ifinish, (sdt - start).days:].sum(axis=0)
        # Hardcode some cases to improve labels' readability
        if resample in ("year", "A"):
            labels = [dt.year for dt in date_granularity_sampling]
        elif resample in ("month", "M"):
            labels = [dt.strftime("%Y %B") for dt in date_granularity_sampling]
        else:
            labels = [dt.date() for dt in date_granularity_sampling]
    else:
        labels = [
            "%s - %s" % ((start + timedelta(days=i * granularity)).date(),
                         (
                         start + timedelta(days=(i + 1) * granularity)).date())
            for i in range(matrix.shape[0])]
        if len(labels) > 18:
            warnings.warn("Too many labels - consider resampling.")
        resample = "M"  # fake resampling type is checked while plotting
        date_range_sampling = pandas.date_range(
            start + timedelta(days=sampling), periods=matrix.shape[1],
            freq="%dD" % sampling)
    return name, matrix, date_range_sampling, labels, granularity, sampling, resample


def load_ownership(header, sequence, contents, max_people):
    pandas = import_pandas()

    start, last, sampling, _ = header
    start = datetime.fromtimestamp(start)
    last = datetime.fromtimestamp(last)
    people = []
    for name in sequence:
        people.append(contents[name].sum(axis=1))
    people = numpy.array(people)
    date_range_sampling = pandas.date_range(
        start + timedelta(days=sampling), periods=people[0].shape[0],
        freq="%dD" % sampling)

    if people.shape[0] > max_people:
        order = numpy.argsort(-people.sum(axis=1))
        people = people[order[:max_people]]
        sequence = [sequence[i] for i in order[:max_people]]
        print("Warning: truncated people to most owning %d" % max_people)
    for i, name in enumerate(sequence):
        if len(name) > 40:
            sequence[i] = name[:37] + "..."

    return sequence, people, date_range_sampling, last


def load_churn_matrix(people, matrix, max_people):
    matrix = matrix.astype(float)
    if matrix.shape[0] > max_people:
        order = numpy.argsort(-matrix[:, 0])
        matrix = matrix[order[:max_people]][:, [0, 1] + list(2 + order[:max_people])]
        people = [people[i] for i in order[:max_people]]
        print("Warning: truncated people to most productive %d" % max_people)
    zeros = matrix[:, 0] == 0
    matrix[zeros, :] = 1
    matrix /= matrix[:, 0][:, None]
    matrix = -matrix[:, 1:]
    matrix[zeros, :] = 0
    for i, name in enumerate(people):
        if len(name) > 40:
            people[i] = name[:37] + "..."
    return people, matrix


def import_pyplot(backend, style):
    import matplotlib
    if backend:
        matplotlib.use(backend)
    from matplotlib import pyplot
    pyplot.style.use(style)
    return matplotlib, pyplot


def apply_plot_style(figure, axes, legend, background, font_size, axes_size):
    foreground = "black" if background == "white" else "white"
    if axes_size is None:
        axes_size = (12, 9)
    else:
        axes_size = tuple(float(p) for p in axes_size.split(","))
    figure.set_size_inches(*axes_size)
    for side in ("bottom", "top", "left", "right"):
        axes.spines[side].set_color(foreground)
    for axis in (axes.xaxis, axes.yaxis):
        axis.label.update(dict(fontsize=font_size, color=foreground))
    for axis in ("x", "y"):
        getattr(axes, axis + "axis").get_offset_text().set_size(font_size)
        axes.tick_params(axis=axis, colors=foreground, labelsize=font_size)
    try:
        axes.ticklabel_format(axis="y", style="sci", scilimits=(0, 3))
    except AttributeError:
        pass
    figure.patch.set_facecolor(background)
    axes.set_facecolor(background)
    if legend is not None:
        frame = legend.get_frame()
        for setter in (frame.set_facecolor, frame.set_edgecolor):
            setter(background)
        for text in legend.get_texts():
            text.set_color(foreground)


def get_plot_path(base, name):
    root, ext = os.path.splitext(base)
    if not ext:
        ext = ".png"
    output = os.path.join(root, name + ext)
    os.makedirs(os.path.dirname(output), exist_ok=True)
    return output


def deploy_plot(title, output, background):
    import matplotlib.pyplot as pyplot

    if not output:
        pyplot.gcf().canvas.set_window_title(title)
        pyplot.show()
    else:
        if title:
            pyplot.title(title, color="black" if background == "white" else "white")
        try:
            pyplot.tight_layout()
        except:  # noqa: E722
            print("Warning: failed to set the tight layout")
        pyplot.savefig(output, transparent=True)
    pyplot.clf()


def default_json(x):
    if hasattr(x, "tolist"):
        return x.tolist()
    if hasattr(x, "isoformat"):
        return x.isoformat()
    return x


def plot_burndown(args, target, name, matrix, date_range_sampling, labels, granularity,
                  sampling, resample):
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
    apply_plot_style(pyplot.gcf(), pyplot.gca(), legend, args.background,
                     args.font_size, args.size)
    pyplot.xlim(date_range_sampling[0], date_range_sampling[-1])
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
    title = "%s %d x %d (granularity %d, sampling %d)" % \
        ((name,) + matrix.shape + (granularity, sampling))
    output = args.output
    if output:
        if args.mode == "project" and target == "project":
            output = args.output
        else:
            if target == "project":
                name = "project"
            output = get_plot_path(args.output, name)
    deploy_plot(title, output, args.style)


def plot_many_burndown(args, target, header, parts):
    if not args.output:
        print("Warning: output not set, showing %d plots." % len(parts))
    itercnt = progress.bar(parts, expected_size=len(parts)) \
        if progress is not None else parts
    stdout = io.StringIO()
    for name, matrix in itercnt:
        backup = sys.stdout
        sys.stdout = stdout
        plot_burndown(args, target, *load_burndown(header, name, matrix, args.resample))
        sys.stdout = backup
    sys.stdout.write(stdout.getvalue())


def plot_churn_matrix(args, repo, people, matrix):
    if args.output and args.output.endswith(".json"):
        data = locals().copy()
        del data["args"]
        data["type"] = "churn_matrix"
        if args.mode == "all":
            output = get_plot_path(args.output, "matrix")
        else:
            output = args.output
        with open(output, "w") as fout:
            json.dump(data, fout, sort_keys=True, default=default_json)
        return

    matplotlib, pyplot = import_pyplot(args.backend, args.style)

    s = 4 + matrix.shape[1] * 0.3
    fig = pyplot.figure(figsize=(s, s))
    ax = fig.add_subplot(111)
    ax.xaxis.set_label_position("top")
    ax.matshow(matrix, cmap=pyplot.cm.OrRd)
    ax.set_xticks(numpy.arange(0, matrix.shape[1]))
    ax.set_yticks(numpy.arange(0, matrix.shape[0]))
    ax.set_yticklabels(people, va="center")
    ax.set_xticks(numpy.arange(0.5, matrix.shape[1] + 0.5), minor=True)
    ax.set_xticklabels(["Unidentified"] + people, rotation=45, ha="left",
                       va="bottom", rotation_mode="anchor")
    ax.set_yticks(numpy.arange(0.5, matrix.shape[0] + 0.5), minor=True)
    ax.grid(False)
    ax.grid(which="minor")
    apply_plot_style(fig, ax, None, args.background, args.font_size, args.size)
    if not args.output:
        pos1 = ax.get_position()
        pos2 = (pos1.x0 + 0.15, pos1.y0 - 0.1, pos1.width * 0.9, pos1.height * 0.9)
        ax.set_position(pos2)
    if args.mode == "all" and args.output:
        output = get_plot_path(args.output, "matrix")
    else:
        output = args.output
    title = "%s %d developers overwrite" % (repo, matrix.shape[0])
    if args.output:
        # FIXME(vmarkovtsev): otherwise the title is screwed in savefig()
        title = ""
    deploy_plot(title, output, args.style)


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

    pyplot.stackplot(date_range, people, labels=names)
    pyplot.xlim(date_range[0], last)
    if args.relative:
        for i in range(people.shape[1]):
            people[:, i] /= people[:, i].sum()
        pyplot.ylim(0, 1)
        legend_loc = 3
    else:
        legend_loc = 2
    legend = pyplot.legend(loc=legend_loc, fontsize=args.font_size)
    apply_plot_style(pyplot.gcf(), pyplot.gca(), legend, args.background,
                     args.font_size, args.size)
    if args.mode == "all" and args.output:
        output = get_plot_path(args.output, "people")
    else:
        output = args.output
    deploy_plot("%s code ownership through time" % repo, output, args.style)


IDEAL_SHARD_SIZE = 4096


def train_embeddings(index, matrix, tmpdir, shard_size=IDEAL_SHARD_SIZE):
    try:
        from . import swivel
    except (SystemError, ImportError):
        import swivel
    import tensorflow as tf

    assert matrix.shape[0] == matrix.shape[1]
    assert len(index) <= matrix.shape[0]
    outlier_threshold = numpy.percentile(matrix.data, 99)
    matrix.data[matrix.data > outlier_threshold] = outlier_threshold
    nshards = len(index) // shard_size
    if nshards * shard_size < len(index):
        nshards += 1
        shard_size = len(index) // nshards
        nshards = len(index) // shard_size
    remainder = len(index) - nshards * shard_size
    if remainder > 0:
        lengths = matrix.indptr[1:] - matrix.indptr[:-1]
        filtered = sorted(numpy.argsort(lengths)[remainder:])
    else:
        filtered = list(range(len(index)))
    if len(filtered) < matrix.shape[0]:
        print("Truncating the sparse matrix...")
        matrix = matrix[filtered, :][:, filtered]
    meta_index = []
    for i, j in enumerate(filtered):
        meta_index.append((index[j], matrix[i, i]))
    index = [mi[0] for mi in meta_index]
    with tempfile.TemporaryDirectory(prefix="hercules_labours_", dir=tmpdir or None) as tmproot:
        print("Writing Swivel metadata...")
        vocabulary = "\n".join(index)
        with open(os.path.join(tmproot, "row_vocab.txt"), "w") as out:
            out.write(vocabulary)
        with open(os.path.join(tmproot, "col_vocab.txt"), "w") as out:
            out.write(vocabulary)
        del vocabulary
        bool_sums = matrix.indptr[1:] - matrix.indptr[:-1]
        bool_sums_str = "\n".join(map(str, bool_sums.tolist()))
        with open(os.path.join(tmproot, "row_sums.txt"), "w") as out:
            out.write(bool_sums_str)
        with open(os.path.join(tmproot, "col_sums.txt"), "w") as out:
            out.write(bool_sums_str)
        del bool_sums_str
        reorder = numpy.argsort(-bool_sums)

        print("Writing Swivel shards...")
        for row in range(nshards):
            for col in range(nshards):
                def _int64s(xs):
                    return tf.train.Feature(
                        int64_list=tf.train.Int64List(value=list(xs)))

                def _floats(xs):
                    return tf.train.Feature(
                        float_list=tf.train.FloatList(value=list(xs)))

                indices_row = reorder[row::nshards]
                indices_col = reorder[col::nshards]
                shard = matrix[indices_row][:, indices_col].tocoo()

                example = tf.train.Example(features=tf.train.Features(feature={
                    "global_row": _int64s(indices_row),
                    "global_col": _int64s(indices_col),
                    "sparse_local_row": _int64s(shard.row),
                    "sparse_local_col": _int64s(shard.col),
                    "sparse_value": _floats(shard.data)}))

                with open(os.path.join(tmproot, "shard-%03d-%03d.pb" % (row, col)), "wb") as out:
                    out.write(example.SerializeToString())
        print("Training Swivel model...")
        swivel.FLAGS.submatrix_rows = shard_size
        swivel.FLAGS.submatrix_cols = shard_size
        if len(meta_index) <= IDEAL_SHARD_SIZE / 16:
            embedding_size = 50
            num_epochs = 100000
        elif len(meta_index) <= IDEAL_SHARD_SIZE:
            embedding_size = 50
            num_epochs = 50000
        elif len(meta_index) <= IDEAL_SHARD_SIZE * 2:
            embedding_size = 60
            num_epochs = 10000
        elif len(meta_index) <= IDEAL_SHARD_SIZE * 4:
            embedding_size = 70
            num_epochs = 8000
        elif len(meta_index) <= IDEAL_SHARD_SIZE * 10:
            embedding_size = 80
            num_epochs = 5000
        elif len(meta_index) <= IDEAL_SHARD_SIZE * 25:
            embedding_size = 100
            num_epochs = 1000
        elif len(meta_index) <= IDEAL_SHARD_SIZE * 100:
            embedding_size = 200
            num_epochs = 600
        else:
            embedding_size = 300
            num_epochs = 300
        if os.getenv("CI"):
            # Travis, AppVeyor etc. during the integration tests
            num_epochs /= 10
        swivel.FLAGS.embedding_size = embedding_size
        swivel.FLAGS.input_base_path = tmproot
        swivel.FLAGS.output_base_path = tmproot
        swivel.FLAGS.loss_multiplier = 1.0 / shard_size
        swivel.FLAGS.num_epochs = num_epochs
        # Tensorflow 1.5 parses sys.argv unconditionally *applause*
        argv_backup = sys.argv[1:]
        del sys.argv[1:]
        swivel.main(None)
        sys.argv.extend(argv_backup)
        print("Reading Swivel embeddings...")
        embeddings = []
        with open(os.path.join(tmproot, "row_embedding.tsv")) as frow:
            with open(os.path.join(tmproot, "col_embedding.tsv")) as fcol:
                for i, (lrow, lcol) in enumerate(zip(frow, fcol)):
                    prow, pcol = (l.split("\t", 1) for l in (lrow, lcol))
                    assert prow[0] == pcol[0]
                    erow, ecol = \
                        (numpy.fromstring(p[1], dtype=numpy.float32, sep="\t")
                         for p in (prow, pcol))
                    embeddings.append((erow + ecol) / 2)
    return meta_index, embeddings


class CORSWebServer(object):
    def __init__(self):
        self.thread = threading.Thread(target=self.serve)
        self.server = None

    def serve(self):
        outer = self

        try:
            from http.server import HTTPServer, SimpleHTTPRequestHandler, test
        except ImportError:  # Python 2
            from BaseHTTPServer import HTTPServer, test
            from SimpleHTTPServer import SimpleHTTPRequestHandler

        class ClojureServer(HTTPServer):
            def __init__(self, *args, **kwargs):
                HTTPServer.__init__(self, *args, **kwargs)
                outer.server = self

        class CORSRequestHandler(SimpleHTTPRequestHandler):
            def end_headers(self):
                self.send_header("Access-Control-Allow-Origin", "*")
                SimpleHTTPRequestHandler.end_headers(self)

        test(CORSRequestHandler, ClojureServer)

    def start(self):
        self.thread.start()

    def stop(self):
        if self.running:
            self.server.shutdown()
            self.thread.join()

    @property
    def running(self):
        return self.server is not None


web_server = CORSWebServer()


def write_embeddings(name, output, run_server, index, embeddings):
    print("Writing Tensorflow Projector files...")
    if not output:
        output = "couples_" + name
    if output.endswith(".json"):
        output = os.path.join(output[:-5], "couples")
        run_server = False
    metaf = "%s_%s_meta.tsv" % (output, name)
    with open(metaf, "w") as fout:
        fout.write("name\tcommits\n")
        for pair in index:
            fout.write("%s\t%s\n" % pair)
    print("Wrote", metaf)
    dataf = "%s_%s_data.tsv" % (output, name)
    with open(dataf, "w") as fout:
        for vec in embeddings:
            fout.write("\t".join(str(v) for v in vec))
            fout.write("\n")
    print("Wrote", dataf)
    jsonf = "%s_%s.json" % (output, name)
    with open(jsonf, "w") as fout:
        fout.write("""{
  "embeddings": [
    {
      "tensorName": "%s %s coupling",
      "tensorShape": [%s, %s],
      "tensorPath": "http://0.0.0.0:8000/%s",
      "metadataPath": "http://0.0.0.0:8000/%s"
    }
  ]
}
""" % (output, name, len(embeddings), len(embeddings[0]), dataf, metaf))
    print("Wrote %s" % jsonf)
    if run_server and not web_server.running:
        web_server.start()
    url = "http://projector.tensorflow.org/?config=http://0.0.0.0:8000/" + jsonf
    print(url)
    if run_server:
        if shutil.which("xdg-open") is not None:
            os.system("xdg-open " + url)
        else:
            browser = os.getenv("BROWSER", "")
            if browser:
                os.system(browser + " " + url)
            else:
                print("\t" + url)


def show_shotness_stats(data):
    top = sorted(((r.counters[i], i) for i, r in enumerate(data)), reverse=True)
    for count, i in top:
        r = data[i]
        print("%8d  %s:%s [%s]" % (count, r.file, r.name, r.internal_role))


def show_sentiment_stats(args, name, resample, start_date, data):
    matplotlib, pyplot = import_pyplot(args.backend, args.style)

    start_date = datetime.fromtimestamp(start_date)
    data = sorted(data.items())
    xdates = [start_date + timedelta(days=d[0]) for d in data]
    xpos = []
    ypos = []
    xneg = []
    yneg = []
    for x, (_, y) in zip(xdates, data):
        y = 0.5 - y.Value
        if y > 0:
            xpos.append(x)
            ypos.append(y)
        else:
            xneg.append(x)
            yneg.append(y)
    pyplot.bar(xpos, ypos, color="g", label="Positive")
    pyplot.bar(xneg, yneg, color="r", label="Negative")
    legend = pyplot.legend(loc=1, fontsize=args.font_size)
    pyplot.ylabel("Lines of code")
    pyplot.xlabel("Time")
    apply_plot_style(pyplot.gcf(), pyplot.gca(), legend, args.background,
                     args.font_size, args.size)
    pyplot.xlim(xdates[0], xdates[-1])
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
        labels[startindex].set_text(xdates[0].date())
        labels[startindex].set_text = lambda _: None
        labels[startindex].set_rotation(30)
        labels[startindex].set_ha("right")
    if endindex >= 0:
        labels[endindex].set_text(xdates[-1].date())
        labels[endindex].set_text = lambda _: None
        labels[endindex].set_rotation(30)
        labels[endindex].set_ha("right")
    overall_pos = sum(2 * (0.5 - d[1].Value) for d in data if d[1].Value < 0.5)
    overall_neg = sum(2 * (d[1].Value - 0.5) for d in data if d[1].Value > 0.5)
    title = "%s sentiment +%.1f -%.1f δ=%.1f" % (
        name, overall_pos, overall_neg, overall_pos - overall_neg)
    deploy_plot(title, args.output, args.style)


def show_devs(args, name, start_date, end_date, data):
    try:
        from fastdtw import fastdtw
    except ImportError as e:
        print("Cannot import fastdtw: %s\nInstall it from https://github.com/slaypni/fastdtw" % e)
        sys.exit(1)
    try:
        from ortools.constraint_solver import pywrapcp, routing_enums_pb2
    except ImportError as e:
        print("Cannot import ortools: %s\nInstall it from "
              "https://developers.google.com/optimization/install/python/" % e)
        sys.exit(1)
    try:
        from hdbscan import HDBSCAN
    except ImportError as e:
        print("Cannot import ortools: %s\nInstall it from "
              "https://developers.google.com/optimization/install/python/" % e)
        sys.exit(1)
    from scipy.signal import convolve, slepian

    days, people = data
    max_people = 50
    if len(people) > max_people:
        print("Picking top 100 developers by commit count")
        # pick top N developers by commit count
        commits = defaultdict(int)
        for devs in days.values():
            for dev, stats in devs.items():
                commits[dev] += stats.Commits
        commits = sorted(((v, k) for k, v in commits.items()), reverse=True)
        chosen_people = {people[k] for _, k in commits[:max_people]}
    else:
        chosen_people = set(people)
    devseries = defaultdict(list)
    devstats = defaultdict(lambda: DevDay(0, 0, 0, 0, {}))
    for day, devs in sorted(days.items()):
        for dev, stats in devs.items():
            if people[dev] in chosen_people:
                devseries[dev].append((day, stats.Commits))
                devstats[dev] = devstats[dev].add(stats)
    print("Calculating the distance matrix")
    # max-normalize the time series using a sliding window
    keys = list(devseries.keys())
    series = list(devseries.values())
    for i, s in enumerate(series):
        arr = numpy.array(s).transpose().astype(numpy.float32)
        commits = arr[1]
        if len(commits) < 7:
            commits /= commits.max()
        else:
            # 4 is sizeof(float32)
            windows = numpy.lib.stride_tricks.as_strided(commits, [len(commits) - 6, 7], [4, 4])
            commits = numpy.concatenate((
                [windows[0, 0] / windows[0].max(),
                 windows[0, 1] / windows[0].max(),
                 windows[0, 2] / windows[0].max()],
                windows[:, 3] / windows.max(axis=1),
                [windows[-1, 4] / windows[-1].max(),
                 windows[-1, 5] / windows[-1].max(),
                 windows[-1, 6] / windows[-1].max()]
            ))
        arr[1] = commits * 7  # 7 is a pure heuristic here and is not related to window size
        series[i] = list(arr.transpose())
    # calculate the distance matrix using dynamic time warping metric
    dists = numpy.full((len(series) + 1, len(series) + 1), -100500, dtype=numpy.float32)
    for x in range(len(series)):
        dists[x, x] = 0
        for y in range(x + 1, len(series)):
            # L1 norm
            dist, _ = fastdtw(series[x], series[y], radius=5, dist=1)
            dists[x, y] = dists[y, x] = dist
    # preparation for seriation ordering
    dists[len(series), :] = 0
    dists[:, len(series)] = 0
    assert (dists >= 0).all()
    print("Ordering the series")
    # solve the TSP on the distance matrix
    routing = pywrapcp.RoutingModel(dists.shape[0], 1, len(series))

    def dist_callback(x, y):
        # ortools wants integers, so we approximate here
        return int(dists[x][y] * 1000)

    routing.SetArcCostEvaluatorOfAllVehicles(dist_callback)
    search_parameters = pywrapcp.RoutingModel.DefaultSearchParameters()
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    search_parameters.time_limit_ms = 2000
    assignment = routing.SolveWithParameters(search_parameters)
    index = routing.Start(0)
    route = []
    while not routing.IsEnd(index):
        node = routing.IndexToNode(index)
        if node < len(keys):
            route.append(node)
        index = assignment.Value(routing.NextVar(index))
    route_map = {v: i for i, v in enumerate(route)}

    # determine clusters
    opt_dist_chain = numpy.cumsum(numpy.array(
        [0] + [dists[route[i], route[i + 1]] for i in range(len(route) - 1)]))
    clusters = HDBSCAN(min_cluster_size=2).fit_predict(opt_dist_chain[:, numpy.newaxis])
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
    prop_cycle = pyplot.rcParams["axes.prop_cycle"]
    colors = prop_cycle.by_key()["color"]
    fig, axes = pyplot.subplots(final.shape[0], 1)
    backgrounds = ("#C4FFDB", "#FFD0CD") if args.background == "white" else ("#05401C", "#40110E")
    for ax, series, cluster, dev_i in zip(axes, final, clusters, route):
        if cluster >= 0:
            color = colors[cluster % len(colors)]
        else:
            # outlier
            color = "grey"
        ax.fill_between(plot_x, series, color=color)
        ax.set_axis_off()
        author = people[dev_i]
        ax.text(0.03, 0.5, author[:36] + (author[36:] and "..."),
                horizontalalignment="right", verticalalignment="center",
                transform=ax.transAxes, fontsize=14,
                color="black" if args.background == "white" else "white")
        ds = devstats[dev_i]
        stats = "%5d %8s %8s" % (ds[0], _format_number(ds[1] - ds[2]), _format_number(ds[3]))
        ax.text(0.97, 0.5, stats,
                horizontalalignment="left", verticalalignment="center",
                transform=ax.transAxes, fontsize=14, family="monospace",
                backgroundcolor=backgrounds[ds[1] <= ds[2]],
                color="black" if args.background == "white" else "white")
    axes[0].text(0.97, 1.75, " cmts    delta  changed",
                 horizontalalignment="left", verticalalignment="center",
                 transform=axes[0].transAxes, fontsize=14, family="monospace",
                 color="black" if args.background == "white" else "white")
    axes[-1].set_axis_on()
    target_num_labels = 12
    num_months = (end_date.year - start_date.year) * 12 + end_date.month - start_date.month
    interval = int(numpy.ceil(num_months / target_num_labels))
    if interval >= 8:
        interval = int(numpy.ceil(num_months / (12 * target_num_labels)))
        axes[-1].xaxis.set_major_locator(matplotlib.dates.YearLocator(base=max(1, interval // 12)))
        axes[-1].xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y"))
    else:
        axes[-1].xaxis.set_major_locator(matplotlib.dates.MonthLocator(interval=interval))
        axes[-1].xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m"))
    for tick in axes[-1].xaxis.get_major_ticks():
        tick.label.set_fontsize(args.font_size)
    axes[-1].spines["left"].set_visible(False)
    axes[-1].spines["right"].set_visible(False)
    axes[-1].spines["top"].set_visible(False)
    axes[-1].get_yaxis().set_visible(False)
    axes[-1].set_facecolor((1.0,) * 3 + (0.0,))

    title = "%s commits" % name
    deploy_plot(title, args.output, args.style)


def show_old_vs_new(args, name, start_date, end_date, data):
    from scipy.signal import convolve, slepian

    days, people = data
    start_date = datetime.fromtimestamp(start_date)
    start_date = datetime(start_date.year, start_date.month, start_date.day)
    end_date = datetime.fromtimestamp(end_date)
    end_date = datetime(end_date.year, end_date.month, end_date.day)
    new_lines = numpy.zeros((end_date - start_date).days + 1)
    old_lines = numpy.zeros_like(new_lines)
    for day, devs in days.items():
        for stats in devs.values():
            new_lines[day] += stats.Added
            old_lines[day] += stats.Removed + stats.Changed
    resolution = 32
    window = slepian(len(new_lines) // resolution, 0.5)
    new_lines = convolve(new_lines, window, "same")
    old_lines = convolve(old_lines, window, "same")
    matplotlib, pyplot = import_pyplot(args.backend, args.style)
    plot_x = [start_date + timedelta(days=i) for i in range(len(new_lines))]
    pyplot.fill_between(plot_x, new_lines, color="#8DB843", label="Changed new lines")
    pyplot.fill_between(plot_x, old_lines, color="#E14C35", label="Changed existing lines")
    pyplot.legend(loc=2, fontsize=args.font_size)
    for tick in chain(pyplot.gca().xaxis.get_major_ticks(), pyplot.gca().yaxis.get_major_ticks()):
        tick.label.set_fontsize(args.font_size)
    deploy_plot("Additions vs changes", args.output, args.style)


def show_languages(args, name, start_date, end_date, data):
    days, people = data
    devlangs = defaultdict(lambda: defaultdict(lambda: numpy.zeros(3, dtype=int)))
    for day, devs in days.items():
        for dev, stats in devs.items():
            for lang, vals in stats.Languages.items():
                devlangs[dev][lang] += vals
    devlangs = sorted(devlangs.items(), key=lambda p: -sum(x.sum() for x in p[1].values()))
    for dev, ls in devlangs:
        print()
        print("#", people[dev])
        ls = sorted(((vals.sum(), lang) for lang, vals in ls.items()), reverse=True)
        for vals, lang in ls:
            if lang:
                print("%s: %d" % (lang, vals))


def _format_number(n):
    if n == 0:
        return "0"
    power = int(numpy.log10(abs(n)))
    if power >= 6:
        n = n / 1000000
        if n >= 10:
            n = str(int(n))
        else:
            n = "%.1f" % n
            if n.endswith("0"):
                n = n[:-2]
        suffix = "M"
    elif power >= 3:
        n = n / 1000
        if n >= 10:
            n = str(int(n))
        else:
            n = "%.1f" % n
            if n.endswith("0"):
                n = n[:-2]
        suffix = "K"
    else:
        n = str(n)
        suffix = ""
    return n + suffix


def main():
    args = parse_args()
    reader = read_input(args)
    header = reader.get_header()
    name = reader.get_name()

    burndown_warning = "Burndown stats were not collected. Re-run hercules with --burndown."
    burndown_files_warning = \
        "Burndown stats for files were not collected. Re-run hercules with " \
        "--burndown --burndown-files."
    burndown_people_warning = \
        "Burndown stats for people were not collected. Re-run hercules with " \
        "--burndown --burndown-people."
    couples_warning = "Coupling stats were not collected. Re-run hercules with --couples."
    shotness_warning = "Structural hotness stats were not collected. Re-run hercules with " \
                       "--shotness. Also check --languages - the output may be empty."
    sentiment_warning = "Sentiment stats were not collected. Re-run hercules with --sentiment."
    devs_warning = "Devs stats were not collected. Re-run hercules with --devs."

    def run_times():
        rt = reader.get_run_times()
        pandas = import_pandas()
        series = pandas.to_timedelta(pandas.Series(rt).sort_values(ascending=False), unit="s")
        df = pandas.concat([series, series / series.sum()], axis=1)
        df.columns = ["time", "ratio"]
        print(df)

    def project_burndown():
        try:
            full_header = header + reader.get_burndown_parameters()
        except KeyError:
            print("project: " + burndown_warning)
            return
        plot_burndown(args, "project",
                      *load_burndown(full_header, *reader.get_project_burndown(),
                                     resample=args.resample))

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
            plot_many_burndown(args, "person", full_header, reader.get_people_burndown())
        except KeyError:
            print("people: " + burndown_people_warning)

    def churn_matrix():
        try:
            plot_churn_matrix(args, name, *load_churn_matrix(
                *reader.get_people_interaction(), max_people=args.max_people))
        except KeyError:
            print("churn_matrix: " + burndown_people_warning)

    def ownership_burndown():
        try:
            full_header = header + reader.get_burndown_parameters()
        except KeyError:
            print(burndown_warning)
            return
        try:
            plot_ownership(args, name, *load_ownership(
                full_header, *reader.get_ownership_burndown(), max_people=args.max_people))
        except KeyError:
            print("ownership: " + burndown_people_warning)

    def couples():
        try:
            write_embeddings("files", args.output, not args.disable_projector,
                             *train_embeddings(*reader.get_files_coocc(),
                                               tmpdir=args.couples_tmp_dir))
            write_embeddings("people", args.output, not args.disable_projector,
                             *train_embeddings(*reader.get_people_coocc(),
                                               tmpdir=args.couples_tmp_dir))
        except KeyError:
            print(couples_warning)
        try:
            write_embeddings("shotness", args.output, not args.disable_projector,
                             *train_embeddings(*reader.get_shotness_coocc(),
                                               tmpdir=args.couples_tmp_dir))
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
        show_sentiment_stats(args, reader.get_name(), args.resample, reader.get_header()[0], data)

    def devs():
        try:
            data = reader.get_devs()
        except KeyError:
            print(devs_warning)
            return
        show_devs(args, reader.get_name(), *reader.get_header(), data)

    def old_vs_new():
        try:
            data = reader.get_devs()
        except KeyError:
            print(devs_warning)
            return
        show_old_vs_new(args, reader.get_name(), *reader.get_header(), data)

    def languages():
        try:
            data = reader.get_devs()
        except KeyError:
            print(devs_warning)
            return
        show_languages(args, reader.get_name(), *reader.get_header(), data)

    modes = {
        "run-times": run_times,
        "burndown-project": project_burndown,
        "burndown-file": files_burndown,
        "burndown-person": people_burndown,
        "churn-matrix": churn_matrix,
        "ownership": ownership_burndown,
        "couples": couples,
        "shotness": shotness,
        "sentiment": sentiment,
        "devs": devs,
        "old-vs-new": old_vs_new,
        "languages": languages,
    }
    try:
        modes[args.mode]()
    except KeyError:
        assert args.mode == "all"
        project_burndown()
        files_burndown()
        people_burndown()
        churn_matrix()
        ownership_burndown()
        couples()
        shotness()
        sentiment()
        devs()

    if web_server.running:
        secs = int(os.getenv("COUPLES_SERVER_TIME", "60"))
        print("Sleeping for %d seconds, safe to Ctrl-C" % secs)
        sys.stdout.flush()
        try:
            time.sleep(secs)
        except KeyboardInterrupt:
            pass
        web_server.stop()


if __name__ == "__main__":
    sys.exit(main())
