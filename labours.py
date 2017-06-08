import argparse
from datetime import datetime, timedelta
import sys
import warnings

import numpy


if sys.version_info[0] < 3:
    # OK, ancients, I will support Python 2, but you owe me a beer
    input = raw_input


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="",
                        help="Path to the output file (empty for display).")
    parser.add_argument("--input", default="-",
                        help="Path to the input file (- for stdin).")
    parser.add_argument("--text-size", default=12,
                        help="Size of the labels and legend.")
    parser.add_argument("--backend", help="Matplotlib backend to use.")
    parser.add_argument("--style", choices=["black", "white"], default="black",
                        help="Plot's general color scheme.")
    parser.add_argument("--relative", action="store_true",
                        help="Occupy 100% height for every measurement.")
    parser.add_argument(
        "--resample", default="year",
        help="The way to resample the time series. Possible values are: "
             "\"month\", \"year\", \"no\", \"raw\" and pandas offset aliases ("
             "http://pandas.pydata.org/pandas-docs/stable/timeseries.html"
             "#offset-aliases).")
    args = parser.parse_args()
    return args


def load_matrix(args):
    import pandas

    if args.input != "-":
        with open(args.input) as fin:
            header = fin.readline()[:-1]
            contents = fin.read()
    else:
        header = input()
        contents = sys.stdin.read()
    start, last, granularity, sampling = header.split()
    start = datetime.fromtimestamp(int(start))
    last = datetime.fromtimestamp(int(last))
    granularity = int(granularity)
    sampling = int(sampling)
    matrix = numpy.array([numpy.fromstring(line, dtype=int, sep=" ")
                          for line in contents.split("\n")[:-1]]).T
    finish = start + timedelta(days=matrix.shape[1] * sampling)
    if args.resample not in ("no", "raw"):
        # Interpolate the day x day matrix.
        # Each day brings equal weight in the granularity.
        # Sampling's interpolation is linear.
        daily_matrix = numpy.zeros(
            (matrix.shape[0] * granularity, matrix.shape[1] * sampling),
            dtype=numpy.float32)
        epsrange = numpy.arange(0, 1, 1.0 / sampling)
        for y in range(matrix.shape[0]):
            for x in range(matrix.shape[1]):
                previous = matrix[y, x - 1] if x > 0 else 0
                value = ((previous + (matrix[y, x] - previous) * epsrange)
                         / granularity)[numpy.newaxis, :]
                if (y + 1) * granularity <= x * sampling:
                    daily_matrix[y * granularity:(y + 1) * granularity,
                    x * sampling:(x + 1) * sampling] = value
                elif y * granularity <= (x + 1) * sampling:
                    for suby in range(y * granularity, (y + 1) * granularity):
                        for subx in range(suby, (x + 1) * sampling):
                            daily_matrix[suby, subx] = matrix[
                                                           y, x] / granularity
        daily_matrix[(last - start).days:] = 0
        # Resample the bands
        aliases = {
            "year": "A",
            "month": "M"
        }
        args.resample = aliases.get(args.resample, args.resample)
        periods = 0
        date_granularity_sampling = [start]
        while date_granularity_sampling[-1] < finish:
            periods += 1
            date_granularity_sampling = pandas.date_range(
                start, periods=periods, freq=args.resample)
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
                daily_matrix[istart:ifinish, (sdt - start).days:].sum(axis=0)
        # Hardcode some cases to improve labels' readability
        if args.resample in ("year", "A"):
            labels = [dt.year for dt in date_granularity_sampling]
        elif args.resample in ("month", "M"):
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
        args.resample = "M"  # fake resampling type is checked while plotting
        date_range_sampling = pandas.date_range(
            start + timedelta(days=sampling), periods=matrix.shape[1],
            freq="%dD" % sampling)
    return matrix, date_range_sampling, labels, granularity, sampling


def plot_matrix(args, matrix, date_range_sampling, labels, granularity,
                sampling):
    import matplotlib
    if args.backend:
        matplotlib.use(args.backend)
    import matplotlib.pyplot as pyplot

    if args.style == "white":
        pyplot.gca().spines["bottom"].set_color("white")
        pyplot.gca().spines["top"].set_color("white")
        pyplot.gca().spines["left"].set_color("white")
        pyplot.gca().spines["right"].set_color("white")
        pyplot.gca().xaxis.label.set_color("white")
        pyplot.gca().yaxis.label.set_color("white")
        pyplot.gca().tick_params(axis="x", colors="white")
        pyplot.gca().tick_params(axis="y", colors="white")
    if args.relative:
        for i in range(matrix.shape[1]):
            matrix[:, i] /= matrix[:, i].sum()
        pyplot.ylim(0, 1)
        legend_loc = 3
    else:
        legend_loc = 2
    pyplot.stackplot(date_range_sampling, matrix, labels=labels)
    legend = pyplot.legend(loc=legend_loc, fontsize=args.text_size)
    frame = legend.get_frame()
    frame.set_facecolor("black" if args.style == "white" else "white")
    frame.set_edgecolor("black" if args.style == "white" else "white")
    for text in legend.get_texts():
        text.set_color(args.style)
    pyplot.ylabel("Lines of code", fontsize=args.text_size)
    pyplot.xlabel("Time", fontsize=args.text_size)
    pyplot.tick_params(labelsize=args.text_size)
    pyplot.xlim(date_range_sampling[0], date_range_sampling[-1])
    pyplot.gcf().set_size_inches(12, 9)
    locator = pyplot.gca().xaxis.get_major_locator()
    # set the optimal xticks locator
    if "M" not in args.resample:
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
    if len(locs) >= 2 and \
            pyplot.xlim()[1] - locs[-1] > (locs[-1] - locs[-2]) / 2:
        locs.append(pyplot.xlim()[1])
        endindex = len(locs) - 1
    startindex = -1
    if len(locs) >= 2 and \
            locs[0] - pyplot.xlim()[0] > (locs[1] - locs[0]) / 2:
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
    if not args.output:
        pyplot.gcf().canvas.set_window_title(
            "Hercules %d x %d (granularity %d, sampling %d)" %
            (matrix.shape + (granularity, sampling)))
        pyplot.show()
    else:
        pyplot.tight_layout()
        pyplot.savefig(args.output, transparent=True)


def main():
    args = parse_args()
    plot_matrix(args, *load_matrix(args))


if __name__ == "__main__":
    sys.exit(main())
