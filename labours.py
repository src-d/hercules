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
    parser.add_argument("--text-size", default=12,
                        help="Size of the labels and legend.")
    parser.add_argument("--backend", help="Matplotlib backend to use.")
    parser.add_argument(
        "--resample", default="year",
        help="The way to resample the time series. Possible values are: "
             "\"month\", \"year\", \"no\", \"raw\" and pandas offset aliases ("
             "http://pandas.pydata.org/pandas-docs/stable/timeseries.html"
             "#offset-aliases).")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    import matplotlib
    if args.backend:
        matplotlib.use(args.backend)
    import matplotlib.pyplot as pyplot
    import pandas

    start, granularity, sampling = input().split()
    start = datetime.fromtimestamp(int(start))
    granularity = int(granularity)
    sampling = int(sampling)
    matrix = numpy.array([numpy.fromstring(line, dtype=int, sep=" ")
                          for line in sys.stdin.read().split("\n")[:-1]]).T
    date_range_granularity = pandas.date_range(
        start, periods=matrix.shape[0], freq="%dD" % granularity)
    date_range_sampling = pandas.date_range(
        start, periods=matrix.shape[1],  freq="%dD" % sampling)
    df = pandas.DataFrame({
        dr: pandas.Series(row, index=date_range_sampling)
        for dr, row in zip(date_range_granularity, matrix)
    }).T
    if args.resample not in ("no", "raw"):
        aliases = {
            "year": "A",
            "month": "M"
        }
        df = df.resample(aliases.get(args.resample, args.resample)).mean()
        matrix = df.as_matrix()
        if args.resample in ("year", "A"):
            labels = [dt.year for dt in df.index]
        elif args.resample in ("month", "M"):
            labels = [dt.strftime("%Y %B") for dt in df.index]
        else:
            labels = [dt.date() for dt in df.index]
    else:
        labels = [
            "%s - %s" % ((start + timedelta(days=i * granularity)).date(),
                         (start + timedelta(days=(i + 1) * granularity)).date())
            for i in range(matrix.shape[0])]
        if len(labels) > 18:
            warnings.warn("Too many labels - consider resampling.")
    pyplot.stackplot(date_range_sampling, matrix, labels=labels)
    pyplot.legend(loc=2, fontsize=args.text_size)
    pyplot.ylabel("Lines of code", fontsize=args.text_size)
    pyplot.ylabel("Time", fontsize=args.text_size)
    pyplot.tick_params(labelsize=args.text_size)
    pyplot.gcf().set_size_inches(12, 9)
    if not args.output:
        pyplot.gcf().canvas.set_window_title(
            "Hercules %d x %d (granularity %d, sampling %d)" %
            (matrix.shape + (granularity, sampling)))
        pyplot.show()
    else:
        pyplot.tight_layout()
        pyplot.savefig(args.output, transparent=True)

if __name__ == "__main__":
    sys.exit(main())
