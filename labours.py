import argparse
from datetime import datetime, timedelta
import sys

import numpy


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="",
                        help="Path to the output file (empty for display).")
    parser.add_argument("--text-size", default=12,
                        help="Size of the labels and legend.")
    parser.add_argument("--backend", help="Matplotlib backend to use.")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    import matplotlib
    if args.backend:
        matplotlib.use(args.backend)
    import matplotlib.pyplot as pyplot
    import pandas
    import seaborn  # to get nice colors, he-he

    start, granularity, sampling = input().split()
    start = datetime.fromtimestamp(int(start))
    granularity = int(granularity)
    sampling = int(sampling)
    matrix = numpy.array([numpy.fromstring(line, dtype=int, sep=" ")
                          for line in sys.stdin.read().split("\n")[:-1]]).T
    pyplot.stackplot(
        pandas.date_range(start, periods=matrix.shape[1], freq="%dD" % sampling),
        matrix,
        labels=["%s - %s" % ((start + timedelta(days=i * granularity)).date(),
                             (start + timedelta(days=(i + 1) * granularity)).date())
                for i in range(matrix.shape[0])])
    pyplot.legend(loc=2, fontsize=args.text_size)
    pyplot.ylabel("Lines of code", fontsize=args.text_size)
    pyplot.tick_params(labelsize=args.text_size)
    pyplot.gcf().set_size_inches(12, 9)
    if not args.output:
        pyplot.gcf().canvas.set_window_title(
            "Hercules %d x %d (granularity %d, sampling %d)" %
            (matrix.shape + (granularity, sampling)))
        pyplot.show()
    else:
        pyplot.tight_layout()
        pyplot.savefig(args.output)

if __name__ == "__main__":
    sys.exit(main())
