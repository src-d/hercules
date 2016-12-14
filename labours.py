from datetime import datetime, timedelta
import sys

import matplotlib.pyplot as pyplot
import numpy
import pandas
import seaborn  # to get nice colors, he-he


def main():
    matrix = []
    start, granularity, sampling = input().split()
    start = datetime.fromtimestamp(int(start))
    granularity = int(granularity)
    sampling = int(sampling)
    for line in sys.stdin.read().split("\n")[:-1]:
        matrix.append(numpy.fromstring(line, dtype=int, sep=" "))
    matrix = numpy.array(matrix).T
    pyplot.stackplot(
        pandas.date_range(start, periods=matrix.shape[1], freq="%dD" % sampling),
        matrix,
        labels=["%s - %s" % ((start + timedelta(days=i * granularity)).date(),
                             (start + timedelta(days=(i + 1) * granularity)).date())
                for i in range(matrix.shape[0])])
    pyplot.legend(loc=2)
    pyplot.show()

if __name__ == "__main__":
    sys.exit(main())
