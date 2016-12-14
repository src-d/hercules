import sys

import matplotlib.pyplot as pyplot
import numpy
import seaborn  # to get nice colors, he-he


def main():
    matrix = []
    for line in sys.stdin.read().split("\n")[:-1]:
        matrix.append(numpy.fromstring(line, dtype=int, sep=" "))
    matrix = numpy.array(matrix).T
    pyplot.stackplot(numpy.arange(matrix.shape[1]), matrix)
    pyplot.show()

if __name__ == "__main__":
    sys.exit(main())
