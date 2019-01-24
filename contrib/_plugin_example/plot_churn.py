import argparse
from datetime import datetime, timedelta
import os
import re
import sys

from matplotlib import pyplot
import matplotlib.dates as mdates
import numpy
import pandas
import yaml


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Path to the input YAML file. \"-\" means stdin.")
    parser.add_argument("-o", "--output", help="Output directory. If empty, display the plots.")
    parser.add_argument("-f", "--format", choices=("png", "svg"), default="png",
                        help="Output format")
    parser.add_argument("--tick-days", type=int, default=7, help="Ticks interval in days.")
    args = parser.parse_args()
    return args


def parse_input(file):
    yaml.reader.Reader.NON_PRINTABLE = re.compile(r"(?!x)x")
    try:
        loader = yaml.CLoader
    except AttributeError:
        print("Warning: failed to import yaml.CLoader, falling back to slow yaml.Loader")
        loader = yaml.Loader
    try:
        if file != "-":
            with open(file) as fin:
                return yaml.load(fin, Loader=loader)
        else:
            return yaml.load(sys.stdin, Loader=loader)
    except (UnicodeEncodeError, yaml.reader.ReaderError) as e:
        print("\nInvalid unicode in the input: %s\nPlease filter it through "
              "fix_yaml_unicode.py" % e)
        sys.exit(1)


def plot_churn(name, data, url, beginTime, endTime, output, fmt, tick_interval):
    days, adds, dels = data["days"], data["additions"], data["removals"]
    dates = [beginTime + timedelta(days=d) for d in days]
    df = pandas.DataFrame(data=list(zip(adds, dels)),
                          index=dates,
                          columns=("additions", "removals"))
    df["removals"] = -df["removals"]
    df = df.reindex(pandas.date_range(beginTime, endTime, freq="D"))
    effective = df["additions"] + df["removals"]
    effective = effective.cumsum()
    effective.fillna(method="ffill", inplace=True)
    scale = numpy.maximum(df.max(), -df.min()).max()
    effective = effective / effective.max() * scale
    pyplot.figure(figsize=(16, 9))
    for spine in pyplot.gca().spines.values():
        spine.set_visible(False)
    pyplot.gca().xaxis.set_major_locator(mdates.DayLocator(interval=tick_interval))
    pyplot.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    pyplot.tick_params(top="off", bottom="off", left="off", right="off",
                       labelleft="off", labelbottom="on")
    pyplot.bar(df.index, df["additions"], label="additions")
    pyplot.bar(df.index, df["removals"], label="removals")
    pyplot.plot(df.index, effective, "black", label="effective")
    pyplot.xticks(rotation="vertical")
    pyplot.legend(loc=2, fontsize=18)
    pyplot.title("%s churn plot, %s" % (name, url), fontsize=24)
    if not output:
        pyplot.show()
    else:
        os.makedirs(output, exist_ok=True)
        pyplot.savefig(os.path.join(output, name.replace("/", "_") + "." + fmt),
                       bbox_inches="tight", transparent=True)


def main():
    args = parse_args()
    data = parse_input(args.input)
    beginTime, endTime = (datetime.fromtimestamp(data["hercules"][t])
                          for t in ("begin_unix_time", "end_unix_time"))
    for key, val in data["ChurnAnalysis"].items():
        plot_churn(key, val, data["hercules"]["repository"], beginTime, endTime,
                   args.output, args.format, args.tick_days)


if __name__ == "__main__":
    sys.exit(main())
