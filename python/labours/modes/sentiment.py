from datetime import datetime, timedelta

import numpy

from labours.plotting import apply_plot_style, deploy_plot, get_plot_path, import_pyplot
from labours.utils import parse_date


def show_sentiment_stats(args, name, resample, start_date, data):
    from scipy.signal import convolve, slepian

    matplotlib, pyplot = import_pyplot(args.backend, args.style)

    start_date = datetime.fromtimestamp(start_date)
    data = sorted(data.items())
    mood = numpy.zeros(data[-1][0] + 1, dtype=numpy.float32)
    timeline = numpy.array(
        [start_date + timedelta(days=i) for i in range(mood.shape[0])]
    )
    for d, val in data:
        mood[d] = (0.5 - val.Value) * 2
    resolution = 32
    window = slepian(len(timeline) // resolution, 0.5)
    window /= window.sum()
    mood_smooth = convolve(mood, window, "same")
    pos = mood_smooth.copy()
    pos[pos < 0] = 0
    neg = mood_smooth.copy()
    neg[neg >= 0] = 0
    resolution = 4
    window = numpy.ones(len(timeline) // resolution)
    window /= window.sum()
    avg = convolve(mood, window, "same")
    pyplot.fill_between(timeline, pos, color="#8DB843", label="Positive")
    pyplot.fill_between(timeline, neg, color="#E14C35", label="Negative")
    pyplot.plot(timeline, avg, color="grey", label="Average", linewidth=5)
    legend = pyplot.legend(loc=1, fontsize=args.font_size)
    pyplot.ylabel("Comment sentiment")
    pyplot.xlabel("Time")
    apply_plot_style(
        pyplot.gcf(), pyplot.gca(), legend, args.background, args.font_size, args.size
    )
    pyplot.xlim(
        parse_date(args.start_date, timeline[0]),
        parse_date(args.end_date, timeline[-1]),
    )
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
        labels[startindex].set_text(timeline[0].date())
        labels[startindex].set_text = lambda _: None
        labels[startindex].set_rotation(30)
        labels[startindex].set_ha("right")
    if endindex >= 0:
        labels[endindex].set_text(timeline[-1].date())
        labels[endindex].set_text = lambda _: None
        labels[endindex].set_rotation(30)
        labels[endindex].set_ha("right")
    overall_pos = sum(2 * (0.5 - d[1].Value) for d in data if d[1].Value < 0.5)
    overall_neg = sum(2 * (d[1].Value - 0.5) for d in data if d[1].Value > 0.5)
    title = "%s sentiment +%.1f -%.1f δ=%.1f" % (
        name,
        overall_pos,
        overall_neg,
        overall_pos - overall_neg,
    )
    if args.mode == "all" and args.output:
        output = get_plot_path(args.output, "sentiment")
    else:
        output = args.output
    deploy_plot(title, output, args.background)
