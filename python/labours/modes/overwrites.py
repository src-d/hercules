import json

import numpy

from labours.plotting import apply_plot_style, deploy_plot, get_plot_path, import_pyplot
from labours.utils import default_json


def load_overwrites_matrix(people, matrix, max_people, normalize=True):
    matrix = matrix.astype(float)
    if matrix.shape[0] > max_people:
        order = numpy.argsort(-matrix[:, 0])
        matrix = matrix[order[:max_people]][:, [0, 1] + list(2 + order[:max_people])]
        people = [people[i] for i in order[:max_people]]
        print("Warning: truncated people to most productive %d" % max_people)
    if normalize:
        zeros = matrix[:, 0] == 0
        matrix[zeros, :] = 1
        matrix /= matrix[:, 0][:, None]
        matrix[zeros, :] = 0
    matrix = -matrix[:, 1:]
    for i, name in enumerate(people):
        if len(name) > 40:
            people[i] = name[:37] + "..."
    return people, matrix


def plot_overwrites_matrix(args, repo, people, matrix):
    if args.output and args.output.endswith(".json"):
        data = locals().copy()
        del data["args"]
        data["type"] = "overwrites_matrix"
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
    ax.set_xticklabels(
        ["Unidentified"] + people,
        rotation=45,
        ha="left",
        va="bottom",
        rotation_mode="anchor",
    )
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
    deploy_plot(title, output, args.background)
