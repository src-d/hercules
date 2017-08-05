import argparse
from datetime import datetime, timedelta
import io
import os
import re
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
    input = raw_input


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", default="",
                        help="Path to the output file/directory (empty for display).")
    parser.add_argument("-i", "--input", default="-",
                        help="Path to the input file (- for stdin).")
    parser.add_argument("--text-size", default=12, type=int,
                        help="Size of the labels and legend.")
    parser.add_argument("--backend", help="Matplotlib backend to use.")
    parser.add_argument("--style", choices=["black", "white"], default="black",
                        help="Plot's general color scheme.")
    parser.add_argument("--relative", action="store_true",
                        help="Occupy 100%% height for every measurement.")
    parser.add_argument("--couples-tmp-dir", help="Temporary directory to work with couples.")
    parser.add_argument("-m", "--mode",
                        choices=["project", "file", "person", "churn_matrix", "people", "couples",
                                 "all"],
                        default="project", help="What to plot.")
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


def read_input(args):
    sys.stdout.write("Reading the input... ")
    sys.stdout.flush()
    yaml.reader.Reader.NON_PRINTABLE = re.compile(r"(?!x)x")
    try:
        loader = yaml.CLoader
    except AttributeError:
        print("Warning: failed to import yaml.CLoader, falling back to slow yaml.Loader")
        loader = yaml.Loader
    try:
        if args.input != "-":
            with open(args.input) as fin:
                data = yaml.load(fin, Loader=loader)
        else:
            data = yaml.load(sys.stdin, Loader=loader)
    except (UnicodeEncodeError, yaml.reader.ReaderError) as e:
        print("\nInvalid unicode in the input: %s\nPlease filter it through fix_yaml_unicode.py" %
              e)
        sys.exit(1)
    print("done")
    return data["burndown"], data["project"], data.get("files"), data.get("people_sequence"), \
           data.get("people"), data.get("people_interaction"), data.get("files_coocc"), \
           data.get("people_coocc")


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
    return (lifetimes.dot(numpy.arange(1, matrix.shape[1], 1))
            / (lifetimes.sum() * matrix.shape[1]))


def load_main(header, name, matrix, resample):
    import pandas

    start = header["begin"]
    last = header["end"]
    granularity = header["granularity"]
    sampling = header["sampling"]
    start = datetime.fromtimestamp(int(start))
    last = datetime.fromtimestamp(int(last))
    granularity = int(granularity)
    sampling = int(sampling)
    matrix = numpy.array([numpy.fromstring(line, dtype=int, sep=" ")
                          for line in matrix.split("\n")]).T
    print(name, "lifetime index:", calculate_average_lifetime(matrix))
    finish = start + timedelta(days=matrix.shape[1] * sampling)
    if resample not in ("no", "raw"):
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
                daily_matrix[istart:ifinish, (sdt - start).days:].sum(axis=0)
        # Hardcode some cases to improve labels" readability
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


def load_churn_matrix(contents):
    matrix = numpy.array([numpy.fromstring(line, dtype=int, sep=" ")
                          for line in contents.split("\n")])
    return matrix


def load_people(header, sequence, contents):
    import pandas

    start = header["begin"]
    last = header["end"]
    sampling = header["sampling"]
    start = datetime.fromtimestamp(int(start))
    last = datetime.fromtimestamp(int(last))
    sampling = int(sampling)
    people = []
    for name in sequence:
        people.append(numpy.array([numpy.fromstring(line, dtype=int, sep=" ")
                                   for line in contents[name].split("\n")]).sum(axis=1))
    people = numpy.array(people)
    date_range_sampling = pandas.date_range(
        start + timedelta(days=sampling), periods=people[0].shape[0],
        freq="%dD" % sampling)
    return sequence, people, date_range_sampling, last


def apply_plot_style(figure, axes, legend, style, text_size):
    figure.set_size_inches(12, 9)
    for side in ("bottom", "top", "left", "right"):
        axes.spines[side].set_color(style)
    for axis in (axes.xaxis, axes.yaxis):
        axis.label.update(dict(fontsize=text_size, color=style))
    for axis in ("x", "y"):
        axes.tick_params(axis=axis, colors=style, labelsize=text_size)
    if legend is not None:
        frame = legend.get_frame()
        for setter in (frame.set_facecolor, frame.set_edgecolor):
            setter("black" if style == "white" else "white")
        for text in legend.get_texts():
            text.set_color(style)


def get_plot_path(base, name):
    root, ext = os.path.splitext(base)
    if not ext:
        ext = ".png"
    output = os.path.join(root, name + ext)
    os.makedirs(os.path.dirname(output), exist_ok=True)
    return output


def deploy_plot(title, output, style):
    import matplotlib.pyplot as pyplot

    if not output:
        pyplot.gcf().canvas.set_window_title(title)
        pyplot.show()
    else:
        if title:
            pyplot.title(title, color=style)
        try:
            pyplot.tight_layout()
        except:
            print("Warning: failed to set the tight layout")
        pyplot.savefig(output, transparent=True)
    pyplot.clf()


def plot_burndown(args, target, name, matrix, date_range_sampling, labels, granularity,
                  sampling, resample):
    import matplotlib
    if args.backend:
        matplotlib.use(args.backend)
    import matplotlib.pyplot as pyplot

    pyplot.stackplot(date_range_sampling, matrix, labels=labels)
    if args.relative:
        for i in range(matrix.shape[1]):
            matrix[:, i] /= matrix[:, i].sum()
        pyplot.ylim(0, 1)
        legend_loc = 3
    else:
        legend_loc = 2
    legend = pyplot.legend(loc=legend_loc, fontsize=args.text_size)
    pyplot.ylabel("Lines of code")
    pyplot.xlabel("Time")
    apply_plot_style(pyplot.gcf(), pyplot.gca(), legend, args.style, args.text_size)
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


def plot_many(args, target, header, parts):
    if not args.output:
        print("Warning: output not set, showing %d plots." % len(parts))
    itercnt = progress.bar(parts.items(), expected_size=len(parts)) \
        if progress is not None else parts.items()
    stdout = io.StringIO()
    for name, matrix in itercnt:
        backup = sys.stdout
        sys.stdout = stdout
        plot_burndown(args, target, *load_main(header, name, matrix, args.resample))
        sys.stdout = backup
    sys.stdout.write(stdout.getvalue())


def plot_churn_matrix(args, repo, people, matrix):
    matrix = matrix.astype(float)
    if matrix.shape[0] > args.max_people:
        order = numpy.argsort(-matrix[:, 0])
        matrix = matrix[order[:args.max_people]][:, [0, 1] + list(order[:args.max_people])]
        people = [people[i] for i in order]
        print("Warning: truncated people to most productive %d" % args.max_people)
    zeros = matrix[:, 0] == 0
    matrix[zeros, :] = 1
    matrix /= matrix[:, 0][:, None]
    matrix = -matrix[:, 1:]
    matrix[zeros, :] = 0
    for i, name in enumerate(people):
        if len(name) > 40:
            people[i] = name[:37] + "..."

    import matplotlib
    if args.backend:
        matplotlib.use(args.backend)
    import matplotlib.pyplot as pyplot

    s = 4 + matrix.shape[1] * 0.3
    fig = pyplot.figure(figsize=(s, s))
    ax = fig.add_subplot(111)
    ax.xaxis.set_label_position("top")
    ax.matshow(matrix, cmap=pyplot.cm.OrRd)
    ax.set_xticks(numpy.arange(0, matrix.shape[1]))
    ax.set_yticks(numpy.arange(0, matrix.shape[0]))
    ax.set_xticklabels(["Unidentified"] + people, rotation=90, ha="center")
    ax.set_yticklabels(people, va="center")
    ax.set_xticks(numpy.arange(0.5, matrix.shape[1] + 0.5), minor=True)
    ax.set_yticks(numpy.arange(0.5, matrix.shape[0] + 0.5), minor=True)
    ax.grid(which="minor")
    apply_plot_style(fig, ax, None, args.style, args.text_size)
    if not args.output:
        pos1 = ax.get_position()
        pos2 = (pos1.x0 + 0.245, pos1.y0 - 0.1, pos1.width * 0.9, pos1.height * 0.9)
        ax.set_position(pos2)
    if args.mode == "all":
        output = get_plot_path(args.output, "matrix")
    else:
        output = args.output
    title = "%s %d developers overwrite" % (repo, matrix.shape[0])
    if args.output:
        # FIXME(vmarkovtsev): otherwise the title is screwed in savefig()
        title = ""
    deploy_plot(title, output, args.style)


def plot_people(args, repo, names, people, date_range, last):
    import matplotlib
    if args.backend:
        matplotlib.use(args.backend)
    import matplotlib.pyplot as pyplot

    if people.shape[0] > args.max_people:
        order = numpy.argsort(-people.sum(axis=1))
        people = people[order[:args.max_people]]
        names = [names[i] for i in order[:args.max_people]]
        print("Warning: truncated people to most owning %d" % args.max_people)
    for i, name in enumerate(names):
        if len(name) > 40:
            names[i] = name[:37] + "..."

    pyplot.stackplot(date_range, people, labels=names)
    pyplot.xlim(date_range[0], last)
    if args.relative:
        for i in range(people.shape[1]):
            people[:, i] /= people[:, i].sum()
        pyplot.ylim(0, 1)
        legend_loc = 3
    else:
        legend_loc = 2
    legend = pyplot.legend(loc=legend_loc, fontsize=args.text_size)
    apply_plot_style(pyplot.gcf(), pyplot.gca(), legend, args.style, args.text_size)
    if args.mode == "all":
        output = get_plot_path(args.output, "people")
    else:
        output = args.output
    deploy_plot("%s code ratio through time" % repo, output, args.style)


def train_embeddings(coocc_tree, tmpdir, shard_size=4096):
    from scipy.sparse import csr_matrix
    import tensorflow as tf
    try:
        from . import swivel
    except SystemError:
        import swivel

    index = coocc_tree["index"]
    nshards = len(index) // shard_size
    if nshards * shard_size < len(index):
        nshards += 1
        shard_size = len(index) // nshards
        nshards = len(index) // shard_size
    remainder = len(index) - nshards * shard_size
    if remainder > 0:
        lengths = numpy.array([len(cd) for cd in coocc_tree["matrix"]])
        filtered = sorted(numpy.argsort(lengths)[remainder:])
    else:
        filtered = list(range(len(index)))
    print("Reading the sparse matrix...")
    data = []
    indices = []
    indptr = [0]
    for row, cd in enumerate(coocc_tree["matrix"]):
        for col, val in sorted(cd.items()):
            data.append(val)
            indices.append(col)
        indptr.append(indptr[-1] + len(cd))
    matrix = csr_matrix((data, indices, indptr), shape=(len(index), len(index)))
    if len(filtered) < len(index):
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
        if len(meta_index) < 10000:
            embedding_size = 50
            num_epochs = 200
        elif len(meta_index) < 100000:
            embedding_size = 100
            num_epochs = 250
        elif len(meta_index) < 500000:
            embedding_size = 200
            num_epochs = 300
        else:
            embedding_size = 300
            num_epochs = 200
        swivel.FLAGS.embedding_size = embedding_size
        swivel.FLAGS.input_base_path = tmproot
        swivel.FLAGS.output_base_path = tmproot
        swivel.FLAGS.loss_multiplier = 1.0 / shard_size
        swivel.FLAGS.num_epochs = num_epochs
        swivel.main(None)
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
        except ImportError: # Python 2
            from BaseHTTPServer import HTTPServer, test
            from SimpleHTTPServer import SimpleHTTPRequestHandler

        class ClojureServer(HTTPServer):
            def __init__(self, *args, **kwargs):
                HTTPServer.__init__(self, *args, **kwargs)
                outer.server = self


        class CORSRequestHandler(SimpleHTTPRequestHandler):
            def end_headers (self):
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
    print("Wrote %s", jsonf)
    if run_server and not web_server.running:
        web_server.start()
    url = "http://projector.tensorflow.org/?config=http://0.0.0.0:8000/" + jsonf
    print(url)
    if run_server:
        os.system("xdg-open " + url)


def main():
    args = parse_args()
    header, main_contents, files_contents, people_sequence, people_contents, people_matrix, \
        files_coocc, people_coocc = read_input(args)
    name = next(iter(main_contents))

    files_warning = "Files stats were not collected. Re-run hercules with -files."
    people_warning = "People stats were not collected. Re-run hercules with -people."

    if args.mode == "project":
        plot_burndown(args, "project",
                      *load_main(header, name, main_contents[name], args.resample))
    elif args.mode == "file":
        if not files_contents:
            print(files_warning)
            return
        plot_many(args, "file", header, files_contents)
    elif args.mode == "person":
        if not people_contents:
            print(people_warning)
            return
        plot_many(args, "person", header, people_contents)
    elif args.mode == "churn_matrix":
        if not people_contents:
            print(people_warning)
            return
        plot_churn_matrix(args, name, people_sequence, load_churn_matrix(people_matrix))
    elif args.mode == "people":
        if not people_contents:
            print(people_warning)
            return
        plot_people(args, name, *load_people(header, people_sequence, people_contents))
    elif args.mode == "couples":
        write_embeddings("files", args.output, not args.disable_projector,
                         *train_embeddings(files_coocc, args.couples_tmp_dir))
        write_embeddings("people", args.output, not args.disable_projector,
                         *train_embeddings(people_coocc, args.couples_tmp_dir))
    elif args.mode == "all":
        plot_burndown(args, "project",
                      *load_main(header, name, main_contents[name], args.resample))
        if files_contents:
            plot_many(args, "file", header, files_contents)
        if people_contents:
            plot_many(args, "person", header, people_contents)
            plot_churn_matrix(args, name, people_sequence, load_churn_matrix(people_matrix))
            plot_people(args, name, *load_people(header, people_sequence, people_contents))
        if people_coocc:
            assert files_coocc
            write_embeddings("files", args.output, not args.disable_projector,
                             *train_embeddings(files_coocc, args.couples_tmp_dir))
            write_embeddings("people", args.output, not args.disable_projector,
                             *train_embeddings(people_coocc, args.couples_tmp_dir))
    if web_server.running:
        print("Sleeping for 60 seconds, safe to Ctrl-C")
        try:
            time.sleep(60)
        except KeyboardInterrupt:
            pass
        web_server.stop()

if __name__ == "__main__":
    sys.exit(main())
