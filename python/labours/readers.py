from argparse import Namespace
from importlib import import_module
import io
import re
import sys
from typing import Any, BinaryIO, Dict, List, Tuple, TYPE_CHECKING

import numpy
import yaml

from labours.objects import DevDay

if TYPE_CHECKING:
    from scipy.sparse.csr import csr_matrix


class Reader(object):
    def read(self, fileobj: BinaryIO):
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
    def read(self, fileobj: BinaryIO):
        yaml.reader.Reader.NON_PRINTABLE = re.compile(r"(?!x)x")
        try:
            loader = yaml.CLoader
        except AttributeError:
            print(
                "Warning: failed to import yaml.CLoader, falling back to slow yaml.Loader"
            )
            loader = yaml.Loader
        try:
            wrapper = io.TextIOWrapper(fileobj, encoding="utf-8")
            data = yaml.load(wrapper, Loader=loader)
        except (UnicodeEncodeError, UnicodeDecodeError, yaml.reader.ReaderError) as e:
            print(
                "\nInvalid unicode in the input: %s\nPlease filter it through "
                "fix_yaml_unicode.py" % e
            )
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
        return header["sampling"], header["granularity"], header["tick_size"]

    def get_project_burndown(self):
        return (
            self.data["hercules"]["repository"],
            self._parse_burndown_matrix(self.data["Burndown"]["project"]).T,
        )

    def get_files_burndown(self):
        return [
            (p[0], self._parse_burndown_matrix(p[1]).T)
            for p in self.data["Burndown"]["files"].items()
        ]

    def get_people_burndown(self):
        return [
            (p[0], self._parse_burndown_matrix(p[1]).T)
            for p in self.data["Burndown"]["people"].items()
        ]

    def get_ownership_burndown(self):
        return (
            self.data["Burndown"]["people_sequence"].copy(),
            {
                p[0]: self._parse_burndown_matrix(p[1])
                for p in self.data["Burndown"]["people"].items()
            },
        )

    def get_people_interaction(self):
        return (
            self.data["Burndown"]["people_sequence"].copy(),
            self._parse_burndown_matrix(self.data["Burndown"]["people_interaction"]),
        )

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

        return munchify(
            {
                int(key): {
                    "Comments": vals[2].split("|"),
                    "Commits": vals[1],
                    "Value": float(vals[0]),
                }
                for key, vals in self.data["Sentiment"].items()
            }
        )

    def get_devs(self):
        people = self.data["Devs"]["people"]
        days = {
            int(d): {
                int(dev): DevDay(*(int(x) for x in day[:-1]), day[-1])
                for dev, day in devs.items()
            }
            for d, devs in self.data["Devs"]["ticks"].items()
        }
        return people, days

    def _parse_burndown_matrix(self, matrix):
        return numpy.array(
            [numpy.fromstring(line, dtype=int, sep=" ") for line in matrix.split("\n")]
        )

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
    def read(self, fileobj: BinaryIO) -> None:
        try:
            from labours.pb_pb2 import AnalysisResults
        except ImportError as e:
            print(
                "\n\n>>> You need to generate python/hercules/pb/pb_pb2.py - run \"make\"\n",
                file=sys.stderr,
            )
            raise e from None
        self.data = AnalysisResults()
        all_bytes = fileobj.read()
        if not all_bytes:
            raise ValueError("empty input")
        self.data.ParseFromString(all_bytes)
        self.contents = {}
        for key, val in self.data.contents.items():
            try:
                mod, name = PB_MESSAGES[key].rsplit(".", 1)
            except KeyError:
                sys.stderr.write(
                    "Warning: there is no registered PB decoder for %s\n" % key
                )
                continue
            cls = getattr(import_module(mod), name)
            self.contents[key] = msg = cls()
            msg.ParseFromString(val)

    def get_run_times(self):
        return {key: val for key, val in self.data.header.run_time_per_item.items()}

    def get_name(self) -> str:
        return self.data.header.repository

    def get_header(self) -> Tuple[int, int]:
        header = self.data.header
        return header.begin_unix_time, header.end_unix_time

    def get_burndown_parameters(self) -> Tuple[int, int, float]:
        burndown = self.contents["Burndown"]
        return burndown.sampling, burndown.granularity, burndown.tick_size / 1000000000

    def get_project_burndown(self) -> Tuple[str, numpy.ndarray]:
        return self._parse_burndown_matrix(self.contents["Burndown"].project)

    def get_files_burndown(self):
        return [self._parse_burndown_matrix(i) for i in self.contents["Burndown"].files]

    def get_people_burndown(self) -> List[Any]:
        return [
            self._parse_burndown_matrix(i) for i in self.contents["Burndown"].people
        ]

    def get_ownership_burndown(self) -> Tuple[List[Any], Dict[Any, Any]]:
        people = self.get_people_burndown()
        return [p[0] for p in people], {p[0]: p[1].T for p in people}

    def get_people_interaction(self):
        burndown = self.contents["Burndown"]
        return (
            [i.name for i in burndown.people],
            self._parse_sparse_matrix(burndown.people_interaction).toarray(),
        )

    def get_files_coocc(self) -> Tuple[List[str], 'csr_matrix']:
        node = self.contents["Couples"].file_couples
        return list(node.index), self._parse_sparse_matrix(node.matrix)

    def get_people_coocc(self) -> Tuple[List[str], 'csr_matrix']:
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

    def get_devs(self) -> Tuple[List[str], Dict[int, Dict[int, DevDay]]]:
        people = list(self.contents["Devs"].dev_index)
        days = {
            d: {
                dev: DevDay(
                    stats.commits,
                    stats.stats.added,
                    stats.stats.removed,
                    stats.stats.changed,
                    {
                        k: [v.added, v.removed, v.changed]
                        for k, v in stats.languages.items()
                    },
                )
                for dev, stats in day.devs.items()
            }
            for d, day in self.contents["Devs"].ticks.items()
        }
        return people, days

    def _parse_burndown_matrix(self, matrix):
        dense = numpy.zeros(
            (matrix.number_of_rows, matrix.number_of_columns), dtype=int
        )
        for y, row in enumerate(matrix.rows):
            for x, col in enumerate(row.columns):
                dense[y, x] = col
        return matrix.name, dense.T

    def _parse_sparse_matrix(self, matrix):
        from scipy.sparse import csr_matrix

        return csr_matrix(
            (list(matrix.data), list(matrix.indices), list(matrix.indptr)),
            shape=(matrix.number_of_rows, matrix.number_of_columns),
        )


READERS = {"yaml": YamlReader, "yml": YamlReader, "pb": ProtobufReader}
PB_MESSAGES = {
    "Burndown": "labours.pb_pb2.BurndownAnalysisResults",
    "Couples": "labours.pb_pb2.CouplesAnalysisResults",
    "Shotness": "labours.pb_pb2.ShotnessAnalysisResults",
    "Devs": "labours.pb_pb2.DevsAnalysisResults",
}


def chain_streams(streams, buffer_size=io.DEFAULT_BUFFER_SIZE):
    """
    Chain an iterable of streams together into a single buffered stream.
    Source: https://stackoverflow.com/a/50770511

    Usage:
        f = chain_streams(open(f, "rb") for f in filenames)
        f.read()
    """

    class ChainStream(io.RawIOBase):
        def __init__(self):
            self.leftover = b""
            self.stream_iter = iter(streams)
            try:
                self.stream = next(self.stream_iter)
            except StopIteration:
                self.stream = None

        def readable(self):
            return True

        def _read_next_chunk(self, max_length):
            # Return 0 or more bytes from the current stream, first returning all
            # leftover bytes. If the stream is closed returns b''
            if self.leftover:
                return self.leftover
            elif self.stream is not None:
                return self.stream.read(max_length)
            else:
                return b""

        def readinto(self, b):
            buffer_length = len(b)
            chunk = self._read_next_chunk(buffer_length)
            while len(chunk) == 0:
                # move to next stream
                if self.stream is not None:
                    self.stream.close()
                try:
                    self.stream = next(self.stream_iter)
                    chunk = self._read_next_chunk(buffer_length)
                except StopIteration:
                    # No more streams to chain together
                    self.stream = None
                    return 0  # indicate EOF
            output, self.leftover = chunk[:buffer_length], chunk[buffer_length:]
            b[:len(output)] = output
            return len(output)

    return io.BufferedReader(ChainStream(), buffer_size=buffer_size)


def read_input(args: Namespace) -> ProtobufReader:
    sys.stdout.write("Reading the input... ")
    sys.stdout.flush()
    if args.input != "-":
        stream = open(args.input, "rb")
    else:
        stream = sys.stdin.buffer
    try:
        if args.input_format == "auto":
            buffer = stream.read(1 << 16)
            try:
                buffer.decode("utf-8")
                args.input_format = "yaml"
            except UnicodeDecodeError:
                args.input_format = "pb"
            ins = chain_streams((io.BytesIO(buffer), stream), len(buffer))
        else:
            ins = stream
        reader = READERS[args.input_format]()
        reader.read(ins)
    finally:
        if args.input != "-":
            stream.close()
    print("done")
    return reader
