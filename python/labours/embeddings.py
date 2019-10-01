import os
import shutil
import sys
import tempfile
from typing import List, Tuple

import numpy
from scipy.sparse.csr import csr_matrix

from labours.cors_web_server import web_server

IDEAL_SHARD_SIZE = 4096


def train_embeddings(
    index: List[str],
    matrix: csr_matrix,
    tmpdir: None,
    shard_size: int = IDEAL_SHARD_SIZE,
) -> Tuple[List[Tuple[str, numpy.int64]], List[numpy.ndarray]]:
    import tensorflow as tf
    from labours._vendor import swivel

    assert matrix.shape[0] == matrix.shape[1]
    assert len(index) <= matrix.shape[0]
    outlier_threshold = numpy.percentile(matrix.data, 99)
    matrix.data[matrix.data > outlier_threshold] = outlier_threshold
    nshards = len(index) // shard_size
    if nshards * shard_size < len(index):
        nshards += 1
        shard_size = len(index) // nshards
        nshards = len(index) // shard_size
    remainder = len(index) - nshards * shard_size
    if remainder > 0:
        lengths = matrix.indptr[1:] - matrix.indptr[:-1]
        filtered = sorted(numpy.argsort(lengths)[remainder:])
    else:
        filtered = list(range(len(index)))
    if len(filtered) < matrix.shape[0]:
        print("Truncating the sparse matrix...")
        matrix = matrix[filtered, :][:, filtered]
    meta_index = []
    for i, j in enumerate(filtered):
        meta_index.append((index[j], matrix[i, i]))
    index = [mi[0] for mi in meta_index]
    with tempfile.TemporaryDirectory(
        prefix="hercules_labours_", dir=tmpdir or None
    ) as tmproot:
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
                        int64_list=tf.train.Int64List(value=list(xs))
                    )

                def _floats(xs):
                    return tf.train.Feature(
                        float_list=tf.train.FloatList(value=list(xs))
                    )

                indices_row = reorder[row::nshards]
                indices_col = reorder[col::nshards]
                shard = matrix[indices_row][:, indices_col].tocoo()

                example = tf.train.Example(
                    features=tf.train.Features(
                        feature={
                            "global_row": _int64s(indices_row),
                            "global_col": _int64s(indices_col),
                            "sparse_local_row": _int64s(shard.row),
                            "sparse_local_col": _int64s(shard.col),
                            "sparse_value": _floats(shard.data),
                        }
                    )
                )

                with open(
                    os.path.join(tmproot, "shard-%03d-%03d.pb" % (row, col)), "wb"
                ) as out:
                    out.write(example.SerializeToString())
        print("Training Swivel model...")
        swivel.FLAGS.submatrix_rows = shard_size
        swivel.FLAGS.submatrix_cols = shard_size
        if len(meta_index) <= IDEAL_SHARD_SIZE / 16:
            embedding_size = 50
            num_epochs = 100000
        elif len(meta_index) <= IDEAL_SHARD_SIZE:
            embedding_size = 50
            num_epochs = 50000
        elif len(meta_index) <= IDEAL_SHARD_SIZE * 2:
            embedding_size = 60
            num_epochs = 10000
        elif len(meta_index) <= IDEAL_SHARD_SIZE * 4:
            embedding_size = 70
            num_epochs = 8000
        elif len(meta_index) <= IDEAL_SHARD_SIZE * 10:
            embedding_size = 80
            num_epochs = 5000
        elif len(meta_index) <= IDEAL_SHARD_SIZE * 25:
            embedding_size = 100
            num_epochs = 1000
        elif len(meta_index) <= IDEAL_SHARD_SIZE * 100:
            embedding_size = 200
            num_epochs = 600
        else:
            embedding_size = 300
            num_epochs = 300
        if os.getenv("CI"):
            # Travis, AppVeyor etc. during the integration tests
            num_epochs /= 10
        swivel.FLAGS.embedding_size = embedding_size
        swivel.FLAGS.input_base_path = tmproot
        swivel.FLAGS.output_base_path = tmproot
        swivel.FLAGS.loss_multiplier = 1.0 / shard_size
        swivel.FLAGS.num_epochs = num_epochs
        # Tensorflow 1.5 parses sys.argv unconditionally *applause*
        argv_backup = sys.argv[1:]
        del sys.argv[1:]
        swivel.main(None)
        sys.argv.extend(argv_backup)
        print("Reading Swivel embeddings...")
        embeddings = []
        with open(os.path.join(tmproot, "row_embedding.tsv")) as frow:
            with open(os.path.join(tmproot, "col_embedding.tsv")) as fcol:
                for i, (lrow, lcol) in enumerate(zip(frow, fcol)):
                    prow, pcol = (l.split("\t", 1) for l in (lrow, lcol))
                    assert prow[0] == pcol[0]
                    erow, ecol = (
                        numpy.fromstring(p[1], dtype=numpy.float32, sep="\t")
                        for p in (prow, pcol)
                    )
                    embeddings.append((erow + ecol) / 2)
    return meta_index, embeddings


def write_embeddings(
    name: str,
    output: str,
    run_server: bool,
    index: List[Tuple[str, numpy.int64]],
    embeddings: List[numpy.ndarray],
) -> None:
    print("Writing Tensorflow Projector files...")
    if not output:
        output = "couples"
    if output.endswith(".json"):
        output = os.path.join(output[:-5], "couples")
        run_server = False
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
        fout.write(
            """{
  "embeddings": [
    {
      "tensorName": "%s %s coupling",
      "tensorShape": [%s, %s],
      "tensorPath": "http://0.0.0.0:8000/%s",
      "metadataPath": "http://0.0.0.0:8000/%s"
    }
  ]
}
"""
            % (output, name, len(embeddings), len(embeddings[0]), dataf, metaf)
        )
    print("Wrote %s" % jsonf)
    if run_server and not web_server.running:
        web_server.start()
    url = "http://projector.tensorflow.org/?config=http://0.0.0.0:8000/" + jsonf
    print(url)
    if run_server:
        if shutil.which("xdg-open") is not None:
            os.system("xdg-open " + url)
        else:
            browser = os.getenv("BROWSER", "")
            if browser:
                os.system(browser + " " + url)
            else:
                print("\t" + url)
