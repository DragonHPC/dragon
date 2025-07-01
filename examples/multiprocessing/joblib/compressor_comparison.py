"""
===============================
Improving I/O using compressors
===============================

This example compares the compressors available in Joblib. In the example,
Zlib, LZMA and LZ4 compression only are used but Joblib also supports BZ2 and
GZip compression methods.
For each compared compression method, this example dumps and reloads a
dataset fetched from an online machine-learning database. This gives 3
information: the size on disk of the compressed data, the time spent to dump
and the time spent to reload the data from disk.
"""

import dragon
import multiprocessing as mp
import os
import os.path
import time

import pandas as pd
from joblib import dump, load

if __name__ == "__main__":
    mp.set_start_method("dragon")
    url = "https://github.com/joblib/dataset/raw/main/kddcup.data.gz"
    names = (
        "duration, protocol_type, service, flag, src_bytes, "
        "dst_bytes, land, wrong_fragment, urgent, hot, "
        "num_failed_logins, logged_in, num_compromised, "
        "root_shell, su_attempted, num_root, "
        "num_file_creations, "
    ).split(", ")

    data = pd.read_csv(url, names=names, nrows=1e6)

    pickle_file = "./pickle_data.joblib"
    start = time.monotonic()
    with open(pickle_file, "wb") as f:
        dump(data, f)
    raw_dump_duration = time.monotonic() - start
    print("Raw dump duration: %0.3fs" % raw_dump_duration)
    raw_file_size = os.stat(pickle_file).st_size / 1e6
    print("Raw dump file size: %0.3fMB" % raw_file_size)
    start = time.monotonic()
    with open(pickle_file, "rb") as f:
        load(f)
    raw_load_duration = time.monotonic() - start
    print("Raw load duration: %0.3fs" % raw_load_duration)

    start = time.monotonic()
    with open(pickle_file, "wb") as f:
        dump(data, f, compress=("lzma", 3))
    lzma_dump_duration = time.monotonic() - start
    print("LZMA dump duration: %0.3fs" % lzma_dump_duration)

    lzma_file_size = os.stat(pickle_file).st_size / 1e6
    print("LZMA file size: %0.3fMB" % lzma_file_size)

    start = time.monotonic()
    with open(pickle_file, "rb") as f:
        load(f)
    lzma_load_duration = time.monotonic() - start
    print("LZMA load duration: %0.3fs" % lzma_load_duration)

    lz4_file_size = os.stat(pickle_file).st_size / 1e6
    print("LZ4 file size: %0.3fMB" % lz4_file_size)

    start = time.monotonic()
    with open(pickle_file, "rb") as f:
        load(f)
    lz4_load_duration = time.monotonic() - start
    print("LZ4 load duration: %0.3fs" % lz4_load_duration)

    os.remove(pickle_file)
