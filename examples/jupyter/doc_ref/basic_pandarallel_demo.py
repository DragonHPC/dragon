# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # `pandarallel` with Controlled Number of Progress Bars
#
# Up until at least version 1.6.4 of `pandarallel`, it displayed 1 progress bar from every 1 worker process.  With a sufficiently large number of workers, this becomes overwhelming.
#
# This notebook demos a modification to `pandarallel` which exposes control over how many progress bars should be displayed and maps each worker process to one and only one of those progress bars.  In a multi-node `dragon` execution configuration (which is _not_ demonstrated here), some nodes may be slower/faster than others and it may be helpful to see the relative progress/speed of one cluster's nodes versus others -- this motivates showing more than just a single progress bar representing all workers.

# !pip install pandarallel

# +
import dragon
import multiprocessing

import cloudpickle

import numpy as np
import pandas as pd
import time

import pandarallel; pandarallel.__version__
# -

multiprocessing.set_start_method("dragon")
pandarallel.core.dill = cloudpickle
ctx = multiprocessing.get_context("dragon")
ctx.Manager = type("PMgr", (), {"Queue": ctx.Queue})
pandarallel.core.CONTEXT = ctx
pandarallel.pandarallel.initialize(progress_bar=True)

# +
num_rows = 10

df = pd.DataFrame(
    {
        "seqnum": np.arange(42, (42 + num_rows), dtype=int),
        #"metric_A": np.random.rand(num_rows),
        #"metric_B": np.random.rand(num_rows),
        "metric_C": np.random.rand(num_rows),
        "alt_seq": np.random.randint(low=42, high=(42 + num_rows), size=(num_rows,)),
        "label": np.array(list("ATCG"))[np.random.randint(0, 4, num_rows)],
    },
)
# -

df.head()

# The use of a global variable inside a lambda function demonstrates key functionality from `cloudpickle` that is not otherwise available through `dill`.

cutoff = 0.3

# Running this next cell will cause as many progress bars to be displayed as there are workers (potentially a lot).

start = time.monotonic()
df['highlow_C'] = df['metric_C'].parallel_apply(lambda x: x < cutoff)
stop = time.monotonic()
tot_time = stop - start
time_dict = {}
time_dict["1"] = tot_time

# Now we have our new column of values in our `pandas.DataFrame`.

df.head()

# We can change our minds about how many progress bars to display, at will.

pandarallel.pandarallel.initialize(progress_bar=10)  # Will display a total of 10 progress bars.

start = time.monotonic()
df['highlow_C'] = df['metric_C'].parallel_apply(lambda x: x < cutoff)
stop = time.monotonic()
tot_time = stop - start
time_dict["2"] = tot_time

# There will be plenty of use cases / scenarios where a single progress bar is all we want.

pandarallel.pandarallel.initialize(progress_bar=1)  # Will display 1 progress bar representing all workers.

start = stop = time.monotonic()
df['highlow_C'] = df['metric_C'].parallel_apply(lambda x: x < cutoff)
stop = time.monotonic()
tot_time = stop - start
time_dict["3"] = tot_time

print("parallel_apply","\t", "Time (nanoseconds)")
for key, value in time_dict.items():
    print("{:<20} {:<20}".format(key, value))

# Though it is very minor compared to the overall wall time, reducing the number of progress bars displayed can shave off a small amount of execution time.
