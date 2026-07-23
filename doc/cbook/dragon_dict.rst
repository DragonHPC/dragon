DDict Function Smoothing Example
=================================

This example demonstrates how to use DragonHPC's :ref:`DDict <Dragon-Data-DDict>` distributed dictionary
with checkpointing for a simple distributed smoothing algorithm. It also
demonstrates how to use persistence in the DDict to be able to restart from a
persisted checkpoint.

The Smoothing Algorithm
--------------------------

The algorithm presented here is primarily intended to showcase a few aspects of
the DDict including:

* Data sharing across processes
* Checkpointing as implemented by the DDict for process synchronization
* Checkpoint persistence when a restart of a long computation might be necessary

The smoothing algorithm implements a simple 1D averaging filter where each
element in the array is replaced by the average of itself and its immediate
neighbors. For an array element at position `i`, the smoothed value becomes:

`smoothed[i] = (array[i-1] + array[i] + array[i+1]) / 3`

The algorithm proceeds as follows:

- It first divides a 1D array into `N` chunks.
- Then it stores each chunk in a distributed dictionary (`DDict`).
- Once initialized, it launches one Dragon worker process per chunk.
- Each worker repeatedly smooths its chunk and at chunk boundaries it uses neighbor values from adjacent
  chunks.
- After each iteration, the worker advances the DDict checkpoint using `ddict.checkpoint()`.
- It then writes its updated chunk vector to the DDict for use on the next iteration.
- The example supports both in-memory communication between processes and persisted checkpointing.

The entire program is provided at
https://github.com/DragonHPC/dragon/tree/main/examples/dragon_data/ddict/smoothing.py
and in :numref:`smoothing-prog` for your reference.

The algorithm begins by firing up `N` workers to work on the chunks. Each worker
averages the values of the function by averaging three value at `i`, `i-1`, and
`i+1` for all i in the chunk assigned to the worker. This works great for the
interior values of the chunk, but the values at the left and right boundaries
depend on values from workers working on neighboring chunks. The code in
:numref:`ddict-ref` shows how a worker retrieves the data it needs from the
chunks to the left or right of its chunk.

.. literalinclude:: ../../examples/dragon_data/ddict/ddict_smoothing.py
  :language: python
  :caption: Transparent Multi-node Data Sharing
  :name: ddict-ref
  :linenos:
  :start-after: start-data-sharing-ref
  :end-before: end-data-sharing-ref

Again, since the array is distributed across multiple chunks, each worker process
is responsible for smoothing one chunk. However, to compute the smoothed values
at the chunk boundaries, workers need access to values from neighboring chunks:

- The worker for chunk `k` needs the last element of chunk `k-1` to smooth its
  first element.
- The worker for chunk `k` needs the first element of chunk `k+1` to smooth its
  last element.

Workers coordinate through the DDict to read neighbor values before computing
each smoothing iteration. This creates a distributed dependency pattern where
workers synchronize their reads and writes through the checkpoint mechanism.

The code on lines 2 and 5 of :numref:`ddict-ref` blocks if the data is not
available. Initially, all chunks are written to the DDict at checkpoint 0 and
since each worker starts at checkpoint 0, they will not block on the first
iteration of the algorithm. For all but the first iteration the worker must wait
for the next computed average value to compute the average of both the first and
last values of the chunk as exhibited in :numref:`ddict-ref`.

.. literalinclude:: ../../examples/dragon_data/ddict/ddict_smoothing.py
  :language: python
  :caption: Update the Average
  :name: ddict-smooth-avg
  :linenos:
  :start-after: start-compute-avg
  :end-before: end-compute-avg

Once the average of a chunk is completely computed, that new computed value is
the value to be used for the next iteration of the worker. In
:numref:`ddict-smooth-avg` the new value is computed by the call to
`smooth-segment`. Then `checkpoint` is called to increment the worker's
checkpoint id, indicating that the client has completed it's computation at step
`i` of the iteration. Then on line 5 it stores its new average for the segment to
be used by its neighbors on iteration `i+1` of the algorithm. Any client
trying to load a neighbor's chunk on lines 2 or 4 of :numref:`ddict-ref` will
block until that neighbor has executed line 5 of :numref:`ddict-smooth-avg`. The
DDict provides the synchronization necessary to block the workers until data is
available at the current checkpoint.

.. literalinclude:: ../../examples/dragon_data/ddict/ddict_smoothing.py
  :language: python
  :caption: Persistent Key/Value Pairs in the DDict
  :name: pput-kvs
  :linenos:
  :start-after: start-pput
  :end-before: end-pput

There is one additional snippet of code worth mentioning. In :numref:`pput-kvs`
the algorithm initially stores `num_chunks` and `chunk_size` as two constants
for use by workers. These constant values don't change and as such they are
called *persistent* key/value pairs. The don't change so they are not affected
by checkpointing. If an algorithm needs to store values that do not change with
each iteration, then they probably should be stored using `pput` as persistent
key/value pairs.

In summary, the algorithm demonstrates how distributed computations can be
checkpointed at regular intervals to handle synchronization and communication
between workers. The next section describes how checkpoints may be optionally
persisted in this example program, demonstrating how persisted checkpoints allow
for fault tolerance and the ability to resume long-running computations from
intermediate states.

Persistence of Checkpoints
----------------------------

Should it be necessary, checkpoints can be persisted to longer-term storage like
disk. For example, this might be necessary for longer computations that may be
prone to hardware failure because of the scale at which they run. In such cases,
resuming the computation from a persisted checkpoint may be required.

.. literalinclude:: ../../examples/dragon_data/ddict/ddict_smoothing.py
  :language: python
  :caption: Specifying Persistence
  :name: smooth-persist
  :linenos:
  :start-after: smooth-kwargs-start
  :end-before: smooth-kwargs-end

No additional code is necessary when persisting checkpoints to disk. The DDict
handles that by writing persisted checkpoints to disk on some user-specified
frequency. When constructing the DDict you can specify four persistence arguments
as shown in :numref:`smooth-persist`.

  * `persister_class` - This selects how you want the checkpoints persisted. It is possible for user to specify their own persister class. Two persister classes are provided with Dragon, a Posix file system persister, and a DAOS persister.
  * `persist_frequency` - This is the frequency that checkpoints should be persisted. With this argument it is possible to only persist every so often as opposed to each checkpoint.
  * `persist_count` - This is the number of persisted checkpoints that are kept during a run. With this argument, the DDict is instructed to only keep the last `persist_count` checkpoints by deleting the oldest when a new one is created beyond the `persist_count`.
  * `persist_path` - The path (`persister_class` specific) where the persisted checkpoints should be stored.

To test persistence, you can run the program to persist checkpoints first,
then run it again to restore from a persisted checkpoint and it will resume from
the last persisted checkpoint. The various run modes and usage for this program
are as follows.

Modes
-------

- `validate`: runs the smoothing algorithm without persistence, purely
  in-memory.
- `persist`: runs the smoothing algorithm while persisting checkpoints using the
  configured persister.
- `restore`: restores the latest persisted checkpoint and continues smoothing
  from that point.

Usage
------

This first example runs the smoothing program to validate that it runs to
completion and utilizes checkpointing correctly.

.. code-block:: console
  :linenos:

  examples/dragon_data/ddict $ dragon ddict_smoothing.py --mode validate --size 128 --chunks 4 --iterations 10
  DDict built in validate mode, populating initial chunks...
  Initial chunks populated, starting smoothing iterations...
  Starting DDict smoothing: mode=validate chunks=4 iterations=10 checkpoint=0
  Finished smoothing mode=validate checkpoint=10 result min=0.012784 max=0.987216
  +++ head proc exited, code 0

This mode of running it runs the program with peristent checkpointing to create some
sample persisted checkpoints which can then be used in the restart mode.

.. code-block:: console
  :linenos:

  examples/dragon_data/ddict $ mkdir ddict_checkpoints
  examples/dragon_data/ddict $ dragon ddict_smoothing.py --mode persist --size 128 --chunks 4 --iterations 10 --persist_path ./ddict_checkpoints
  Starting DDict smoothing: mode=persist chunks=4 iterations=10 checkpoint=0
  Finished smoothing mode=persist checkpoint=10 result min=0.012784 max=0.987216
  Persisted checkpoint ids: [3, 11, 12, 13, 14]
  +++ head proc exited, code 0

Finally, this mode will restart from the last persisted checkpoint and run for the number of specified iterations
from that last checkpoint.

.. code-block:: console
  :linenos:

  examples/dragon_data/ddict $ dragon ddict_smoothing.py --mode restore --resume_iterations 8 --persist_path ./ddict_checkpoints
  Restoring persisted checkpoint 14
  Starting DDict smoothing: mode=restore chunks=4 iterations=8 checkpoint=14
  Finished smoothing mode=restore checkpoint=22 result min=0.020463 max=0.979537
  Restored to checkpoint 14 and continued smoothing.
  +++ head proc exited, code 0

Arguments
----------

- `--size`: total length of the array to smooth.
- `--chunks`: number of distributed chunks / worker processes.
- `--iterations`: number of smoothing iterations to perform.
- `--resume_iterations`: number of iterations to run after restore.
- `--working_set_size`: checkpoint working set size in the DDict.
- `--persist_path`: directory or storage path for persisted checkpoints.
- `--persister`: use `POSIX` or `DAOS` checkpoint backends.
- `--trace`: print per-chunk progress during smoothing.

Full Program
---------------

.. literalinclude:: ../../examples/dragon_data/ddict/ddict_smoothing.py
  :language: python
  :caption: The Complete Function Smoothing Program
  :name: smoothing-prog
  :linenos: