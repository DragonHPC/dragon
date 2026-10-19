# Heat Diffusion with Python and C++

This example runs a 2D heat diffusion simulation with the simulation loop in
Python and the stencil arithmetic in C++. It is built around Dragon's
Serializable support, which lets Dragon objects created in Python be used
directly from C++.

A cold plate is held at a fixed hot temperature along its top edge. Each step
replaces every interior cell with the average of its four neighbors, and the
heat spreads down through the plate.

## What it demonstrates

The C++ workers are started with two command line arguments: a serialized
Distributed Dictionary and a worker id. Everything else they need is read out of
that dictionary, because every Dragon object involved is serializable:

| Stored by Python | Read in C++ as |
| --- | --- |
| `XNDArray` (the grid and each worker's band) | `SerializableDoubleNDArray` |
| `Barrier` (step synchronization) | `Barrier` |
| `Semaphore` (startup handshake) | `Semaphore` |
| `Queue` (result reporting) | `Queue<Serializable>` |
| `int` (sizes and row ranges) | `int` |

The dictionary is created with `XPickler` for its keys and values, which is what
makes the entries readable as `Serializable` values on the C++ side:

```python
config = store.pickler(key_pickler=XPickler(), value_pickler=XPickler())
config["barrier"] = barrier
```

```cpp
DDict<Serializable, Serializable> config(ser_config, &TIMEOUT);
Barrier barrier = config[SerializableString("barrier")];
```

An `XNDArray` is worth calling out. Its data lives in the Distributed Dictionary
and only its meta data is passed between processes, so handing the grid to four
workers does not copy the grid four times. A process calls `refresh()` to pull
the current data and `sync()` to publish what it has changed.

## How the work is split

The interior rows are divided into contiguous bands, one per worker. Each worker
writes its results into its own band array rather than into the shared grid,
because `sync()` publishes an entire array. Python stitches the bands back into
the grid between steps.

Each step is two barrier waits:

```
worker                          driver
------                          ------
grid.refresh()
compute band from grid
band.sync()
barrier.wait()  <-- all bands published -->  barrier.wait()
                                             merge bands into grid
                                             grid.sync()
barrier.wait()  <-- new grid published  -->  barrier.wait()
```

The `Semaphore` starts at zero. Every worker releases it once after it has
attached to everything, and the driver acquires it once per worker before timing
the run. At the end each worker reports the largest change it made on its last
step through the `Queue`.

## Running it

Build the worker and run the driver under `dragon`, passing the number of
workers and, optionally, the number of steps:

```
> make
> dragon heat_simulation.py 4
> dragon heat_simulation.py 4 200
```

### Example Output

```
> dragon heat_simulation.py 4

Diffusing heat across a 34x34 plate for 500 steps using 4 C++ workers.

    @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
      ==****##################**++
      ::==++++**************++==--
      ..--====++++++++++++====--::
      ..::----============----::..
        ..::::------------::::....
        ....::::::::::::::::....
          ......::::::::........
            ..................
              ..............




    center temperature  19.5732
    largest last change 1.917e-02
    500 steps in 4.31 seconds
```

## Files

| File | Description |
| --- | --- |
| `heat_simulation.py` | The driver. Builds the grid, starts the workers and merges results. |
| `heat_worker.cpp` | The C++ worker. Applies the stencil to one band of rows. |
| `Makefile` | Builds `heat_worker`. |
