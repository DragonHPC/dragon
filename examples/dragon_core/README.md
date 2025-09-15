# Dragon Core API Examples

The purpose of examples here is to show how the Dragon API is used directly to accomplish a variety of tasks
including shared memory access and communication.

The example codes associated with the core API are meant to be run directly without the Dragon runtime (i.e.,
do not launch with `dragon`). They are an example of using the lowest portion of the Dragon stack directly.

_Before you proceed_, build the examples with

```
make
```

Run `make clean` to remove all compilation artifacts later.

These examples are not designed to work multi-node.

## Simple Managed Memory Demo

The purpose of this demo is to show basic usage of Dragon Managed Memory between C and Python applications.

You need two terminals to proceed.  In the first terminal:

```
python3 pool_demo.py
```

This will proceed to a prompt.  Initially, it will create a new Managed Memory Pool.  By pressing `Enter` it will then create a serialized descriptor of the Managed Memory Pool.  At this point, by pressing `Enter` again, it will write the serialized descriptor into a `*.bin` file for the other applications to see.

In the second terminal:

```
./c_pool
```

This code writes data into an allocation in the Managed Memory Pool for the next application to see.

Back on the first terminal, by following the prompt and pressing `Enter` consecutively, we read the memory serializer from the other application, we attach to the memory and finally read the data.  Next, we press `Enter` and we create another memory allocation with new data.  We follow the prompt and keep pressing `Enter` until we write the new serializer to another `*.bin` file.

Back in the second terminal:

```
python3 py_pool.py
```

This will read the data written by the C application and print it out.  If we continue pressing `Enter`, the data in the allocations will be modified.

Finally, back in the first terminal, by pressing `Enter`, we get the updated data and clean up the Managed Memory Pool.

## Simple Logging Demo

This demo showcases the use of Dragon core's logging API.

Run it Python demo with

```
python3 py_logging.py
```

To run the C version of the demo with

```
./c_logging
```

## Performance Tests

A set of performance tests for Dragon channels and managed memory can be found in the directory labeled performance. See the `performance/README.md` for details.