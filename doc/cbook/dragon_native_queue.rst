Creating and Using a Queue in Dragon Native
-------------------------------------------

The Dragon Native Queue implementation is Dragon's specialized
implementation of a Queue, working in both single and multi node settings.
It is interoperable in all supported languages. The API is similar to Python's
Multiprocessing.Queue in many ways, but has a number of extensions and
simplifications. In particular, the queue can be initialized as joinable, which
allows to join on the completion of an item.


Using a Queue with Python
^^^^^^^^^^^^^^^^^^^^^^^^^

The Dragon Native Queue in Python mirrors the :external+python:py:class:`multiprocessing.Queue` interface
but works across multiple nodes and supports joinable queues. Import it from :py:mod:`dragon.native.queue`:

.. code-block:: python
    :linenos:
    :caption: **Creating and using a Dragon Native Queue in Python**

    import dragon
    from multiprocessing import set_start_method
    from dragon.native.process import Process
    from dragon.native.queue import Queue

    def producer(q):
        for i in range(5):
            q.put(f"item-{i}")
        q.put(None)  # sentinel to signal completion

    def consumer(q):
        while True:
            item = q.get()
            if item is None:
                break
            print(f"Received: {item}", flush=True)

    if __name__ == "__main__":
        set_start_method("dragon")

        q = Queue()
        p_prod = Process(target=producer, args=(q,))
        p_cons = Process(target=consumer, args=(q,))

        p_cons.start()
        p_prod.start()
        p_prod.join()
        p_cons.join()

The queue is serializable: it can be passed as an argument to any managed process, on any node in the
Dragon runtime, just like a :external+python:py:class:`multiprocessing.Queue`. For joinable queues,
initialize with ``joinable=True`` and call :py:meth:`~dragon.native.queue.Queue.task_done` after processing
each item, then use :py:meth:`~dragon.native.queue.Queue.join` to wait for all items to be processed:

.. code-block:: python
    :linenos:
    :caption: **Using a joinable Dragon Native Queue**

    import dragon
    from multiprocessing import set_start_method
    from dragon.native.process import Process
    from dragon.native.queue import Queue

    def worker(q):
        while True:
            item = q.get()
            if item is None:
                q.task_done()  # balance the sentinel's task count
                break
            print(f"Processing: {item}", flush=True)
            q.task_done()

    if __name__ == "__main__":
        set_start_method("dragon")

        q = Queue(joinable=True)
        p = Process(target=worker, args=(q,))
        p.start()

        for i in range(10):
            q.put(f"work-{i}")

        q.join()    # block until all items have been processed
        q.put(None)  # stop the worker
        p.join()

See :any:`dragon.native.queue.Queue` for the full API reference.

Using a Queue with C++
^^^^^^^^^^^^^^^^^^^^^^

Dragon's native Queue has a C++ interface provided by the ``dragon::Queue<T>``
class template declared in ``<dragon/queue.hpp>``. There is no separate C API;
C and C++ code both use this C++ template. A Queue is created in Python and then
attached from C++ using its serialized descriptor, so the Queue's lifetime is
managed by the Python code that created it.

The ``T`` type parameter is a serializable value type. Dragon ships several
ready-made serializable wrappers in ``<dragon/serializable.hpp>`` (for example
``dragon::SerializableString``, ``dragon::SerializableByteBuffer``,
``dragon::SerializableScalar``, and ``dragon::SerializableVector``). You can also
implement your own by subclassing ``dragon::SerializableBase``.

First, create the Queue in Python and pass its serialized descriptor to the C++
program. Use the Queue's :py:meth:`~dragon.native.queue.Queue.serialize` method,
which returns a base64-encoded descriptor intended for the C++ Queue to attach to:

.. code-block:: python
    :linenos:
    :caption: **run_cpp_queue.py — create the Queue and launch the C++ program**

    import dragon
    from multiprocessing import set_start_method
    from dragon.native.queue import Queue
    import subprocess

    if __name__ == "__main__":
        set_start_method("dragon")

        q = Queue()
        serialized = q.serialize()  # base64 FLI descriptor for the C++ side

        result = subprocess.run(
            ["./cpp_queue_demo", serialized],
            capture_output=True, text=True,
        )
        print("C++ stdout:", result.stdout.strip())

The C++ program attaches to that Queue, then puts and gets a value:

.. code-block:: c++
    :linenos:
    :caption: **cpp_queue_demo.cpp — attach to a Dragon Queue and put/get a value**

    #include <dragon/queue.hpp>
    #include <dragon/serializable.hpp>
    #include <iostream>

    int main(int argc, char* argv[]) {
        if (argc < 2) {
            std::cerr << "usage: " << argv[0] << " <serialized_queue_descr>\n";
            return 1;
        }

        try {
            // Attach to the Python-created queue (NULL => use the default pool).
            dragon::Queue<dragon::SerializableString> q(argv[1], nullptr);

            // Put a value, then get it back.
            dragon::SerializableString msg(std::string("Hello from C++"));
            q.put(msg);

            dragon::SerializableString got = q.get();
            std::cout << "C++ received: " << got.getVal() << std::endl;
        } catch (const dragon::DragonError& e) {
            std::cerr << "DragonError: " << e.err_str() << std::endl;
            return 1;
        }
        return 0;
    }

Compile against the Dragon headers and link against ``libdragon``. The
``dragon-config`` command emits the correct include and link flags for your
installation:

.. code-block:: console

    g++ -std=c++17 cpp_queue_demo.cpp \
        $(dragon-config -o) $(dragon-config -l) -o cpp_queue_demo

Then run the Python driver under the Dragon runtime. The C++ subprocess inherits
the runtime environment, so it can attach to the default memory pool:

.. code-block:: console

    dragon run_cpp_queue.py

The program prints::

    C++ stdout: C++ received: Hello from C++

See ``<dragon/queue.hpp>`` for the full C++ Queue API, including ``put``, ``get``,
``get_nowait``, ``poll``, and their timeout-aware overloads.
