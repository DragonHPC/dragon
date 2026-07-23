Core
++++

These examples cover Dragon's lower-level building blocks: native process
launching, communication channels, queue behavior, and explicit placement
policies. They are most useful when the higher-level multiprocessing or
workflow interfaces are not specific enough for the system behavior you need.

Start with this section if you are experimenting with Dragon internals,
debugging low-level runtime behavior, or learning the primitives that the
higher-level APIs build on.

.. toctree::
    :maxdepth: 1

    dragon_native_pi.rst
    c_channels_demo.rst
    dragon_native_queue.rst
    dragon_native_policy_demo.rst
