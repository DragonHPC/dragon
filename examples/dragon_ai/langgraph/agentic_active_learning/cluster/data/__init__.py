"""Where the hundreds of megabytes go: nowhere.

Each MPI rank writes its mesh into the DDict manager on its own machine. The
field is never moved to the graph, the worker host, or another node. Instead,
`reduce_worker` is sent there as a placed process, opens the snapshot through
`mesh.local_view`, and sends back about ten numbers. `mesh` is the reader for
the four-key protocol the C hook writes.
"""
