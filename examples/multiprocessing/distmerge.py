import dragon
import multiprocessing as mp
import heapq
import random

# In a real use of the MergePool class defined in this module, the sorter
# function below would be replaced by possibly many different functions that all
# produce ordered data that needs to be merged together into one sequence in an
# application. Think of many simulations that have a timecode associated with
# steps in the simulation. If you want to correlate the data by timecode, this
# MergePool class could be used and each of the functions producing ordered data
# would be different invocations of a simulation. Of course, there would be many
# more possible uses of merging sorted data into one stream.

def sorter(queue):
    # For this example, we'll generate some data, randomize the order, then sort it.
    # Presumably this would be the data that your function produces that you want to
    # combine, in sorted order, with data from other sources.
    data = list(range(10))
    random.shuffle(data)
    data.sort()

    # Then the process writes its sorted data to the provided queue to be merged with
    # other data from other sources
    for item in data:
        queue.put(item)

    # When this process's data is exhausted, we close the queue.
    queue.close()

# A SentinelQueue is a queue that raise EOFError when end of
# file is reached. It knows to do this by writing a sentinel
# into the queue when the queue is closed. A SentinelQueue
# contains a multiprocessing Queue. Other methods could
# be implemented for SentinelQueue, but are not needed in
# this example.
class SentinelQueue:
    _SENTINEL = 'sentinel_queue_sentinel'

    def __init__(self, maxsize=10):
        self.queue = mp.Queue(maxsize)

    def get(self):
        item = self.queue.get()
        if item == SentinelQueue._SENTINEL:
            raise EOFError('At EOF')

        return item

    def put(self, item):
        self.queue.put(item)

    def close(self):
        self.queue.put(SentinelQueue._SENTINEL)
        self.queue.close()

# The PQEntry class is needed by the priority
# queue which is used to always know which queue
# to get the next value from. The __lt__ orders
# the priority queue elements by their original
# values. But the queue index of where the value
# came from is carried along with the value in the
# priority queue so the merging algorithm knows where
# to get the next value. In this way, the total number
# of entries in the priority queue is never more than the
# fanin value of the MergePool.
class PQEntry:
    def __init__(self, value, queue_index):
        self._value = value
        self._queue_index = queue_index

    def __lt__(self, other):
        return self.value < other.value

    @property
    def queue_index(self):
        return self._queue_index

    @property
    def value(self):
        return self._value

    def __repr__(self):
        return 'PQEntry('+str(self.value)+','+str(self.queue_index)+')'

    def __str__(self):
        return repr(self)

# The MergePool is written so that start should be called to start
# a new MergePool object. Calling start returns a new process and a
# SentinelQueue from which to get the sorted values. The MergePool is
# given a list of functions which are the sources of sorted data that
# is to be merged together. This merging is done in a scalable fashion so
# this algorithm could be used to produce a sorted sequence of literally
# billions of items. The fanin_size can be set to a relatively high value
# since this will result in one priority queue with the number of elements
# equal to the fanin_size. The algorithm creates a tree of processes, each
# with a priority_queue of no more than fanin_size. The data is then merged
# up the tree until the original caller gets a sorted sequence of all the
# data from all the source functions.
class MergePool:
    def __init__(self):
        raise ValueError('Call MergePool.start to create a MergePool and run it.')

    # This is the hidden constructor that should only be called internally.
    def _init(sink_queue, source_functions, source_args=None, fanin_size=10, queue_depth=10):
        self = MergePool.__new__(MergePool)
        num_sources = len(source_functions)
        self._sources = []
        self._procs = []
        if num_sources > fanin_size:
            chunk_size = num_sources // fanin_size
            for i in range(0, num_sources, chunk_size):
                fun_chunk = source_functions[i:i+chunk_size]
                args_chunk = None if source_args is None else source_args[i:i+chunk_size]
                proc, sorted_queue = MergePool.start(fun_chunk, args_chunk, fanin_size, queue_depth)
                self._procs.append(proc)
                self._sources.append(sorted_queue)
        else:
            for i in range(len(source_functions)):
                sorted_queue = SentinelQueue(queue_depth)
                args = (sorted_queue,) if source_args is None else (sorted_queue,) + source_args[i]
                proc = mp.Process(target=source_functions[i], args=args)
                self._procs.append(proc)
                proc.start()
                self._sources.append(sorted_queue)

        self.sink_queue = sink_queue

        self.priority_queue = []

        self._prime_heap()

        return self

    def _prime_heap(self):
        for i in range(len(self._sources)):
            try:
                item = self._sources[i].get()
                heapq.heappush(self.priority_queue, PQEntry(item, i))
            except EOFError:
                pass

    def _start_merge_pool(sink_queue, source_functions, source_args, fanin_size, queue_depth):
        pool = MergePool._init(sink_queue, source_functions, source_args, fanin_size, queue_depth)
        pool._merge()

    def _merge(self):
        while len(self.priority_queue) > 0:
            item = heapq.heappop(self.priority_queue)
            self.sink_queue.put(item.value)

            try:
                next = self._sources[item.queue_index].get()
                heapq.heappush(self.priority_queue, PQEntry(next, item.queue_index))
            except EOFError:
                pass

        self.sink_queue.close()
        for proc in self._procs:
            proc.join()

    def start(source_functions, source_args=None, fanin_size=10, queue_depth=10):
        sink_queue = SentinelQueue(queue_depth)
        proc = mp.Process(target=MergePool._start_merge_pool,
                          args=(sink_queue, source_functions, source_args, fanin_size, queue_depth))
        proc.start()
        return (proc, sink_queue)

def main():
    mp.set_start_method('dragon')

    functions = [sorter] * 20
    proc, queue = MergePool.start(functions)

    try:
        while True:
            print(queue.get(), flush=True)
    except EOFError:
        pass

    proc.join()

if __name__ == "__main__":
    main()