"""Pascal Triangle Shared State Example

The Pascal Triange is a famous mathematics concept that gives the binomial coefficients for any binomial expansion. 
The Pascal triangle row is constructed by summing up the elements in the preceding row.
The following example has an implementation of the Pascal triangle where the user provides the number of rows. 
The main multiprocessing process starts the manager multiprocessing process and the client multiprocessing process. 
The manager multiprocessing process starts the context multiprocessing process which creates a shared state with the array that contains all the elements of the Pascal array 
and the value that is the sum of the Pascal triangle. 
The manager, client, and context multiprocessing processes share the state and pass it to each other via a series of queues. 
The manager process increments the value, and the client process adds rows from the Pascal triangle to the Pascal triangle array. 
The context process uses an event to signal when the number of rows provided by the user has been reached, the Pascal triangle array has been filled, 
and the sum of the Pascal triangle has been calculated with the expected value.
The main process outputs the Pascal triangle array and the Pascal triangle sum.
The shared state is guarded by a lock, and the process that is accessing and modifying the state needs the lock. 
"""


import cloudpickle
import dragon
import multiprocessing as mp
import time
import sys
import argparse


def pascal(rows):
    # create pascal array for index
    rows -= 1
    pascal_row = [1]
    for row in range(max(rows, 0)):
        pascal_row.append(pascal_row[row] * (rows - row) // (row + 1))
    return pascal_row


def create_shared_state(queue, rows, shared_state_queue_created):
    # create value and array for shared state
    value = mp.Value("i", 0)
    # create the array the size of the completed Pascal triangle array
    array = mp.Array("i", [0] * sum(range(rows + 1)))
    # shared state that is used by all the processes
    shared_state = cloudpickle.dumps((value, array))
    # place the Pascal queue
    queue.put(shared_state)
    # the context process does not create the shared state again
    shared_state_queue_created.set()


def context(serialized_args: bytes) -> None:
    """
    Context checks if Pascal sum from mp.Value is correct
    """
    # if a certain length is reached, outputs the Pascal triangle
    (
        shared_state_queue,
        lock_queue,
        rows,
        pascal_event,
        shared_state_queue_created,
        final_shared_state_queue,
    ) = cloudpickle.loads(serialized_args)
    # if pascal event is set
    while not pascal_event.is_set():
        # create lock
        lock = None
        if lock_queue.empty():
            lock = mp.Lock()
        else:
            try:
                lock = cloudpickle.loads(lock_queue.get(timeout=1))
            except:
                pass
        # if lock is not None, enter this step
        if lock is not None:
            with lock:
                # create the shared state if shared state not set
                if shared_state_queue.empty() and not shared_state_queue_created.is_set():
                    create_shared_state(shared_state_queue, rows, shared_state_queue_created)
                else:
                    value, array = None, None
                    try:
                        # grab the value and array from the pascal creation queue
                        value, array = cloudpickle.loads(shared_state_queue.get(timeout=1))
                    except:
                        pass
                    # this comparison happens outside the try and except; checks that array and value exists and checks that the operations on Pascal triangle are complete.
                    if value is not None and array is not None and value.value == (2**rows):
                        # set pascal event and break out
                        pascal_event.set()
                        array = [1] + array[:]
                        # put the values in the queue that communicates with master
                        final_shared_state_queue.put(cloudpickle.dumps((value, array)))
                        break
                    # place value and array back in the pascal queue
                    shared_state_queue.put(cloudpickle.dumps((value, array)))
            # put lock back in lock queue
            lock_queue.put(cloudpickle.dumps(lock))
            time.sleep(0.1)
        else:
            pass


def start_context(serialized_args: bytes) -> None:
    (
        shared_state_queue,
        lock_queue,
        rows,
        pascal_event,
        shared_state_queue_created,
        final_shared_state_queue,
    ) = cloudpickle.loads(serialized_args)
    context_proc = mp.Process(target=context, args=(serialized_args,))
    context_proc.start()
    # join the context process if the pascal_event is set
    if pascal_event.is_set():
        context_proc.join()


def manager(serialized_args: bytes) -> None:
    """
    Manager sums Pascal triangle array for mp.Value
    """
    # receive context, pascal queue where value and array are passed, lock queue where lock is passed, rows that is passed to context, pascal event and addition event
    (
        context,
        shared_state_queue,
        lock_queue,
        rows,
        pascal_event,
        addition_event,
        pascal_iterator,
        context_event,
        shared_state_queue_created,
        final_shared_state_queue,
    ) = list(cloudpickle.loads(serialized_args))
    # start context with the pascal queue, lock queue, rows, and pascal event
    if not context_event.is_set():
        context_serialized_args = cloudpickle.dumps(
            (
                shared_state_queue,
                lock_queue,
                rows,
                pascal_event,
                shared_state_queue_created,
                final_shared_state_queue,
            )
        )
        start_context(context_serialized_args)
        # do not create any more context processes
        context_event.set()
    # keep manager process alive while pascal event is not set
    while not pascal_event.is_set():
        # check if the addition event is set where a new row was added
        try:
            lock = cloudpickle.loads(lock_queue.get(timeout=1))
            with lock:
                # client added a row to the pascal array
                if addition_event.is_set():
                    # grab value and array from pascal queue
                    value, array = None, None
                    try:
                        value, array = cloudpickle.loads(shared_state_queue.get(timeout=1))
                    except:
                        pass
                    # this comparison happens outside the try and except; check if the value needs to be incremented
                    if (
                        value is not None
                        and array is not None
                        and value.value < (2**rows)
                        and pascal_iterator.value < rows
                    ):
                        # find the sum of the Pascal triangle array
                        value.value = sum((array[:])) + 1
                        # clear addition event
                        addition_event.clear()
                        # add another row to the Pascal triangle
                        pascal_iterator.value += 1
                    # put the value and array in the pascal queue
                    shared_state_queue.put(cloudpickle.dumps((value, array)))
                # put the lock in the lock queue
                lock_queue.put(cloudpickle.dumps(lock))
                # manager finished summation of pascal array
                addition_event.clear()
        except:
            pass


def client(serialized_args: bytes) -> None:
    """
    Client adds array to mp.Array
    """
    (
        shared_state_queue,
        lock_queue,
        pascal_event,
        addition_event,
        rows,
        pascal_iterator,
        index_iterator,
    ) = list(cloudpickle.loads(serialized_args))
    # keep client process alive while pascal event is not set
    while not pascal_event.is_set():
        try:
            lock = cloudpickle.loads(lock_queue.get(timeout=1))
            with lock:
                # manager has completed addition event
                if not addition_event.is_set():
                    # grab value and array from pascal queue
                    value, array = None, None
                    try:
                        value, array = cloudpickle.loads(shared_state_queue.get(timeout=1))
                    except:
                        pass
                    # this comparison happens outside the try and except; if the Pascal value is less than what is expected from a Pascal triangle of that size and the index within the Pascal triangle array is less than the largest index of the array
                    if (
                        value is not None
                        and array is not None
                        and value.value < (2**rows)
                        and index_iterator.value <= (sum(range(rows)))
                    ):
                        # create the array for the index of interest
                        pascal_row = pascal_iterator.value
                        new_array = pascal(pascal_row + 1)
                        # add the element from the array generated to the Pascal triangle array
                        for element in new_array:
                            array[index_iterator.value] = element
                            index_iterator.value += 1
                # put the value and array in the pascal queue
                shared_state_queue.put(cloudpickle.dumps((value, array)))
            # put the lock in the lock queue
            lock_queue.put(cloudpickle.dumps(lock))
            # array has been added to pascal triangle array
            addition_event.set()
        except:
            pass


def main():
    # create parser that grabs the row of interest from the user
    parser = argparse.ArgumentParser(description="Pascal Triangle Test")
    # the default argument is 5
    parser.add_argument("--rows", type=int, default=5, help="number of rows in Pascal triangle")
    my_args = parser.parse_args()
    rows = my_args.rows
    # pascal queue is used for creating the pascal triangle array and value for pascal triangle, lock is passed between processes, and answer queue is used to pass the final pascal triangle between manager and context
    shared_state_queue, lock_queue, final_shared_state_queue = mp.Queue(), mp.Queue(), mp.Queue()
    # pascal event signals completion of event, addition process signals that the client process added another row of the Pascal triangle to the array, context event is used to signal context process is created, and shared_state_queue_created signals that the shared state is created
    pascal_event, addition_event, context_event, shared_state_queue_created = (
        mp.Event(),
        mp.Event(),
        mp.Event(),
        mp.Event(),
    )
    # pascal iterator provides row of the Pascal triangle and the index iterator provides the index in the Pascal triangle array
    pascal_iterator, index_iterator = mp.Value("i", 0), mp.Value("i", 0)
    # client adds the rows to the pascal triangle array until pascal event is triggered. Adds rows when addition event is set.
    client_serialized_args = cloudpickle.dumps(
        (
            shared_state_queue,
            lock_queue,
            pascal_event,
            addition_event,
            rows,
            pascal_iterator,
            index_iterator,
        )
    )
    # manager starts context. Adds to the triangle value and sets addition event. Waits on pascal event to be triggered.
    manager_serialized_args = cloudpickle.dumps(
        (
            context,
            shared_state_queue,
            lock_queue,
            rows,
            pascal_event,
            addition_event,
            pascal_iterator,
            context_event,
            shared_state_queue_created,
            final_shared_state_queue,
        )
    )
    manager_proc = mp.Process(target=manager, args=(manager_serialized_args,))
    client_proc = mp.Process(target=client, args=(client_serialized_args,))
    # start manager process
    manager_proc.start()
    # context created
    context_event.wait()
    # start client process once manager and context processes started
    client_proc.start()
    # pascal triangle array filled
    pascal_event.wait()
    value, array = cloudpickle.loads(final_shared_state_queue.get(timeout=1))
    # print the Pascal triangle statistics
    print(
        "Pascal Triangle Array Calculated for",
        rows,
        "rows from the Pascal row of 0 to the Pascal row of",
        rows,
        ", and the associated sum of the Pascal triangle array.",
        flush=True,
    )
    print("Pascal Triangle Array", array[:], flush=True)
    print("Pascal Triangle Sum:", value.value, flush=True)
    # join the manager and client processes
    manager_proc.join()
    client_proc.join()


if __name__ == "__main__":
    # set dragon start process
    mp.set_start_method("dragon")
    # start main process
    main()
