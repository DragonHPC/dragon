import os
import shutil
import sys
import time
import cloudpickle
from uuid import uuid4

from dragon.data.ddict import DDict
from dragon.workflows.batch import Batch
from pathlib import Path
from typing import Optional

hi = "Hi-diddly-ho, neighborino!!!"
supersingular_primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 41, 47, 59, 71]
ITERATED_DEP_MODULUS = 97


class FileUtils:
    def __init__(self, batch, use_ddict: bool = True) -> None:
        self.use_ddict = use_ddict
        self.num_tasks = 8

        dir_name = "batch_unittest_fib_seq"
        self.base_dir = Path(f"{os.getcwd()}/{dir_name}")

        if use_ddict:
            self.ddict = get_ddict(batch)
        else:
            os.makedirs(dir_name, exist_ok=True)

    def read(self, file_name: str) -> int:
        if self.use_ddict:
            val_str = self.ddict[file_name]
        else:
            file_path = f"{self.base_dir}/{file_name}"
            with open(file_path, "r") as file:
                val_str = file.read()
        return int(val_str)

    def write(self, file_name: str, val: int) -> None:
        if self.use_ddict:
            self.ddict[file_name] = str(val)
        else:
            file_path = f"{self.base_dir}/{file_name}"
            with open(file_path, "w") as file:
                file.write(str(val))

    def cleanup(self) -> None:
        if self.use_ddict:
            self.ddict.destroy()
        else:
            shutil.rmtree(self.base_dir, ignore_errors=True)


def get_ddict(batch) -> DDict:
    batch_nodes = batch.topology().total_nodes
    bytes_per_node = 1 << 25
    return DDict(
        1,
        batch_nodes,
        batch_nodes * bytes_per_node,
        wait_for_keys=True,
        working_set_size=2,
        name=str(uuid4()),
    )


def fib(fs: FileUtils, i: int) -> int:
    try:
        a = fs.read(f"key_{i}")
        b = fs.read(f"key_{i + 1}")
    except Exception as e:
        print(f"caught exception when reading in data: {e}", flush=True)

    try:
        fs.write(f"key_{i + 2}", f"{a + b}")
    except Exception as e:
        print(f"caught exception when writing to file: {e}", flush=True)

    return a + b


def div_by_zero(x: int) -> int:
    return x / 0


def sleep_and_return(value, delay: float):
    time.sleep(delay)
    return value


def signal_start_and_sleep(marker_path: str, value, delay: float):
    with open(marker_path, "w", encoding="utf-8") as marker_file:
        marker_file.write("started\n")
    time.sleep(delay)
    return value


def return_constant(value):
    return value


def add_values(a, b):
    return a + b


def submit_batch_value_from_client(batch, output_queue, value):
    try:
        task = batch.function(return_constant, value, name=f"remote-client-{value}")
        output_queue.put({"client_id": batch.client_id, "result": task.get(), "value": value})
    finally:
        batch.join()


def submit_batch_value_from_serialized_client(serialized_batch, output_queue, value):
    batch = cloudpickle.loads(serialized_batch)
    submit_batch_value_from_client(batch, output_queue, value)


def submit_batch_value_from_batch_worker(serialized_batch, output_queue, value):
    from dragon.native.process import Process

    proc = Process(target=submit_batch_value_from_serialized_client, args=(serialized_batch, output_queue, value))
    proc.start()
    return proc.puid


def create_joined_batch_and_send(batch_queue, output_queue, values):
    batch = Batch(num_nodes=1, scheduler_workers=1)
    creator_client_id = batch.client_id
    output_queue.put({"role": "creator", "stage": "created", "client_id": creator_client_id})

    try:
        # The creating client detaches before handing the handle to another
        # process so the consumer proves it is not relying on creator ownership.
        batch.join()
        output_queue.put({"role": "creator", "stage": "joined", "client_id": creator_client_id})
        batch_queue.put(batch)
        output_queue.put(
            {
                "role": "creator",
                "stage": "handed_off",
                "client_id": creator_client_id,
                "values": tuple(values),
            }
        )
    except Exception as exc:
        output_queue.put({"role": "creator", "stage": "error", "client_id": creator_client_id, "error": repr(exc)})
        raise
    finally:
        batch_queue.close()
        output_queue.close()


def consume_batch_from_queue_and_destroy(batch_queue, output_queue, values):
    batch = batch_queue.get(timeout=20)
    consumer_client_id = batch.client_id
    output_queue.put({"role": "consumer", "stage": "acquired", "client_id": consumer_client_id})

    try:
        values = tuple(values)
        tasks = [batch.function(return_constant, value, name=f"handoff-client-{value}") for value in values]
        results = [task.get() for task in tasks]
        batch.join()
        batch.destroy()
        output_queue.put(
            {
                "role": "consumer",
                "stage": "destroyed",
                "consumer_client_id": consumer_client_id,
                "results": results,
                "values": values,
            }
        )
    except Exception as exc:
        output_queue.put({"role": "consumer", "stage": "error", "client_id": consumer_client_id, "error": repr(exc)})
        raise
    finally:
        batch_queue.close()
        output_queue.close()


def raise_runtime_error(message: str):
    raise RuntimeError(message)


def get_fib_sequence(batch, use_ddict) -> list:
    fs = FileUtils(batch, use_ddict)
    fs.write("key_0", "0")
    fs.write("key_1", "1")

    num_tasks = 8
    program = []

    for i in range(num_tasks):
        read = batch.read(fs.base_dir, f"key_{i}", f"key_{i + 1}")
        write = batch.write(fs.base_dir, f"key_{i + 2}")
        task = batch.function(fib, fs, i, reads=[read], writes=[write])
        program.append(task)

    fib_seq = []
    for task in program:
        fib_seq.append(task.get())

    fs.cleanup()

    return fib_seq


def foo_3_1(return_me, b, c) -> str:
    return return_me


def foo_3_2(a, return_me, c) -> str:
    return return_me


def foo_3_3(a, b, return_me) -> str:
    return return_me


def foo_5_1(return_me, b, c, d, e) -> str:
    return return_me


def foo_5_3(a, b, return_me, d, e) -> str:
    return return_me


def foo_5_5(a, b, c, d, return_me) -> str:
    return return_me


def check_exit_code(junk: int, more_junk: int, exit_code: int) -> bool:
    if exit_code == 0:
        return True
    else:
        return False


def check_gpu_affinity() -> bool:
    cuda_set = os.getenv("CUDA_VISIBLE_DEVICES") is not None
    rocr_set = os.getenv("ROCR_VISIBLE_DEVICES") is not None
    hip_set = os.getenv("HIP_VISIBLE_DEVICES") is not None
    intel_set = os.getenv("ZE_AFFINITY_MASK") is not None

    if cuda_set or rocr_set or hip_set or intel_set:
        return True
    else:
        return False


def record_process_placement(output_queue) -> None:
    import socket

    gpu_env_name = None
    gpu_env_value = None

    for env_name in ("CUDA_VISIBLE_DEVICES", "ROCR_VISIBLE_DEVICES", "HIP_VISIBLE_DEVICES", "ZE_AFFINITY_MASK"):
        env_value = os.getenv(env_name)
        if env_value is not None:
            gpu_env_name = env_name
            gpu_env_value = env_value
            break

    payload = {
        "hostname": socket.gethostname(),
        "gpu_env_name": gpu_env_name,
        "gpu_env_value": gpu_env_value,
    }

    output_queue.put(payload)


def get_prime(i: int) -> Optional[int]:
    if i < len(supersingular_primes):
        return supersingular_primes[i]
    else:
        return None


def next_idx(i: int, num_items: int) -> int:
    if i + 1 >= num_items:
        return i
    else:
        return i + 1


def update_dict(the_dict, i: int, num_items: int) -> None:
    x = the_dict[i]
    j = next_idx(i, num_items)
    y = x * (x + 1)
    max_val = 1024 * 1024
    if y > max_val:
        y = y / (2 * the_dict[i])
    the_dict[j] = y


def iterated_return_value(previous_sum: int, offset: int) -> int:
    return (previous_sum + offset + 1) % ITERATED_DEP_MODULUS


def iterated_write_value(shared_ddict: DDict, key: str, previous_sum: int, offset: int) -> int:
    value = (previous_sum + offset + 11) % ITERATED_DEP_MODULUS
    shared_ddict[key] = value
    return value


def iterated_sum_values(shared_ddict: DDict, keys: tuple[str, ...], *values: int) -> int:
    total = sum(values)

    for key in keys:
        total += shared_ddict[key]

    return total


def mpi_job_func() -> bool:
    """Trivial worker function used by TestRapidMPIJobSubmission to exercise
    job_done_q recycling without requiring an actual MPI environment."""
    return True


def mpi_f(i, secs):
    import mpi4py
    import time

    mpi4py.rc.initialize = False

    from mpi4py import MPI

    MPI.Init()
    time.sleep(secs)
    MPI.Finalize()

    return True


def producer_value(idx: int, prev_first_codes: Optional[list] = None):
    """Producer function that returns a tuple (idx, value).

    The value is derived from the index and the sum of previous first
    job exit codes when provided. This allows jobs to validate that the
    values passed from producers are correct across iterations.
    """
    sum_prev = sum(prev_first_codes) if prev_first_codes else 0
    return (idx, idx + sum_prev)


def mpi_job_arg_checker(*args):
    """Job function that accepts an arbitrary number of args.

    The last argument is expected to be the list of previous first-job
    exit codes (or None). The preceding arguments are expected to be
    tuples of the form (idx, value) produced by `producer_value`.

    Raises an Exception if any value does not match the expected value.
    Returns True on success.
    """
    if len(args) == 0:
        return True

    # last arg is prev codes (may be None)
    prev_codes = args[-1]
    data_args = args[:-1]

    sum_prev = sum(prev_codes) if prev_codes else 0

    for item in data_args:
        try:
            idx, val = item
        except Exception:
            raise Exception(f"mpi_job_arg_checker expected (idx, val) tuple, got: {item}")

        expected = idx + sum_prev
        if val != expected:
            raise Exception(f"Value mismatch for idx {idx}: got {val}, expected {expected}")

    return True
