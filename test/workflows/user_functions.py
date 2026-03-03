import os
import shutil
import sys

from dragon.data.ddict import DDict
from dragon.native.machine import System
from pathlib import Path
from typing import Optional


hi = "Hi-diddly-ho, neighborino!!!"
darn = "Son-of-a-diddly"
cider_vs_juice = "If it's clear and yella, you've got juice there fella! If its tangy and brown, you're in cider town!"


supersingular_primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 41, 47, 59, 71]


class FileUtils:
    def __init__(self, batch, use_ddict: bool = True) -> None:
        self.use_ddict = use_ddict
        self.num_tasks = 8

        dir_name = "batch_unittest_fib_seq"
        self.base_dir = Path(f"{os.getcwd()}/{dir_name}")

        if use_ddict:
            self.ddict = get_ddict(batch, self.num_tasks)
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
            del self.ddict
        else:
            shutil.rmtree(self.base_dir, ignore_errors=True)


def get_ddict(batch, num_tasks: int) -> DDict:
    # reserve 1MB per task
    bytes_per_task = 1 << 20
    num_bytes = int(num_tasks * bytes_per_task)
    # we want at least one node, and we'll try to limit the memory footprint per node
    # to be under 8GB (mostly arbitrary choice)
    eight_gb = 8 * (1 << 30)
    num_nodes = min(1 + num_bytes // eight_gb, System().nnodes)
    return batch.ddict(2, num_nodes, num_bytes, wait_for_keys=True, working_set_size=2)


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


def get_fib_sequence(batch, use_ddict) -> list:
    fs = FileUtils(batch, use_ddict)
    fs.write("key_0", "0")
    fs.write("key_1", "1")

    num_tasks = 8
    program = []

    for i in range(num_tasks):
        task_f = batch.function(fib, fs, i)
        task_f.read(fs.base_dir, f"key_{i}", f"key_{i + 1}")
        task_f.write(fs.base_dir, f"key_{i + 2}")
        program.append(task_f)

    batch.compile(program).run()

    fs.cleanup()

    fib_seq = []
    for task in program:
        fib_seq.append(task.result.get())

    return fib_seq


def foo_result(return_me) -> str:
    return return_me


def foo_stdout(print_me: str) -> None:
    print(print_me, flush=True)


def foo_stderr(print_me: str) -> None:
    print(print_me, file=sys.stderr, flush=True)


def print_it(junk: int, print_me: str, more_junk: int) -> None:
    print(print_me, flush=True)


def print_several(print_me: str, me_too: str, and_one_more: str) -> None:
    print(f"{print_me}, {me_too}, {and_one_more}", flush=True)


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
