# Batch Benchmarks

This directory contains benchmarks for the `Batch` workflow service.

## Task Throughput (`perf/task_throughput.py`)

Measures the end-to-end task throughput of `Batch` by submitting batches of
no-op functions and timing how long it takes for all of them to complete.

### Usage

```bash
dragon perf/task_throughput.py [--min_tasks MIN] [--max_tasks MAX]
```

| Argument | Default | Description |
|---|---|---|
| `--min_tasks` | 4 | Smallest batch size to benchmark |
| `--max_tasks` | 128 | Largest batch size to benchmark |

The benchmark doubles the number of tasks from `min_tasks` up to `max_tasks`,
printing a throughput table (tasks/second) for each size.

## PTD Template (`ptd/ptd_template.yml`)

A heavily-commented Parameterized Task Descriptor (PTD) YAML template showing
all available fields. Pass a PTD file to `Batch.import_func()` to get a
callable that submits typed tasks with pre-declared data dependencies:

```python
from dragon.workflows.batch import Batch

# Increase or decrease the internal results DDict size in bytes as needed.
batch = Batch(results_ddict_mem=2 * 1024**3)
run_task = batch.import_func("ptd/my_task.yml", my_func, base_dir, get_file)

for i in range(1000):
    run_task(arg1, arg2)  # enqueues a task; dispatched automatically in the background

batch.fence()  # wait for all submitted tasks to complete
batch.join()
```
