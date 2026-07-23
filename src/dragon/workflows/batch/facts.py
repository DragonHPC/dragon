# TODO: these defaults needs to be reevaluated after gather performance
# data with a number of possible values

default_workers_per_manager = 128
# Number of results-DDict managers per worker pool.
default_results_ddict_managers_per_pool = 4
default_timeout = 1e9
default_progress_timeout = 10
default_block_size = 4096

client_task_batch_linger_sec = 0.005
client_task_batch_maxsize = 256
# Bound on the client-side completion queue drained by Batch.poll(). Completions
# beyond this cap are buffered locally so the Dragon return queue keeps draining.
client_completion_queue_maxsize = 512

manager_work_queue_maxsize = 256
manager_work_queue_max_batch_size = 256
return_queue_maxsize = 256
