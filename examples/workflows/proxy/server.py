import os
import time
import dragon.infrastructure.parameters as dparm
import dragon.workflows.runtime as runtime


def wait_for_exit():
    path = '/home/users/nradclif/hpc-pe-dragon-dragon/examples/dragon_workflows/client_exit'
    while not os.path.exists(path):
        time.sleep(1)
    time.sleep(1)
    if dparm.this_process.index == 0:
        os.remove(path)


sdesc = runtime.publish('my-runtime')
print(f'Runtime serialized descriptor: {sdesc}', flush=True)
wait_for_exit()
