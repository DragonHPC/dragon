import os
import sys
import cloudpickle
import signal
import time

import dragon

from dragon.globalservices import node
from dragon.globalservices import group
from dragon.globalservices import process
from dragon.globalservices import policy_eval
from dragon.infrastructure import process_desc
from dragon.utils import host_id


def hello(sleep_time):
    my_host_id = host_id()
    my_node = node.query(my_host_id)
    print(f'Hello from {my_node.name}', flush=True)
    print(f'Sleeping for {sleep_time} secs', flush=True)
    time.sleep(sleep_time)
    print(f'Goodbye from {my_node.name}', flush=True)


def get_python_process_parameters(target, args, kwargs) -> tuple:

    new_target = sys.executable
    new_args = [
        "-c",
        "from dragon.native.process import _dragon_native_python_process_main; _dragon_native_python_process_main()",
    ]
    argdata = cloudpickle.dumps((target, args or (), kwargs or {}))
    return new_target, new_args, argdata


def create_process_msg(wait_time):
    target, args, argdata = get_python_process_parameters(target=hello, args=(wait_time,), kwargs=None)

    # Pipe the stdout output from the head process to a Dragon connection
    process_create_msg = process.get_create_message_with_argdata(
        exe=target,
        run_dir=os.getcwd(),
        args=args,
        argdata=argdata,
        pmi_required=False,
        env=None,
    )

    return process_create_msg.serialize()

def main() -> None:
    num_processes = node.query_total_cpus() // 2
    print(f'Starting {num_processes} processes', flush=True)

    # Establish the list and number of process ranks that should be started
    items = [
        (num_processes // 2, create_process_msg(wait_time=10)),
        (num_processes // 2, create_process_msg(wait_time=60*5)),
    ]

    # Ask Dragon to create the process group
    grp = group.create(items=items, policy=policy_eval.Policy(), soft=False)

    print('Sleeping for 30 seconds before killing the group', flush=True)
    time.sleep(30)

    print('Killing group', flush=True)
    grp = group.kill(grp.g_uid, signal.SIGKILL)
    
    group_puids = []
    for resources in grp.sets:
        group_puids.extend(
            [
                resource.desc.p_uid
                for resource in resources
                if resource.desc.state == process_desc.ProcessDescriptor.State.ACTIVE
            ]
        )
    if len(group_puids) > 0:
        process.multi_join(group_puids, join_all=True)


if __name__ == "__main__":
    main()
