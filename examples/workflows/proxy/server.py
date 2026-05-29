import os
import time
import dragon.infrastructure.parameters as dparm
import dragon.workflows.runtime as runtime
import sys
import argparse


def wait_for_exit(exit_file_path):
    while not os.path.exists(exit_file_path):
        time.sleep(1)
    time.sleep(1)
    if dparm.this_process.index == 0:
        os.remove(exit_file_path)


def parse_args():
    parser = argparse.ArgumentParser(description="Dragon Proxy Server")
    parser.add_argument(
        "--exit-path",
        type=str,
        default=os.path.join(os.getcwd(), "client_exit"),
        help="Path to the exit file to monitor",
    )
    parser.add_argument(
        "--publish-path",
        type=str,
        default=os.getcwd(),
        help="Path to publish the runtime serialized descriptor",
    )
    parser.add_argument(
        "--name",
        type=str,
        default="my-runtime",
        help="Name of the runtime to publish",
    )

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    sdesc = runtime.publish(args.name, args.publish_path)
    print(f"Runtime serialized descriptor: {sdesc}", flush=True)
    wait_for_exit(args.exit_path)
