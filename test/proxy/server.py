import argparse
import os
import time
import dragon.infrastructure.parameters as dparm
import dragon.workflows.runtime as runtime


def wait_for_exit(path):
    while not os.path.exists(path):
        time.sleep(1)
    time.sleep(1)
    if dparm.this_process.index == 0:
        os.remove(path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime-path', type=str, default=None, help='Path to the runtime file')
    parser.add_argument('--exit-path', type=str, required=True, help='Path to the exit file')
    args = parser.parse_args()
    sdesc = runtime.publish('my-runtime', publish_dir=args.runtime_path)
    print(f'Runtime serialized descriptor: {sdesc}', flush=True)
    wait_for_exit(args.exit_path)

