import os
import sys
import argparse
import subprocess
from tempfile import NamedTemporaryFile

from .src import get_host_list
from .src.facts import ENV_DRAGON_RUN_NODE_FILE
from .src.common_args import add_common_args


LIST_HELP="List known hosts in the current dragon run environment"

def get_parser():
    parser = argparse.ArgumentParser(prog="dhosts", description="Dragon Run Hosts tool")
    add_common_args(parser)

    parser.add_argument('--list', action="store_true", default=False, help=LIST_HELP)
    return parser


def get_args(args_input=None):
    parser = get_parser()
    args = parser.parse_args(args_input)
    return {key: value for key, value in vars(args).items() if value is not None}


def main():

    # Parse our arguments.
    args = get_args(sys.argv[1:])

    # Determine hostlist to use.
    host_list = get_host_list(**args)
    if not host_list:
        print(
            "Could not determine host(s) to use. Please specify --hostlist, --hostfile or ensure that you have an active slurm allocation.",
            file=sys.stderr,
            flush=True,
        )
        return 1

    if args["list"]:
        print("\n".join(host_list))
        return 0

    # Determine the user's default shell
    user_shell = os.environ.get("SHELL", "")
    if not user_shell:
        print(
            "Could not determine user shell. Please ensure that the SHELL environment variable is set",
            file=sys.stderr,
            flush=True,
        )
        return 1

    # Create temporary file. This will automatically be deleted.
    with NamedTemporaryFile(mode="w+t", suffix=".txt") as tmpfile:

        # Add environment variable pointing to the temporary file created above.
        env = os.environ.copy()
        env[ENV_DRAGON_RUN_NODE_FILE] = tmpfile.name
        tmpfile.write("\n".join(host_list))
        tmpfile.flush()

        try:
            return subprocess.run(
                [user_shell],
                shell=False,
                env=env,
                stdin=sys.stdin,
                stdout=sys.stdout,
                stderr=sys.stderr,
            ).returncode
        except Exception as exc:
            print("Error running subprocess: %s", exc, file=sys.stderr, flush=True)
            return 1


if __name__ == "__main__":
    sys.exit(main())
