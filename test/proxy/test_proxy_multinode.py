import dragon
import multiprocessing as mp
import os
import socket
import sys
import time
import unittest
import subprocess
import importlib
import argparse

from dragon.native.process import ProcessTemplate, Popen, Process
import dragon.workflows.runtime as runtime

def signal_exit(path):
    file = open(path, "w")

def shutdown(path):
    exit_proc = mp.Process(target=signal_exit, args=(path,))
    exit_proc.start()
    exit_proc.join()

def import_from_path(module_name, file_path):
    """
    Imports a module dynamically given its name and file path.
    """
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None:
        raise ImportError(f"Could not find module spec for {module_name} at {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module  # Add the module to sys.modules
    spec.loader.exec_module(module)
    return module

def generate_test_list(args):
    # set up path to tests and add to pythonpath so that imports work correctly
    multinode_path = os.path.abspath(os.path.join(os.environ["DRAGON_BASE_DIR"], "../../test/multi-node/"))
    os.chdir(multinode_path)
    if multinode_path not in sys.path:
        sys.path.insert(0, multinode_path)
        os.environ['PYTHONPATH'] = multinode_path + os.pathsep + os.environ.get('PYTHONPATH', '')

    # get list of tests to run from makefile
    if args.test is not None:
        test_list = [args.test]
    else:
        make_target = f"make print_test_list"
        result = subprocess.run(make_target, shell=True, check=True, stdout=subprocess.PIPE)
        test_list = result.stdout.decode('utf-8').strip().split('\n')

    return multinode_path, test_list

def run_test(test, multinode_path):
    module_file = test.strip()
    module_name = os.path.splitext(module_file)[0]
    print(f"Running {module_name} from file {module_file} from {socket.gethostname()} with code executing remotely using proxy", flush=True)
    dynamic_module = import_from_path(module_name, os.path.join(multinode_path, module_file))
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(dynamic_module)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

def main_single_proxy_env(args):
    username = os.environ["USER"]
    system = "pippin" # full path for system. need passwordless ssh set up for this to work
    runtime_name = "my-runtime"
    timeout = 2
    # needs to be set again in proxy context for some tests to work
    mp.set_start_method("dragon")

    print(f"Connecting to remote runtime on system: {system}", flush=True)
    runtime_sdesc = runtime.lookup(system, runtime_name, timeout, publish_dir=args.runtime_path)
    proxy = runtime.attach(runtime_sdesc)

    print("\n")


    proxy.enable()
    os.environ["DRAGON_PROXY_ENABLED"] = "True"
    # needs to be set again in proxy context for some tests to work
    # mp.set_start_method("dragon")

    multinode_path, test_list = generate_test_list(args)

    print(f"Current working directory: {os.getcwd()}", flush=True)
    for test in test_list:
        if test in ["test_fli.py", "test_zarr_store.py"]:
            print(f"\nSkipping {test} as it is not compatible with proxy environment.", flush=True)
            continue
        run_test(test, multinode_path)

    print("\nAll tests complete. Shutting down.", flush=True)
    shutdown(args.exit_path)

    # do I need to be more explicit about cleanup here?
    print("\nDisabling proxy...", flush=True)
    proxy.disable()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A simple script demonstrating argparse.')
    parser.add_argument('--test', type=str, help='Name of the file you want to run test from', default=None)
    parser.add_argument('--runtime-path', type=str, default=None, help='Path to the runtime file')
    parser.add_argument('--exit-path', type=str, required=True, help='Path to the exit file')
    args = parser.parse_args()

    main_single_proxy_env(args)
