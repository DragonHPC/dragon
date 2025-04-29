#!/usr/bin/env python3

import shim_dragon_paths
import subprocess
import os
from glob import glob

from dragon.infrastructure.facts import DRAGON_LIB_DIR

base_dir = os.getcwd()

# Directories to scan for tests in
test_dirs = ["broadcast", "channels_subtests", "hashtable", "pmod", "utils"]

if __name__ == "__main__":
    print()

    tests_run = 0
    tests_failed = []

    the_env = dict(os.environ)
    the_env["LD_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + str(the_env.get("LD_LIBRARY_PATH", ""))

    print()
    print("######## RUNNING C TESTS ########")
    for d in test_dirs:
        print()
        print(f"++++++++ {str(d).upper()} ++++++++")
        os.chdir(d)  # Enter test dir
        subprocess.run(["make", "clean"], capture_output=True, env=the_env)
        tests = glob("test_*.c")  # Grab list of test files
        for t in tests:
            fname = os.path.splitext(os.path.basename(t))[0]
            print(f"Testing {fname} ...... ", end="", flush=True)

            tests_run += 1
            # If any of these fail do we want to skip to the next test or halt testing entirely?
            make_res = subprocess.run(["make", fname], capture_output=True, env=the_env)  # Build test
            if make_res.returncode != 0:
                print("FAIL")
                print(make_res.stdout.decode("utf-8"))
                print(make_res.stderr.decode("utf-8"))
                tests_failed.append(d + " " + fname)
                continue
            t_out = subprocess.run(["./" + fname], capture_output=True, env=the_env)  # Run test
            if t_out.returncode != 0:
                print("FAIL")
                print(t_out.stdout.decode("utf-8"))
                tests_failed.append(d + " --  " + fname)
            else:
                print("OK")

        os.chdir(base_dir)  # Reset to base test dir

    total_passed = tests_run - len(tests_failed)
    print()
    print("######## C TESTS RESULTS ########")
    print(f"{total_passed} / {tests_run} PASSED")
    if len(tests_failed) != 0:
        print()
        print("!!!!!!!! Tests failed:")
        print()
        for t in tests_failed:
            print(t)
