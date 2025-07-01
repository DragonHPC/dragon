import glob
import subprocess
import unittest
import os
import time
import re
import ast

DRAGON_HSTA_TEST_EXAMPLE = ["dragon", "../../examples/multiprocessing/p2p_lat.py", "--dragon"]
# DRAGON_HSTA_TEST_EXAMPLE = ["dragon", "-l", "DEBUG", "../../examples/multiprocessing/p2p_lat.py", "--dragon"]
DRAGON_HSTA_REBOOT_TEST_EXAMPLE = ["dragon", "--resilient", "--nodes=2", "p2p_lat_test.py"]

class BaseTestHSTA(unittest.TestCase):

    def abnormal_termination(self, test_mode, time_limit):

        os.environ["_DRAGON_HSTA_TESTING_MODE"] = str(test_mode)

        print(f'HSTA Test Mode: {os.environ.get("_DRAGON_HSTA_TESTING_MODE")}', flush=True)
        process = subprocess.Popen(
            DRAGON_HSTA_TEST_EXAMPLE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        start_time = time.time()
        try:
            stdout, stderr = process.communicate(timeout=time_limit)
        except subprocess.TimeoutExpired:
            process.kill()
            cleanup_process = subprocess.Popen(["dragon-cleanup"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            cleanup_process.wait()
            self.assertTrue(False, msg="Communication timed out. This likely means that it was hung.")
        diff_time = time.time() - start_time

        # Check the exit status
        return_code = process.returncode

        # Print output
        stdout_output = stdout.decode()
        stderr_output = stderr.decode()
        # This can come from a failure during start up or from the exceptionless abort in the backend
        print("stdout:", flush=True)
        print(stdout_output, flush=True)
        print("stderr:", flush=True)
        print(stderr_output, flush=True)
        self.assertTrue("Abnormal Exit detected" in stdout_output)
        self.assertTrue(return_code != 0)
        self.assertTrue(diff_time < time_limit)

    def tearDown(self):
        # cleanup_process = subprocess.Popen(['dragon-cleanup'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # cleanup_process.wait()
        time.sleep(1) 
        for f in glob.glob("core*"):
            try:
                print("Found core file, removing it", flush=True)
                os.remove(f)
            except FileNotFoundError:
                print(f"Error trying to remove generated core file. This may need to be removed manually.")


class TestHSTA(BaseTestHSTA):

    def test_0_normal_run(self):

        time_limit = 60
        os.environ.pop("_DRAGON_HSTA_TESTING_MODE", None)

        process = subprocess.Popen(
            DRAGON_HSTA_TEST_EXAMPLE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            start_time = time.time()
            stdout, stderr = process.communicate(timeout=time_limit)
        except subprocess.TimeoutExpired:
            process.kill()
            # I'm sure there is a better way to do this.
            # Just want automatic failure if dragon didn't exit on own
            cleanup_process = subprocess.Popen(["dragon-cleanup"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            cleanup_process.wait()
            self.assertTrue(False, msg="Communication timed out. This likely means that it was hung.")
        # both of these should be unnecessary
        process.wait()
        diff_time = time.time() - start_time

        # Check the exit status
        return_code = process.returncode

        # Print output
        stdout_output = stdout.decode()
        # This can come from a failure during start up or from the exceptionless abort in the backend
        self.assertTrue("using Dragon" in stdout_output)
        self.assertTrue(return_code == 0)
        self.assertTrue(diff_time < time_limit)

    # if debugging, timeout on test debugging should be changed
    def test_1_abnormal_termination(self):
        self.abnormal_termination(1, 60)

    def test_2_abnormal_termination(self):
        self.abnormal_termination(2, 60)

    def test_3_abnormal_termination(self):
        self.abnormal_termination(3, 60)

    def test_4_abnormal_termination(self):
        self.abnormal_termination(4, 60)

    def test_5_abnormal_termination(self):
        self.abnormal_termination(5, 60)

    def test_6_abnormal_termination(self):
        self.abnormal_termination(6, 60)

    def test_7_abnormal_termination(self):
        self.abnormal_termination(7, 60)

    def test_8_abnormal_termination(self):
        self.abnormal_termination(8, 60)

    def test_9_abnormal_termination(self):
        self.abnormal_termination(9, 60)

class TestHSTAReboot(unittest.TestCase):
    
    def test_hsta_runtime_reboot(self):

        os.environ["_DRAGON_HSTA_TESTING_MODE"] = str(9)
        time_limit = 60

        print(f'HSTA Test Mode: {os.environ.get("_DRAGON_HSTA_TESTING_MODE")}', flush=True)

        process = subprocess.Popen(
            DRAGON_HSTA_REBOOT_TEST_EXAMPLE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        
        try:
            start_time = time.time()
            stdout, stderr = process.communicate(timeout=time_limit)
            result = []
            
            # Result before restart and after restart is between delimiters in stdout
            result = re.findall(r"UNITTEST_DATA_START(.*?)UNITTEST_DATA_END", stdout.decode('utf-8'), re.DOTALL)
            result = [ast.literal_eval(i) for i in result]
            
        except subprocess.TimeoutExpired:
            self.assertTrue(False, msg="Communication timed out. This likely means that it was hung.")
        diff_time = time.time() - start_time
        returncode = process.returncode

        self.assertFalse(result[0]["is_restart"])
        self.assertTrue(result[1]["is_restart"])
        self.assertNotEqual(set(result[0]["node_hostnames"]), set(result[1]["node_hostnames"]))
        self.assertTrue(returncode == 0)
        self.assertTrue(diff_time < time_limit)


    def tearDown(self):
        time.sleep(1) 
        for f in glob.glob("core*"):
            try:
                print("Found core file, removing it", flush=True)
                os.remove(f)
            except FileNotFoundError:
                print(f"Error trying to remove generated core file. This may need to be removed manually.")

if __name__ == "__main__":
    unittest.main()
