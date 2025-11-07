import os
import unittest
import time
import pickle
import sys
import shutil

import dragon

# from dragon.native.process import current
# from dragon.native.queue import Queue
# from dragon.native.barrier import Barrier, BrokenBarrierError
from dragon.native.process import Popen
from dragon.utils import B64


class TestIORedirection(unittest.TestCase):

    def test_process_stdout(self):

        exe = sys.executable
        proc = Popen(executable=exe, args=["-c", 'print("Hello World")'], stdout=Popen.PIPE)

        result = ""
        try:
            while True:
                data = proc.stdout.recv()
                result += data
        except EOFError:
            pass

        self.assertEqual("Hello World\n", result)
        proc.stdout.close()

    @unittest.skip("Needs work - hangs - AICI-1999")
    def test_native_process_stdout_to_devnull(self):
        """
        No output should appear on the terminal from running this test.
        """
        exe = shutil.which("echo")
        proc = Popen(executable=exe, args=["Hello World"], stdout=Popen.DEVNULL)
        proc.wait()

        self.assertEqual(proc.stdout, None)

    def test_native_process_stderr(self):
        exe = shutil.which("cat")
        proc = Popen(executable=exe, args=["notafile"], stderr=Popen.PIPE)

        result = ""
        try:
            while True:
                data = proc.stderr.recv()
                result += data
        except EOFError:
            pass

        self.assertEqual("/usr/bin/cat: notafile: No such file or directory\n", result)
        proc.stderr.close()

    def test_native_process_stderr_to_devnull(self):
        """
        No output should appear on the terminal from running this test.
        """
        exe = shutil.which("cat")
        proc = Popen(executable=exe, args=["notafile"], stderr=Popen.DEVNULL)
        proc.wait()

        self.assertEqual(proc.stderr, None)

    def test_native_process_stdin(self):
        exe = shutil.which("cat")
        proc = Popen(executable=exe, args=["-"], stdin=Popen.PIPE, stdout=Popen.PIPE)

        x = "Hello, How Are You?"
        proc.stdin.send(x)
        proc.stdin.close()

        result = ""
        try:
            while True:
                data = proc.stdout.recv()
                result += data
        except EOFError:
            pass

        self.assertEqual(x, result)
        proc.stdout.close()

    def test_native_process_stderr_to_stdout(self):
        exe = shutil.which("cat")
        proc = Popen(executable=exe, args=["notafile"], stderr=Popen.STDOUT, stdout=Popen.PIPE)

        result = ""
        try:
            while True:
                data = proc.stdout.recv()
                result += data
        except EOFError:
            pass

        self.assertEqual("/usr/bin/cat: notafile: No such file or directory\n", result)
        proc.stdout.close()


if __name__ == "__main__":
    unittest.main()
