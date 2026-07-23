import os
import unittest
import time
import pickle
import sys
import shutil
import tempfile

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

        self.assertIn("No such file or directory", result)
        proc.stderr.close()

    def test_native_process_stderr_to_devnull(self):
        """
        No output should appear on the terminal from running this test.
        """
        exe = shutil.which("cat")
        proc = Popen(executable=exe, args=["notafile"], stderr=Popen.DEVNULL)
        proc.wait()

        self.assertEqual(proc.stderr, None)

    @unittest.skip("Needs work - hangs - AICI-1999")
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

        self.assertIn("No such file or directory", result)
        proc.stdout.close()


class TestFileRedirection(unittest.TestCase):
    """Tests for file-path based stdout/stderr redirection."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp(prefix="dragon_test_redirect_")

    def tearDown(self):
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _tmpfile(self, name):
        return os.path.join(self._tmpdir, name)

    def test_stdout_to_file(self):
        """Process stdout is written to the specified file."""
        outfile = self._tmpfile("stdout.log")
        exe = sys.executable
        proc = Popen(executable=exe, args=["-c", 'print("Hello File")'], stdout=outfile)
        proc.wait()

        with open(outfile, "rb") as f:
            content = f.read()
        self.assertIn(b"Hello File", content)

    def test_stderr_to_file(self):
        """Process stderr is written to the specified file."""
        errfile = self._tmpfile("stderr.log")
        exe = sys.executable
        proc = Popen(
            executable=exe,
            args=["-c", 'import sys; sys.stderr.write("Error output\\n")'],
            stderr=errfile,
        )
        proc.wait()

        with open(errfile, "rb") as f:
            content = f.read()
        self.assertIn(b"Error output", content)

    def test_stderr_to_stdout_file(self):
        """When stdout is a file and stderr=STDOUT, both streams go to the file."""
        outfile = self._tmpfile("combined.log")
        exe = sys.executable
        proc = Popen(
            executable=exe,
            args=["-c", 'import sys; print("out"); sys.stderr.write("err\\n")'],
            stdout=outfile,
            stderr=Popen.STDOUT,
        )
        proc.wait()

        with open(outfile, "rb") as f:
            content = f.read()
        self.assertIn(b"out", content)
        self.assertIn(b"err", content)

    def test_stdout_to_file_stderr_to_pipe(self):
        """Mixed mode: stdout goes to file, stderr goes to PIPE."""
        outfile = self._tmpfile("stdout_only.log")
        exe = sys.executable
        proc = Popen(
            executable=exe,
            args=["-c", 'import sys; print("file out"); sys.stderr.write("pipe err\\n")'],
            stdout=outfile,
            stderr=Popen.PIPE,
        )

        err_result = ""
        try:
            while True:
                data = proc.stderr.recv()
                err_result += data
        except EOFError:
            pass
        proc.wait()

        # stdout went to file
        with open(outfile, "rb") as f:
            content = f.read()
        self.assertIn(b"file out", content)

        # stderr came via PIPE
        self.assertIn("pipe err", err_result)
        proc.stderr.close()

    def test_invalid_file_path_fails(self):
        """An invalid file path causes process creation to fail, not crash."""
        exe = sys.executable
        with self.assertRaises(Exception):
            proc = Popen(
                executable=exe,
                args=["-c", 'print("should not run")'],
                stdout="/nonexistent_dir/nonexistent_subdir/output.log",
            )

    def test_unwritable_file_path_fails(self):
        """A file path with no write permission causes process creation to fail."""
        readonly_dir = self._tmpfile("readonly_dir")
        os.makedirs(readonly_dir, mode=0o555)
        unwritable_path = os.path.join(readonly_dir, "output.log")

        exe = sys.executable
        try:
            # Some CI environments run with privileges that bypass mode-bit checks.
            # If the path is actually writable, this test cannot validate the failure case.
            try:
                with open(unwritable_path, "a"):
                    pass
                os.remove(unwritable_path)
                print("Unwritable path is actually writable, skipping test.", flush=True)
                return
            except OSError:
                pass

            with self.assertRaises(Exception):
                proc = Popen(
                    executable=exe,
                    args=["-c", 'print("should not run")'],
                    stdout=unwritable_path,
                )
        finally:
            # Restore permissions so tearDown can clean up
            os.chmod(readonly_dir, 0o755)

    def test_append_mode(self):
        """Multiple processes writing to the same file append rather than overwrite."""
        outfile = self._tmpfile("append.log")
        exe = sys.executable

        proc1 = Popen(executable=exe, args=["-c", 'print("first")'], stdout=outfile)
        proc1.wait()

        proc2 = Popen(executable=exe, args=["-c", 'print("second")'], stdout=outfile)
        proc2.wait()

        with open(outfile, "rb") as f:
            content = f.read()
        self.assertIn(b"first", content)
        self.assertIn(b"second", content)

    @unittest.skip("Currently only support string output")
    def test_binary_output_to_file(self):
        """Non-UTF-8 binary output is written to the file without corruption."""
        outfile = self._tmpfile("binary.log")
        exe = sys.executable
        # Write raw bytes that are not valid UTF-8
        proc = Popen(
            executable=exe,
            args=["-c", "import sys, os; os.write(sys.stdout.fileno(), b'\\x80\\x81\\x82\\xff')"],
            stdout=outfile,
        )
        proc.wait()

        with open(outfile, "rb") as f:
            content = f.read()
        self.assertIn(b"\x80\x81\x82\xff", content)


if __name__ == "__main__":
    unittest.main()
