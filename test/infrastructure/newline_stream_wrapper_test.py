from contextlib import closing
import io
import os
import sys
import unittest

from dragon.infrastructure.util import NewlineStreamWrapper, _get_access_modes


class NewlineStreamWrapperTest(unittest.TestCase):

    def _make_pipe(self):
        r, w = os.pipe()
        return os.fdopen(r), os.fdopen(w, 'w')

    def test_auto_intents(self):
        reader, writer = map(NewlineStreamWrapper, self._make_pipe())
        with closing(reader):
            self.assertTrue(reader.read_intent)
            self.assertFalse(reader.write_intent)
        with closing(writer):
            self.assertFalse(writer.read_intent)
            self.assertTrue(writer.write_intent)

    def test_inverse_intents(self):
        r, w = self._make_pipe()
        with self.assertWarns(UserWarning):
            reader = NewlineStreamWrapper(r, read_intent=False, write_intent=True)
        with closing(reader):
            self.assertFalse(reader.read_intent)
            self.assertTrue(reader.write_intent)
        with self.assertWarns(UserWarning):
            writer = NewlineStreamWrapper(w, read_intent=True, write_intent=False)
        with closing(writer):
            self.assertTrue(writer.read_intent)
            self.assertFalse(writer.write_intent)

    def test_stdin(self):
        readable, writeable = _get_access_modes(sys.stdin)
        self.assertTrue(readable)
        # XXX Unless redirected from a file, stdin actually has access
        # XXX mode O_RDWR even though sys.stdin.mode is 'r'.
        if sys.stdin.isatty():
            self.assertTrue(writeable)
        else:
            self.assertFalse(writeable)

    def test_stdin_wrapper(self):
        stream = NewlineStreamWrapper(sys.stdin)
        # Do not call stream.close() otherwise sys.stdin will be closed
        self.assertTrue(stream.read_intent)
        # XXX Unless redirected from a file, stdin actually has access
        # XXX mode O_RDWR even though sys.stdin.mode is 'r'.
        if sys.stdin.isatty():
            self.assertTrue(stream.write_intent)
        else:
            self.assertFalse(stream.write_intent)
        
    def test_stdout(self):
        readable, writeable = _get_access_modes(sys.stdout)
        # XXX Unless redirected from a file, stdout actually has access
        # XXX mode O_RDWR even though sys.stdout.mode is 'w'.
        if sys.stdout.isatty():
            self.assertTrue(readable)
        else:
            self.assertFalse(readable)
        self.assertTrue(writeable)

    def test_stdout_wrapper(self):
        stream = NewlineStreamWrapper(sys.stdout)
        # Do not call stream.close() otherwise sys.stdout will be closed
        # XXX Unless redirected from a file, stdout actually has access
        # XXX mode O_RDWR even though sys.stdout.mode is 'w'.
        if sys.stdout.isatty():
            self.assertTrue(stream.read_intent)
        else:
            self.assertFalse(stream.read_intent)
        self.assertTrue(stream.write_intent)

    def test_poll(self):
        r, w = self._make_pipe()
        reader = NewlineStreamWrapper(r)
        with closing(w):
            w.write("line 1\nline 2\n")
        with closing(reader):
            self.assertTrue(reader.poll())
        with self.assertRaises(ValueError):
            reader.poll()

    def test_poll_stringio(self):
        # Although one might think io.StringIO() is a valid file-like object
        # that can be polled, their fileno() method raises
        # io.UnsupportedOperation, e.g.:
        #
        #   >>> import io
        #   >>> f = io.StringIO()
        #   >>> hasattr(f, 'fileno')
        #   True
        #   >>> f.fileno
        #   <built-in method fileno of _io.StringIO object at 0x7f399d5363a0>
        #   >>> f.fileno()
        #   Traceback (most recent call last):
        #     File "<stdin>", line 1, in <module>
        #   io.UnsupportedOperation: fileno
        #
        stream = NewlineStreamWrapper(io.StringIO())
        with closing(stream):
            with self.assertRaises(ValueError):
                stream.poll()

    def test_poll_bytesio(self):
        # Although one might think io.BytesIO() is a valid file-like object
        # that can be polled, their fileno() method raises
        # io.UnsupportedOperation, e.g.:
        #
        #   >>> import io
        #   >>> f = io.BytesIO()
        #   >>> hasattr(f, 'fileno')
        #   True
        #   >>> f.fileno
        #   <built-in method fileno of _io.BytesIO object at 0x7efef9feac70>
        #   >>> f.fileno()
        #   Traceback (most recent call last):
        #     File "<stdin>", line 1, in <module>
        #   io.UnsupportedOperation: fileno
        #
        stream = NewlineStreamWrapper(io.BytesIO())
        with closing(stream):
            with self.assertRaises(ValueError):
                stream.poll()

    def test_recv(self):
        r, w = self._make_pipe()
        reader = NewlineStreamWrapper(r)
        with closing(w):
            w.write("line 1\nline 2\n")
        with closing(reader):
            self.assertTrue(reader.poll())
            self.assertEqual(reader.recv(), "line 1\n")
            self.assertTrue(reader.poll())
            self.assertEqual(reader.recv(), "line 2\n")
            # Once all data has been read, poll() will return True and recv()
            # will return an empty string.
            self.assertTrue(reader.poll())
            self.assertEqual(reader.recv(), '')

    def test_send(self):
        r, w = self._make_pipe()
        writer = NewlineStreamWrapper(w)
        with closing(writer):
            writer.send("line 1")
            writer.send("line 2")
        with closing(r):
            self.assertEqual(r.read(), "line 1\nline 2\n")


if __name__ == "__main__":
    unittest.main()
