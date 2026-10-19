import unittest

from dragon.transport.tcp import errno


class ErrnoTestCase(unittest.TestCase):

    def test_get_errno(self):
        # TODO Add tests for Dragon exceptions that have the `lib_err` attribute
        self.assertEqual(errno.get_errno(Exception()), errno.DRAGON_FAILURE)
        self.assertEqual(errno.get_errno(ValueError()), errno.DRAGON_INVALID_ARGUMENT)
        self.assertEqual(errno.get_errno(NotImplementedError()), errno.DRAGON_NOT_IMPLEMENTED)
        self.assertEqual(errno.get_errno(TimeoutError()), errno.DRAGON_TIMEOUT)


if __name__ == "__main__":
    unittest.main()
