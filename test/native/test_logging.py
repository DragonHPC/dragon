import unittest
from dragon.native.logging import log_to_dragon, log_to_stderr


@unittest.skip(f"Dragon native logging interface not implemented yet (PE-41692)")
class TestDragonNativeMisc(unittest.TestCase):
    def test_get_logger():
        pass

    def test_log_to_stderr():
        pass


if __name__ == "__main__":
    unittest.main()
