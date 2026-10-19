import unittest
import subprocess
import dragon


class DragonVersionTester(unittest.TestCase):
    def test_version(self):

        # Make sure it's defined
        self.assertIsInstance(dragon.__version__, str)

        # Make sure it's not empty
        self.assertNotEqual(dragon.__version__, "")

        # Make sure it has three parts separated by dots
        parts = dragon.__version__.split(".")
        self.assertEqual(len(parts), 3)

        # Make sure the first 2 parts are digits (the patch part can be a string like "1.0.0rc1")
        for part in parts[:2]:
            self.assertTrue(part.isdigit())

        # Make sure the patch part is not empty
        self.assertNotEqual(parts[2], "")

        # Make sure it matches `dragon --version` output
        result = subprocess.run(["dragon", "--version"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        version_output = result.stdout.strip()
        self.assertEqual(version_output, f"Dragon Version {dragon.__version__}")
