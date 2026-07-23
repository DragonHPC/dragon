import unittest


class AgentTestCase(unittest.IsolatedAsyncioTestCase):
    pass


# Add dummy tests to avert py3.12 raising no tests run error
class DummyTest(unittest.TestCase):
    def test_dummy(self):
        self.assertEqual(0, 0)


if __name__ == "__main__":
    unittest.main()
