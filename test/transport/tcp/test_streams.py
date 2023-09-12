import unittest


class StreamsTestCase(unittest.IsolatedAsyncioTestCase):

    @unittest.skip
    async def test_close_writer(self):
        raise NotImplementedError

    @unittest.skip
    async def test_create_streams(self):
        raise NotImplementedError

    @unittest.skip
    async def test_create_pipe_streams(self):
        raise NotImplementedError

    @unittest.skip
    async def test_create_pipe_connections(self):
        raise NotImplementedError


if __name__ == '__main__':
    unittest.main()
