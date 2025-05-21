import unittest

from test_transport import TestMessages


class ClientTestCase(TestMessages, unittest.IsolatedAsyncioTestCase):

    @unittest.skip
    async def test_run(self):
        raise NotImplementedError

    @unittest.skip
    def test_open(self):
        raise NotImplementedError

    @unittest.skip
    def test_close(self):
        raise NotImplementedError

    @unittest.skip
    async def test_recv(self):
        raise NotImplementedError

    @unittest.skip
    def test_process(self):
        raise NotImplementedError

    @unittest.skip
    async def test_wait_for_response(self):
        raise NotImplementedError

    @unittest.skip
    async def test_handle_unsupported_response(self):
        raise NotImplementedError

    @unittest.skip
    async def test_handle_error_response(self):
        raise NotImplementedError

    @unittest.skip
    async def test_handle_send_response(self):
        raise NotImplementedError

    @unittest.skip
    async def test_handle_recv_response(self):
        raise NotImplementedError

    @unittest.skip
    async def test_handle_event_response(self):
        raise NotImplementedError


if __name__ == "__main__":
    unittest.main()
