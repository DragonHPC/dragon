import asyncio
import base64
import unittest
from unittest.mock import MagicMock, patch

from dragon.transport.tcp.io import CodableIO, FixedBytesIO
from dragon.transport.tcp import transport


class FixedBytesIOTestCase(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        cls.types = [
            type(f'FixedBytesIO{size}', (FixedBytesIO,), dict(size=size))
            for size in (16, 64, 256, 1024)
        ]

    async def test_read(self):
        for cls in self.types:
            data = b'\xff' * cls.size
            r, w = await transport.create_pipe_streams()
            try:
                w.write(data)
                await w.drain()
            finally:
                await transport.close_writer(w)
            _data = await cls().read(r)
            self.assertEqual(_data, data)

    async def test_incomplete_read(self):
        for cls in self.types:
            data = b'\xff' * (cls.size - 1)
            r, w = await transport.create_pipe_streams()
            try:
                w.write(data)
                await w.drain()
            finally:
                await transport.close_writer(w)
            with self.assertRaises(asyncio.IncompleteReadError):
                await cls().read(r)

    async def test_write(self):
        for cls in self.types:
            data = b'\xff' * cls.size
            r, w = await transport.create_pipe_streams()
            try:
                cls().write(w, data)
                await w.drain()
            finally:
                await transport.close_writer(w)
            _data = await r.readexactly(cls.size)
            self.assertEqual(_data, data)

    async def test_incomplete_write(self):
        for cls in self.types:
            data = b'\xff' * (cls.size - 1)
            r, w = await transport.create_pipe_streams()
            try:
                with self.assertRaises(AssertionError):
                    cls().write(w, data)
            finally:
                await transport.close_writer(w)


class CodableIOTestCase(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        cls.cls = type('Base64CodableIO', (CodableIO,), dict(
            encode=staticmethod(base64.standard_b64encode),
            decode=staticmethod(base64.standard_b64decode),
        ))
        cls.DATA = b'\xffTEST DATA\xff'
        cls.ENCDATA = base64.standard_b64encode(cls.DATA)

    @patch.object(FixedBytesIO, 'read')
    async def test_read(self, read):
        read.return_value = self.ENCDATA
        reader = MagicMock(spec=asyncio.StreamReader)
        data = await self.cls().read(reader)
        self.assertEqual(data, self.DATA)
        read.assert_called_once_with(reader)

    @patch.object(FixedBytesIO, 'write')
    def test_write(self, write):
        writer = MagicMock(spec=asyncio.StreamWriter)
        self.cls().write(writer, self.DATA)
        write.assert_called_once_with(writer, self.ENCDATA)


class StructIOTestCase(unittest.IsolatedAsyncioTestCase):

    @unittest.skip
    async def test_read(self):
        raise NotImplementedError

    @unittest.skip
    def test_write(self):
        raise NotImplementedError


class VariableBytesIOTestCase(unittest.IsolatedAsyncioTestCase):

    @unittest.skip
    async def test_read(self):
        raise NotImplementedError

    @unittest.skip
    def test_write(self):
        raise NotImplementedError


class VariableTestIOTestCase(unittest.IsolatedAsyncioTestCase):

    @unittest.skip
    async def test_read(self):
        raise NotImplementedError

    @unittest.skip
    def test_write(self):
        raise NotImplementedError


class IPv4AddressIOTestCase(unittest.IsolatedAsyncioTestCase):

    @unittest.skip
    async def test_read(self):
        raise NotImplementedError

    @unittest.skip
    def test_write(self):
        raise NotImplementedError


class IPv6AddressIOTestCase(IPv4AddressIOTestCase):
    pass


class EnumIOTestCase(unittest.IsolatedAsyncioTestCase):

    @unittest.skip
    async def test_read(self):
        raise NotImplementedError

    @unittest.skip
    def test_write(self):
        raise NotImplementedError


if __name__ == '__main__':
    unittest.main()
