import unittest


class TaskTestCase(unittest.IsolatedAsyncioTestCase):

    @unittest.skip
    def test_run_forever(self):
        raise NotImplementedError

    @unittest.skip
    async def test_cancel_all_tasks(self):
        raise NotImplementedError

    @unittest.skip
    async def test_task_mixin(self):
        raise NotImplementedError


if __name__ == '__main__':
    unittest.main()
