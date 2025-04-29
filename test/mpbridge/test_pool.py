import unittest

import dragon
import multiprocessing as mp

# last val should be (NUM_WORKERS ** INCEPTION_DEPTH - 1) ** 2
NUM_WORKERS = 2
INCEPTION_DEPTH = 3


def square(x):
    return x**2


def flatten(list_of_lists):
    flat = []
    for val in list_of_lists:
        flat.extend(val)
    return flat


def square_or_pool(args):
    value, depth_val = args
    if depth_val > 1:
        to_square = [x for x in range(value, value + NUM_WORKERS ** (depth_val - 1), NUM_WORKERS ** (depth_val - 2))]
        depth = [depth_val - 1] * len(to_square)
        pool = mp.Pool(NUM_WORKERS)
        result = pool.map(square_or_pool, zip(to_square, depth))
        if depth_val > 2:
            result = flatten(result)
        pool.close()
        pool.join()
    else:
        result = square(value)

    return result


def setUpModule():
    mp.set_start_method("dragon", force=True)


class TestMPBridgePool(unittest.TestCase):
    def setUp(self):
        self.assertEqual(mp.get_start_method(), "dragon")

    def test_pool_recursion(self):
        to_square = [x for x in range(0, NUM_WORKERS**INCEPTION_DEPTH, NUM_WORKERS ** (INCEPTION_DEPTH - 1))]
        depth = [INCEPTION_DEPTH] * len(to_square)

        pool = mp.Pool(NUM_WORKERS)

        squared = pool.map(square_or_pool, zip(to_square, depth))
        squared = flatten(squared)

        pool.close()
        pool.join()
        self.assertEqual(squared, [square(x) for x in range(NUM_WORKERS**INCEPTION_DEPTH)])


if __name__ == "__main__":
    unittest.main()
