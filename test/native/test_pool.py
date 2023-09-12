import unittest
import time

import dragon  # DRAGON import before multiprocessing
from dragon.native.pool import Pool

def sqr(x, wait=0.0):
    time.sleep(wait)
    return x * x

def raising():
    raise KeyError("key")

class TestDragonNativePool(unittest.TestCase):
    """Unit tests for the Dragon Native Pool.
    """
    def test_async(self):

        pool = Pool(processes=4)
    
        test_timeout=1.2 
     
        res = pool.apply_async(sqr, (7, test_timeout,))
   
        start = time.monotonic()
        result = res.get() 
        stop = time.monotonic()
  
        self.assertEqual(result, 49)
        self.assertAlmostEqual(stop-start , test_timeout, 0)
      
        pool.close()
        pool.join()

    def test_async_timeout(self):
        
        pool = Pool(processes=4)
    
        test_timeout=1.2 
        res = pool.apply_async(sqr, (6, test_timeout + 1.0))
        get = res.get
        self.assertRaises(TimeoutError, get, timeout=test_timeout)
        
        pool.close()
        pool.join()

    def test_map_async(self):
       
        pool = Pool(processes=4)
        
        self.assertEqual(pool.map_async(sqr, list(range(10))).get(), list(map(sqr, list(range(10)))))
        self.assertEqual(pool.map_async(sqr, list(range(100)), chunksize=20).get(), list(map(sqr, list(range(100)))))
        
        pool.close()
        pool.join()

    # if max value of range is huge then we can wait in main thread for a long time before returning
    def test_terminate(self):
        pool = Pool(processes=4)
        _ = pool.map_async(time.sleep, [0.1 for i in range(100)], chunksize=1)
        pool.terminate()
        start = time.monotonic()
        pool.join()
        stop = time.monotonic()
  
        # Sanity check the pool didn't wait for all work to be done 
        self.assertLess(stop-start, 2.0)

    def test_empty_iterable(self):
        p = Pool(1)
        self.assertEqual(p.map_async(sqr, []).get(), [])
        p.close()
        p.join()


    def test_async_error_callback(self):
        p = Pool(2)

        scratchpad = [None]

        def errback(exc):
            scratchpad[0] = exc

        res = p.apply_async(raising, error_callback=errback)
        self.assertRaises(KeyError, res.get)
        self.assertTrue(scratchpad[0])
        self.assertIsInstance(scratchpad[0], KeyError)

        p.close()
        p.join()


if __name__ == "__main__":
    unittest.main()
