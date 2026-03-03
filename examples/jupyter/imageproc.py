import scipy.signal
import dragon
import os
import socket
import multiprocessing as mp

def hello(arg=None):
    print(f'Hello DRAGON from {socket.gethostname()} with {mp.cpu_count()} cores!!', flush=True)

def f(args):
    image, random_filter = args
    # Do some image processing.
    return scipy.signal.convolve2d(image, random_filter)[::5, ::5]

if __name__ == "__main__":
    hello()