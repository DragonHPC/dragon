import os
import dragon
import multiprocessing as mp

def runit():
    print("Hello from Worker", os.uname().nodename)

def main():
    mp.set_start_method('dragon')
    print("Hello Dragon", os.uname().nodename)
    p = mp.Process(target=runit, args=())
    p.start()
    p.join()

if __name__ == "__main__":
    main()