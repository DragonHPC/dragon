import dragon
import multiprocessing as mp
import dragon.globalservices.process as gproc
import dragon.infrastructure.util as dutil
import example as ex

def main():
    mp.set_start_method('dragon')
    dutil.enable_logging()
    gproc.start_capturing_child_mp_output()

    proc = mp.Process(target=ex.hello, args=())
    proc.start()
    proc.join()
    gproc.stop_capturing_child_mp_output()

if __name__ == "__main__":
    main()

