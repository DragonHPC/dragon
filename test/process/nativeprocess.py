import dragon
import dragon.native.process as nativeproc
import logging

LOG = logging.getLogger('NativeProcTest :')

def main():
    proc = nativeproc.Popen([],"hello.py",stdout=nativeproc.PIPE)

    try:
        while True:
            output = proc.stdout.recv()
            print("From the pipe:", output, end="", flush=True)
    except EOFError:
        pass
    except Exception as ex:
        print(f'Unexpected error: {repr(ex)}')

    del proc

if __name__=="__main__":
    main()