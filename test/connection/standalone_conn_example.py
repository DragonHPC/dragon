#!/usr/bin/env python3


import multiprocessing

import dragon.infrastructure.standalone_conn


def write_actor(writer):
    writer.send('hyenas')


def read_actor(reader):
    msg = reader.recv()
    if msg == 'hyenas':
        print('got hyenas')
    else:
        print('got {}, no hyenas'.format(msg))


def main():
    reader, writer = dragon.infrastructure.standalone_conn.Pipe(duplex=False)
    # reader, writer = multiprocessing.Pipe(duplex=False)
    write_proc = multiprocessing.Process(target=write_actor, args=(writer,))
    read_proc = multiprocessing.Process(target=read_actor, args=(reader,))

    write_proc.start()
    read_proc.start()

    write_proc.join()
    read_proc.join()


if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')
    main()
