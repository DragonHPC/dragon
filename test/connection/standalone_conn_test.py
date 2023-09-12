import atexit
import multiprocessing
import unittest

import dragon.infrastructure.standalone_conn as disc


def write_actor(writer):
    writer.send('hyenas')


SUCCESS = 0
FAILURE = 1


def read_actor(reader, expected):
    msg = reader.recv()

    if msg == expected:
        exit(SUCCESS)
    else:
        exit(FAILURE)


def echo_actor(handle):
    try:
        while msg := handle.recv():
            handle.send(msg)
    except EOFError:
        handle.close()
        exit(SUCCESS)
    except Exception as e:
        print(f'got some other exception: {e}')
        import traceback
        traceback.print_exc()

    exit(FAILURE)


def swap_actor(first, second):
    first_msg = first.recv()
    second_msg = second.recv()
    second.send(first_msg)
    first.send(second_msg)


class TestStandaloneConn(unittest.TestCase):

    def tearDown(self) -> None:
        # this runs what would normally happen at exit of the first process
        # to make a call to Pipe.
        disc.destroy_my_channels()
        disc.destroy_standalone()
        atexit.unregister(disc.destroy_my_channels)
        atexit.unregister(disc.destroy_standalone)
        disc._IS_INIT = False

    def test_single_oneway(self):
        reader, writer = disc.Pipe(duplex=False)
        send_msg = 'hyenas'
        read_proc = multiprocessing.Process(target=read_actor, args=(reader, send_msg))
        read_proc.start()
        writer.send(send_msg)
        read_proc.join()
        self.assertEqual(read_proc.exitcode, SUCCESS)

    def test_single_duplex(self):
        local, remote = disc.Pipe()

        echo_proc = multiprocessing.Process(target=echo_actor, args=(remote,))
        echo_proc.start()

        msg = 'hyenas'
        local.send(msg)
        recvmsg = local.recv()

        local.close()
        echo_proc.join()

        self.assertEqual(msg, recvmsg)
        self.assertEqual(echo_proc.exitcode, SUCCESS)

    def test_single_swap(self):
        firstlocal, firstremote = disc.Pipe()
        secondlocal, secondremote = disc.Pipe()
        first_msg = 'hyenas'
        second_msg = 'weasels'
        swap_proc = multiprocessing.Process(target=swap_actor, args=(firstremote, secondremote))
        swap_proc.start()
        firstlocal.send(first_msg)
        secondlocal.send(second_msg)
        frep = firstlocal.recv()
        srep = secondlocal.recv()
        swap_proc.join()
        self.assertEqual(frep, second_msg)
        self.assertEqual(srep, first_msg)


if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')
    unittest.main()
