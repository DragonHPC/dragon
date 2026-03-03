"""Dragon's replacements for Multiprocessings 'reduction' module.

To prevent users from sharing file descriptors between child processes, we always
raise an exception.

See the documentation 'Multiprocessing with Dragon'.

:raises NotImplementedError: Always raise this.
"""

import array
import socket


def dragon_DupFd(fd: int) -> int:
    """Duplicates a file descriptor for a child process in Multiprocessing.

    Dragon does not support sharing file descriptors as it does not generalize to multi-node
    or federated systems.

    See the documentation 'Multiprocessing with Dragon'.

    :param fd: the file descriptor of the parent process to be duplicated
    :type fd: int
    :return: the duplicated file descriptor
    :rtype: int
    :raises NotImplementedError: Always raises an exception.
    """
    raise NotImplementedError(f"Dragon does not support duplicate file descriptors for child processes.")


def dragon_sendfds(sock: socket.socket, fds: array.array) -> None:
    """Send an array of file descriptors to another process via a socket.

    Dragon does not support this behaviour as it does not generalize to multi-node
    or federated systems.

    See the documentation 'Multiprocessing with Dragon'.

    :param sock: the socket to be used for sending
    :type sock: socket.socket
    :param fds: the array of file descriptors
    :type fds: array.array
    :raises NotImplementedError: Always raises an exception.
    """
    raise NotImplementedError(f"Dragon does not support sending file descriptors to other processes.")


def dragon_recvfds(sock: socket.socket, size: int) -> list:
    """Receive a list of file descriptors from another process via a socket.

    Dragon does not support this behaviour as it does not generalize to multi-node
    or federated systems.

    See the documentation 'Multiprocessing with Dragon'.

    :param sock: the socket to be used for sending
    :type sock: socket.socket
    :param size: the number of file descriptor to receive.
    :type size: int
    :raises NotImplementedError: Always raises an exception.
    :return: the file descriptors
    :rtype: list
    """
    raise NotImplementedError(f"Dragon does not support sending file descriptors to other processes.")
