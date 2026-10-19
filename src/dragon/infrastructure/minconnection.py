import time

import dragon.channels as dch
import dragon.managed_memory as dmm


class MinConnection:
    """Minimal one way Connection style class based on channels

    DEPRECATED

    Only trying to get this to work for local channels now.

    Also, this only works for sending newline-free strings around, intended
    to be handled as infrastructure
    messages, and there is no attempt to avoid copying the data
    multiple times.
    """

    def __init__(self, the_pool, the_channel, reading: bool = False, sleepy_poll_interval_sec=0.001):
        """Construct from a channel descriptor

        :param reading: is this a read-side connection
        :type reading: bool

        :param the_channel: handle to initialize stuff from
        :type the_channel: dragon.channels.Channel
        """

        assert isinstance(the_channel, dch.Channel)
        self.the_channel = the_channel

        assert isinstance(the_pool, dmm.MemoryPool)
        self.the_pool = the_pool

        self._is_read_direction = bool(reading)

        if self._is_read_direction:
            self.handle = self.the_channel.recvh()
        else:
            self.handle = self.the_channel.sendh()

        self.handle.open()
        self.opened = True

        self._stashed_msg = None

        self.sleepy_poll_interval_sec = sleepy_poll_interval_sec

    def __del__(self):
        # unclear whether detaching from or destroying the
        # underlying channel is correct.  I think it is not
        # correct to do in this object for the time being.

        try:
            self.close()
        except Exception as e:
            pass  # No penalty for attempting to close if already closed.

    def send(self, data):
        assert not self._is_read_direction
        assert isinstance(data, str)
        msg_bytes = data.encode().strip().replace(b"\n", b" ") + b"\n"
        the_msg = dch.Message.create_alloc(self.the_pool, len(msg_bytes))
        mmv = the_msg.bytes_memview()
        mmv[:] = msg_bytes

        sent_successfully = False
        while not sent_successfully:
            try:
                self.handle.send(the_msg)
                sent_successfully = True
            except dch.ChannelSendError as cse:
                if cse.ex_code == dch.ChannelSendEx.Errors.CH_FULL:
                    time.sleep(self.sleepy_poll_interval_sec)

        the_msg.destroy()

    @staticmethod
    def _extract(msg):
        return bytes(msg.bytes_memview()).decode()

    def recv(self):
        """Blocking read from channel.

        This blocking read does a sleepy poll on the channel.

        If poll was called ahead of time, the message may already
        be present, and is returned in that case.

        :return: The string received from the channel.
        """

        assert self._is_read_direction

        # is there something leftover from polling before?
        # if so return that

        if self._stashed_msg is not None:
            result = self._extract(self._stashed_msg)
            self._stashed_msg.destroy()
            self._stashed_msg = None
            return result

        # otherwise sleepy poll

        acquired = False
        result = None

        while not acquired:
            try:
                the_msg = self.handle.recv()
                acquired = True
                result = self._extract(the_msg)
                the_msg.destroy()
            except dch.ChannelRecvError as cre:
                if cre.ex_code == dch.ChannelRecvEx.Errors.CH_EMPTY:
                    time.sleep(self.sleepy_poll_interval_sec)
                else:
                    raise cre

        return result

    def poll(self, timeout=0):
        """Polls to see whether a message can be received.

        Note this only makes any sense if there is only one entity
        reading from a given channel, just like a pipe.  The object
        may read and cache a message from the channel and return
        this on subsequent recv calls.

        This is basically a hack to get a good consistent interface with
        other objects being used in the infrastructure between production
        and test situations.

        :param timeout: None means infinite timeout
        :type timeout: float or None
        :return: True or False according to whether there is a message there
        or the timeout has expired.

        """
        if self._stashed_msg is not None:
            return True

        if timeout is not None:
            remaining_timeout = timeout
        else:
            remaining_timeout = -1

        got_something = False

        while timeout is None or remaining_timeout >= 0:
            try:
                self._stashed_msg = self.handle.recv()
                got_something = True
                break
            except dch.ChannelRecvError as cre:
                if cre.ex_code == dch.ChannelRecvEx.Errors.CH_EMPTY:
                    time.sleep(self.sleepy_poll_interval_sec)
                else:
                    raise cre

            # imprecise but it avoids doing a time system call
            remaining_timeout = remaining_timeout - self.sleepy_poll_interval_sec

        return got_something

    def close(self):
        if self.opened:
            self.handle.close()
            self.opened = False

    @property
    def is_read(self):
        return self._is_read_direction

    @property
    def is_write(self):
        return not self._is_read_direction


class MinBConnection(MinConnection):
    def send(self, data):
        assert not self._is_read_direction
        if not isinstance(data, bytes):
            # Forcibly convert or die; helpful when handed a memoryview as data.
            data = bytes(data)
        msg_bytes = data  # .strip().replace(b'\n', b' ') + b'\n'
        the_msg = dch.Message.create_alloc(self.the_pool, len(msg_bytes))
        mmv = the_msg.bytes_memview()
        mmv[:] = msg_bytes

        sent_successfully = False
        while not sent_successfully:
            try:
                self.handle.send(the_msg)
                sent_successfully = True
            except dch.ChannelSendError as cse:
                if cse.ex_code == dch.ChannelSendEx.Errors.CH_FULL:
                    time.sleep(self.sleepy_poll_interval_sec)

        the_msg.destroy()

    @staticmethod
    def _extract(msg):
        return bytes(msg.bytes_memview())
