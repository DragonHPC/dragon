"""Utility functions to support testing"""

import dragon.infrastructure.messages as dmsg


DEFAULT_TIMEOUT = 20


def get_and_parse(read_handle, timeout=DEFAULT_TIMEOUT):
    if hasattr(read_handle, "poll"):
        if read_handle.poll(timeout):
            the_msg = dmsg.parse(read_handle.recv())
        else:
            raise TimeoutError()
    else:
        the_msg = dmsg.parse(read_handle.recv())

    return the_msg


def get_and_check_type(read_handle, the_type, timeout=DEFAULT_TIMEOUT):
    the_msg = get_and_parse(read_handle, timeout=timeout)

    assert isinstance(the_msg, the_type), "Wrong type. Expected: {}, got {}".format(the_type, the_msg)

    return the_msg


def get_and_check_several(self, read_handle, expected_dict, timeout=DEFAULT_TIMEOUT):
    # self is the test object that is currently executing.

    rv = {x: [] for x in expected_dict}
    msgs_expected = sum(expected_dict.values())

    for k in range(msgs_expected):
        if read_handle.poll(timeout):
            the_msg = dmsg.parse(read_handle.recv())
        else:
            raise TimeoutError(f"Got {rv} messages so far but timed out while waiting for the rest in {expected_dict}.")

        msg_type = type(the_msg)
        self.assertIn(
            msg_type,
            expected_dict,
            f"Found an unexpected message {repr(the_msg)} while looking for one of these {expected_dict}",
        )
        rv[msg_type].append((the_msg, k))

    for key in expected_dict:
        self.assertEqual(
            expected_dict[key],
            len(rv[key]),
            f"The message type {key} was expected {expected_dict[key]} times and was found {len(rv[key])} times.",
        )

    return rv


def get_and_check_several_ignore_SHFwdOutput(self, read_handle, expected_dict, timeout=DEFAULT_TIMEOUT):
    # self is the test object that is currently executing.

    rv = {x: [] for x in expected_dict}
    rv[dmsg.SHFwdOutput] = []
    msgs_expected = sum(expected_dict.values())

    msgs_received = 0
    when_received = 0
    while msgs_received < msgs_expected:
        if read_handle.poll(timeout):
            the_msg = dmsg.parse(read_handle.recv())
        else:
            raise TimeoutError(f"Got {rv} messages so far but timed out while waiting for the rest in {expected_dict}.")

        msg_type = type(the_msg)
        if not isinstance(the_msg, dmsg.SHFwdOutput):
            self.assertIn(
                msg_type,
                expected_dict,
                f"Found an unexpected message {repr(the_msg)} while looking for one of these {expected_dict}",
            )
            msgs_received += 1
        rv[msg_type].append((the_msg, when_received))
        when_received += 1

    for key in expected_dict:
        self.assertEqual(
            expected_dict[key],
            len(rv[key]),
            f"The message type {key} was expected {expected_dict[key]} times and was found {len(rv[key])} times.",
        )

    return rv
