TAG_ID = 0


def next_tag():
    global TAG_ID
    tmp = TAG_ID
    TAG_ID += 1
    return tmp


def route(msg_type, routing_table, metadata=None):
    """Decorator routing adapter.

    This is a function decorator used to accumulate handlers for a particular
    kind of message into a routing table indexed by type, used
    by server classes processing infrastructure messages.

    The routing table is usually a class attribute and the message type
    is typically an infrastructure message.  The metadata, if used,
    is typically information used to check that the state the server object
    is in, is appropriate for that type of message.

    :param msg_type: the type of an infrastructure message class
    :param routing_table: dict, indexed by msg_type with values a tuple (function, metadata)
    :param metadata: metadata to check before function is called, usage dependent
    :return: the function.
    """

    def decorator_route(f):
        assert msg_type not in routing_table
        routing_table[msg_type] = (f, metadata)
        return f

    return decorator_route
