import threading
from functools import wraps


def catch_thread_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        exceptions_caught_in_threads = {}

        def custom_excepthook(args):
            thread_name = args.thread.name
            exceptions_caught_in_threads[thread_name] = {
                'thread': args.thread,
                'exception': {
                    'type': args.exc_type,
                    'value': args.exc_value,
                    'traceback': args.exc_traceback
                }
            }
        # Registering our custom excepthook to catch the exception in the threads
        old_excepthook = threading.excepthook
        threading.excepthook = custom_excepthook

        result = func(*args + (exceptions_caught_in_threads, ), **kwargs)

        threading.excepthook = old_excepthook
        return result

    return wrapper
