.. _debugging:

Debugging
+++++++++

Debugging is an essential part of software development. Dragon provides several tools and techniques
to help you trace, identify and fix issues in your code. Below are some common debugging methods:

.. _using_the_dragon_logger:

Using the Dragon Logger
-----------------------

Dragon includes a built-in logging system that allows you to log messages at various levels
(e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL). You can use the logger to track the flow of your
application and identify where issues may be occurring. The Dragon logger is available both from
Python and from C/C++. Depending on the `-l LOGLEVEL` passed to the :ref:`dragon` launcher, your
application's logs will be collected and written to stdout and to the `dragon_*.log` file.

Python Dragon Logger
^^^^^^^^^^^^^^^^^^^^
To use the logger in Python, you can import it and log messages as follows:

.. code-block:: python

    import logging

    from dragon.dlogging.util import setup_BE_logging


    def main():
        _level, _fname = setup_BE_logging(service="My Application")
        log = logging.getLogger("user_app").getChild("multinode")

        log.warning("Hello from Python!")


    if __name__ == "__main__":
        main()

To run the above example, save it to a file called `py_logger.py` and run it with the Dragon launcher:

.. code-block:: console

    dragon -l DEBUG py_logger.py

Depending upon the logging configuration and the log level of the message, the logged values will be printed to one
or more of the following: stdout, `dragon_*.log` file, and/or `LA_BE_*.log` files in the current directory. The
log message will be similar to the following:

.. code-block:: console

    2026-05-04 19:19:16,970 WARNING  user_app.multinode Logging Monitor (281471656268064) :: Hello from Python!

To help locate the relevant messages, you can search for the name passed to `getLogger` in
the log files. In the above example, you would search for "user_app" to find the relevant log messages.

C/C++ Dragon Logger
^^^^^^^^^^^^^^^^^^^^

In C/C++, to use the Dragon logger:

1. Include the "dragon/messages_api.h" and "err.h" header files
2. Attach to the dragon logger using `dragon_logging_attach()`
3. Log messages using `dragon_log_message()`
4. Detach from the logger using `dragon_logging_detach()`
5. Ensure that you link to the `dragon` library when compiling your application.

.. code-block:: Makefile

    CC = gcc
    INCLUDE = -I $(DRAGON_INCLUDE_DIR)
    LIBS = -L $(DRAGON_BASE_DIR)/lib

    %.c.o: %.c
        $(CC) $(INCLUDE) $(CFLAGS) -c $< -o $@

    default: logging

    logging: logging.c.o
        $(CC) $(INCLUDE) $(CFLAGS) -o logging $< $(LIBS) -ldragon -lrt

.. code-block:: c

    #include <stdio.h>
    #include "err.h"
    #include "dragon/messages_api.h"

    int main(int argc, char *argv[]) {

        dragonError_t err;
        err = dragon_logging_attach();
        if (err != DRAGON_SUCCESS) {
            printf("Error attaching to logging FLI: %s\n", dragon_getlasterrstr(err));
            return -1;
        }

        err = dragon_log_message(
            "user_app",        /* name */
            "Hello from C!",   /* message */
            NULL,              /* time */
            NULL,              /* func */
            NULL,              /* hostname */
            NULL,              /* ipAddress */
            0,                 /* port */
            NULL,              /* service */
            LOG_LEVEL_INFO,    /* level */
            NULL               /* timeout */
        );
        if (err != DRAGON_SUCCESS) {
            printf("Error logging message: %s\n", dragon_getlasterrstr(err));
            return -1;
        }

        err = dragon_logging_detach();
        if (err != DRAGON_SUCCESS) {
            printf("Error detaching from logging FLI: %s\n", dragon_getlasterrstr(err));
            return -1;
        }

        return 0;
    }

To run the above example, save it to a file called `logger.c`, compile it,and run it with the Dragon launcher:

.. code-block:: console

    dragon -l DEBUG ./logger

Depending upon the logging configuration and the log level of the message, the logged values will be printed to one
or more of the following: stdout, `dragon_*.log` file, and/or `LA_BE_*.log` files in the current directory. The
log message will be similar to the following:

.. code-block:: console

    2026-05-04 19:25:45,452 INFO     user_app Logging Monitor (281471899472160) :: Hello from C!

To help locate the relevant messages, you can search for the name value to `dragon_log_message` in
the log files. In the above example, you would search for "user_app" to find the relevant log messages.

Note: Not all values passed to `dragon_log_message` are currently used. The only required values are
the name, message, and level. The other values can be set to NULL or 0 if not needed.