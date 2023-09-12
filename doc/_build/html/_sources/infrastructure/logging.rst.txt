.. _Logging:

Logging
+++++++

Overview of the structure, classes, and functions that enable orderly logging
of Dragon's multiple backend components.

General Design
==============

The goal of the infrastructure is to make all service logging easy to parse with
grep or similar command line tools and get information out that actually makes sense.
With that target in mind, the goals are:

* Get all logging messages from all backend services to the frontend's file system
  and/or stderr
* Make it easy to see what service and and host/IP/port the logging message was
  generated from
* Make sure timestamps are actually meaningful
* Keep it simple for other devs to plug into their infrastructure and not
  irreparably break already existing logging infrastructure.
* Stick to Python's logging module as much as possible

Frontend Services
=================

The MRNet frontend service is the ultimate destination for all the logging messages
created by backend services.

Those messages are received via the MRNet server's callback function that also provides
all the other infrastructure related messages unrelated to logging. In order to avoid clobbering
infrastructure messages, the :py:func:`dragon.launcher.util.logging_queue_monitor` decorator
pulls any messages received from the callback and puts them in a separate queue so
the MRNet server can write them to log as infrastructure requests allow. All the messages
received by the frontend server must be an instance of
:py:class:`dragon.infrastructure.messages.LoggingMsgList`

In order to keep management of all the messages sane and easily parsible, the launcher
frontend has a separate Python log for each service. Each log is referenced by names defined as
attributes of :py:class:`dragon.dlogging.util.DragonLoggingServices`. Each message is
consequently logged under the scope of the service it came from. Many children can
be created from this parent log, but they should belong to a given parent (ie: service log)

.. code-block:: python
    :caption: This is essentially what dragon.dlogging.util.log_FE_msg_list() performs

    import logging
    from dragon.dlogging.util import DragonLoggingServices

    # Log a global services messages
    log = logging.getLogger(DragonLoggingServices.GS)
    log.log(level, log_msg, extra=dict_of_extra_record_attributed)

Backend Services
================

During bring-up, the MRNet server backend creates a :py:class:`DragonLogger` instance and
passes its serialized descriptor to all backend services on the same node as it:
global services (for primary node), local services, transport agent, and the launcher
backend.

All the listed services then connect to the MRNet server's :py:class:`DragonLogger` instance
by attaching to it. All the services' log messages are passed through a :py:class:DragonLogginerHandler
that simply put them int the MRNet backend's :py:class:`DragonLogger` channel queue. Timestamps are
generated when a given service emits its message, not when the MRNet backend plucks it from the queue.
That way log messages reflect the time they were emitted by the given backend service.

.. code-block:: python
    :caption: Launcher backend attaching to a logging channel and logging

    import logging
    import dragon.dlogging.util as dlog

    dlog.setup_BE_logging(service=dlog.DragonLoggingServices.LA_BE, logger_sdesc=logger_sdesc, level=level)
    log.info(f'start in pid {os.getpid()}, pgid {os.getpgid(0)}')

Formatting
==========

Log entries posted by the launcher frontend contain hostname, service, and function name
the log entry came from::

    2022-07-28 13:07:49,988 INFO     LA_BE.get_host_info (pinoak0202) :: sockets returned ip_addr 172.23.0.11 for pinoak0202
    2022-07-28 13:07:49,988 INFO     LA_BE.get_host_info (pinoak0202) :: primary_hostname = pinoak0201 | hostname = pinoak0202
    2022-07-28 13:07:49,988 INFO     LA_BE.get_host_info (pinoak0202) :: primary node status = False
    2022-07-28 13:07:49,988 INFO     LA_BE.main (pinoak0202) :: sent BENodeIdxSH(node_idx=1) - m2.1
    2022-07-28 13:07:50,006 DEBUG    LS.multinode (pinoak0202) :: dragon logging initiated on pid=17735
    2022-07-28 13:07:50,006 INFO     LS.multinode (pinoak0202) :: got BENodeIdxSH (id=1, ip=172.23.0.11, host=pinoak0202, primmary=False) - m2.1



Module Classes and functions
============================
.. automodule:: dragon.dlogging.util
    :members:
    :member-order: bysource

.. automodule:: dragon.dlogging.logger
    :members: DragonLogger
