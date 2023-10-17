.. _Messages:

Infrastructure Messages
+++++++++++++++++++++++

:ref:`Services` in the Dragon runtime interact with each other using messages transported with a variety of
different means (mostly :ref:`Channels`).  Although typically there will be a Python API to construct and send
these messages, the messages themselves constitute the true internal interface. To that end, they are a
convention.

Here we document this internal message API.

Generalities about Messages
===========================

Many messages are part of a request/response pair.  At the level discussed here, successful and error
responses are carried by the same response type but the fields present or valid may change according to
whether the response is successful or an error.

All messages are serialized using JSON; the reason for this is to allow non-Python actors to interact with the
Dragon infrastructure as well as allowing the possibility for the different components of the Dragon runtime
to be implemented in something other than Python.

The serialization method may change in the future as an optimization; JSON is chosen here as a reasonable
cross language starting point.  In particular, the fields discussed here for basic message parsing (type and
instance tagging, and errors) are defined in terms of integer valued enumerations suitable for a fixed field
location and width binary determination.

The top level of the JSON structure will be a dictionary (map) where the '_tc' key's value is an integer
corresponding to an element of the ``dragon.messages.MsgTypes`` enumeration class.  The other fields of this
map will be defined on a message by message basis.

The canonical form of the message will be taken on the JSON level, not the string level.  This means that
messages should not be compared or sorted as strings.  Internally the Python interface will construct inbound
messages into class objects of distinct type according to the '_tc' field in the initializer by using a
factory function. The reason to have a lot of different message types instead of just one type differentiated
by the value of the '_tc' field is that this allows IDEs and documentation tools to work better by having
explicit knowledge of what fields and methods are expected in any particular message.

.. _cfs:

Common Fields
-------------
These fields are common to every message described below depending on their category
of classification.

The _tc TypeCode Field
^^^^^^^^^^^^^^^^^^^^^^^

The ``_tc`` *typecode* field is used during parsing to determine the type of a received message. The message
format is JSON. The ``_tc`` field can be used to determine of all expected fields of a message are indeed in
the message to verify its format.

Beyond the ``_tc`` *typecode* field, there are other fields expected to belong to every message.

The tag Field
^^^^^^^^^^^^^^

Every message has a 64 bit positive unsigned integer ``tag`` field. Together with the identity of the sender
is implicit or explicitly identified in some other field, this serves to uniquely identify the message.

Ideally, this would be throughout the life of the runtime, but it's enough to say that no two messages should
be simultaneously relevant with the same (sender, tag).

Message Categories
^^^^^^^^^^^^^^^^^^^^

There are three general categories of messages:
 - request messages, where one entity asks another one to do something
 - response messages, returning the result to the requester
    - the name of these messages normally ends "Response"
 - other messages, mostly to do with infrastructure sequencing

Every request message will contain ``p_uid`` and ``r_c_uid`` fields.  These fields denote the unique process
ID of the entity that created the request and the unique channel ID to send the response to. See
:ref:`ConventionalIDs` for more details on process identification.

The ref Field
^^^^^^^^^^^^^^

Every response message generated in reply to a request message will contain a ``ref`` field that echos the
``tag`` field of the request message that caused it to be generated.  If a response message is generated
asynchronously - for instance, as part of a startup notification or some event such as a process exiting
unexpectedly - then the ``ref`` field will be 0, which is an illegal value for the ``tag`` field.

The err Field
^^^^^^^^^^^^^^^

Every response message will also have an ``err`` field, holding an integer enumeration particular to the
response. This enumeration will be the class attribute ``Errors`` in the response message class.  The
enumeration element with value *0* will always indicate success.  Which fields are present in the response
message may depend on the enumeration value.

Format of Messages List
-----------------------

0.  Class Name

    *type enum*
        member of ``dragon.messages.MsgTypes`` enum class

    *purpose*
        succinct description of purpose, sender and receiver, where
        and when typically seen.  Starts with 'Request' if it is a
        request message and 'Response' if it is a response message.

    *fields*
        Fields in the message, together with description of how they
        are to get parsed out.  Some fields may themselves be
        dictionary structures used to initialize member objects.

        Fields in common to every message (such as ``tag``, ``ref``,
        and ``err``) are not mentioned here.

        An expected type will be mentioned on each field where it
        is an obvious primitive (such as a string or an integer).
        Where a type isn't mentioned, it is assumed to be another
        key-value map to be interpreted as the initializer for some
        more complex type.

    *response*
        If a request kind of message, gives the class name of the
        corresponding response message, if applicable

    *request*
        If a request kind of message, gives the class name of the
        corresponding response message.

    *see also*
        Other related messages

.. start the actual message documentation here.

.. _GlobalServicesAPI:

Global Services Messages API
============================

These messages are the ones underlying the Global Services API and are sent only to and from Global Services.

All global services request messages have an integer ``p_uid`` field that contains the process UID of the
requester and an integer ``r_c_uid`` field that contains the channel ID to send the response to.  This field
is not echoed in the corresponding response. For details on IDs, see :ref:`ConventionalIDs`.

GS Process Messages
---------------------

.. _gsprocesscreate:

1. **GSProcessCreate**

    *type enum*
        GS_PROCESS_CREATE  (= 1)

    *purpose*
        Request to global services to create a new managed process.

    *fields*

        **exe**
            - string
            - executable name

        **args**
            - list of strings
            - arguments to the executable.  [exe, args] == argv

        **env**
            - map, key=string value=string
            - Environment variables to be exported before running.

              Note that this is additive/override to the shepherd
              environment - may need to be more fancy here if it
              turns out we need to add to paths

        **rundir**
            - string, absolute path
            - Indicates where the process is to be run from. If empty,
              does not matter.

        **user_name**
            - string
            - requested user name for the process

        **options**
            - **options**

                - initializer for other process options object

                - **output_c_uid** is the channel id where output, both *stdout* and *stderr*, should be
                forwarded if present in an *SHFwdOutput* message. If this field is not present in the options,
                then output will be forwarded to the *Backend*. The *SHFwdOutput* message indicates whether it
                was from *stdout* or *stderr*.

                *response*
                GSProcessCreateResponse

    *see also*
        GSProcessDestroy

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSProcessCreate>`

.. _gsprocesscreateresponse:

2. **GSProcessCreateResponse**

    *type enum*
        GS_PROCESS_CREATE_RESPONSE  (= 2)

    *purpose*
        Response to process creation request.

    *fields*

        Alternatives on ``err``:

        SUCCESS ( = 0)
            A new process was created

            **desc**
                - map
                - initializer for ProcessDescriptor - includes at a minimum
                  p_uid of new process and assigned user name.

        FAIL
            No process was created

            **err_info**
                - string
                - explanation of what went wrong

            Might add some more here, e.g. is this a GS related problem
            or a Shepherd related problem.

        ALREADY
            The user name was already in use

            **desc**
                - map
                - init for the ProcessDescriptor of the process
                  that already exists with that user name

        *request*
            GSProcessCreate

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSProcessCreateResponse>`

.. _gsprocesslist:

3. **GSProcessList**

    *type enum*
        GS_PROCESS_LIST  (= 3)

    *purpose*
        Return a list of the p_uid for all the processes
        currently being managed

    *fields*
        None additional

    *response*
        GSProcessListResponse

    *see also*
        GSProcessQuery

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSProcessList>`

.. _gsprocesslistresponse:

4. **GSProcessListResponse**

    *type enum*
        GS_PROCESS_LIST_RESPONSE  (= 4)

    *purpose*
        Responds with a list of the p_uid for all the
        processes currently being managed

    *fields*
        **plist**
            - list of nonnegative integers for all the processes

    *request*
        GSProcessList

    *see also*
        GSProcessQuery

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSProcessListResponse>`

.. _gsprocessquery:

5. **GSProcessQuery**

    *type enum*
        GS_PROCESS_QUERY  (= 5)

    *purpose*
        Request the ProcessDescriptor for a managed process

    *fields*
        One of the ``t_p_uid`` and ``user_name`` fields must be
        present.

        If both are specified, then the ``user_name`` field is ignored.

        **t_p_uid**
            - integer
            - target process UID

        **user_name**
            - string
            - user supplied name for the target process

    *response*
        GSProcessQueryResponse

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSProcessQuery>`

.. _gsprocessqueryresponse:

6. **GSProcessQueryResponse**

    *type enum*
        GS_PROCESS_QUERY_RESPONSE  (= 6)

    *purpose*
        Response to request for ProcessDescriptor for a managed process

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Process has been found

            **desc**
                - map
                - initializer for ProcessDescriptor - includes at a minimum
                  p_uid of new process and assigned user name.

        UNKNOWN (= 1)
            No such process is known.

            **err_info**
                - string
                - explanation of what went wrong

    *request*
        GSProcessQuery

    *see also*
        GSProcessList

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSProcessQueryResponse>`

.. _gsprocesskill:

7. **GSProcessKill**

    *type enum*
        GS_PROCESS_KILL  (= 7)

    *purpose*
        Request a managed process get killed

    *fields*
        One of the ``t_p_uid`` and ``user_name`` fields must be
        present.

        If both are specified, then the ``user_name`` field is ignored.

        **t_p_uid**
            - integer
            - target process UID

        **user_name**
            - string
            - user supplied name for the target process

        **sig**
            - integer
            - signal to kill the process with

    *response*
        GSProcessKillResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSProcessKill>`

.. _gsprocesskillresponse:

8. **GSProcessKillResponse**

    *type enum*
        GS_PROCESS_KILL_RESPONSE  (= 8)

    *purpose*
        Response to GSProcessKill message

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Process has been found and has been killed.

            **exit_code**
                - integer
                - exit code the process returned

        UNKNOWN (= 1)
            No such process is known.

            **err_info**
                - string
                - explanation of what went wrong

        FAIL_KILL (= 2)
            Something went wrong in killing the process.

            **err_info**
                - string
                - explanation of what went wrong

        DEAD (= 3)
            The process is already dead.

            No other fields.

    *request*
        GSProcessKill

    *see also*
        GSProcessQuery

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSProcessKillResponse>`

.. _gsprocessjoin:

9. **GSProcessJoin**

    *type enum*
        GS_PROCESS_JOIN  (= 9)

    *purpose*
        Request notification when a given process exits

    *fields*
        One of the ``t_p_uid`` and ``user_name`` fields must be
        present.

        If both are specified, then the ``user_name`` field is ignored.

        **t_p_uid**
            - integer
            - target process UID

        **user_name**
            - string
            - user supplied name for the target process

        **timeout**
            - integer, interpreted as microseconds
            - timeout value; any value < 0 interpreted as infinite timeout

    *response*
        GSProcessJoinResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSProcessJoin>`


.. _gsprocessjoinresponse:

10. **GSProcessJoinResponse**

    *type enum*
        GS_PROCESS_JOIN_RESPONSE  (= 10)

    *purpose*
        Response to request for notification when a process exits.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Target process has exited.  If the target process has
            already exited, this is not an error.

            **exit_code**
                - integer
                - the Unix return value of the process

        UNKNOWN (= 1)
            No such process is known.

            **err_info**
                - string
                - explanation of what went wrong

        TIMEOUT (= 2)
            The timer has expired and the process still hasn't exited.

            No fields.

    *request*
        GSProcessJoin

    *see also*
        GSProcessQuery

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSProcessJoinResponse>`


.. _gsprocessjoinlist:

95. **GSProcessJoinList**

    *type enum*
        GS_PROCESS_JOIN_LIST  (= 95)

    *purpose*
        Request notification when any/all process from a given list exits.

    *fields*
        Both ``t_p_uid_list`` and ``user_name_list`` fields could be
        present. Any process in the list could be identified by either
        p_uid or user_name.

        **t_p_uid_list**
            - list of integers
            - list of target processes UID

        **user_name_list**
            - list of strings
            - list of user supplied names for the target processes

        **timeout**
            - integer, interpreted as microseconds
            - timeout value; any value < 0 interpreted as infinite timeout

        **join_all**
            - bool
            - default False, request notification when any process from the list exits.
              When True, request notification when all processes exit.

    *response*
        GSProcessJoinListResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSProcessJoinList>`


.. _gsprocessjoinlistresponse:

96. **GSProcessJoinListResponse**

    *type enum*
        GS_PROCESS_JOIN_LIST_RESPONSE  (= 96)

    *purpose*
        Response to request for notification when any/all process from a given list exits.

    *fields*
        Reflect the status of every process in the list:

        **puid_status*
            - dictionary with the status of every process
            - key: p_uid
            - value: tuple (status, info)

        The status of a process in the list can be one of the following:

        SUCCESS (= 0)
            Target process has exited.  If the target process has
            already exited, this is not an error.

            info:
                - integer
                - the Unix return value of the process

        UNKNOWN (= 1)
            No such process is known.

            info:
                - string
                - explanation of what went wrong

        TIMEOUT (= 2)
            The timer has expired and the process still hasn't exited.

            info:
                None

        SELF (= 3)
            Attempt to join itself.

            info:
                - string
                - explanation of what went wrong

        PENDING (= 4)
            The process is still pending (not exited, no timeout).

            info:
                None

    *request*
        GSProcessJoinList

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSProcessJoinListResponse>`


Pool Messages
-------------

These messages concern the lifecycle of named memory pools. All requests
related to channels will include the ``p_uid`` field of the requesting entity
and the ``r_c_uid`` field for the channel to send the response to.


Pools are identified in the runtime system with a nonnegative globally unique
ID number, the 'memory UID' normally abbreviated as ``m_uid``.

.. _gspoolcreate:

71. **GSPoolCreate**

    *type enum*
        GS_POOL_CREATE (= 71)

    *purpose*
        Requests that a new user memory pool be created.

    *fields*

        **user_name**
            - string
            - optional user specified name for the pool.  If absent
              or empty, Global Services will select a name.
            - this may not necessarily be the same name used on
              the node to name the pool object to the OS

        **options**
            - map
            - Initializer for globalservices.pool.PoolOptions object.  This is an
              open type intended to encompass what is in a
              ``dragonMemoryPoolAttr_t`` but also will carry system
              level options such as what node to create the pool on.

        **size**
            - integer > 0
            - size of pool requested, in bytes.  Note that the actual size of
              the pool delivered may be larger than this


    *response*
        GSPoolCreateResponse

    *see also*
        SHPoolCreate

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSPoolCreate>`

.. _gspoolcreateresponse:

72. **GSPoolCreateResponse**

    *type enum*
        GS_POOL_CREATE_RESPONSE (= 72)

    *purpose*
        Response to request for a pool creation

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            **desc**
                - map
                - Initializer for a globalservices.pool.PoolDescriptor object.  This
                  object will contain the assigned ``m_uid`` and name
                  for the pool, as well as the (serialized)
                  dragonMemoryPoolSerial_t library descriptor.


        FAIL (= 1)
            Something went wrong in pool creation. This can be an error
            on the node level (found when the call to allocate the resources
            is actually made) or a logical error with the request.

            **err_info**
                - string
                - explanation of what went wrong

            **err_code**
                - integer
                - The error code from the library call, if applicable.  Not
                  all errors will be revealed when the call is made; if the error
                  isn't due to the underlying API call


        ALREADY (= 2)
            The user name for the pool was already in use; the current
            descriptor is returned.

            **desc**
                - map
                - Initializer for a globalservices.pool.PoolDescriptor object.  This
                  object will contain the assigned ``m_uid`` and name
                  for the pool as well as the (serialized)
                  dragonMemoryPoolSerial_t library descriptor.

    *request*
        GSPoolCreate

    *see also*
        SHPoolCreate, SHPoolCreateResponse

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSPoolCreateResponse>`

.. _gspooldestroy:

73. **GSPoolDestroy**

    *type enum*
        GS_POOL_DESTROY  (= 73)

    *purpose*
        Request destruction of a managed memory pool.

    *fields*
        One of the ``m_uid`` and ``user_name`` fields must be
        present.

        If both are specified, then the ``user_name`` field is ignored.

        **m_uid**
            - integer
            - target memory pool ID

        **user_name**
            - string
            - user supplied name for the target pool


    *response*
        GSPoolDestroyResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSPoolDestroy>`

.. _gspooldestroyresponse:

74. **GSPoolDestroyResponse**

    *type enum*
        GS_POOL_DESTROY_RESPONSE  (= 74)

    *purpose*
        Response to GSPoolDestroy message

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Pool has been found and has been removed.

            No other fields.

        UNKNOWN (= 1)
            No such pool is known.

            **err_info**
                - string
                - explanation of what went wrong

        FAIL (= 2)
            Something went wrong in removing the pool.  This could be for a number
            of reasons.

            **err_info**
                - string
                - explanation of what went wrong

            **err_code**
                - integer
                - The error code from the library call, if applicable.  Not
                  all errors will be revealed when the call is made; if the error
                  isn't due to the underlying API call


        GONE (= 3)
            The pool is already destroyed

            No other fields.

        PENDING (= 4)
            Pool creation is pending, and it can't be destroyed.  Wait until
            the GSPoolCreateResponse message has been received.

    *request*
        GSPoolDestroy

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSPoolDestroyResponse>`

.. _gspoollist:

75. **GSPoolList**

    *type enum*
        GS_POOL_LIST  (= 75)

    *purpose*
        Return a list of tuples of ``m_uid`` for all pools currently alive.

    *fields*
        None additional

    *response*
        GSPoolListResponse

    *see also*
        GSPoolQuery

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSPoolList>`


.. _gspoollistresponse:

76. **GSPoolListResponse**

    *type enum*
        GS_POOL_LIST_RESPONSE  (= 76)

    *purpose*
        Responds with a list of ``m_uid`` for all the
        pools currently alive

    *fields*
        **mlist**
            - list of nonnegative integers

    *request*
        GSPoolList

    *see also*
        GSPoolQuery

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSPoolListResponse>`


.. _gspoolquery:

77. **GSPoolQuery**

    *type enum*
        GS_POOL_QUERY  (= 77)

    *purpose*
        Request the PoolDescriptor for a managed memory pool.

    *fields*
        One of the ``m_uid`` and ``user_name`` fields must be
        present.

        If both are specified, then the ``user_name`` field is ignored.

        **m_uid**
            - integer
            - target pool UID

        **user_name**
            - string
            - user supplied name for the target pool

    *response*
        GSPoolQueryResponse

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSPoolQuery>`

.. _gspoolqueryresponse:

78. **GSPoolQueryResponse**

    *type enum*
        GS_POOL_QUERY_RESPONSE  (= 78)

    *purpose*
        Response to request for PoolDescriptor for a managed memory pool.   This object
        carries a serialized library pool descriptor and which node the pool is found on.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Pool has been found

            **desc**
                - map
                - initializer for PoolDescriptor

        UNKNOWN (= 1)
            No such pool is known.

            **err_info**
                - string
                - explanation of what went wrong

    *request*
        GSPoolQuery

    *see also*
        GSPoolList

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSPoolQueryResponse>`


Channel Messages
----------------

These messages are related to the lifecycle of channels.  All requests
related to channels will include a ``p_uid`` field, of the requesting
process and the ``r_c_uid`` field for the channel to send the response to.

Channels are identified in the runtime system with a nonnegative
globally unique ID number, the 'channel UID' normally abbreviated
as ``c_uid``.  Note that every channel is associated with a memory pool
which itself has its own ``m_uid``.

.. _gschannelcreate:

11. **GSChannelCreate**

    *type enum*
        GS_CHANNEL_CREATE (= 11)

    *purpose*
        Requests that a new channel be created.

    *fields*
        **user_name**
            - string
            - optional user specified name for the channel.  If absent
              or empty, Global Services will select a name.

        **options**
            - map
            - Initializer for a (global) ChannelOptions object.  This is an
              open type intended to completely describe the
              functionality of the channel.  It contains fields pertaining
              to the global function and local characteristics of the channel.

        **m_uid**
            - integer
            - m_uid of the pool to create the channel in
            - if 0, use the default pool on the target node.

    *response*
        GSChannelCreateResponse

    *see also*
        GSChannelQuery

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelCreate>`
                         :class:`Python<dragon.globalservices.channel_desc.ChannelOptions>`

.. _gschannelcreateresponse:

12. **GSChannelCreateResponse**

    *type enum*
        GS_CHANNEL_CREATE_RESPONSE (= 12)

    *purpose*
        Response to channel creation request.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            **desc**
                - map
                - Initializer for a ChannelDescriptor object.  This
                  object will contain the assigned ``c_uid`` and name
                  for the channel and the serialized channel descriptor
                  as well as the ``m_uid`` for the pool associated with
                  the channel.

        FAIL (= 1)
            Something went wrong in channel creation.

            **err_info**
                - string
                - explanation of what went wrong


        ALREADY (= 2)
            The user name was already in use

            **desc**
                - map
                - Initializer for a ChannelDescriptor object.  This
                  object will contain the assigned ``c_uid`` and name
                  for the channel as well as the serialized channel descriptor.

    *request*
        GSChannelCreate

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelCreateResponse>`

.. _gschannellist:

13. **GSChannelList**

    *type enum*
        GS_CHANNEL_LIST (= 13)

    *purpose*
        Request list of currently active channels.

    *fields*
        None other than mandatory fields.  To-do: add something
        to make this more selective.

    *response*
        GSChannelListResponse

    *see also*
        GSChannelQuery

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelList>`

.. _gschannellistresponse:

14. **GSChannelListResponse**

    *type enum*
        GS_CHANNEL_LIST_RESPONSE (= 14)

    *purpose*
        Response to request to list of currently active channels
        with a list of tuples of (c_uid, name) for all the
        channels currently being managed

    *fields*
        **clist**
            - list of integers
            - The list contains the currently active c_uids

    *request*
        GSChannelList

    *see also*
        GSChannelQuery

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelListResponse>`


.. _gschannelquery:

15. **GSChannelQuery**

    *type enum*
        GS_CHANNEL_QUERY (= 15)

    *purpose*
        Request the descriptor for an already created channel by
        channel UID or user name.

    *fields*
        One of the ``c_uid`` and ``user_name`` fields must be
        present.

        If both are specified, then the ``user_name`` field is ignored.

        **c_uid**
            - integer
            - target channel UID

        **user_name**
            - string
            - user supplied name for the target channel

        **inc_refcnt**
            - bool
            - whether to increment refcnt on the Channel.  Default False.

    *response*
        GSChannelQueryResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelQuery>`

.. _gschannelqueryresponse:

16. **GSChannelQueryResponse**

    *type enum*
        GS_CHANNEL_QUERY_RESPONSE (= 16)

    *purpose*
        Response to request for a channel descriptor

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            **desc**
                - map
                - Initializer for a ChannelDescriptor object.  This
                  object will contain the assigned ``c_uid`` and name
                  for the channel as well as the ``m_uid`` of the pool
                  associated with the channel.  It also includes the
                  serialized library level descriptor.

        UNKNOWN (= 1)
            No such channel is active.

    *request*
        GSChannelQuery

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelQueryResponse>`


.. _gschanneldestroy:

17. **GSChannelDestroy**

    *type enum*
        GS_CHANNEL_DESTROY (= 17)

    *purpose*
        Request that a channel be destroyed or a refcount on the channel be released.


    *fields*
        One of the ``c_uid`` and ``user_name`` fields must be
        present.

        If both are specified, then the ``user_name`` field is ignored.

        **c_uid**
            - integer
            - target channel UID

        **user_name**
            - string
            - user supplied name for the target channel

        **reply_req**
            - bool
            - default True, reply to this message requested

        **dec_ref**
            - bool
            - default False, release refcount on this channel

    *response*
        GSChannelDestroyResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelDestroy>`


.. _gschanneldestroyresponse:

18. **GSChannelDestroyResponse**

    *type enum*
        GS_CHANNEL_DESTROY_RESPONSE (= 18)

    *purpose*
        Response to request to destroy a channel.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            No fields.

        UNKNOWN (= 1)
            No such channel is known.

        FAIL (= 2)
            The channel exists but

            **err_info**
                - string
                - explanation of what went wrong

            **err_code**
                - integer
                - error code from the library call that failed.  If
                  the problem is not related to a library call, this
                  value is 0.

        BUSY (= 3)
            Some entity is currently using the channel in some way that
            prevents its destruction.

            **err_info**
                - string
                - explanation of what went wrong

    *request*
        GSChannelDestroy

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelDestroyResponse>`


.. _gschanneljoin:

19. **GSChannelJoin**

    Request to be notified when a channel with a given name is created.

        Note: when we refactor the messages, we may want to get rid of GSChannelJoin
        and its response and ride all this on top of GSChannelQuery by adding a timeout.

    *type enum*
        GS_CHANNEL_JOIN (= 19)

    *purpose*
        Sometimes two processes want to communicate through a channel but we
        want to create the channel lazily with either the send side or receive
        side doing it.  This lets the other side meet the creation by name
        in global services without polling.

    *fields*
        **identifier**
            - string
            - name of channel to join to

        **timeout**
            - integer, interpreted as microseconds
            - timeout value; any value < 0 interpreted as infinite timeout

    *response*
        GSChannelJoinResponse

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelJoin>`

.. _gschanneljoinresponse:

20. **GSChannelJoinResponse**


    *type enum*
        GS_CHANNEL_JOIN_RESPONSE (= 20)

    *purpose*
        Response to request to join a channel

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            **desc**
                - map
                - Initializer for a ChannelDescriptor object.  This
                  object will contain the assigned ``c_uid`` and name
                  for the channel as well as the ``m_uid`` of the pool
                  associated with the channel.  It also includes the
                  serialized library level descriptor.

        TIMEOUT (= 1)
            The specified timeout has expired.

            No fields

        DEAD (= 2)
            The channel used to exist, but it has been destroyed.

    *request*
        GSChannelJoin

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelJoinResponse>`

.. _gschanneldetach:

21. **GSChannelDetach**

    PLACEHOLDER

    This probably needs to mean giving back all the send and receive
    handles currently acquired.

    *type enum*
        GS_CHANNEL_DETACH (= 21)

    *purpose*
        Request local detachment from channel.

    *fields*
        **c_uid**
            - integer
            - id of channel to detach from

    *response*
        GSChannelDetachResponse

    *see also*
        GSChannelAttach

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelDetach>`

.. _gschanneldetachresponse:

22. **GSChannelDetachResponse**

    PLACEHOLDER

    *type enum*
        GS_CHANNEL_DETACH_RESPONSE (= 22)

    *purpose*
        Response to request to detach from a channel.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            No fields.

        UNKNOWN (= 1)
            No such process is known.

            No fields

        UNKNOWN_CHANNEL (= 2)
            No such channel is known.

            No fields

        NOT_ATTACHED (= 3)
            Requested to detach from something you weren't attached
            to to begin with.

    *request*
        GSChannelDetach

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelDetachResponse>`

.. _gschannelgetsendh:

23. **GSChannelGetSendh**

    PLACEHOLDER

    *type enum*
        GS_CHANNEL_GET_SENDH (= 23)

    *purpose*
        Request send handle for channel

    *fields*
        **c_uid**
            - integer
            - id of channel to get a send handle for

    *response*
        GSChannelGetSendhResponse

    *see also*
        GSChannelGetRecvh

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelGetSendh>`

.. _gschannelgetsendhresponse:

24. **GSChannelGetSendhResponse**

    PLACEHOLDER

    *type enum*
        GS_CHANNEL_GET_SENDH_RESPONSE (= 24)

    *purpose*
        Response to request to get a channel send handle

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            **sendh**
                - map
                - info needed to set up the send handle locally

        UNKNOWN (= 1)
            No such process is known.

            No fields

        UNKNOWN_CHANNEL (= 2)
            No such channel is known.

            No fields

        NOT_ATTACHED (= 3)
            Descriptor not attached, can't get send handle.

        CANT (= 4)
            Can't get a recv handle - too many already gotten or
            some other reason related to the type.

            **err_info**
                - string
                - explanation of why not


    *request*
        GSChannelGetSendh

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelGetSendhResponse>`


.. _gschannelgetrecvh:

25. **GSChannelGetRecvh**

    PLACEHOLDER

    *type enum*
        GS_CHANNEL_GET_RECVH (= 25)

    *purpose*
        Request recv handle for channel

    *fields*
        **c_uid**
            - integer
            - id of channel to get a recv handle for

    *response*
        GSChannelGetRecvhResponse

    *see also*
        GSChannelGetSendh

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelGetRecvh>`

.. _gschannelgetrecvhresponse:

26. **GSChannelGetRecvhResponse**

    PLACEHOLDER

    *type enum*
        GS_CHANNEL_GET_RECVH_RESPONSE (= 26)

    *purpose*
        Response to request to get a channel recv handle

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            **recvh**
                - map
                - info needed to set up the recv handle locally

        UNKNOWN (= 1)
            No such process is known.

            No fields

        UNKNOWN_CHANNEL (= 2)
            No such channel is known.

            No fields

        NOT_ATTACHED (= 3)
            Descriptor not attached, can't get recv handle.

        CANT (= 4)
            Can't get a recv handle - too many already gotten or
            some other reason related to the type.

            **err_info**
                - string
                - explanation of why not

    *request*
        GSChannelGetRecvh

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelGetRecvhResponse>`

Node Messages
----------------

These messages are related to hardware transactions with Global Services, e.g.
querying the number of cpus in the system.

.. _gsnodelist:

102. **GSNodeList**

    *type enum*
        GS_NODE_LIST (= 102)

    *purpose*
        Return a list of tuples of ``h_uid`` for all nodes currently registered.

    *fields*
        None additional

    *response*
        GSNodeListResponse

    *see also*
        GSNodeQuery

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSNodeList>`

.. _gsnodelistresponse:

103. **GSNodeListResponse**

    *type enum*
        GS_NODE_LIST_RESPONSE (= 103)

    *purpose*
        Responds with a list of ``h_uid`` for all the
        nodes currently registered.

    *fields*
        **hlist**
            - list of nonnegative integers

    *request*
        GSNodeList

    *see also*
        GSNodeQuery

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSNodeListResponse>`

.. _gsnodequery:

97. **GSNodeQuery**

    *type enum*
        GS_NODE_QUERY (= 97)

    *purpose*
        Ask Global Services for a node descriptor of the hardware Dragon
        is running on.

    *fields*

        **name**
            - str
            - Name of the node to query

        **h_uid**
            - int
            - Unique host identifier (h_uid) held by GS.
            - The host running the Launcher Frontend has h_uid 0
            - The host running Global Services has h_uid 1

    *response*
        GSNodeQueryResponse

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSNodeQuery>`

.. _gsnodequeryresponse:

98. **GSNodeQueryResponse**

    *type enum*
        GS_NODE_QUERY_RESPONSE (= 98)

    *purpose*
         Return the machine descriptor after a GSNodeQuery.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)

            The machine descriptor was successfully constructed

            **desc**
                - NodeDescriptor
                - Contains the status of the node Dragon is running on.

        UNKNOWN ( = 1)
            An unknown error has occured.

    *see also*
        GSMachineQuery

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSMachineQueryResponse>`

.. _gsnodequerytotalcpucount:

104. **GSNodeQueryTotalCPUCount**

    *type enum*
        GS_NODE_QUERY_TOTAL_CPU_COUNT (= 104)

    *purpose*
        Asks GS to return the total number of CPUS beloging to all of the registered nodes.

    *response*
        GSNodeQueryTotalCPUCountResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSNodeQueryTotalCPUCount>`

.. _gsnodequeryresponse:

105. **GSNodeQueryTotalCPUCountResponse**

    *type enum*
        GS_NODE_QUERY_TOTAL_CPU_COUNT_RESPONSE (= 105)

    *purpose*
         Return the total number of CPUS beloging to all of the registered nodes.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)

            The machine descriptor was successfully constructed

            **total_cpus**
                - total number of CPUS beloging to all of the registered nodes.

        UNKNOWN ( = 1)
            An unknown error has occured.

    *request*
        GSNodeQueryTotalCPUCount

    *see also*
        GSNodeQuery

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSNodeQueryTotalCPUCountResponse>`

<<<<<<< HEAD
=======
.. _gsgroupdestroy:

>>>>>>> internal/open-source
121. **GSGroupDestroy**

    *type enum*
        GS_GROUP_DESTROY (= 121)

    *purpose*
        Ask Global Services to destroy a group of resources. This means destroy all the member-resources, as well as the container/group.

    *fields*

        **g_uid**
            - integer
            - group UID

        **user_name**
            - string
            - user supplied name for the group

    *response*
        GSGroupDestroyResponse

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupDestroy>`

<<<<<<< HEAD
=======
.. _gsgroupdestroyresponse:

>>>>>>> internal/open-source
122. **GSGroupDestroyResponse**

    *type enum*
        GS_GROUP_DESTROY_RESPONSE (= 122)

    *purpose*
        Response to GSGroupDestroy message

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Group has been found and has been destroyed.

            **desc**
                - base64 encoded string
                - The group descriptor

        UNKNOWN (= 1)
            No such group is known.

            **err_info**
                - string
                - explanation of what went wrong

        DEAD (= 2)
            The group is already dead.

            No other fields.

        PENDING (= 3)
            The group is pending.

            **err_info**
                - string
                - informing message that the group is still pending

    *request*
        GSGroupDestroy

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupDestroyResponse>`

<<<<<<< HEAD
=======
.. _gsgroupaddto:

>>>>>>> internal/open-source
123. **GSGroupAddTo**

    *type enum*
        GS_GROUP_DESTROY (= 123)

    *purpose*
        Ask Global Services to add specific resources to an existing group of resources. The resources are already created.

    *fields*

        **g_uid**
            - integer
            - group UID

        **user_name**
            - string
            - user supplied name for the group

        **items**
            - list of strings or integers
            - list of user supplied names or UIDs for the resources

    *response*
        GSGroupAddToResponse

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupAddTo>`

<<<<<<< HEAD
=======
.. _gsgroupaddtoresponse:

>>>>>>> internal/open-source
124. **GSGroupAddToResponse**

    *type enum*
        GS_GROUP_ADD_TO_RESPONSE (= 124)

    *purpose*
        Response to GSGroupAddTo message

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Group has been found and the resources have been added to it.

            **desc**
                - base64 encoded string
                - The group descriptor

        UNKNOWN (= 1)
            No such group is known.

            **err_info**
                - string
                - explanation of what went wrong

        FAIL (= 2)
            The request has failed. This can happen when at least one of the resources to be added to the group is dead or not found.

            **err_info**
                - string
                - informing message with the UIDs of the resources that were not found or are dead

        DEAD (= 3)
            The group is already dead.

            No other fields.

        PENDING (= 4)
            The group is pending.

            **err_info**
                - string
                - informing message that the group is still pending

    *request*
        GSGroupAddTo

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupAddToResponse>`

<<<<<<< HEAD
=======
.. _gsgroupremovefrom:

>>>>>>> internal/open-source
125. **GSGroupRemoveFrom**

    *type enum*
        GS_GROUP_REMOVE_FROM (= 125)

    *purpose*
        Ask Global Services to remove specific resources from an existing group of resources.

    *fields*

        **g_uid**
            - integer
            - group UID

        **user_name**
            - string
            - user supplied name for the group

        **items**
            - list of strings or integers
            - list of user supplied names or UIDs for the resources

    *response*
        GSGroupRemoveFromResponse

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupRemoveFrom>`

<<<<<<< HEAD
=======
.. _gsgroupremovefromresponse:

>>>>>>> internal/open-source
126. **GSGroupRemoveFromResponse**

    *type enum*
        GS_GROUP_REMOVE_FROM_RESPONSE (= 126)

    *purpose*
        Response to GSGroupRemoveFrom message

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Group has been found and the resources have been removed from it.

            **desc**
                - base64 encoded string
                - The group descriptor

        UNKNOWN (= 1)
            No such group is known.

            **err_info**
                - string
                - explanation of what went wrong

        FAIL (= 2)
            The request has failed. This can happen when at least one of the resources to be removed from the group was not found.

            **err_info**
                - string
                - informing message with the UIDs of those resources that were not found

        DEAD (= 3)
            The group is already dead.

            No other fields.

        PENDING (= 4)
            The group is pending.

            **err_info**
                - string
                - informing message that the group is still pending

    *request*
        GSGroupRemoveFrom

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupRemoveFromResponse>`

<<<<<<< HEAD
=======
.. _gsgroupcreate:

>>>>>>> internal/open-source
127. **GSGroupCreate**

    *type enum*
        GS_GROUP_CREATE (= 127)

    *purpose*
        Ask Global Services to create a group of resources.

    *fields*

        **user_name**
            - string
            - user supplied name for the group

        **items**
            - list[tuple[int, dragon.infrastructure.messages.Message]]
            - list of tuples where each tuple contains a replication factor and the Dragon
                create message

        **policy**
            - infrastructure.policy.Policy or dict
            - policy object that controls the placement of the resources

    *response*
        GSGroupCreateResponse

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupCreate>`

<<<<<<< HEAD
=======
.. _gsgroupcreateresponse:

>>>>>>> internal/open-source
128. **GSGroupCreateResponse**

    *type enum*
        GS_GROUP_CREATE_RESPONSE (= 128)

    *purpose*
        Response to GSGroupCreate message

    *fields*

        **desc**
            - base64 encoded string
            - The group descriptor

    *request*
        GSGroupCreate

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupCreateResponse>`

<<<<<<< HEAD
=======
.. _gsgroupkill:

>>>>>>> internal/open-source
129. **GSGroupKill**

    *type enum*
        GS_GROUP_KILL (= 129)

    *purpose*
        Ask Global Services to send the processes belonging to a specified group a specified signal.

    *fields*

        **g_uid**
            - integer
            - group UID

        **user_name**
            - string
            - user supplied name for the group

        **sig**
            - int
            - signal to use to kill the process, default=signal.SIGKILL

    *response*
        GSGroupKillResponse

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupKill>`

<<<<<<< HEAD
=======
.. _gsgroupkillresponse:

>>>>>>> internal/open-source
130. **GSGroupKillResponse**

    *type enum*
        GS_GROUP_KILL_RESPONSE (= 130)

    *purpose*
        Response to GSGroupKill message

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Group has successfully sent the kill requests.

            **desc**
                - base64 encoded string
                - The group descriptor

        UNKNOWN (= 1)
            No such group is known.

            **err_info**
                - string
                - explanation of what went wrong

        DEAD (= 2)
            The group is already dead.

            No other fields.

        PENDING (= 3)
            The group is pending.

            **err_info**
                - string
                - informing message that the group is still pending

        ALREADY (= 4)
            All the processes of the group were already dead.

            **desc**
                - base64 encoded string
                - The group descriptor

    *request*
        GSGroupKill

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupKillResponse>`

<<<<<<< HEAD
=======
.. _gsgroupcreateaddto:

>>>>>>> internal/open-source
131. **GSGroupCreateAddTo**

    *type enum*
        GS_GROUP_CREATE_ADD_TO (= 131)

    *purpose*
        Ask Global Services to create and add resources to an existing group of resources.

    *fields*

        **g_uid**
            - integer
            - group UID

        **user_name**
            - string
            - user supplied name for the group

        **items**
            - list of strings or integers
            - list of user supplied names or UIDs for the resources

        **policy**
            - policy object that controls the placement of the resources

    *response*
        GSGroupCreateAddToResponse

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupCreateAddTo>`

<<<<<<< HEAD
=======
.. _gsgroupcreateaddtoresponse:

>>>>>>> internal/open-source
132. **GSGroupCreateAddToResponse**

    *type enum*
        GS_GROUP_CREATE_ADD_TO_RESPONSE (= 132)

    *purpose*
        Response to GSGroupCreateAddTo message

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Group has been found and the resources have been created and added to it.

            **desc**
                - base64 encoded string
                - The group descriptor

        UNKNOWN (= 1)
            No such group is known.

            **err_info**
                - string
                - explanation of what went wrong

        DEAD (= 2)
            The group is already dead.

            No other fields.

        PENDING (= 3)
            The group is pending.

            **err_info**
                - string
                - informing message that the group is still pending

    *request*
        GSGroupCreateAddTo

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupCreateAddToResponse>`

<<<<<<< HEAD
=======
.. _gsgroupdestroyremovefrom:

>>>>>>> internal/open-source
133. **GSGroupDestroyRemoveFrom**

    *type enum*
        GS_GROUP_DESTROY_REMOVE_FROM (= 133)

    *purpose*
        Ask Global Services to destroy and remove specific resources from an existing group of resources.

    *fields*

        **g_uid**
            - integer
            - group UID

        **user_name**
            - string
            - user supplied name for the group

        **items**
            - list of strings or integers
            - list of user supplied names or UIDs for the resources

    *response*
        GSGroupDestroyRemoveFromResponse

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupDestroyRemoveFrom>`

<<<<<<< HEAD
=======
.. _gsgroupdestroyremovefromresponse:

>>>>>>> internal/open-source
134. **GSGroupDestroyRemoveFromResponse**

    *type enum*
        GS_GROUP_REMOVE_FROM_RESPONSE (= 134)

    *purpose*
        Response to GSGroupDestroyRemoveFrom message

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Group has been found and the resources have been removed from it.

            **desc**
                - base64 encoded string
                - The group descriptor

        UNKNOWN (= 1)
            No such group is known.

            **err_info**
                - string
                - explanation of what went wrong

        FAIL (= 2)
            The request has failed. This can happen when at least one of the resources to be removed from the group was not found.

            **err_info**
                - string
                - informing message with the UIDs of those resources that were not found

        DEAD (= 3)
            The group is already dead.

            No other fields.

        PENDING (= 4)
            The group is pending.

            **err_info**
                - string
                - informing message that the group is still pending

    *request*
        GSGroupDestroyRemoveFrom

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupDestroyRemoveFromResponse>`

<<<<<<< HEAD
=======
.. _gsgrouplist:

117. **GSGroupList**

    *type enum*
        GS_GROUP_LIST (= 117)

    *purpose*
        Request a list of the g_uid for all the groups of resources
        currently being managed

    *fields*
        None additional

    *response*
        GSGroupListResponse

    *see also*
        GSGroupQuery

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupList>`

.. _gsgrouplistresponse:

118. **GSGroupListResponse**

    *type enum*
        GS_GROUP_LIST_RESPONSE (= 118)

    *purpose*
        Response to GSGroupList message

    *fields*
        **glist**
            - list of nonnegative integers for all the groups

    *request*
        GSGroupList

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupListResponse>`

.. _gsgroupquery:

119. **GSGroupQuery**

    *type enum*
        GS_GROUP_QUERY  (= 119)

    *purpose*
        Request the GroupDescriptor for a managed group of resources

    *fields*
        One of the ``g_uid`` and ``user_name`` fields must be
        present.

        If both are specified, then the ``user_name`` field is ignored.

        **g_uid**
            - integer
            - target group UID

        **user_name**
            - string
            - user supplied name for the target group

    *response*
        GSGroupQueryResponse

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupQuery>`

.. _gsgroupqueryresponse:

120. **GSGroupQueryResponse**

    *type enum*
        GS_GROUP_QUERY_RESPONSE  (= 120)

    *purpose*
        Response to request for GroupDescriptor for a managed group

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Group has been found

            **desc**
                - map
                - initializer for GroupDescriptor - includes at a minimum
                  g_uid of new group and assigned user name.

        UNKNOWN (= 1)
            No such group is known.

            **err_info**
                - string
                - explanation of what went wrong

    *request*
        GSGroupQuery

    *see also*
        GSGroupList

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSGroupQueryResponse>`

>>>>>>> internal/open-source
Other Messages
--------------

These messages are related to other transactions on Global Services,
for example ones related to sequencing runtime startup and teardown.

.. _abnormaltermination:

27. **AbnormalTermination**

    *type enum*
        ABNORMAL_TERMINATION (= 27)

    *purpose*
        Error result for startup and teardown messages, as well
        as for reporting problems that happen during execution that
        are not tied to any specific request.

        Morally speaking, this message backs an exception class
        related to things going wrong in Dragon Run-time Services.
        These are generally nonrecoverable errors, but
        it is desirable to propagate an explanation of what went wrong,
        back to the front end when possible - users won't want to
        collect disparate log files for basic bug reports.

    *fields*
        **err_info**
            - string
            - explanation of what went wrong.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.AbnormalTermination>`

.. _gsstarted:

28. **GSStarted**

    *type enum*
        GS_STARTED (= 28)

    *purpose*
        Confirm to Launcher that the Global Services process (and if
        applicable, subsidiary processes) are started.

    *fields*
        None

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSStarted>`


.. _gspingsh:

29. **GSPingSH**

    *type enum*
        GS_PING_SH (= 29)

    *purpose*
        Confirm to Shepherd(s) that Global Services has started and
        request handshake response.

    *fields*
        None

    *see also*
        SHPingGS

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSPingSH>`

.. _gsisup:

30. **GSIsUp**

    *type enum*
        GS_IS_UP (= 30)

    *purpose*
        Confirm to Launcher that Global Services is completely up

    *fields*
        None

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSIsUp>`

.. _gsheadexit:

31. **GSHeadExit**

    *type enum*
        GS_HEAD_EXIT (= 31)

    *purpose*
        Notify Launcher that the head process has exited. At this point
        the Launcher can start teardown or start a new head process.

    *fields*
        **exit_code**
            - integer
            - exit code the process left with

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSHeadExit>`

.. _gschannelrelease:

32. **GSChannelRelease**

    *type enum*
        GS_CHANNEL_RELEASE (= 32)

    *purpose*
        Tell the Shepherd(s) that Global Services is exiting and will
        no longer be sending anything through Channels

    *fields*
        None

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSChannelRelease>`

.. _gshalted:

33. **GSHalted**

    *type enum*
        GS_HALTED (= 33)

    *purpose*
        Notify Launcher that Global Services is halted.  The Global
        Services processes are expected to exit after sending this
        message, having done everything possible to clean up.

    *fields*
        None

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSHalted>`


.. _gsteardown:

55. **GSTeardown**

    *type enum*
        GS_TEARDOWN (= 55)

    *purpose*
        Direct Global Services to do a clean exit - clean up any remaining
        global structures and its own sub-workers, if any, and detach from
        channels.

        This should only be given when there isn't an active head process.
        If there is such a thing, we should probably take everything down anyway
        but complain about it.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSTeardown>`

.. _gspingproc:

65. **GSPingProc**

    *type enum*
        GS_PING_PROC (= 65)

    *purpose*
        When a new managed process wanting to use Global Services
        comes up, it must wait to receive this message on its Global Services
        return channel before sending any messages to Global Services.

        This message also carries 'argument' data, which is defined as binary
        data to be delivered to the target at startup. This can be used in whatever
        way the target would like.  For example, the implementation of the Python Process object
        uses this to deliver the Python target function and arguments.

            Remark.  Even if no arguments are needed, this message must be sent
            to solve a race with the local Shepherd's notification to Global Services
            that the process was successfully started.

    *fields*

        **mode**
            - integer
            - explains how the argdata field should be interpreted
            - Alternatives:
                - NONE (=0)  No arguments are carried
                - PYTHON_IMMEDIATE (=1) The arguments are found in fields of the argdata field.
                - PYTHON_CHANNEL (=2) The argdata has fields for a serialized channel to attach to and get the args from
                  otherwise challenge the available space in the pool the argument delivery channel is found in.


        **argdata**
            - base64 encoded string, fixme, with the binary argument data to be delivered, or nil.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSPingProc>`

.. _gsdump:

66. **GSDump**

    *type enum*
        GS_DUMP (= 66)

    *purpose*
        Primarily debugging.  Makes global services dump its state in
        human readable form to a file specified in the message.

            Remark. May in the future try for a formal serialization of GS state once
            more of the design is concrete.  May also have a reply to this message.

    *fields*
        **filename**
            - string
            - file to open and write the dump to

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSDump>`

.. _gsunexpected:

85. **GSUnexpected**

    *type enum*
        GS_UNEXPECTED (= 85)

    *purpose*
        Whenever GS gets a message that is not expected as an input to GS
        (which can depend on the state it is in) and a valid reply handle
        can be found corresponding to that message, this message gets returned as
        a response.

        If a valid reply handle can't be found, then GS will log an error
        but continue to run.

        This kind of thing can happen when issuing interactive commands to
        a global services process from the launcher. If the head process leaves
        unexpectedly there can be a race; this message allows handling this
        without putting GS into error.

        TODO: put this in GS documentation and xref, post GS doc update.

    *fields*
        **ref**
            - integer
            - matches the tag value of the unexpected message, if it has one.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.GSUnexpected>`


.. _exceptionlessabort:

27. **ExceptionlessAbort**

    *type enum*
        EXCEPTIONLESS_ABORT (= 115)

    *purpose*
        It can be benefical to pass a message through the services runtime that
        communicates an abnormal termination has been encountered without
        immediatesly raising an exception. This message can thus be used
        to coordinate tearing down of resources without immediately raising an
        exception.

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.ExceptionlessAbort>`





.. _ShepherdAPI:

Local Services Messages API
=============================

These messages are the ones underlying the :ref:`LocalServices` API and are sent only to and from the Shepherd.
Like all request messages, these Shepherd request messages have a ``p_uid`` field denoting the process that
sent the request message and ``r_c_uid`` field denoting the channel ID the response should be returned to.

LS Process Messages
--------------------

.. _shprocesscreate:

34. **SHProcessCreate**

    *type enum*
        SH_PROCESS_CREATE (= 34)

    *purpose*
        Request to Shepherd to launch a process locally.

    *fields*
        **t_p_uid**
            - integer
            - process UID of the target process - Global Services will
              use this to refer to the process in other operations.
        **exe**
            - string
            - executable name

        **args**
            - list of strings
            - arguments to the executable.  [exe, args] == argv

        **env**
            - map, key=string value=string
            - Environment variables to be exported before running.

              Note that this is additive/override to the shepherd
              environment - may need to be more fancy here if it
              turns out we need to add to paths

        **rundir**
            - string, absolute path
            - Indicates where the process is to be run from. If empty,
              does not matter.

        **options**

            **output_c_uid** is the channel id where output, both *stdout*
              and *stderr*, should be forwarded if present in an
              *SHFwdOutput* message. If this field is not present in the
              options, then output will be forwarded to the *Backend*. The
              *SHFwdOutput* message indicates whether it was from *stdout*
              or *stderr*.

    *response*
        SHProcessCreateResponse

    *see also*
        SHProcessKill

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHProcessCreate>`

.. _shprocesscreateresponse:

35. **SHProcessCreateResponse**

    *type enum*
        SH_PROCESS_CREATE_RESPONSE (= 35)

    *purpose*
        Response to process creation request.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            no fields

        FAIL (= 1)
            **err_info**
                - string
                - what went wrong

    *request*
        SHProcessCreate

    *see also*
        SHProcessKill

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHProcessCreateResponse>`

.. _shprocesskill:

36. **SHProcessKill**

    *type enum*
        SH_PROCESS_KILL (= 36)

    *purpose*
        Request to kill a process owned by this shepherd, with
        a specified signal.

    *fields*
        **t_p_uid**
            - integer
            - process UID of the target process

        **sig**
            - integer, valid signal number
            - signal to send to the process

    *response*
        SHProcessExit
        Note difference in nomenclature.

    *see also*
        SHProcessCreate

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHProcessKill>`


.. _shprocesskillresponse:

93. **SHProcessKillResponse**

    *type enum*
        SH_PROCESS_KILL_RESPONSE (= 93)

    *purpose*
        Response to request to kill a process owned by this shepherd.
        Returns confirmation that the signal was delivered only.  The
        process's exit comes automatically via SHProcessExit.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            no fields

        FAIL (= 1)
            **err_info**
                - string
                - what went wrong

    *response*
        SHProcessKill

    *see also*
        SHProcessCreate
        SHProcessExit

        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHProcessKillResponse>`


.. _shprocessexit:

37. **SHProcessExit**

    *type enum*
        SH_PROCESS_EXIT (= 37)

    *purpose*
        This message is sent to Global Services when a managed process exits.
        There are no ``ref`` or ``err`` fields.

    *fields
        **exit_code**
            - int
            - numerical exit code.

        **p_uid**
            - int
            - the :ref:`p_uid` of the process that exited. When a process exits of its own accord, this is the
            only thing to identify which one it is.

    *see also*
        SHProcessKill

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHProcessExit>`


Pool Messages
-------------

These messages are directives to a :ref:`LocalServices` process to execute pool lifecycle commands.  They are posed
in the form of individual messages instead of generic operation completions because the Shepherd holds these
OS resources.

.. _shpoolcreate:

79. **SHPoolCreate**

    *type enum*
        SH_POOL_CREATE (= 79)

    *purpose*
        Create a new memory pool.

    *fields*
        **m_uid**
            - positive integer
            - system wide unique identifier for this pool.  All future
              operations on this pool will refer to it using the ``m_uid``.

        **name**
            - string
            - name of the pool to use when creating it.
              This name is guaranteed to be globally unique.

        **size**
            - positive integer
            - size in bytes of the pool.

        **attr**
            - string
            - this string is the base64 encoding of a serialized pool
              attribute object to use when creating the pool

    *response*
        SHPoolCreateResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHPoolCreate>`


.. _shpoolcreateresponse:

80. **SHPoolCreateResponse**

    *type enum*
        SH_POOL_CREATE_RESPONSE (= 80)

    *purpose*
        Response to request to create a new memory pool.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            The pool was created successfully.

            **desc**
                - string
                - the string is the base64 encoding of a serialized pool
                  descriptor for the pool that just got created

        FAIL (= 1)
            The pool was not created successfully.  This could be
            because the pool creation call failed or because there
            was something wrong with the pool creation message.

            **err_info**
                - string
                - what went wrong


    *request*
        SHPoolCreate

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHPoolCreateResponse>`


.. _shpooldestroy:

81.  **SHPoolDestroy**

    *type enum*
        SH_POOL_DESTROY (= 81)

    *purpose*
        Request to destroy a memory pool.

    *fields*
        **m_uid**
            - positive integer
            - ID of the pool to destroy

    *response*
        SHPoolDestroyResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHPoolCreateResponse>`

.. _shpooldestroyresponse:

82.  **SHPoolDestroyResponse**

    *type enum*
        SH_POOL_DESTROY_RESPONSE (= 82)

    *purpose*
        Response to request to destroy a memory pool

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            The pool was destroyed.  No other fields


        FAIL (= 1)
            The pool was not destroyed successfully.  This could be
            because the pool destroy call failed or because there
            was something wrong with the pool destroy message.

            **err_info**
                - string
                - what went wrong


    *request*
        SHPoolDestroy

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHPoolCreateResponse>`

.. _shexecmemrequest:

83.  **SHExecMemRequest**

    *type enum*
        SH_EXEC_MEM_REQUEST (= 83)

    *purpose*
        Request to execute a memory request.   This message contains a serialized
        operation given to the Shepherd to execute on behalf of the requesting
        process using ``dragon_memory_exec_req`` or ``dragon_memory_pool_exec_request``

    *fields*
        **kind**
            - integer, legal values are 0 or 1
            - If 0, the request is a serialized ``dragonMemoryPoolRequest``
            - If 1, the request is a serialized ``dragonMemoryRequest``

        **request**
            - string
            - this is the base64 encoding of the request.

    *response*
        SHExecMemResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHPoolCreateResponse>`

.. _shexecmemresponse:

84.  **SHExecMemResponse**

    *type enum*
        SH_EXEC_MEM_RESPONSE (= 84)

    *purpose*
        Response to request to execute a memory request.  Note that there is no
        need for this message to include what kind of request it was - the sender
        knows this.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            The memory request call completed.  Even if the call did not
            complete with a success return value, the response has
            the details about what went wrong including the error
            code and this will be apparent when the original requester
            calls the completion function on the response.

            **response**
                - string
                - this is the base64 encoding of the serialized response

        FAIL (= 1)
            The request could not be completed.

            **err_info**
                - string
                - what went wrong

            **err_code**
                - integer
                - if the error is due to a library call, this should
                  carry the return value of the call.

                  Otherwise its value should be 0.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHPoolCreateResponse>`

Channel Messages
----------------

.. _shchannelcreate:

38. **SHChannelCreate**

    *type enum*
        SH_CHANNEL_CREATE (= 38)

    *purpose*
        Request to create a channel in a memory pool known to this shepherd.

    *fields*
        **m_uid**
            - integer
            - Unique id of the pool to create the channel inside.

        **c_uid**
            - integer
            - Unique id of the channel to be created.  Future operations will
              use this number to refer to the channel

        **options**
            - map
            - Initializer for a (local) ChannelOptions object.  This is an
              open type intended to completely describe the controllable parameters for
              channel creation at the library level.  Will contain attributes
              string from library.

    *response*
        SHChannelCreateResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHChannelCreate>`
                            :class:`Python<dragon.shepherd.options.ChannelOptions>`

.. _shchannelcreateresponse:

39. **SHChannelCreateResponse**

    *type enum*
        SH_CHANNEL_CREATE_RESPONSE (= 39)

    *purpose*
        Response to channel allocation request.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            **desc**
                - string
                - serialized and ascii encoded library channel descriptor

        FAIL (= 1)
            **err_info**
                - string
                - what went wrong


    *request*
        SHChannelCreate

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHChannelCreateResponse>`

.. _shchanneldestroy:

40. **SHChannelDestroy**

    *type enum*
        SH_CHANNEL_DESTROY (= 40)

    *purpose*
        Request to free a previously allocated channel.

    *fields*
        **c_uid**
            - integer
            - unique id of the channel to be destroyed.

    *response*
        SHChannelDestroyResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHChannelDestroy>`

.. _shchanneldestroyresponse:

41. **SHChannelDestroyResponse**

    *type enum*
        SH_CHANNEL_DESTROY_RESPONSE (= 41)

    *purpose*
        Response to request to free a previously allocated channel.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            No additional fields

        FAIL (= 1)
            **err_info**
                - string
                - what went wrong


    *request*
        SHChannelDestroy

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHChannelDestroyResponse>`

.. _shlockchannel:

42. **SHLockChannel**

    PLACEHOLDER

    *type enum*
        SH_LOCK_CHANNEL (= 42)

    *purpose*
        Request to lock a channel

        TODO: should only something attached to a channel be allowed
        to lock it?

    *fields*
        **read_lock**
            - bool
            - locking this for reading

        **write_lock**
            - bool
            - locking this for writing

    *response*
        SHLockChannelResponse

      *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHLockChannel>`

.. _shlockchannelresponse:

43. **SHLockChannelResponse**

    PLACEHOLDER

    *type enum*
        SH_LOCK_CHANNEL_RESPONSE (= 43)

    *purpose*
        Response to request to lock a channel

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Successfully locked.

        ALREADY (= 1)
            Some other entity has already locked the channel.

    *request*
        SHLockChannel

    *see also*
        x

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHLockChannelResponse>`

Memory Allocation Messages
--------------------------

.. _shallocmsg:

44. **SHAllocMsg**

    PLACEHOLDER - maybe OBE

    *type enum*
        SH_ALLOC_MSG (= 44)

    *purpose*
        Request a shared memory allocation for a large message

    *fields*
        x

    *response*
        SHAllocMsgResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHAllocMsg>`

.. _shallocmsgresponse:

45. **SHAllocMsgResponse**

    PLACEHOLDER - OBE?

    *type enum*
        SH_ALLOC_MSG_RESPONSE (= 45)

    *purpose*
        Response to a requested allocation for a large message

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)

    *request*
        SHAllocMsg

    *see also*
        x

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHAllocMsgResponse>`


.. _shallocblock:

46. **SHAllocBlock**

    PLACEHOLDER - OBE?


    *type enum*
        SH_ALLOC_BLOCK (= 46)

    *purpose*
        Request a shared memory allocation for generic memory

    *fields*
        **p_uid**
            - integer
            - Process UID of the requesting process.

            Note that the
            Shepherd may need to know this to do cleanup if the process
            goes away.  There probably needs to be an option as to
            which is the owning process.

        **len**
            - integer
            - length of allocation desired

    *response*
        SHAllocBlockResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHAllocBlock>`


.. _shallocblockresponse:

47. **SHAllocBlockResponse**

    PLACEHOLDER - OBE?

    *type enum*
        SH_ALLOC_BLOCK_RESPONSE (= 47)

    *purpose*
        Response to a requested allocation for generic memory

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)
            Successfully allocated the block.  Do we want a reference
            ID for the block to let other processes claim ownership?
            E.g. we could have the shepherd count ref on such blocks.

            **seg_name**
                - string
                - name of shared memory segment the block is found in.

            **offset**
                - nonnegative integer
                - byte offset of the allocated block within segment



        UNKNOWN (= 1)
            The p_uid of the requester is not a process currently
            running and being managed by the shepherd

        FAIL (= 2)
            The block could not be allocated.

            **err_info**
                - string
                - what went wrong


    *request*
        SHAllocBlock

    *see also*
        x

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHAllocBlockResponse>`



.. _sh-other-messages:

Other Messages
--------------

These :ref:`LocalServices` messages are for other purposes, such as sequencing
bringup and teardown.  Where appropriate they have the ``idx`` field, indicating
which Shepherd instance there is.  This is an integer inside [0,N),
where N is the number of compute nodes in the allocation.  It has the
value 0 in the single node case.

.. _shchannelsup:

49. **SHChannelsUp**

    *type enum*
        SH_CHANNELS_UP (= 49)

    *purpose*
        Notify Launcher that this Shepherd has allocated the shared
        memory for the channels and initialized whatever default
        channels there are.

    *fields*
        **ip_addr**
            - string
            - the ip address of the node sending this message.

        **host_name**
            - string
            - the hostname of the node sending this message.

        **host_id**
            - integer
            - the hostid of the node sending this message.

        **idx**
            - integer
            - which Shepherd in the allocation this is. Index is 0 in the single node
              case and lives in [0,N) for N multi node back end allocation

        **shep_cd**
            - base64 encoded string
            - The channel descriptor of the local shepherd

        **gs_cd**
            - base64 encoded string
            - When idx is the primary node, then this is the channel descriptor
              of global services. Otherwise it is ignored and should be set to an
              empty string.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHChannelsUp>`


.. _shpinggs:

50. **SHPingGS**

    *type enum*
        SH_PING_GS (= 50)

    *purpose*
        Acknowledge to Global Services that this Shepherd is up and
        ready.

    *fields*
        **idx**
            - integer
            - which Shepherd in the allocation this is.  Index is 0 in the single node
              case and lives in [0,N) for N multi node back end allocation.

        **node_sdesc**
            - dict
            - serialized :func:`NodeDescriptor<dragon.infrastructure.node_desc.NodeDescriptor>`
            of the node the Shepherd is running on.
            - Note that the h_uid in the descriptor will be None in this message. It is set later by GS.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHPingGS>`


.. _shhalted:

51. **SHHalted**

    *type enum*
        SH_HALTED (= 51)

    *purpose*
        Notify launcher that this Shepherd is halted.

    *fields*
        **idx**
            - integer
            - which Shepherd in the allocation this is.  Index is 0 in the single node
              case and lives in [0,N) for N multi node back end allocation

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHHalted>`


.. _shteardown:

56. **SHTeardown**

    *type enum*
        SH_TEARDOWN (= 56)

    *purpose*
        Direct Shepherd to do a clean teardown.

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHTeardown>`


.. _shpingbe:

57. **SHPingBE**

    *type enum*
        SH_PING_BE (= 57)

    *purpose*
        Shepherd handshake with MRNet backend

    *fields*
        **shep_cd**

            - Channel Descriptor for the Shepherd (serialized, base64 encoded)

        **be_cd**

            - The Launcher Back End Channel Descriptor (serialized, base64 encoded)

        **gs_cd**

            - The Global Services Channel Descriptor (serialized, base64 encoded)

        **default_pd**

            - The Default Pool Descriptor (serialized, base64 encoded)

        **inf_pd**

            - The Infrastructure Pool Descriptor (serialized, base64 encoded)

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHPingBE>`


.. _shhaltbe:

62. **SHHaltBE**

    *type enum*
        SH_HALT_BE (= 62)

    *purpose*
        Shepherd telling the MRNet backend to exit.

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHHaltBE>`

.. _shfwdinput:

52. **SHFwdInput**

    *type enum*
        SH_FWD_INPUT (= 52)

    *purpose*
        Message carrying data intended to be written into the
        standard input of a process managed by this shepherd.

        If the process is present and the stdin can be written without
        any problems, there is no response message.

    *fields*
        **t_p_uid**
            - integer
            - target p_uid

        **input**
            - string
            - input to be sent to target process
            - The max length of any input to be sent through this mechanism in one message is 1024 bytes.

        **confirm**
            - boolean
            - request confirmation of forwarded input.

    *response*
        SHFwdInputErr

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHFwdInput>`


.. _shfwdinputerr:

53. **SHFwdInputErr**

    *type enum*
        SH_FWD_INPUT (= 53)

    *purpose*
        Error response to a forward input message. This message
        is returned to the sender if (for instance) the target process
        isn't present at the time of delivery.

    *fields*

        All messages:

        **idx**
            - integer
            - which Shepherd in the allocation this is.  Index is 0 in the single node
              case and lives in [0,N) for N multi node back end allocation


        Alternatives on ``err``:

        SUCCESS (= 0)
            No fields other than defaults.

            No error.  This message isn't expected ever to be emitted,
            but is called out here to reserve the '0' code for future
            use in case there needs to be positive confirmation
            of message delivery.

        FAIL (= 1)

            **err_info**
                - string
                - explanation of what went wrong.

    *request*
        SHFwdInput

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHFwdInputErr>`


.. _shfwdoutput:

54. **SHFwdOutput**

    *type enum*
        SH_FWD_OUTPUT (= 54)

    *purpose*
        Message carrying data from either stdout or stderr of a process
        managed by this shepherd.  This comes from the Shepherd and
        no response is expected.

    *fields*

        **fdnum**

            Indicates which stream, stdout or stderr, is being forwarded.

            Alternatives for ``fdnum``:

            stdout (= 1)
            stderr (= 2)

        **data**
            - string
            - The data read from the stream and being forwarded
            - The max length of any output per message is 1024 bytes.

        **idx**
            - integer
            - which Shepherd in the allocation this is.  Index is 0 in the single node
              case and lives in [0,N) for N multi node back end allocation

        **p_uid**
            - integer
            - The process UID this came from

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHFwdOutput>`


.. _shhaltta:

60. **SHHaltTA**

    *type enum*
        SH_HALT_TA (= 60)

    *purpose*
        Message coming from Launcher to the Shepherd, telling it to tell TA to halt.
        The Shepherd forwards this same message to TA.

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHHaltTA>`

.. _shdumpstate:

67. **SHDumpState**

    *type enum*
        SH_DUMP (= 67)

    *purpose*
        Primarily debugging.  Makes the Shepherd dump its state in
        human readable form to a file specified in the message or dump
        to the log if no filename is provided.

    *fields*
        **filename**
            - string
            - file to open and write the dump to. Defaults to None. If
              None, then the dump goes to the log file for the Shepherd.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.SHDumpState>`


.. _LauncherAPI:

Launcher Messages API
=====================

These messages go to the :ref:`Launcher` frontend in standard and server mode via the *LA Channel*.

.. _labroadcast:

68. **LABroadcast**

    *type enum*

        LA_BROADCAST (= 68)

    *purpose*

        This message is sent by the launcher to the Launch Router. This message contains another message in
        the **data** field. The message in the data field is to be broadcast to the *Shepherd* on all nodes
        through the *Backend*. The *LABroadcast* message is sent to the backend. The *Backend* extracts the
        embedded message from the *LABroadcast* message and sets the embedded message's *r_c_uid* as the
        return channel id in the message and forwards the embedded message to the *Shepherd*'s channel. All
        broadcast messages are sent to the *Shepherd* on each node.

        *fields*

        **data**

            - A string to be broadcast to all nodes. For the Dragon
              infrastructure this data field will itself be a valid, serialized
              message.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.LABroadcast>`,
    :ref:`LauncherNetworkFrontend` has a dependency on the type enum being 68.



.. _lapassthrufb:

69. **LAPassThruFB**

    *type enum*

        LA_PASSTHRU_FB (= 69)

    *purpose*

        This message is a pass thru message from the *Launcher* to the *Backend*.

        When sent to the *Launcher Router*, it forwards the message to the
        *Backend* on the primary node.

        This message type is used by the *Dragon Kernel Server* to communicate
        its channel id to the *Dragon Kernel* indicating that the *Dragon
        Kernel* is up and ready to accept requests.

    *fields*

        **c_uid**

            - The channel to send this message to running on a compute node.

        **data**

            - A user-defined serialized message that is unwrapped by the receiving end.

        **NOTE**

            - The *r_c_uid* is set in this message by the *Backend* to the
              *Backend* channel id before it is forwarded on to its destination
              on the compute node.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.LAPassThruFB>`



.. _lapassthrubf:

70. **LAPassThruBF**

    *type enum*

        LA_PASSTHRU_BF (= 70)

    *purpose*

        This message is a pass thru message from the *Backend* to the *Launcher*.

        Note: One use of this message type is by a server back end to
        communicate its channel id to a server front end in the *r_c_uid*
        field. This serves as an indicator in this case that a front end that its
        corresponding back end is ready to accept requests (see :ref:`LauncherServerMode`).

    *fields*

        **data**

            - A user-defined serialized message that is unwrapped by the
            - receiving end.

        **NOTE**

            - The *r_c_uid* is set to the channel id of the source of the
            - passthrough message on a compute node.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.LAPassThruBF>`


.. _laservermode:

71. **LAServerMode**

    *type enum*

        LA_SERVER_MODE (= 86)

    *purpose*

        This is a message that is used internally in the Launcher to set
        up Server Mode. It is not ever to be sent between components in
        the Dragon run-time.

    *fields*

        **frontend**

            - A string with a file specification for the server front end to
              be run on the login node.

        **backend**

            - A string with a file specification for the server back end to
              be run on the primary compute node.

        **frontendargs**

            - A list of strings containing any command-line arguments to the
              front end.

        **backendargs**

            - A list of strings containing any command-line arguments to the
              back end.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.LAServerMode>`


.. _laservermodeexit:

72. **LAServerModeExit**

    *type enum*

    LA_SERVER_MODE_EXIT (= 87)

    *purpose*

        This is a message that is used internally in the Launcher to exit
        Server Mode. It is not ever to be sent between components in
        the Dragon run-time.

    *fields*

        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.LAServerModeExit>`


.. _laprocessdict:

74. **LAProcessDict**

    *type enum*
        LA_PROCESS_DICT  (= 88)

    *purpose*
        Return a dictionary of process information  for all the processes
        that were started in the launcher via the run command.

    *fields*
        None additional

    *response*
        LAProcessDictResponse

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.LAProcessDict>`

.. _laprocessdictresponse:

75. **LAProcessDictResponse**

    *type enum*
        LA_PROCESS_DICT_RESPONSE  (= 89)

    *purpose*
        Responds with a dictionary for all the processes
        were started in the launcher via the run command. The dictionary
        includes fields for exe, args, env, run directory, user name, options,
        create return code, and exit code.

    *fields*
        **pdict**
            - dictionary of p_uid to process information as described.

    *request*
        LAProcessList

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.LAProcessDictResponse>`


.. _ladumpstate:

76. **LADumpState**

    *type enum*
        LA_DUMP (= 90)

    *purpose*
        Primarily debugging.  Makes the Launcher dump its state in
        human readable form to a file specified in the message or dump
        to the log if no filename is provided.

    *fields*
        **filename**
            - string
            - file to open and write the dump to. Defaults to None. If
              None, then the dump goes to the log file for the Launcher.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.LADumpState>`

.. _lanodeidxsh:

77. **LANodeIdxSH**

    *type enum*
        LA_NODEINDEX_SH (= 91)

    *purpose*
        Communicates the node index from the Launcher Back End to the
        Shepherd during bootstrapping of the environment.

    *fields*
        **node_idx**
            - integer
            - The index of this node in the allocation upon which the
              Dragon runtime services are running.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.LANodeIdxSH>`


.. _lachannelsinfo:

78. **LAChannelsInfo**

    *type_enum*
        LA_CHANNELS_INFO (=92)

    *purpose*
        Broadcast to all nodes to provide hostnames, node indices,
        and channel descriptors to all nodes in the allocation so
        services like global services and the inter-node transport
        service (HSTA, TCP or otherwise) can be started.

        When the launcher backend receives this message, it forwards
        it to the local shepherd. The local shepherd provides this
        message to the inter-node transport service when it is
        started and to global services (on the primary node) when it
        is started.

    *fields*
        **nodes_desc**
            - dictionary with keys corresponding to string node indices and values of class
            ``dragon.launcher.node_desc._NodeDescriptor``(eg: The hostname info for node 5 is accessed via
            ``la_channels_msg.nodes_desc['4'].host_name`` )
            - Attributed for each value of ``dragon.launcher.node_desc._NodeDescriptor``:

              - 'host_name': hostname string
              - 'host_id': integer which is determined by calling the Posix gethostid function
              - 'ip_addr': ip address string
              - 'shep_cd': base64 encoded shepherd channel descriptor

        **gs_cd**
            - string
            - The base64 encoded channel descriptor of global services

        **num_gw_channels**
            - int
            - The number of gateway channels per node

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.LAChannelsInfo>`
                         :class:`Python<dragon.launcher.node_desc._NodeDescriptor>`

94. **Breakpoint**

     *type_enum*
         BREAKPOINT (=94)

     *purpose*
         Inform front end that a managed process has reached a breakpoint for the first time
         and wants to establish a debug connection.

     *fields*
         **p_uid**
             - int
             - process uid that has just reached a breakpoint

         **index**
             - int
             - node that process is on

         **out_desc**
             - string
             - encoded binary serialized channel descriptor for debug connection outbound

         **in_desc**
             - string
             - encoded binary serialized channel descriptor for debug connection inbound

     *implementation(s):* :func:`Python<dragon.infrastructure.messages.Breakpoint>`

.. _BELAAPI:

Launcher Backend Messages APIs
==============================

These messages are used by a combination of the :ref:`Launcher` and the  Launcher Backend to support
the launcher in both regular and server mode. They are communicated through the *BELA Channel*.

.. _beisup:

106. **BEIsUp**

    *type enum*
        BE_IS_UP (= 106)

    *purpose*
        Confirm to Launcher Frontend that the Backend is up and send its serialized
        channel descriptor and host_id. This message is not communicated through the
        *BELA Channel*, as this is not set up yet.

    *fields*

        **be_ch_desc**

            - Backend serialized channel descriptor. The frontend will attach to
            this channel and establish a connection with the backend.

        **host_id**

            - integer
            - the hostid of the node sending this message.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.BEIsUp>`

.. _bepingsh:

58. **BEPingSH**

    *type enum*
        BE_PING_SH (= 58)

    *purpose*
        MRNet backend handshake with Shepherd

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.BEPingSH>`

.. _behalted:

63. **BEHalted**

    *type enum*
        BE_HALTED (= 63)

    *purpose*
        Indicate that the MRNet backend instance on this node has exited normally.

    *fields*

        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.BEHalted>`

.. _benodeidxsh:

91. **BENodeIdxSH**

    *type enum*
        BE_NODEINDEX_SH (= 91)

    *purpose*

        This message is sent from the Launcher Backend to Local Services during
        initialization to tell Local Services its node index and related host info for the current run. All nodes are numbered *0* to *n-1* where *n* is
        the number of nodes in the allocation.

    *fields*

        **node_idx**

            - An integer representing the node index value for this node.

        **host_name**

            - string
            - name of host as returned by MRNet backend

        **ip_addr**
            - string
            - IP address as returned by DNS lookup of hostname

        **primary**
            - string
            - hostname of primary node

        **logger_sdesc**
            - dragon.utils.B64
            - serialized descriptor of backend logging channel


    *implementation(s):* :func:`Python<dragon.infrastructure.messages.BENodeIdxSH>`

.. _FELAAPI:

Launcher Frontend Messages APIs
==============================

These messages are used by a combination of the :ref:`Launcher` and the  Launcher Frontend to support
the launcher in both regular and server mode.

.. _fenodeidxbe:

107. **FENodeIdxBE**

    *type enum*
        FE_NODE_IDX_BE (= 107)

    *purpose*
        The Frontend sends the node index to the Backend, based on the backend's host_id.

    *fields*

        **node_index**
            - integer
            - The index of this node in the allocation upon which the
              Dragon runtime services are running.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.FENodeIdxBE>`

.. _haltoverlay:

108. **HaltOverlay**

    *type enum*
        HALT_OVERLAY (= 108)

    *purpose*
        Indicate that monitoring of the overlay network should cease

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.HaltOverlay>`

.. _haltlogginginfra:

109. **HaltLoggingInfra**

    *type enum*
        HALT_LOGGING_INFRA (= 109)

    *purpose*
        Indicate that monitoring of logging messages from the backend should cease

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.HaltLoggingInfra>`


.. _laexit:

116. **LAExit**

    *type enum*
        LA_EXIT (= 116)

    *purpose*
        Indicate the launcher should exit. Use in case the launcher teardown was unable to
        complete correctly.

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.LAExit>`


.. _LoggingMessagesAPI:

Logging Messages API
====================

In order to transmit messages from backend services to the launcher frontend,
the logging infrastructure packs logging strings into messages that are passed
from a given backend service to the MRNet server backend, which aggregates any
logging messages its received and sends them to the launcher frontend to be offically
logged to file or terminal.

.. _loggingmsg:

97. **LoggingMsg**

    *type enum*
        LOGGING_MSG (= 97)

    *purpose*
        To take logging strings provided via python logging (``eg: log.info('message')``)
        and pack all information that's necessary to make log messages useful on the
        frontend

    *fields*

        All fields are auto-populated by the logging infrastructure found in :ref:`LoggingInfrastructure`

        **name**
            - string
            - name of logger message came from. Typically formatted ``f{service}.{func}``

        **msg**
            - string
            - log mesage

        **time**
            - string
            - time generated by the sending logger's formatter

        **func**
            - string
            - name of function the logging msg was made in

        **hostname**
            - string
            - hostname of sender

        **ip_address**
            - string
            - IP address of sender

        **port**
            - string
            - Port of sender (placeholder for now but likely useful in single-node testing)

        **service**
            - string
            - service as defined in :func:`Python<dragon.dlogging.util.LOGGING_SERVICES>`

        **level**
            - int
            - logging level as defined by ``dragonMemoryPoolAttr_t`` and is consistent with
              Python's only loggin int values

    *implementation(s)* :func:`Python<dragon.infrastructure.messages.LoggingMsg>`

.. _loggingmsglist:

98. **LoggingMsgList**

    *type enum*
        LOGGING_MSG_LIST (= 98)

    *purpose*
        Takes a list of :ref:`LoggingMsg <loggingmsg>` and aggregates them into a single
        message the MRNet server backend can send up to the MRNet frontend
        server

    *fields*
        **records**
            list of :ref:`LoggingMsg <loggingmsg>` messages with attributes as defined therein

    *implementation(s)* :func:`Python<dragon.infrastructure.messages.LoggingMsgList>`

.. _logflushed:

99. **LogFlushed**

    *type enum*
        LOG_FLUSHED (= 99)

    *purpose*
        Sent by MRNet server backend to frontend after it has completed its final
        flushing and transmission of logs to the fronted. Part of the transaction
        diagram for teardown in :ref:`MultiNodeTeardown`

    *fields*
        None other than default

    *implementation(s)* :func:`Python<dragon.infrastructure.messages.LogFlushed>`


.. _TransportAgentAPI:

Overlay/Transport Agent Messages API
====================================

In multi node cases, one or more transport agent processes live on each node and they need a few messages
related to setup and teardown control through the *TA Channel*.

.. _tapingsh:

59. **TAPingSH**

    *type enum*
        TA_PING_SH (= 59)

    *purpose*
        Indicate that the TA instance on this node has come up and
        succeeded in getting communication with the other TA instances.

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.TAPingSH>`


.. _tahalted:

61. **TAHalted**

    *type enum*
        TA_HALTED (= 61)

    *purpose*
        Indicate that the TA instance on this node has exited normally.

    *fields*
        **idx**
            - integer
            - which node in the allocation this is.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.TAHalted>`

.. _taup:

64. **TAUp**

    *type enum*
        TA_UP (= 64)

    *purpose*
        Indicate that the TA instance on this node is up and ready.

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.TAUp>`

.. _overlaypingbe:

109. **OverlayPingBE**

    *type enum*
        OVERLAY_PING_BE (= 109)

    *purpose*
        Indicate that the Overlay instance on this backend node is up and ready.

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.OverlayPingBE>`

.. _overlaypingla:

110. **OverlayPingLA**

    *type enum*
        OVERLAY_PING_LA (= 110)

    *purpose*
        Indicate that the Overlay instance on this frontend node is up and ready.

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.OverlayPingLA>`


.. _lahaltoverlay:

112. **LAHaltOverlay**

    *type enum*
        LA_HALT_OVERLAY (= 112)

    *purpose*]
        Launcher frontend requests a shutdown of Overlay agent

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.LAHaltOverlay>`

.. _behaltoverlay:

113. **BEHaltOverlay**

    *type enum*
        BE_HALT_OVERLAY (= 113)

    *purpose*]
        This backend node requests a shutdown of its Overlay agent

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.BEHaltOverlay>`

.. _overlayhalted:

114. **OverlayHalted**

    *type enum*
        OVERLAY_HALTED (= 114)

    *purpose*]
        This overlay instance has shutdown

    *fields*
        None other than default

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.OverlayHalted>`

.. _MessagesPython:

Python Infrastructure Messages Implementation
====================================================

.. py:currentmodule:: dragon.infrastructure.messages

The following subsections contain structures relating to the Python implementation of the Dragon
infrastructure message definitions. The specification of these message types i`` in the :ref:`previous
section<MessagesAPI>` and cross-referenced from these two sections for convenience.

Python Implementation of Infrastructure Messages
--------------------------------------------------
Alphabetical listing of all message classes in the Python implementation of the infrastructure messages.

.. automodule:: dragon.infrastructure.messages
    :members:
    :exclude-members: MessageTypes

Python Message Type Value Enumeration
--------------------------------------------
This contains the message type ``_tc`` definition for the Python implementation
of the infrastructure messages.

.. autoclass:: MessageTypes
    :members:


.. maybe add examples here ?

