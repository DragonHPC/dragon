.. _ConventionalIDs:

Conventional IDs
++++++++++++++++

The Dragon infrastructure uses process UIDs (`p_uid`), channel UIDs (`c_uid`), and memory pool ids (`m_uid`)
to uniquely identify processes, channels, and memory pools in the runtime system.

Normally these are assigned when these entities are created, but infrastructure actors and default pools will
themselves need to have such IDs for uniformity.   This means that by convention some communications
structures used in bringup and teardown carrying infrastructure messages may have channel uids assigned even
though the channels are not globally accessible, but some special casing is unavoidable.

Infrastructure IDs
==================

These are for now for the single node scenario.

The shepherds don't issue commands; they only execute and reply to them, so shepherd instances don't need to
have a p_uid.

.. _p_uid:

Process ID
----------

+--------------------+------------------+---------------------------------------------------+
| Actor              | Value            | Description and notes                             |
+====================+==================+===================================================+
| Launcher           | 0                | Launcher starts head user process                 |
+--------------------+------------------+---------------------------------------------------+
| Global Services    | 1                | When GS goes parallel the workers might not need  |
|                    |                  | their own IDs.                                    |
+--------------------+------------------+---------------------------------------------------+
| Transport Services | 2^8 + node_index | Transport services are assigned p_uid of their    |
|                    |                  | node index offset by base of 2^8. A node index of |
|                    |                  | 2 has a p_uid of 10                               |
+--------------------+------------------+---------------------------------------------------+

Managed processes' IDs are assigned starting with 2^32.

.. _c_uid:

Channel ID
----------

Assigning a c_uid even to things that are not backed by Channels helps because then, as long as the object API
matches on some level, these handles can be dealt with uniformly from a point of view of caching,
initialization, and cleanup - the special casing in the code is only in how they are wrapped.

The shared memory segment name and offset corresponding to the physical location of these channels should come
in along with the launch parameters.

Generally, c_uid below 2^63 are reserved for use by infrastructure.

+-------------------+------------------------------------+----------------------------------------------------+
| Channel           | Value                              | Description and notes                              |
+===================+====================================+====================================================+
|| GS stdin         || 0                                 || Held by Shepherd - wrapped stream                 |
||                  ||                                   || Currently unused                                  |
+-------------------+------------------------------------+----------------------------------------------------+
|| GS stdout        || 1                                 || Held by Shepherd - wrapped stream                 |
+-------------------+------------------------------------+----------------------------------------------------+
|| Global Services  || 2                                 || input to global services head.                    |
|| Channel          ||                                   ||                                                   |
+-------------------+------------------------------------+----------------------------------------------------+
|| Transport Agent  || 2^55                              || input to transport agent N.                       |
|| Channel          || + N                               ||                                                   |
+-------------------+------------------------------------+----------------------------------------------------+
|| Shepherd         || 2^56                              || input to shepherd N;                              |
|| Channel          || + N                               ||                                                   |
+-------------------+------------------------------------+----------------------------------------------------+
|| Launcher/Backend || 2^57                              || There is one launcher backend per node. When the  |
|| Channel          || + N                               || the Shepherd communicates to the front end during |
||                  ||                                   || startup or teardown it goes through this backend. |
+-------------------+------------------------------------+----------------------------------------------------+
|| Gateway          || 2^58                              || Gateway channels starting point.                  |
|| Channel          || + N                               || Adder is node index N.                            |
+-------------------+------------------------------------+----------------------------------------------------+
|| Logging Services || 2^59                              || Channels used from the logging infrastructure.    |
|| Channel          ||                                   ||                                                   |
+-------------------+------------------------------------+----------------------------------------------------+
|| Backend          || 2^60                              || Backend cuid used for infrastructure              |
|| Infrastructure   || + host_id                         || transport gateway channels.                       |
|| Transport        ||                                   || We use the host_id as the adder (the node index   |
|| Gateway          ||                                   || is not known yet) and we use the modulo operation |
|| Channel          ||                                   || to project the produced cuid value inside the     |
||                  ||                                   || allowed range of values.                          |
+-------------------+------------------------------------+----------------------------------------------------+
|| Backend          || 2^61                              || Backend cuid used for communication with the      |
|| Infrastructure   || + host_id                         || infrastructure transport agent.                   |
|| Local            ||                                   || We use the host_id as the adder (the node index   |
|| Channel          ||                                   || is not known yet) and we use the modulo operation |
||                  ||                                   || to project the produced cuid value inside the     |
||                  ||                                   || allowed range of values.                          |
+-------------------+------------------------------------+----------------------------------------------------+
|| Backend          || 2^62                              || Backend cuid used for communication with the      |
|| Infrastructure   || + host_id                         || frontend, while setting up its infrastructure.    |
|| Frontend         ||                                   || We use the host_id as the adder (the node index   |
|| Channel          ||                                   || is not known yet) and we use the modulo operation |
||                  ||                                   || to project the produced cuid value inside the     |
||                  ||                                   || allowed range of values.                          |
+-------------------+------------------------------------+----------------------------------------------------+
|| Debug channels   || 2^33                              || Channels made dynamically to support debugging    |
||                  || + N                               || (only in infrastructure pool)                     |
+-------------------+------------------------------------+----------------------------------------------------+
|| Process GS       || 2^62 +                            || Managed process msg return channel from global    |
|| return channel   || M                                 || services. M not necessarily p_uid                 |
+-------------------+------------------------------------+----------------------------------------------------+
|| Process local    || 3 * 2^61 +                        ||                                                   |
|| M                || services. M not necessarily p_uid ||                                                   |
+-------------------+------------------------------------+----------------------------------------------------+


In a multi node case, there will be N conventional shepherd inputs, with IDs assigned consecutively.  Channel
uids for normal processes will be assigned starting at 2^63.  This allows capacity for the runtime to handle
2^60 unique processes and 2^60 nodes, with 2^63 unique channels over a single run, which 'should be enough for
anybody'.

Managed Process default channel IDs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Managed processes will need to have some channels created for use by the infrastructure itself.  Each new
process will be able to attach to the GS input and the Shepherd input used by all the other processes, but
each new process also has two other channels unique to itself that are needed to implement multiprocessing
APIs.

    - return messages from Global Services
    - return messages from the Shepherd

It is Global Services's responsibility to make sure these channels are in place before the managed process
gets started.

    Note: as a future optimization there might be a reason to have a single message to the Shepherd to do all
    these operations as a fell swoop, to save some message round trips involved in building the channels
    first.

The details of which shared memory segment and the offset of these channels will be passed along with the IDs
in the environment.

.. _m_uid:

Memory Pool ID
--------------

Every node has a default memory pool. This is always used for infrastructure channels, and may also be used by
the user application.  The `m_uid` of the default memory pool on each node is the node index.

In order to separate logging infrastructure from the rest of the services infrastructure, the logging
infrastructure on a node creates a logging memory pool with `m_uid` starting at 2^61.

The `m_uid` from user created pools will start at 2^63.

Debug channel IDs
-----------------

Debug channels are created as-needed by processes in the infrastructure.  Their :ref:`c_uid` only has meaning
in the context of the 'infrastructure pool' on each node, and start at 2^33.  Uniqueness is assured in that
context via the creating process's Unix pid.  These channels are not registered by the infrastructure.
Debugging systems must manage these descriptors directly, including knowing what node they correspond to and
what :ref:`p_uid` made them.
