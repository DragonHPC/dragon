.. _ColocatedProcessAttach:

Co-located Process Attach
+++++++++++++++++++++++++

The **co-located process attach** feature allows a Python process started
*outside* the Dragon runtime — for example, from a plain ``python`` invocation —
to register itself with a running Dragon runtime, use Dragon resources
(channels, queues, managed-memory pools, and managed processes), and later
detach cleanly.

Typical use cases include:

- Interactive Python shells or notebooks that want to start a Dragon runtime or leverage an already-running Dragon runtime (including remote runtimes).
- Orchestration scripts that start Dragon in the background and then
  connect programmatically.

.. note::
   This page documents **co-located attach** (the process talks to its
   node-local Local Services first). In multi-node runtimes this same local
   attach path is used, and gateway descriptors may be returned so the process
   can issue off-node operations after bootstrap. This will not work if local services does not exist on the current node. If you need to attach to a remote runtime without local services co-located with the current process, you must use the Proxy attach method to attach to the remote runtime.


Public API
==========

The primary Python entry points for this feature are in
:py:mod:`dragon.workflows.runtime`:

.. code-block:: python

   from dragon.workflows import runtime

   rt = runtime.attach(timeout=30)    # returns LocalRuntime handle
   # … use Dragon APIs (Queue, Channel, Process, etc.) …
   rt.detach()                        # gracefully disconnect this process

The key entry points are:

:py:func:`dragon.workflows.runtime.attach`
   Register this process with a running Dragon runtime.  Reads the
   attach-info file, sends ``LSExternalRegister`` to node-local Local
   Services (which allocates the external PUID, provisions both return
   queues, and relays prepared registration to Global Services), waits for
   ``GSExternalRegisterComplete`` from GS, and then sets up the four
   infrastructure queues.

   Returns a :py:class:`~dragon.workflows.runtime.LocalRuntime` handle
   (not a PUID integer).  The assigned PUID is stored on the handle as
   ``_puid``.

   Raises :exc:`RuntimeError` if no runtime is found, attach-info is
   invalid, registration fails, or the process is already attached.
   Raises :exc:`PermissionError` on auth-token mismatch.

:py:meth:`dragon.workflows.runtime.LocalRuntime.detach`
   Gracefully detach from the Dragon runtime. Sends ``GSProcessDetach`` to
   Global Services, waits for ``GSProcessDetachResponse`` on the GS-return
   queue, then sends ``GSProcessDetachAck`` and tears down local
   infrastructure connections.

:py:meth:`dragon.workflows.runtime.LocalRuntime.stop`
   Request runtime shutdown via the control event registered by the runtime
   head process. For attach-only handles this signals shutdown but does not
   wait for runtime exit.

See :py:mod:`dragon.workflows.runtime` for full parameter and exception
documentation. This page focuses on co-located attach
(``runtime.attach(..., sdesc_str=None)``), not Proxy attach via
``sdesc_str``.


Security Model
==============

Authentication Token
--------------------

At startup, the launcher generates a random 32-byte hex token using
``secrets.token_hex(32)`` and shares it with Local and Global Services.
The token is **not** written into the attach-info file; it is printed by
the launcher when the runtime is started ``--attachable`` (``-a``) and is
supplied to :py:func:`dragon.workflows.runtime.attach` via the
``auth_token=`` argument or the ``DRAGON_ATTACH_TOKEN`` environment
variable. Any process calling ``runtime.attach()`` must present this exact
token in the ``LSExternalRegister`` message; a mismatch is rejected with an
``AUTH_FAILURE`` error code that maps to a :exc:`PermissionError`.

File Permissions
----------------

The attach-info directory (see `Attach-Info File`_) is created with
mode ``0o700`` and the attach-info file itself with mode ``0o600``,
ensuring only the owning OS user can read the runtime's channel/pool
descriptors.  An existing directory named by ``DRAGON_ATTACH_DIR`` keeps
its own permissions.  The auth token is delivered out of band (launcher output /
``DRAGON_ATTACH_TOKEN``), not stored in the file.  Attempting to attach
as a different OS user will fail at the file-read step before a
registration request is even sent. This could be eased if more than one user needs access to the runtime by changing the file permissions.

.. note::
   There is a race window between teardown being initiated and the
   attach-info file being removed.  A process that reads the file just
   before teardown may time out waiting for ``LSExternalResourcesReady``
   or ``GSExternalRegisterComplete`` if the runtime finishes shutting
   down first.  The existence of the file is not a guarantee that
   registration will succeed.


Attach-Info File
================

Written by **Local Services** once the runtime is up, the file is
located at:

.. code-block:: text

   <tmpdir>/dragon_{uid}/attach_info.txt

where ``<tmpdir>`` is a usable system temporary directory (usually
``/tmp``) found with Python's :py:func:`tempfile.gettempdir`; C/C++ code
uses an equivalent search.  The file, and the directory if it is then
empty, are removed (best-effort) at teardown and by ``dragon-cleanup``.

.. tip::
   If an attaching process cannot find ``attach_info.txt``, set the
   ``DRAGON_ATTACH_DIR`` environment variable to the same directory for
   both the Dragon runtime and the attaching process.  When it is set,
   Local Services writes the file there, every attach path reads it from
   there, and the cleanup tools remove it from there (as well as from the
   default location).

Contents: a single line of Base64 text encoding a serialized Cap'n Proto
:class:`~dragon.infrastructure.messages.AttachableRuntimeDescr` message
(``AttachableRuntimeDescrDef`` in ``message_defs.capnp``). Python readers
decode it with ``dragon.utils.b64decode`` followed by
``dragon.infrastructure.messages.parse``; C/C++ readers use
``dragon_base64_decode`` followed by ``dragon_deserialize_msg``. The
message carries these fields:

==============  ===============================================================
Field           Description
==============  ===============================================================
``node_index``  Index of the node the file was written on
``gs_qd``       B64-encoded GS input queue descriptor
``ls_qd``       B64-encoded LS input queue descriptor
``default_pd``  B64-encoded default managed-memory pool descriptor
``inf_pd``      B64-encoded infrastructure pool descriptor
``rt_uid``      ``DRAGON_RT_UID`` value
==============  ===============================================================

``gs_qd`` and ``ls_qd`` are serialized **InfraQueue** (FLI) descriptors,
not raw channel descriptors.  The auth token is **not** in this file
(see Security Model).  ``rt_uid`` is written into the ``DRAGON_RT_UID``
environment variable of the attaching process before the default pool is
attached, so the C memory-pool layer recognises the pool as local; if it
is empty the attach is rejected.


High-Level Flow
===============

The following diagram summarises the interactions during attach and
detach.  The external process talks **only** to its node-local Local
Services; LS provisions resources and relays to Global Services, and GS
replies to the process directly:

.. code-block:: text

   External Process            Local Services (LS)      Global Services (GS)
   ────────────────            ───────────────────      ────────────────────
   reads attach_info.txt
   (+ DRAGON_ATTACH_TOKEN)
      │ LSExternalRegister
      ├───────────────────────────>
      │                     allocate external PUID
      │                     + LS-return & GS-return queues
      │ LSExternalResourcesReady
      │<───────────────────────────┤  (on temp reply queue)
      │                            │ GSExternalRegisterPrepared
      │                            ├────────────────────────────>
      │                                       adopt GS-return;
      │                                       commit ProcessContext
      │ GSExternalRegisterComplete                     │
      │<────────────────────────────────────────────────┤  (on GS-return)

            (uses Dragon APIs)

      │ GSProcessDetach
      ├─────────────────────────────────────────────────>
      │ GSProcessDetachResponse                         │
      │<─────────────────────────────────────────────────┤  (on GS-return)
      │ GSProcessDetachAck
      ├─────────────────────────────────────────────────>
      │                            │ LSProcessDetach     │
      │                            │<────────────────────┤
      │                            │ LSProcessDetachResponse
      │                            ├────────────────────>│
      │                            │ LSChannelDestroy    │
      │                            │<────────────────────┤  (destroy GS-return)
      │                                       mark process DEAD


Registration Message Flow
=========================

1. The external process reads ``attach_info.txt``, sets
   ``DRAGON_RT_UID``, and calls ``MemoryPool.attach()`` on the default
   pool.

2. It allocates a **temporary reply channel** in that pool (CUID computed
   deterministically from the OS PID, see :ref:`cuid-allocation`), wraps
   it in an ``InfraQueue``, and takes the serialized queue descriptor
   (``proc_resp_sdesc``).

3. It attaches the node-local LS input queue (``ls_qd``) and sends
   ``LSExternalRegister`` carrying the auth token, its OS PID, the
   runtime incarnation, a fresh ``attempt_id``, and
   ``proc_resp_sdesc``.

4. LS validates the request identity and token, allocates an **external
   PUID** from its node-local block, and provisions both the LS-return
   and GS-return queues.  It replies ``LSExternalResourcesReady`` on the
   temp reply queue with the PUID, the LS/GS return queue descriptors,
   the pool descriptors, and the gateway environment.

5. LS then sends ``GSExternalRegisterPrepared`` to Global Services on the
   GS input queue, carrying the PUID, node index, PID/start time, and the
   GS-return raw-channel and FLI-queue descriptors.

6. The external process consumes the provisioning message, destroys the
   temp reply channel, installs the gateway environment variables, and
   attaches the provisioned GS-return queue to await final completion.

   .. warning::
      Current implementation detail: this step updates ``DRAGON_GW*`` and
      related environment variables but does not perform an explicit
      gateway-registry refresh call. In a process that already initialized
      gateway state before attach, stale gateway cache state can survive the
      environment update.

7. GS **adopts** the GS-return channel, commits an ``ACTIVE``
   :class:`~dragon.globalservices.process_int.ProcessContext` for the
   external process, and sends ``GSExternalRegisterComplete`` directly on
   the GS-return queue.

8. On receipt of ``SUCCESS`` the external process builds a
   :class:`~dragon.infrastructure.parameters.LaunchParameters` via
   :py:meth:`~dragon.infrastructure.parameters.LaunchParameters.from_attach_info`
   and calls
   :py:func:`~dragon.globalservices.api_setup.connect_as_external`
   to install the parameters and connect all four infrastructure queues.
   The ``GSPingProc`` handshake is skipped for external processes.

.. note::
   If the process abandons the attach before completion it sends
   ``LSExternalRegisterCancel`` to LS, which forwards
   ``GSExternalRegisterCancel`` to GS so any adopted GS-return channel is
   cleaned up. There is no client cancellation-response message.

.. warning::
   ``attach(timeout=...)`` enforces the deadline while waiting for
   provisioning/completion responses, but the initial
   ``LSExternalRegister`` send currently uses a blocking put with no timeout.
   Under LS input queue congestion, that send may block beyond the advertised
   attach timeout.


Detach Message Flow
===================

1. The external process calls ``LocalRuntime.detach()``, which sends
   ``GSProcessDetach`` (with its PUID) to GS via the GS input queue.

2. GS validates that the PUID belongs to an external process, records a
   pending detach, and sends ``GSProcessDetachResponse`` with ``SUCCESS``
   on the GS-return queue.  The return channel is **not** destroyed yet.

3. The external process consumes the response and sends
   ``GSProcessDetachAck`` back to GS, then releases its client-side
   attachments.  (Public ``detach()`` may return before server-side
   cleanup finishes.)

4. On the ack, GS sends ``LSProcessDetach`` to LS on the process's node.
   LS removes the process from ``external_procs`` / ``puid2pid`` and
   destroys the **LS-return** channel (not the GS-return), then replies
   ``LSProcessDetachResponse``.

5. GS then drives physical destruction of the GS-return channel via
   ``LSChannelDestroy`` / ``LSChannelDestroyResponse``, removes the
   adopted channel from its tables, and marks the
   ``ProcessContext`` as ``DEAD`` (exit code 0 for a clean detach).

6. Meanwhile, ``LocalRuntime.detach()`` calls
   :py:func:`~dragon.globalservices.api_setup.disconnect_infrastructure`
   to close all four infrastructure connections (no EOT is sent to the
   shared GS/LS input queues) and detach the default memory pool.


Unexpected Death Detection
==========================

Local Services runs a ``watch_external_death()`` daemon thread that
polls all registered external processes every 2 seconds using
``os.kill(pid, 0)`` (signal 0: existence check only, no signal
delivered).  When the probe returns ``ESRCH`` (no such process), LS:

1. Destroys and reclaims the LS-return channel(s) for that process.
2. Removes the process from ``external_procs`` and ``puid2pid``.
3. Sends ``LSProcessExit`` to GS so GS can mark the ``ProcessContext``
   as ``DEAD`` (and destroy the adopted GS-return channel).

In the common case this reclaims resources even if the external process
crashes or is killed without calling ``LocalRuntime.detach()``.

.. warning::
   A known race remains if the external process dies after LS provisioning but
   before GS commits the prepared registration: LS can clean local state while
   GS later adopts the GS-return channel from a delayed prepared message,
   leaving that GS-return channel orphaned until broader runtime cleanup.


.. _cuid-allocation:

CUID Allocation for Temporary Reply Channels
=============================================

Temporary reply channels use a dedicated CUID range that does not
overlap with any runtime-managed range:

- ``BASE_ATTACH_TEMP_CUID = 2**53``
- ``RANGE_ATTACH_TEMP_CUID = 2**20``

The function
:py:func:`~dragon.infrastructure.facts.attach_temp_cuid_from_pid`
computes:

.. code-block:: python

   BASE_ATTACH_TEMP_CUID + (pid % RANGE_ATTACH_TEMP_CUID)

This gives a deterministic, collision-free CUID for processes whose OS
PIDs differ (within the range of 1 048 576 distinct PIDs).


LaunchParameters Construction
==============================

After a successful ``LSExternalRegister`` / ``GSExternalRegisterComplete``
exchange, the external process builds a
:class:`~dragon.infrastructure.parameters.LaunchParameters` object via
:py:meth:`~dragon.infrastructure.parameters.LaunchParameters.from_attach_info`.
The following fields are set:

==========================================  =============================================
Field                                       Source
==========================================  =============================================
``mode``                                    ``SINGLE_MODE`` (``CRAYHPE_MULTI_MODE`` if gateways present)
``index``                                   ``attach_descr.node_index``
``my_puid``                                 PUID from ``LSExternalResourcesReady``
``gs_ret_qd``                               GS-return queue descriptor from the response
``ls_ret_qd``                               LS-return queue descriptor from the response
``default_pd``                              Response value, falling back to ``attach_info.txt``
``inf_pd``                                  Response value, falling back to ``attach_info.txt``
``gs_qd``                                   From ``attach_info.txt``
``local_ls_qd``                             From ``attach_info.txt`` (``ls_qd``)
``num_gw_channels_per_node``                From the response (``0`` on single-node)
==========================================  =============================================

This object is passed to
:py:func:`~dragon.globalservices.api_setup.connect_as_external`, which
installs it as the process-level singleton, exports ``DRAGON_*``
environment variables, and connects the four infrastructure queues.


Example
=======

A working example is in ``examples/workflows/outer_space_proc/`` and can be
run in two ways with ``client.py``.

1. Start-and-attach in one process (``--start``)
------------------------------------------------

This mode launches a fresh runtime, attaches, runs the workload, and
then stops the runtime from the same client process.

.. code-block:: bash

   cd examples/workflows/outer_space_proc
   python client.py --start

2. Attach to an already-running runtime (``--attach``)
------------------------------------------------------

In terminal 1, start an attachable runtime with the same command used by
``run_interactive_demo.sh``:

.. code-block:: bash

   cd examples/workflows/outer_space_proc
   dragon -a dragon-waiting-runtime-head

The launcher prints the attach token in terminal 1. In terminal 2, explicitly
export that token before running the attach client:

.. code-block:: bash

   cd examples/workflows/outer_space_proc
   export DRAGON_ATTACH_TOKEN="<attach token printed in terminal 1>"
   python client.py --attach

The attach token export in terminal 2 is required for
``runtime.attach()`` authentication.
