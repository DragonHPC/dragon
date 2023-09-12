Control Structures in C
+++++++++++++++++++++++

Within the C language, policy decisions will be specified via a `dragonPolicy` structure
as given here.

API Specification
=================

.. doxygenenum:: dragonAffinity_t

.. doxygenenum:: dragonWaitMode_t

.. doxygenstruct:: dragonPolicy_t
    :members:

Examples
========

The programmer can specify a policy by instantiating an instance of this structure in their code and supplying that policy while
creating Dragon objects, like queues.

.. code-block:: C
  :linenos:
  :caption: **Creating a Queue with a non-default policy using C**

  #include <dragon/global_types.h>
  #include <dragon/queue.h>

  void sample() {
    dragonPolicy_t policy;
    dragonError_t err;
    dragonQueueDescr_t queue;

    err = dragon_policy_init(&policy);

    policy.wait_type = DRAGON_IDLE_WAIT;

    err = dragon_managed_queue_create("my_unique_queue_name", 100, true, NULL, &policy, &queue);

    /* being a managed queue, multiple processes can "create" this queue instance. The first one will actually
       create it while all others will automatically be given access to this queue instance. It can also
       be shared across languages since the same API is available in all languages and interacts with the Dragon
       run-time in the same way. */

  }

