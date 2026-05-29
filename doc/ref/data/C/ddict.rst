.. _distdictAPI:

The Distributed Dictionary C Client
=====================================

.. _DragonDDictCClient:

The C DDict client api uses the same underlying messaging that is used by the
Python client. The C Client cannot create a DDict, but it can attach to one that
was created in Python. Python code can serialize a DDict (i.e. create a
serialized handle to it) and pass that handle to C code which can then attach to
it. After attaching, a C DDict client has full access to the DDict.

Because objects are serialized differently between Python and C, without special
handling, C and Python are not going to be able to share key/value pairs. A
custom pickler would have to be used in Python and similar code would have to be
written in C to have cross-language compatibility of key/value pairs. Both
languages provide full access to the DDict, so there is nothing preventing
cross-language sharing. It is a serialization and deserialization problem to
solve for cross-language sharing.

The design of the C DDict client revolves around the idea of a request for
continuity between multi-step operations requiring more than one API call to
complete. A DDict Request object is created at the beginning of multi-step DDict
interactions. That request object is used on the multi-step calls until the
request has been completed and finalized.

.. code-block:: C
    :linenos:
    :caption: Storing a key/value pair in the DDict using C

    dragonDDictDescr_t ddict;
    dragonDDictRequestDescr_t req;
    dragonError_t err;
    dragonFLISendHandleDescr_t value_sendh;
    dragonFLISendHandleDescr_t key_sendh;

    // Attach to the instance of the dictionary. The ddict_ser could be
    // provided as a command-line argument or it could receive it via some
    // other means (like a Dragon Queue for instance).
    dragonError_t err = dragon_ddict_attach(ddict_ser, &ddict, &TIMEOUT);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Could not attach");

    err = dragon_ddict_create_request(&ddict, &req);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not create DDict put request.");

    err = dragon_ddict_request_key_sendh(&req, &key_sendh);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not access the request key send handle.");

    /* One or more key send_bytes can be called to send the key. The key is buffered
       as send_bytes is called so it can be used to select a manager where this
       key will be/is stored. The bytes is a pointer to num_bytes of data.
       Timeout can be specified as needed. NULL for timeout will block
       until the key can be written. The arg and buffer arguments are
       ignored on the send_bytes called when used with the DDict.
    */

    err = dragon_fli_send_bytes(&key_sendh, num_key_bytes, key_bytes, 0, false, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not send the key.");

    /* Calling put selects put as the operation. Notice the key is written first, then
       put it called. */
    err = dragon_ddict_put(&req);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not send DDict put message.");

    /* Getting a value send handle will allow the code to send the value on a put
       operation. */
    err = dragon_ddict_request_value_sendh(&req, &value_sendh);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not access the request send handle.");

    /* One or more value send_bytes can be called. The buffer arg can be true or false. Using
       true will do one send across the network (if going to another node) but either will work.
       When buffer is false, the value is streamed to the manager, which for values that take a
       large amount of memory may be advantageous. */
    err = dragon_fli_send_bytes(&value_sendh, num_value_bytes, value_bytes, VALUE_HINT, buffer, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not send the value.");

    /* Finally, the request is finalized once it is complete. */
    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not finalize the DDict put request.");

This code captures attaching to a DDict and creating a request and using it to
store a key/value pair in the DDict. When a DDict operation can be completed with
one API call, a request object is not created and required as in calling
something like the length function which returns the number of keys stored in the
DDict. However, when the operation extends across multiple API calls, a request
object ties the calls together into one operation.

For sample code for other operations, you can consult the C code used to
implement the C++ DDict client in
:ref:`dictionary.hpp <https://github.com/DragonHPC/dragon/blob/main/src/include/dragon/dictionary.hpp>`.
Better yet, use the C++ DDict client!

C API Reference
================

Structures
--------------

.. doxygengroup:: ddict_structs
   :content-only:
   :members:

Lifecycle Management
----------------------

.. doxygengroup:: ddict_lifecycle
   :content-only:
   :members:

Request Management
---------------------

.. doxygengroup:: ddict_requests
   :content-only:
   :members:

Send/Recv Functions
---------------------
These functions are depracated. Instead, please use the
dragon_ddict_request_value_sendh, dragon_ddict_request_key_sendh,
and the dragon_ddict_request_recvh methods functions along with
FLI send and receive functions for sending and receiving data.

.. doxygengroup:: ddict_sendrecv
   :content-only:
   :members:

Dictionary Operations
------------------------

.. doxygengroup:: ddict_ops
   :content-only:
   :members:



