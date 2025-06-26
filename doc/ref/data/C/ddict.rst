.. _distdictAPI:

Distributed Dictionary API
==========================

.. _DragonDDictCClient:

C Reference
______________

.. contents:: Table of Contents
    :local:

Description
-------------

Dragon Distributed Dictionary supports C/C++ user APIs, allowing C/C++ clients to
interact with the dictionary like a map. The internal design of C client is similar
to Python client, except for the way clients are created. User must create Dragon
Distributed Dictionary through Python API, and create processes to run dictionary C++
client program using `dragon.native.Popen`. Each process stands for exactly a single
client, and it should attach to only a single dictionary throughout the program. In
the C++ program, the client uses the the argument serialized dictionary to attatch
to the dictionary created earlier through Python API.

Please note that creating multiple client processes inside a single C/C++ program using
`fork()` is not supported in Dragon Distributed Dictionary. You must create client processes
from Python API and attach to the dictionary in the C++ program.

For C++ client, the data written into dictionary must be serializable. Under this
requirement, user must implement the serializable class based on the existing `DDictSerializable`
interface. The functions `serialize` and `deserialize` are used while writing and reading
data from the dictionary, while the function `create` is used to reconstruct the key when
we request a list of keys from the dictionary. The example below demonstrates implementation
of the interface `DDictSerializable` that is defined in `dragon/dictionary.hpp`.

.. code-block:: CPP
    :linenos:
    :name: serializable
    :caption: **Implementing Serializable Class Based on the Interface DDictSerializable**

    // serializable int
    #include <dragon/dictionary.hpp>

    class SerializableInt : public DDictSerializable {
        public:
        SerializableInt();
        SerializableInt(int x);
        virtual void serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout);
        virtual void deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout);
        static SerializableInt* create(size_t num_bytes, uint8_t* data);
        int getVal() const;

        private:
        int val=0;
    };

    SerializableInt::SerializableInt(): val(0) {}
    SerializableInt::SerializableInt(int x): val(x) {}

    SerializableInt* SerializableInt::create(size_t num_bytes, uint8_t* data) {
        auto val = new SerializableInt((int)*data);
        free(data);
        return val;
    }

    void SerializableInt::serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) {
        dragonError_t err;
        err = dragon_ddict_write_bytes(req, sizeof(int), (uint8_t*)&val);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, dragon_getlasterrstr());
    }

    void SerializableInt::deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) {
        dragonError_t err = DRAGON_SUCCESS;
        size_t actual_size;
        uint8_t * received_val = nullptr;
        size_t num_val_expected = 1;


        err = dragon_ddict_read_bytes(req, sizeof(int), &actual_size, &received_val);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, dragon_getlasterrstr());

        if (actual_size != sizeof(int))
            throw DragonError(DRAGON_INVALID_ARGUMENT, "The size of the integer was not correct.");

        val = *reinterpret_cast<int*>(received_val);

        free(received_val);

        err = dragon_ddict_read_bytes(req, sizeof(int), &actual_size, &received_val);

        if (err != DRAGON_EOT) {
            fprintf(stderr, "Did not received expected EOT, ec: %s\ntraceback: %s\n", dragon_get_rc_string(err), dragon_getlasterrstr());
            fflush(stderr);
        }
    }

    int SerializableInt::getVal() const {return val;}

    // begin user program
    int main(int argc, char* argv[]) {
        const char* ddict_ser = argv[1]; // serialized dictionary
        SerializableInt x(6); // key
        SerializableInt y(42); // value

        // attach current process to the dictionary
        DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
        // write
        dd[x] = y
        // read
        SerializableInt received_val = dd[x];
        assert received_val.getVal() == y.getVal();
        // get list of keys
        auto dd_keys = dd.keys();
        assert(dd.size() == 1);
        for (int i=0; i<dd_keys.size() ; i++)
            int val = dd_keys[i]->getVal();
    }

Under certain conditions where sharing data across processes is necessary, FLI
(i.e. :ref:`File Like Interfaces <fli_overview>`) serves as a mean of communication
among processes. Each FLI is created from stream channels. To create a FLI from C/C++
dictionary client, the user can create channels using API `dragon_create_process_local_channel`
from `dragon/channels.h`. You can then create FLI using the API `dragon_fli_create` from
`dragon/fli.h` and retrieve the serialized FLI by calling `dragon_fli_serialize`. By doing this,
any process can send or receive content after deserializing and attaching to the serialized FLIs.



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

.. doxygengroup:: ddict_sendrecv
   :content-only:
   :members:

Dictionary Operations
------------------------

.. doxygengroup:: ddict_ops
   :content-only:
   :members:




Messages
___________

.. _ddmessages:

#. **DDRandomManager**

    *type enum*
        DD_RANDOM_MANAGER

    *purpose*
        Client request a main manager from Orchestrator. This is for off-node bringup for a client.

    *fields*

        **response_fli**
            - string
            - b64 encoded serialized response fli.

    *see also*
        DDRandomManagerResponse, DDRegisterClient, DDConnectToManager

        Refer to the cfs section for additional request message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDRandomManager>`

#. **DDRandomManagerResponse**

    *type enum*
        DD_RANDOM_MANAGER_RESPONSE

    *purpose*
        Orchestrator return fli of a main manager for clients to request connection to other managers.

    *fields*

        **mainFLI**
            - string
            - b64 encoded serialized main fli of the manager.

    *see also*
        DDRandomManager

        Refer to the cfs section for additional request message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDRandomManagerResponse>`

#. **DDRegisterClient**

    *type enum*
        DD_REGISTER_CLIENT

    *purpose*
        Obtain unique client id from main manager and register client id to main manager.

    *fields*

        **response_fli**
            - string
            - b64 encoded serialized fli for response.

        **buffered_response_fli**
            - string
            - b64 encoded serialized fli for buffered response.

    *see also*
        DDRegisterClientResponse, DDRegisterClientID

        Refer to the cfs section for additional request message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDRegisterClient>`

#. **DDRegisterClientResponse**

    *type enum*

        DD_REGISTER_CLIENT_RESPONSE

    *purpose*
        Provide the unique client id and number of managers.

    *fields*

        **client_id**
            - uint32
            - unique for this client.

        **num_managers**
            - uint32
            - number of managers in the dictionary.

        **manager_id**
            - uint32
            - id of of the main manager to the client.

        **timeout**
            - uint32
            - timeout of the dictionary, same as the timeout in the initialization.

    *see also*
        DDRegisterClient

        Refer to the cfs section for additional response message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDRegisterClientResponse>`

#. **DDRegisterManager**

    *type enum*
        DD_REGISTER_MANAGER

    *purpose*
        Manager registers with Orchestrator and get a list of managers from Orchestrator.

    *fields*

        **response_fli**
            - string
            - b64 encoded serialized fli for the response to this request.

        **mainFLI**
            - string
            - b64 encoded serialized fli for the main channel for the manager.

    *see also*
        DDRegisterManagerResponse

        Refer to the cfs section for additional request message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDRegisterManager>`

#. **DDRegisterManagerResponse**

    *type enum*
        DD_REGISTER_MANAGER_RESPONSE

    *purpose*
        Provide the acknowledgement that the manager is registered and a list of managers. This serves as a
        synchronization point for client/manager interaction. Clients can request other manager's fli from the main manager assigned to them.

    *fields*

       **manager_id**
            - uint32
            - unique for this manager.

       **managers**
            - list
            - a list of b64 encoded serialized flis for the main channels of all managers.

    *see also*
        DDRegisterManager

        Refer to the cfs section for additional response message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDRegisterManagerResponse>`

#. **DDConnectToManager**

    *type enum*
        DD_CONNECT_TO_MANAGER

    *purpose*
        Obtain the manager mainFLI from the main manager so a client can attach to the manager.

    *fields*

        **client_id**
            - uint32
            - unique client id assigned by main manager.

        **manager_id**
            - uint32
            - the ID of the manager that client requests to connect to.

    *see also*
        DDConnectToManagerResponse

        Refer to the cfs section for additional request message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDConnectToManager>`

#. **DDConnectToManagerResponse**

    *type enum*
        DD_CONNECT_TO_MANAGER_RESPONSE

    *purpose*
        return the mainFLI of the manager whose ID was provided on the request.

    *fields*

        **mainFLI**
            - string
            - b64 encoded serialized fli for the main channel for the manager.

    *see also*
        DDConnectToManager

        Refer to the cfs section for additional request message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDConnectToManagerResponse>`


#. **DDRegisterClientID**

    *type enum*
        DD_REGISTER_CLIENT_ID

    *purpose*
        Register the client ID and associated client response fli with a manager so the
        response fli does not need to be included in future messages and client ID can be
        used instead.

    *fields*

        **client_id**
            - uint32
            - unique client id assigned by main manager.

        **response_fli**
            - string
            - b64 encoded serialized response fli for client requests.

        **buffered_response_fli**
            - string
            - b64 encoded serialized response fli for client requests.

    *see also*
        DDRegisterClientIDResponse, DDRegisterClient

        Refer to the cfs section for additional request message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDRegisterClientID>`

#. **DDRegisterClientIDResponse**

    *type enum*
        DD_REGISTER_CLIENT_ID_RESPONSE

    *purpose*
        Provide the acknowledgement that the client is registered with the manager.
        This serves as a synchronization point for client/manager interaction.

    *fields*

        **None other than the err field which will hold a dragon return code.**

    *see also*
        DDRegisterClientID, DDRegisterClient

        Refer to the cfs section for additional response message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDRegisterClientIDResponse>`

#. **DDDestroy**

    *type enum*
        DD_DESTROY

    *purpose*
        Sent by a client to the orchestrator to destroy the distributed dictionary.

    *fields*

        **client_id**
            - uint32
            - The client id of the requesting client.

        **response_fli**
            - string
            - b64 encoded serialized response fli.

    *see also*
        DDDestroyResponse, DDDestroyManager

        Refer to the cfs section for additional request message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDDestroy>`

#. **DDDestroyResponse**

    *type enum*
        DD_DESTROY_RESPONSE

    *purpose*
        Provide the acknowledgement that the distributed dictionary destruction has
        completed.

    *fields*

        **None other than the err field which will hold a dragon return code.**

    *see also*
        DDDestroy

        Refer to the cfs section for additional response message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDDestroyResponse>`

#. **DDDestroyManager**

    *type enum*
        DD_DESTROY_MANAGER

    *purpose*
        Sent by the orchestrator to destroy a distributed manager.

    *fields*

        **response_fli**
            - string
            - b64 encoded serialized response fli.

    *see also*
        DDDestroyManagerResponse, DDDestroy

        Refer to the cfs section for additional request message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDDestroyManager>`

#. **DDDestroyManagerResponse**

    *type enum*
        DD_DESTROY_MANAGER_RESPONSE

    *purpose*
        Provide the acknowledgement that the distributed dictionary manager destruction has
        completed.

    *fields*

        **None other than the err field which will hold a dragon return code.**

    *see also*
        DDDestroyManager

        Refer to the cfs section for additional response message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDDestroyManagerResponse>`

#. **DDPut**

    *type enum*
        DD_PUT

    *purpose*
        Sent by a client to put a key/value pair into the distributed dictionary. It is sent
        to a particular manager which is chosen by pre-hashing the key and dividing modulo the
        number of managers.

    *fields*

        **client_id**
            - uint32
            - The client id of the requesting client.

        **chkpt_id**
            - uint64
            - The checkpoint identifier for this operation.

        **persist**
            - bool
            - Persistent or non-persisting key.

        *NOTE* The key and value are written separately from the message using the fli api.

    *see also*
        DDPutResponse

        Refer to the cfs section for additional request message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDPut>`

#. **DDPutResponse**

    *type enum*
        DD_PUT_RESPONSE

    *purpose*
        Provide the acknowledgement that the distributed dictionary manager that the
        put has completed.

    *fields*

        **None other than the err field which will hold a dragon return code.**

    *see also*
        DDPut

        Refer to the cfs section for additional response message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDPutResponse>`

#. **DDGet**

    *type enum*
        DD_GET

    *purpose*
        Sent by a client to a manager to get a value for a key.

    *fields*

        **client_id**
            - uint32
            - The client id of the requesting client.

        **chkpt_id**
            - uint64
            - The checkpoint identifier for this operation.

        *NOTE* The key is written separately from the message using the fli api.

    *see also*
        DDGetResponse

        Refer to the cfs section for additional request message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDGet>`

#. **DDGetResponse**

    *type enum*
        DD_GET_RESPONSE

    *purpose*
        Provide the value for the associated key or an error code indicating what happened.

    *fields*

        **None other than the err field which will hold a dragon return code.**

        *NOTE* The value is written separately from the message using the fli api.

    *see also*
        DDGet

        Refer to the cfs section for additional response message fields.

    *implementation(s):* :func:`Python<dragon.infrastructure.messages.DDGetResponse>`

