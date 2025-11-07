#ifndef DRAGON_DICTIONARY_HPP
#define DRAGON_DICTIONARY_HPP

#include <string>
#include <cstring>
#include <vector>
#include <memory>
#include <iostream>
#include <dragon/ddict.h>
#include <dragon/global_types.h>
#include <dragon/utils.h>
#include <dragon/messages.hpp>
#include <dragon/exceptions.hpp>
#include <dragon/serializable.hpp>

/* used internally in the C++ client. Not for external use. */
typedef dragonError_t (*key_collector)(void* user_arg, dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout);
dragonError_t dragon_ddict_keys_vec(const dragonDDictDescr_t * dd_descr, key_collector function, void* key_collector_arg);
dragonError_t dragon_ddict_local_keys_vec(const dragonDDictDescr_t * dd_descr, key_collector function, void* key_collector_arg);
dragonError_t dragon_ddict_empty_managers_vec(const dragonDDictDescr_t * dd_descr, std::vector<uint64_t>& ids);
dragonError_t dragon_ddict_local_managers_vec(const dragonDDictDescr_t * dd_descr, std::vector<uint64_t>& ids);
dragonError_t dragon_ddict_persisted_ids_vec(const dragonDDictDescr_t * dd_descr, std::vector<uint64_t>& persisted_ids);

namespace dragon {

/**
 * @class DDict
 * @brief A Dragon Distributed Dictionary Client
 *
 * This class provides the functionality to attach to and interact
 * with a DDict. The DDict should be created from Python code. After it
 * is serialized, the serialized descriptor can then be passed to a C++
 * program/code via some means (e.g. a command-line argument). Then
 * it can be attached to from this C++ client. To fully interact with
 * a DDict from C++, Serializable keys and values must be defined. See
 * the Serializable abtract class for details on implementing one or more
 * of those classes.
 */
template  <class SerializableKey, class SerializableValue>
class DDict {
    /**
     * @class KeyRef
     * @brief Internal class for References to Keys within a DDict.
     *
     * This internal only class provides references to keys and a means
     * of looking up a value based on its key and a means of storing a
     * new value associated with a key.
     */
    class KeyRef
    {
        public:
        /**
         * @brief Construct a KeyRef from a Serializable Key and an
         * asociated DDict.
         */
        KeyRef(DDict& dict, SerializableKey& key) : mDict(dict), mKey(key) {}

        /**
         * @brief Store a new value for the given key reference.
         *
         * This stores a new value for the given key reference.
         *
         * @param value is the new Serializable value to be stored in the
         * DDict.
         */
        void operator= (SerializableValue& value) {
            dragonDDictRequestDescr_t req;
            dragonError_t err;
            dragonFLISendHandleDescr_t value_sendh;
            dragonFLISendHandleDescr_t key_sendh;

            err = dragon_ddict_create_request(&mDict.mCDict, &req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not create DDict put request.");

            err = dragon_ddict_request_key_sendh(&req, &key_sendh);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not access the request key send handle.");

            mKey.serialize(&key_sendh, KEY_HINT, true, mDict.mTimeout);

            err = dragon_ddict_put(&req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not send DDict put message.");

            err = dragon_ddict_request_value_sendh(&req, &value_sendh);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not access the request send handle.");

            value.serialize(&value_sendh, VALUE_HINT, false, mDict.mTimeout);

            err = dragon_ddict_finalize_request(&req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not finalize DDict put request.");
        }

        /**
         * @brief Returns the SerializableValue associated with a KeyRef.
         *
         * When a lookup occurs and the C++ code wants to cast it
         * (i.e. lookup the key in the DDict) this code takes care of the
         * lookup and returning the associated Serializable value.
         */
        operator SerializableValue () const {
            dragonDDictRequestDescr_t req;
            dragonError_t err;
            dragonFLISendHandleDescr_t key_sendh;
            dragonFLIRecvHandleDescr_t recvh;
            uint64_t hint;

            err = dragon_ddict_create_request(&mDict.mCDict, &req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not create DDict get request.");

            err = dragon_ddict_request_key_sendh(&req, &key_sendh);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not access the request send handle.");

            mKey.serialize(&key_sendh, KEY_HINT, true, mDict.mTimeout);

            err = dragon_ddict_get(&req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not send DDict get message.");

            err = dragon_ddict_request_recvh(&req, &recvh);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not access the request send handle.");

            SerializableValue value = SerializableValue::deserialize(&recvh, &hint, mDict.mTimeout);
            if (hint != VALUE_HINT)
                throw DragonError(DRAGON_INVALID_OPERATION, "The value hint when deserializing was not correct.");

            err = dragon_ddict_finalize_request(&req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not finalize DDict get request.");

            return value;
        }

        private:
        DDict& mDict;
        SerializableKey& mKey;
    };

    public:
    /**
     * @brief A Constructor for DDict
     *
     * This constructor takes a C DDict descriptor as an argument and
     * constructs a C++ DDict over the C descriptor. Not likely used,
     * it is provided since it is a way of moving to C++ from C. Using
     * this constructor will imply that operations on the DDict will
     * not timeout.
     *
     * @param cDict A C DDict descriptor.
     */
    DDict(dragonDDictDescr_t* cDict) {
        if (cDict == nullptr)
            throw DragonError(DRAGON_INVALID_ARGUMENT, "The cDict argument cannot be null and must point to an attached C ddict descriptor.");
        mCDict = *cDict;
        mDetachOnDestroy = false;
        mTimeout = nullptr;
    }

    /**
     * @brief A Constructor for DDict with a timeout
     *
     * This constructor takes a C DDict descriptor as an argument and
     * constructs a C++ DDict over the C descriptor. Not likely used,
     * it is provided since it is a way of moving to C++ from C.
     *
     * @param cDict A C DDict descriptor.
     * @param timeout A default timeout to be applied to operations.
     */
    DDict(dragonDDictDescr_t* cDict, const timespec_t* timeout) {
        if (cDict == nullptr)
            throw DragonError(DRAGON_INVALID_ARGUMENT, "The cDict argument cannot be null and must point to an attached C ddict descriptor.");
        mCDict = *cDict;
        mDetachOnDestroy = false;
        mTimeoutVal = *timeout;
        mTimeout = &mTimeoutVal;

        char * serialize_ddict = nullptr;
        dragonError_t err;
        err = dragon_ddict_serialize(&mCDict, (const char**)&serialize_ddict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not serialize DDict.");

        mSerialized = serialize_ddict;
        free(serialize_ddict);
    }

    /**
     * @brief Attach to a serialized DDict.
     *
     * This constructor will attach to a DDict using its
     * serialized descriptor as passed to it from Python or another
     * C++ client. A serialized DDict is a valid string variable
     * and can be passed as a string to other instances that want
     * to access the DDict and store values to it from C++.
     *
     * @param serialized_dict is a base64 encoded serialized descriptor.
     * @param timeout is a timeout to apply to all operations on the DDict.
     */
    DDict(const char* serialized_dict, const timespec_t* timeout) {
        dragonError_t err;
        mTimeout = nullptr;

        if (serialized_dict == nullptr) {
            string estr = "Cannot pass NULL serialized_dict to DDict attach.";
            throw DragonError(DRAGON_INVALID_ARGUMENT, estr.c_str());
        }

        mSerialized = serialized_dict;

        if (timeout != nullptr) {
            mTimeoutVal = *timeout;
            mTimeout = &mTimeoutVal;
        }

        err = dragon_ddict_attach(serialized_dict, &mCDict, mTimeout);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not attach to DDict.");

        mDetachOnDestroy = true;
    }

    /**
     * @brief The DDict Destructor
     *
     * The destructor is applied automatically when the object is declared
     * on the stack and gets called when delete is called on a heap allocated
     * DDict.
     */
    ~DDict() {
        dragonError_t err;

        if (mDetachOnDestroy) {
            err = dragon_ddict_detach(&mCDict);
            if (err != DRAGON_SUCCESS)
                cerr << "Error while destroying C++ DDict: " << dragon_get_rc_string(err) << " : " << dragon_getlasterrstr();
        }
    }

    // DDictKeyIterator& begin() {

    // }

    // DDictKeyIterator& end() {

    // }

    /**
     * @brief Return a serialized handle to the DDict.
     *
     * Return a Base64 encoded string that can be shared with other
     * clients that want to attach to the same dictionary.
     *
     * @returns A DDict serialized handle that can be handed off to other
     * clients.
     */
    const char* serialize() {
        return mSerialized.c_str();
    }

    /**
     * @brief Get the number of key/value pairs in the DDict.
     *
     * This method cannot be counted on to return an exact number
     * of key/value pairs. Due to the the nature of a parallel
     * distributed dictionary the value may have changed by the
     * time this method returns to the user.
     *
     * @returns The number of key/value pairs at a moment in time.
     */

    uint64_t size() {
        uint64_t len = 0;
        dragonError_t err;
        err = dragon_ddict_length(&mCDict, &len);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get DDict length.");
        return len;
    }

    /**
     * @brief Clear the DDict
     *
     * Removes all key/value pairs as it is executed from
     * all managers of the DDict. This does not mean that other
     * clients couldn't immediately start adding in key/value
     * pairs again.
     */
    void clear() {
        dragonError_t err;
        err = dragon_ddict_clear(&mCDict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not clear DDict.");
    }

    /**
     * @brief Get a KeyRef
     *
     * This method constructs a key reference which can then
     * be used in either a lookup of a value in the DDict or
     * or used to set a key/value pair in the DDict.
     */
    KeyRef operator[] (SerializableKey& key) {
        return DDict::KeyRef(*this, key);
    }

    /**
     * @brief Put a persistent key/value pair to the DDict
     *
     * Persistent key/value pairs persist across checkpoints.
     * These pairs remain in the DDict until explicitly deleted.
     */

    void pput(SerializableKey& key, SerializableValue& value) {
        dragonDDictRequestDescr_t req;
        dragonFLISendHandleDescr_t value_sendh;
        dragonFLISendHandleDescr_t key_sendh;
        dragonError_t err;

        err = dragon_ddict_create_request(&mCDict, &req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not create DDict persistent put request.");

        err = dragon_ddict_request_key_sendh(&req, &key_sendh);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not access the request key send handle.");

        key.serialize(&key_sendh, KEY_HINT, true, mTimeout);

        err = dragon_ddict_pput(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not send DDict persistent put message.");

        err = dragon_ddict_request_value_sendh(&req, &value_sendh);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not access the request send handle.");

        value.serialize(&value_sendh, VALUE_HINT, false, mTimeout);

        err = dragon_ddict_finalize_request(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not finalize DDict persistent put message.");
    }

    /**
     * @brief Check that the DDict contains the key.
     *
     * Returns true if the key is in the DDict. They key provided must be
     * serializable and the binary equivalent of a key in the DDict.
     *
     * @returns A bool to indicate membership.
     */
    bool contains(SerializableKey& key) {
        dragonDDictRequestDescr_t req;
        dragonFLISendHandleDescr_t key_sendh;
        dragonError_t err;
        bool result;

        err = dragon_ddict_create_request(&mCDict, &req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not create DDict contains request.");

        err = dragon_ddict_request_key_sendh(&req, &key_sendh);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not access the request send handle.");

        key.serialize(&key_sendh, KEY_HINT, true, mTimeout);

        err = dragon_ddict_contains(&req);
        if (err != DRAGON_SUCCESS && err != DRAGON_KEY_NOT_FOUND)
            throw DragonError(err, "Could not issue DDict contains.");

        result = err == DRAGON_SUCCESS? true : false;

        err = dragon_ddict_finalize_request(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not finalize the DDict Request.");

        return result;
    }

    /**
     * @brief Return which manager will hold this key should it be stored in the DDict.
     *
     * This does not check to see if the key is already in the DDict. It will tell you
     * which manager will be used to store this key. The manager_id will be
     * 0 <= manager_id < num_managers for the DDict.
     *
     * @param key A serializable key for the DDict
     * @returns the manager_id to be used for this key
     */
    uint64_t which_manager(SerializableKey& key) {
        dragonDDictRequestDescr_t req;
        dragonFLISendHandleDescr_t key_sendh;
        dragonError_t err;
        uint64_t manager_id;

        err = dragon_ddict_create_request(&mCDict, &req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not create DDict contains request.");

        err = dragon_ddict_request_key_sendh(&req, &key_sendh);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not access the request send handle.");

        key.serialize(&key_sendh, KEY_HINT, true, mTimeout);

        err = dragon_ddict_which_manager(&req, &manager_id);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get which manager.");

        err = dragon_ddict_finalize_request(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not finalize the DDict Request.");

        return manager_id;
    }

    /**
     * @brief Remove a key from the DDict
     *
     * Delete a key/value pair from the DDict. If the key does not exist a
     * DragonError is raised.
     *
     * @param A DDict Key
     * @throws DragonError if the key does not exist.
     */
    SerializableValue erase(SerializableKey& key) {
        dragonDDictRequestDescr_t req;
        dragonFLISendHandleDescr_t key_sendh;
        dragonFLIRecvHandleDescr_t recvh;
        dragonError_t err;
        uint64_t hint;

        err = dragon_ddict_create_request(&mCDict, &req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not create DDict erase request.");

        err = dragon_ddict_request_key_sendh(&req, &key_sendh);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not access the request send handle.");

        key.serialize(&key_sendh, KEY_HINT, true, mTimeout);

        err = dragon_ddict_pop(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not issue DDict pop.");

        err = dragon_ddict_request_recvh(&req, &recvh);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not access the request send handle.");

        SerializableValue value = SerializableValue::deserialize(&recvh, &hint, mTimeout);
        if (hint != VALUE_HINT)
            throw DragonError(DRAGON_INVALID_OPERATION, "The value hint when deserializing was not correct.");

        err = dragon_ddict_finalize_request(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not finalize DDict pop request.");

        return value;
    }

    static dragonError_t client_key_collector(void* user_arg, dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
        std::vector<SerializableKey>* key_vec = (std::vector<SerializableKey>*)user_arg;
        key_vec->push_back(SerializableKey::deserialize(recvh, arg, timeout));
        if (*arg != KEY_HINT)
            throw DragonError(DRAGON_FAILURE, "Received unexpected arg value.");

        return DRAGON_SUCCESS;
    }

    /**
     * @brief Return the keys of the DDict
     *
     * This returns a vector of keys of the DDict. The RVO
     * (Return Value Optimization) that C++ provides you can
     * assign this result to a stack allocated vector and it
     * will be initialized in place. Note that you get all the keys
     * of the DDict at once.
     *
     * @returns A vector of all DDict keys
     */
    std::vector<SerializableKey> keys() {
        dragonError_t err;
        size_t idx;
        std::vector<SerializableKey> ret_val;

        err = dragon_ddict_keys_vec(&mCDict, DDict::client_key_collector, &ret_val);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could get DDict keys vector.");

        return ret_val;
    }

    /**
     * @brief Increment the client checkpoint id
     *
     * This is a local only operation that increments the client's checkpoint id.
     * Subsequent operations will work with the next checkpoint in any manager's
     * working set.
     */
    void checkpoint() {
        dragonError_t err;
        err = dragon_ddict_checkpoint(&mCDict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not do DDict checkpoint.");
    }

    /**
     * @brief Decrement the client checkpoint id
     *
     * This is a local only operation that decrements the client's checkpoint id.
     * Subsequent operations will work with the previous checkpoint in any manager's
     * working set.
     */

    void rollback() {
        dragonError_t err;
        err = dragon_ddict_rollback(&mCDict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not do DDict rollback.");
    }

    /**
     * @brief Go to the latest checkpoint
     *
     * This will query all managers to find the latest checkpoint id and
     * then set this client's checkpoint id to it.
     */
    void sync_to_newest_checkpoint() {
        dragonError_t err;
        err = dragon_ddict_sync_to_newest_checkpoint(&mCDict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not sync to newest DDict checkpoint.");
    }

    /**
     * @brief Retrieve the checkpoint id of this client
     *
     * Returns the client's checkpoint id.
     *
     * @returns The client's current checkpoint id.
     */
    uint64_t checkpoint_id() {
        dragonError_t err;
        uint64_t chkpt_id = 0;
        err = dragon_ddict_checkpoint_id(&mCDict, &chkpt_id);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get the DDict checkpoint id.");
        return chkpt_id;
    }

    /**
     * @brief Set the client's checkpoint id
     *
     * This allows the checkpoint id of the client to be set to a particular value.
     * The chosen checkpoint id if older than the working set will be rejected on
     * DDict operations. If newer than any checkpoint in the working set, the
     * result will be to advance the working set in affected managers.
     *
     * @returns The checkpoint id that was set.
     */
    uint64_t set_checkpoint_id(uint64_t chkpt_id) {
        dragonError_t err;
        err = dragon_ddict_set_checkpoint_id(&mCDict, chkpt_id);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not set the DDict checkpoint id.");
        return chkpt_id;
    }

    /**
     * @brief Get a manager id for a local manager
     *
     * Returns the manager id of a local manager. if none exist, a DragonError
     * will be thrown. If more than one exists, one will be chosen at random.
     *
     * @returns 0 <= manager_id < number of managers.
     */
    uint64_t local_manager() {
        dragonError_t err;
        uint64_t manager_id = 0;
        err = dragon_ddict_local_manager(&mCDict, &manager_id);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get local DDict manager id.");
        return manager_id;
    }

    /**
     * @brief Return a manager id
     *
     * Calling this will always return a manager id that can be used in
     * certain operations. It will be a local manager if one exists. If
     * the node has no local managers, then it will be a random manager
     * from a different node.
     *
     * @returns 0 <= manager_id < number of managers.
     */
    uint64_t main_manager() {
        dragonError_t err;
        uint64_t manager_id = 0;
        err = dragon_ddict_main_manager(&mCDict, &manager_id);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get client's DDict main manager.");
        return manager_id;
    }

    /**
     * @brief Get a manager directed copy of the DDict
     *
     * Calling this will return a new DDict reference that
     * targets only the given manager. All put and get operations will be
     * directed at this manager only. All other operations that interact
     * with the DDict will also only query or modify the given manager.
     *
     * @param manager_id The manager id of the chosen manager.
     * @returns A new DDict reference that targets only the chosen manager.
     */
    DDict manager(uint64_t manager_id) {
        dragonError_t err;
        dragonDDictDescr_t newCDict;
        err = dragon_ddict_manager(&mCDict, &newCDict, manager_id);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not select a DDict manager.");
        return DDict(&newCDict, mTimeout);
    }

    /**
     * @brief Get the empty managers of the DDict
     *
     * Calling this returns a vector of all empty manager
     * ids in this DDict.
     *
     * @returns A vector of integer manager ids.
     */
    std::vector<uint64_t> empty_managers() {
        dragonError_t err;
        std::vector<uint64_t> ids;

        err = dragon_ddict_empty_managers_vec(&mCDict, ids);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get the DDict empty managers.");

        return ids;
    }

    /**
     * @brief Get the local managers of the DDict
     *
     * Calling this returns a vector of all local manager
     * ids in this DDict.
     *
     * @returns A vector of integer manager ids.
     */
    std::vector<uint64_t> local_managers() {
        dragonError_t err;
        std::vector<uint64_t> ids;

        err = dragon_ddict_local_managers_vec(&mCDict, ids);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get the DDict local managers.");

        return ids;
    }

    /**
     * @brief Return a vector of all keys stored on this node
     *
     * Calling this queries only local managers to find the keys
     * that are stored on this node. Using this can help optimize code
     * around minimizing data movement across the network.
     *
     * @returns A vector of all local keys.
     */
    std::vector<SerializableKey> local_keys() {
        dragonError_t err;
        size_t idx;
        std::vector<SerializableKey> ret_val;

        err = dragon_ddict_local_keys_vec(&mCDict, DDict::client_key_collector, &ret_val);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get the DDict local keys.");

        return ret_val;
    }

    /**
     * @brief Duplicate one or more DDicts
     *
     * Calling this assumes that all DDicts in the vector are exact duplicates of
     * each other. Some managers within the set of duplicates may be empty due
     * to a previous error and a restart. If this occurs, then this synchronize
     * method will use non-empty managers to re-populate the empty managers in
     * these parallel dictionaries.
     *
     * @param serialized_ddicts A list of the serialized descriptors of the
     * parallel dictionaries. Each parallel dictionary must have the same number
     * of managers and same data across all of them.
     */
    static void synchronize(std::vector<std::string>& serialized_ddicts) {

        dragonError_t err;

        // convert vector to char array
        size_t num_ser_ddicts = serialized_ddicts.size();
        char** ser_ddicts_arr = new char*[num_ser_ddicts];
        for (size_t i = 0; i < num_ser_ddicts; i++) {
            size_t strLen = strlen(serialized_ddicts[i].c_str()) + 1;
            ser_ddicts_arr[i] = new char[strLen];
            memcpy((void*)ser_ddicts_arr[i], serialized_ddicts[i].c_str(), strLen);
        }

        err = dragon_ddict_synchronize((const char**)ser_ddicts_arr, num_ser_ddicts, nullptr);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not synchronize the DDict with another.");

        for (size_t i=0 ; i<num_ser_ddicts ; i++)
            delete ser_ddicts_arr[i];
        delete[] ser_ddicts_arr;
    }

    /**
     * @brief Copy the current DDict to one or more existing DDicts.
     *
     * This clones the current DDict to each of the serialized
     * descriptors that is passed to it. It will not create
     * new DDicts. It will clone the contents of this DDict to the
     * DDicts passed in this vector.
     *
     * @param serialized_ddicts A vector of DDicts that will get cloned into
     * from this one.
     *
     */
    void clone(std::vector<std::string>& serialized_ddicts) {
        dragonError_t err;

        if (serialized_ddicts.size() == 0)
            throw DragonError(DRAGON_INVALID_ARGUMENT, "Number of serialized dictionaries must be greater than 0.");

        // convert vector to char array
        size_t num_ser_ddicts = serialized_ddicts.size();
        char** ser_ddicts_arr = new char*[num_ser_ddicts];
        for (size_t i = 0; i < num_ser_ddicts; i++) {
            size_t strLen = strlen(serialized_ddicts[i].c_str()) + 1;
            ser_ddicts_arr[i] = new char[strLen];
            memcpy((void*)ser_ddicts_arr[i], serialized_ddicts[i].c_str(), strLen);
        }

        err = dragon_ddict_clone(&mCDict, (const char**)ser_ddicts_arr, num_ser_ddicts);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not clone the DDict.");

        for (size_t i=0 ; i<num_ser_ddicts ; i++)
            delete ser_ddicts_arr[i];
        delete[] ser_ddicts_arr;
    }

    /**
     * @brief Get the frozen state of the DDict.
     *
     * Calling this retrieve the frozen state of the dictionary.
     *
     * @returns A bool to indicate whether the DDict is frozen or not.
     */
    bool is_frozen() {
        dragonError_t err;
        bool frozen;
        err = dragon_ddict_is_frozen(&mCDict, &frozen);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get frozen state of the DDict.");
        return frozen;
    }

    /**
     * @brief Freeze the DDict.
     *
     * Freeze the dictionary turns it to read-only mode. Freezing the DDict would
     * gain better performance for reading keys.
     */
    void freeze() {
        dragonError_t err;
        err = dragon_ddict_freeze(&mCDict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not freeze the DDict.");
    }

    /**
     * @brief Unfreeze the DDict.
     *
     * Unfreeze the dictionary turns off the read-only mode.
     */
    void unfreeze() {
        dragonError_t err;
        err = dragon_ddict_unfreeze(&mCDict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not unfreeze the DDict.");
    }

    /**
     * @brief Start batch put.
     *
     * @param persist A bool to indicate whether the keys to be put in this batch
     * are persistent or not. Using batch put to get better performance especially
     * when writing a large number of keys.
     */
    void start_batch_put(bool persist) {
        dragonError_t err;
        err = dragon_ddict_start_batch_put(&mCDict, persist);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not start batch put.");
    }

    /**
     * @brief End batch put.
     *
     * Calling this to complete the batch put.
     */
    void end_batch_put() {
        dragonError_t err;
        err = dragon_ddict_end_batch_put(&mCDict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not end batch put.");
    }

    /**
     * @brief Broadcast put.
     *
     * Broadcast the key to every DDict manager.
     *
     * @param key A serializable key for the DDict.
     *
     * @param value A serializable value for the DDict.
     */
    void bput(SerializableKey& key, SerializableValue& value) {
        dragonDDictRequestDescr_t req;
        dragonFLISendHandleDescr_t value_sendh;
        dragonFLISendHandleDescr_t key_sendh;
        dragonError_t err;

        err = dragon_ddict_create_request(&mCDict, &req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not create DDict bput request.");

        err = dragon_ddict_request_key_sendh(&req, &key_sendh);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not access the request key send handle.");

        key.serialize(&key_sendh, KEY_HINT, true, mTimeout);

        err = dragon_ddict_bput(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not send DDict bput message.");

        err = dragon_ddict_request_value_sendh(&req, &value_sendh);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not access the request send handle.");

        value.serialize(&value_sendh, VALUE_HINT, false, mTimeout);

        err = dragon_ddict_finalize_request(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not finalize DDict bput message.");
    }

    /**
     * @brief Broadcast get the key.
     *
     * Reading the key that was broadcasted from the main manager of the DDict.
     *
     * @param key A serializable key for the DDict. This key should be written through broadcast put.
     */
    SerializableValue bget(SerializableKey& key) const {
        dragonDDictRequestDescr_t req;
        dragonError_t err;
        dragonFLISendHandleDescr_t key_sendh;
        dragonFLIRecvHandleDescr_t recvh;
        uint64_t hint;

        err = dragon_ddict_create_request(&mCDict, &req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not create DDict bget request.");

        err = dragon_ddict_request_key_sendh(&req, &key_sendh);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not access the request send handle.");

        key.serialize(&key_sendh, KEY_HINT, true, mTimeout);

        err = dragon_ddict_bget(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not send DDict bget message.");

        err = dragon_ddict_request_recvh(&req, &recvh);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not access the request send handle.");

        SerializableValue value = SerializableValue::deserialize(&recvh, &hint, mTimeout);
        if (hint != VALUE_HINT)
            throw DragonError(DRAGON_INVALID_OPERATION, "The value hint when deserializing was not correct.");

        err = dragon_ddict_finalize_request(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not finalize DDict bget request.");

        return value;
    }

    /**
     * @brief Advance to next available persisted checkpoint.
     *
     * Calling this to advance to next available persisted checkpoint. This operation is for read only mode.
     */
    void advance() {
        dragonError_t err = dragon_ddict_advance(&mCDict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not advance checkpoint.");
    }

    /**
     * @brief Persist current checkpoint to disk.
     *
     * Calling this to persist current checkpoint to disk.
     */
    void persist() {
        dragonError_t err = dragon_ddict_persist(&mCDict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not persist checkpoint.");
    }

    /**
     * @brief Restore from a persisted checkpoint.
     *
     * Restore from the persisted checkpoint.
     *
     * @param chkpt ID of the checkpoint to restore from.
     */
    void restore(uint64_t chkpt) {
        dragonError_t err = dragon_ddict_restore(&mCDict, chkpt);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not restore checkpoint.");
    }

    /**
     * @brief Get a list of persisted checkpoint IDs.
     *
     * Get a list of persisted checkpoint IDs.
     *
     * @returns A vector of available persisted IDs in the DDict.
     */
    std::vector<uint64_t> persisted_ids() {
        dragonError_t err;
        std::vector<uint64_t> chkptIDs;

        err = dragon_ddict_persisted_ids_vec(&mCDict, chkptIDs);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get the persisted checkpoint IDs.");

        return chkptIDs;
    }

    /**
     * @brief Get the number of on-node key/value pairs in the DDict.
     *
     * This method cannot be counted on to return an exact number
     * of on-node key/value pairs. Due to the the nature of a parallel
     * distributed dictionary the value may have changed by the
     * time this method returns to the user.
     *
     * @returns The number of on-node key/value pairs at a moment in time.
     */
    uint64_t local_size() {
        uint64_t len = 0;
        dragonError_t err;
        err = dragon_ddict_local_length(&mCDict, &len);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get DDict local length.");
        return len;
    }

    /*
        This is for debugging purpose only.
    */
    uint64_t _clientID() {
        dragonError_t err;
        uint64_t client_id = 0;
        err = _dragon_ddict_client_ID(&mCDict, &client_id);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get the DDict client id.");
        return client_id;
    }

    private:
    bool mDetachOnDestroy;
    dragonDDictDescr_t mCDict;
    std::string mSerialized;
    timespec_t* mTimeout; /* to be used on all timeout operations. */
    timespec_t mTimeoutVal;

};

}
#endif