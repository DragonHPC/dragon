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

/************************************************************************************************
 * @brief An abstract base class for deriving key and value classes for a Dragon
 * Distributed Dictionary.
 *
 * Classes derived from this DDictSerializable are
 * passed to the DDict template to be used by the DDict for serializing and
 * deserializing keys and values.
 * */
class DDictSerializable {
    public:
    /* This method should be overridden in the implementing derived class. It should
       write the bytes of the serialized object to the request descriptor req. */
    virtual void serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) = 0;

    /* This method should be overridden in the implementing derived class. It may
       throw a DragonError exception when a byte stream is not deserializable.
       The deserialize method should return once it has read the serialized object's bytes. */
    virtual void deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) = 0;

    /* This function should be defined at least for keys within the ddict. The
       create function is passed bytes which should then be used in a newly
       constructed object which is returned by create. The data passed to the
       create function is owned by the create function call and it should be
       freed when no longer needed. This is a static function that
       creates a new instance of the object and deserializes the data passed to
       it. The create function is a factory for new DDictSerializable objects. */
    // static DDictSerializable create(size_t num_bytes, uint8_t* data) {}

};

/* used internally in the C++ client. */
dragonError_t dragon_ddict_keys_vec(const dragonDDictDescr_t * dd_descr, std::vector<dragonDDictKey_t*>& keys);

dragonError_t dragon_ddict_local_keys_vec(const dragonDDictDescr_t * dd_descr, std::vector<dragonDDictKey_t*>& keys);

dragonError_t dragon_ddict_empty_managers_vec(const dragonDDictDescr_t * dd_descr, std::vector<uint64_t>& ids);

dragonError_t dragon_ddict_local_managers_vec(const dragonDDictDescr_t * dd_descr, std::vector<uint64_t>& ids);

// class DDictKeyIterator {
//     public:
//     DDictKeyIterator(DDict& dict);
//     operator=
//     operator!=
//     operator++
//     // DDict<SerializableInt>::iterator it;
//     // for (it = dd.begin(); it != dd.end(); it++)
// };

// Add a hash function here for keys. Then make a function template that given
// just the key and value classes supplies nullptr as the third argument.
template  <class DDictSerializableKey, class DDictSerializableValue>
class DDict {
    class KVPair
    {
        public:
        KVPair(DDict& dict, DDictSerializableKey& key) : mDict(dict), mKey(key) {}

        void operator= (DDictSerializableValue& value) {
            dragonDDictRequestDescr_t req;
            dragonError_t err;

            err = dragon_ddict_create_request(&mDict.mCDict, &req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not create DDict put request.");

            mKey.serialize(&req, mDict.mTimeout);

            err = dragon_ddict_put(&req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not send DDict put message.");

            value.serialize(&req, mDict.mTimeout);

            err = dragon_ddict_finalize_request(&req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not finalize DDict put request.");
        }

        operator DDictSerializableValue () const {
            dragonDDictRequestDescr_t req;
            dragonError_t err;
            DDictSerializableValue value;

            err = dragon_ddict_create_request(&mDict.mCDict, &req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not create DDict get request.");

            mKey.serialize(&req, mDict.mTimeout);

            err = dragon_ddict_get(&req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not send DDict get message.");

            value.deserialize(&req, mDict.mTimeout);

            err = dragon_ddict_finalize_request(&req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not finalize DDict get request.");

            return value;
        }

        private:
        DDict& mDict;
        DDictSerializableKey& mKey;
    };

    public:
    DDict(dragonDDictDescr_t* cDict) {
        if (cDict == nullptr)
            throw DragonError(DRAGON_INVALID_ARGUMENT, "The cDict argument cannot be null and must point to an attached C ddict descriptor.");
        mCDict = *cDict;
        mDetachOnDestroy = false;
    }

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

    // Destructor detaches client from ddict
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

    const char* serialize() {
        return mSerialized.c_str();
    }

    uint64_t size() {
        uint64_t len = 0;
        dragonError_t err;
        err = dragon_ddict_length(&mCDict, &len);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get DDict length.");
        return len;
    }

    void clear() {
        dragonError_t err;
        err = dragon_ddict_clear(&mCDict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not clear DDict.");
    }

    KVPair operator[] (DDictSerializableKey& key) {
        return DDict::KVPair(*this, key);
    }

    void pput(DDictSerializableKey& key, DDictSerializableValue& value) {
        dragonDDictRequestDescr_t req;
        dragonError_t err;

        err = dragon_ddict_create_request(&mCDict, &req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not create DDict persistent put request.");

        key.serialize(&req, mTimeout);

        err = dragon_ddict_pput(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not send DDict persistent put message.");

        value.serialize(&req, mTimeout);

        err = dragon_ddict_finalize_request(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not finalize DDict persistent put message.");
    }

    bool contains(DDictSerializableKey& key) {
        dragonDDictRequestDescr_t req;
        dragonError_t err;

        err = dragon_ddict_create_request(&mCDict, &req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not create DDict contains request.");

        key.serialize(&req, mTimeout);

        err = dragon_ddict_contains(&req);
        if (err != DRAGON_SUCCESS && err != DRAGON_KEY_NOT_FOUND)
            throw DragonError(err, "Could not issue DDict contains.");
        return err == DRAGON_SUCCESS? true : false;
    }

    DDictSerializableValue erase(DDictSerializableKey& key) {
        dragonDDictRequestDescr_t req;
        dragonError_t err;
        DDictSerializableValue value;

        err = dragon_ddict_create_request(&mCDict, &req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not create DDict erase request.");

        key.serialize(&req, mTimeout);

        err = dragon_ddict_pop(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not issue DDict pop.");

        value.deserialize(&req, mTimeout);

        err = dragon_ddict_finalize_request(&req);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not finalize DDict pop request.");
        return value;
    }

    std::vector<DDictSerializableKey*> keys() {
        dragonError_t err;
        size_t idx;
        std::vector<dragonDDictKey_t*> key_vec;
        std::vector<DDictSerializableKey*> ret_val;

        err = dragon_ddict_keys_vec(&mCDict, key_vec);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could get DDict keys vector.");

        for (idx=0; idx<key_vec.size(); idx++) {
            ret_val.push_back(DDictSerializableKey::create(key_vec[idx]->num_bytes, key_vec[idx]->data));
        }

        return ret_val;
    }

    void checkpoint() {
        dragonError_t err;
        err = dragon_ddict_checkpoint(&mCDict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not do DDict checkpoint.");
    }

    void rollback() {
        dragonError_t err;
        err = dragon_ddict_rollback(&mCDict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not do DDict rollback.");
    }

    void sync_to_newest_checkpoint() {
        dragonError_t err;
        err = dragon_ddict_sync_to_newest_checkpoint(&mCDict);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not sync to newest DDict checkpoint.");
    }

    uint64_t checkpoint_id() {
        dragonError_t err;
        uint64_t chkpt_id = 0;
        err = dragon_ddict_checkpoint_id(&mCDict, &chkpt_id);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get the DDict checkpoint id.");
        return chkpt_id;
    }

    uint64_t set_checkpoint_id(uint64_t chkpt_id) {
        dragonError_t err;
        err = dragon_ddict_set_checkpoint_id(&mCDict, chkpt_id);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not set the DDict checkpoint id.");
        return chkpt_id;
    }

    uint64_t local_manager() {
        dragonError_t err;
        uint64_t manager_id = 0;
        err = dragon_ddict_local_manager(&mCDict, &manager_id);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get local DDict manager id.");
        return manager_id;
    }

    uint64_t main_manager() {
        dragonError_t err;
        uint64_t manager_id = 0;
        err = dragon_ddict_main_manager(&mCDict, &manager_id);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get client's DDict main manager.");
        return manager_id;
    }

    DDict manager(uint64_t manager_id) {
        dragonError_t err;
        dragonDDictDescr_t newCDict;
        err = dragon_ddict_manager(&mCDict, &newCDict, manager_id);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not select a DDict manager.");
        return DDict(&newCDict, mTimeout);
    }

    std::vector<uint64_t> empty_managers() {
        dragonError_t err;
        std::vector<uint64_t> ids;

        err = dragon_ddict_empty_managers_vec(&mCDict, ids);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get the DDict empty managers.");

        return ids;
    }

    std::vector<uint64_t> local_managers() {
        dragonError_t err;
        std::vector<uint64_t> ids;

        err = dragon_ddict_local_managers_vec(&mCDict, ids);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get the DDict local managers.");

        return ids;
    }

    std::vector<DDictSerializableKey*> local_keys() {
        dragonError_t err;
        size_t idx;
        std::vector<dragonDDictKey_t*> key_vec;
        std::vector<DDictSerializableKey*> ret_val;

        err = dragon_ddict_local_keys_vec(&mCDict, key_vec);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not get the DDict local keys.");

        for (idx=0; idx<key_vec.size(); idx++) {
            ret_val.push_back(DDictSerializableKey::create(key_vec[idx]->num_bytes, key_vec[idx]->data));
        }

        return ret_val;
    }

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

#endif