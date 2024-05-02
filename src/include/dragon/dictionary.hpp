#ifndef DRAGON_DICTIONARY_HPP
#define DRAGON_DICTIONARY_HPP

#include <string>
#include <cstring>
#include <dragon/ddict.h>
#include <dragon/global_types.h>
#include <dragon/utils.h>
#include <dragon/messages.hpp>

class DDictSerializable {
    virtual void serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) = 0;
    virtual void deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) = 0;
};

template  <typename DDictSerializableKey, typename DDictSerializableValue>
class DDict {
    class KVPair
    {
        public:
        KVPair(DDict& dict, DDictSerializableKey& key) : dict(dict), key(key) {}

        void operator= (DDictSerializableValue& value) {
            dragonDDictRequestDescr_t req;
            dragonError_t err;

            err = dragon_ddict_create_request(&this->dict.c_dict, &req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, dragon_getlasterrstr());

            key.serialize(&req, this->dict.timeout);

            err = dragon_ddict_put(&req, this->dict.timeout);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, dragon_getlasterrstr());

            value.serialize(&req, this->dict.timeout);

            err = dragon_ddict_finalize_request(&req, this->dict.timeout);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, dragon_getlasterrstr());
        }

        operator DDictSerializableValue () const {
            dragonDDictRequestDescr_t req;
            dragonError_t err;
            DDictSerializableValue value;

            err = dragon_ddict_create_request(&this->dict.c_dict, &req);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, dragon_getlasterrstr());

            key.serialize(&req, this->dict.timeout);

            err = dragon_ddict_get(&req, this->dict.timeout);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, dragon_getlasterrstr());

            value.deserialize(&req, this->dict.timeout);

            err = dragon_ddict_finalize_request(&req, this->dict.timeout);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, dragon_getlasterrstr());

            return value;
        }

        private:
        DDict& dict;
        DDictSerializableKey& key;
    };

    public:
    DDict(const char* serialized_dict, const timespec_t* timeout) {
        dragonError_t err;
        dragonDDictSerial_t ser;
        this->timeout = NULL;

        if (serialized_dict == NULL) {
            string estr = "Cannot pass NULL serialized_dict to DDict attach.";
            throw DragonError(DRAGON_INVALID_ARGUMENT, estr.c_str());
        }

        this->serialized = serialized_dict;

        if (timeout != NULL) {
            timeout_val = *timeout;
            this->timeout = &this->timeout_val;
        }

        ser.data = dragon_base64_decode(serialized_dict, &ser.len);

        err = dragon_ddict_attach(&ser, &this->c_dict, this->timeout);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, dragon_getlasterrstr());

        err = dragon_ddict_serial_free(&ser);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, dragon_getlasterrstr());
    }

    char* serialize() {
        return this->serialized.c_str();
    }

    KVPair operator[] (DDictSerializableKey& key) {
        return DDict::KVPair(*this, key);
    }

    private:
    dragonDDictDescr_t c_dict;
    std::string serialized;
    timespec_t* timeout; /* to be used on all timeout operations. */
    timespec_t timeout_val;

};

#endif