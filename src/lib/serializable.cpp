#include <string>
#include <cstring>
#include <sstream>
#include <iostream>
#include <utility>
#include <map>
#include <functional>
#include <dragon/serializable.hpp>

namespace dragon {

std::map<int, Serializable::DeserializeFn> Serializable::sDeserializers = {
    {DragonSerType::SERTYPE_STR,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableString::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_INT,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableInt::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_DOUBLE,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableDouble::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_INTVECTOR,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableIntVector::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_DOUBLEVECTOR,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableDoubleVector::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_INTMATRIX,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(Serializable2DIntMatrix::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_DOUBLEMATRIX,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(Serializable2DDoubleMatrix::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_BYTEBUFFER,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableByteBuffer::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_DOUBLENDARRAY,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableDoubleNDArray::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_FLOATNDARRAY,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableFloatNDArray::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_INTNDARRAY,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableIntNDArray::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_LONGNDARRAY,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableLongNDArray::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_QUEUE,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableQueue<Serializable>::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_DDICT,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableDDict<Serializable, Serializable>::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_BARRIER,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableBarrier::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_SEMAPHORE,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) {
            return Serializable(SerializableSemaphore::deserialize(h, a, t));
        }},
};

/**************************************************************/
/*********     SerializableBase Code                  *********/
/**************************************************************/
SerializableBase::~SerializableBase() {}

void SerializableBase::serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const {
    throw DragonError(DRAGON_INVALID_ARGUMENT, "Cannot serialize a base SerializableBase object.");
}

int SerializableBase::type_id() const {
    throw DragonError(DRAGON_INVALID_ARGUMENT, "Cannot get type of base SerializableBase object.");
}

SerializableBase SerializableBase::deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
        throw DragonError(DRAGON_INVALID_OPERATION, "This class should not be instantiated. Inherit from SerializableBase instead.");
}

/**************************************************************/
/*********     SerializableString Implementation      *********/
/**************************************************************/

SerializableString::SerializableString() = default;

SerializableString::SerializableString(std::string x): mVal(x) {}

void SerializableString::serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const {
    dragonError_t err;
    size_t len = mVal.size();
    err = dragon_fli_send_bytes(sendh, sizeof(len), (uint8_t*)&len, arg, buffer, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not serialize the SerializableString length.");

    err = dragon_fli_send_bytes(sendh, mVal.size(), (uint8_t*)&mVal[0], arg, buffer, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not serialize the SerializableString data.");
}

SerializableString SerializableString::deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
    dragonError_t err = DRAGON_SUCCESS;
    size_t len = 0;
    size_t actual_size;
    char* val;

    err = dragon_fli_recv_bytes_into(recvh, sizeof(size_t), &actual_size, (uint8_t*)&len, arg, timeout);
    if (err == DRAGON_TIMEOUT)
        throw TimeoutError(err, "Operation timeout.");

    if (err == DRAGON_EOT)
        throw EmptyError(err, "EOT of stream");

    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not get the string length while deserializing");

    if (actual_size != sizeof(size_t))
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The size of the length was not correct.");

    err = dragon_fli_recv_bytes(recvh, len, &actual_size, (uint8_t**)&val, arg, timeout);
    if (err == DRAGON_TIMEOUT)
        throw TimeoutError(err, "Operation timeout.");

    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "There was an unexpected error while deserializing the string.");

    if (actual_size != len)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The size of the string was not correct.");

    return SerializableString(std::string(val, actual_size)); //RVO
}

int SerializableString::type_id() const {
        return DragonSerType::SERTYPE_STR;
}

std::string SerializableString::val() const {return mVal;}

/**************************************************************/
/*********    SerializableByteBuffer Implementation   *********/
/**************************************************************/

SerializableByteBuffer::SerializableByteBuffer(): mSize(0), mPtr(nullptr) {}

SerializableByteBuffer::SerializableByteBuffer(size_t size, uint8_t* ptr): mSize(size), mPtr(ptr) {}

void SerializableByteBuffer::serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const {
    dragonError_t err;
    size_t size = mSize;
    err = dragon_fli_send_bytes(sendh, sizeof(size_t), (uint8_t*)&size, arg, buffer, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not serialize the SerializableByteBuffer size.");

    err = dragon_fli_send_bytes(sendh, mSize, mPtr, arg, buffer, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not serialize the SerializableByteBuffer data.");
}

SerializableByteBuffer SerializableByteBuffer::deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
    dragonError_t err = DRAGON_SUCCESS;
    size_t size = 0;
    size_t actual_size;
    uint8_t* data;

    err = dragon_fli_recv_bytes_into(recvh, sizeof(size_t), &actual_size, (uint8_t*)&size, arg, timeout);
    if (err == DRAGON_TIMEOUT)
        throw TimeoutError(err, "Operation timeout.");

    if (err == DRAGON_EOT)
        throw EmptyError(err, "EOT of stream");

    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not get the buffer size while deserializing");

    if (actual_size != sizeof(size_t))
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The length of the size value was not correct.");

    err = dragon_fli_recv_bytes(recvh, size, &actual_size, (uint8_t**)&data, arg, timeout);
    if (err == DRAGON_TIMEOUT)
        throw TimeoutError(err, "Operation timeout.");

    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "There was an unexpected error while deserializing the byte buffer.");

    if (actual_size != size)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The size of the buffer was not correct.");

    return SerializableByteBuffer(actual_size, data);
}

uint8_t* SerializableByteBuffer::getPtr() const {
    return mPtr;
}

size_t SerializableByteBuffer::getSize() const {
    return mSize;
}

int SerializableByteBuffer::type_id() const {
    return DragonSerType::SERTYPE_BYTEBUFFER;
}

/**************************************************************/
/*********   SerializableNDArray Support Functions   ***********/
/**************************************************************/

int ndarray_unique_id(dragonDDictDescr_t* ddict, timespec_t* timeout) {
    dragonDDictRequestDescr_t req;
    dragonFLISendHandleDescr_t key_sendh;
    dragonFLIRecvHandleDescr_t recvh;
    dragonError_t err;
    uint64_t hint;
    int val=1;

    if (ddict == nullptr) {
        string estr = "Cannot pass NULL ddict to ndarray_unique_id.";
        throw DragonError(DRAGON_INVALID_ARGUMENT, estr.c_str());
    }

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not create dragon_ddict_fetch_add request.");

    err = dragon_ddict_request_key_sendh(&req, &key_sendh);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not access the request send handle.");

    /* xkey will be serializable and deserializable in both C++ and Python */
    SerializableString xkey("dragon_serializable_ndarray");
    xkey.serialize(&key_sendh, KEY_HINT, true, timeout);

    err = dragon_ddict_fetch_add(&req, val);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not issue the dragon_ddict_fetch_add command.");

    err = dragon_ddict_request_recvh(&req, &recvh);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not access the request send handle.");

    SerializableInt value = Serializable::deserialize(&recvh, &hint, timeout);
    if (hint != VALUE_HINT)
        throw DragonError(DRAGON_INVALID_OPERATION, "The value hint when deserializing was not correct.");

    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not finalize the dragon_ddict_fetch_add request.");

    return value.val();
}

void ndarray_attach(dragonDDictDescr_t* ddict, const char* serialized_dict, timespec_t* timeout) {
    dragonError_t err;

    if (serialized_dict == nullptr) {
        string estr = "Cannot pass NULL serialized_dict to ndarray_attach.";
        throw DragonError(DRAGON_INVALID_ARGUMENT, estr.c_str());
    }

    err = dragon_ddict_attach(serialized_dict, ddict, timeout);
    if (err != DRAGON_SUCCESS) {
        string traceback= dragon_getlasterrstr();
        string message = "Could not attach to DDict in ndarray_attach.\n";
        message = message + traceback;

        throw DragonError(err, message.c_str());
    }
}

void ndarray_detach(dragonDDictDescr_t* ddict, const char* /*serialized_dict*/) {
    dragonError_t err;

    if (ddict == nullptr) {
        string estr = "Cannot pass NULL ddict to ndarray_detach.";
        throw DragonError(DRAGON_INVALID_ARGUMENT, estr.c_str());
    }

    err = dragon_ddict_detach(ddict);
    if (err != DRAGON_SUCCESS)
       throw DragonError(err, "Error while detaching from DDict in ndarray_detach.");
}

void ndarray_destroy(dragonDDictDescr_t* ddict, const SerializableString& key, timespec_t* timeout) {
    dragonDDictRequestDescr_t req;
    dragonFLISendHandleDescr_t key_sendh;
    dragonFLIRecvHandleDescr_t recvh;
    dragonError_t err;
    uint64_t hint;

    if (ddict == nullptr) {
        string estr = "Cannot pass NULL ddict to ndarray_destroy.";
        throw DragonError(DRAGON_INVALID_ARGUMENT, estr.c_str());
    }

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not create DDict erase request.");

    err = dragon_ddict_request_key_sendh(&req, &key_sendh);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not access the request send handle.");

    key.serialize(&key_sendh, KEY_HINT, true, timeout);

    err = dragon_ddict_pop(&req);
    if (err != DRAGON_SUCCESS && err != DRAGON_KEY_NOT_FOUND)
        throw DragonError(err, "Could not issue DDict pop.");

    if (err != DRAGON_KEY_NOT_FOUND) {
        err = dragon_ddict_request_recvh(&req, &recvh);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not access the request send handle.");

        SerializableByteBuffer value = SerializableByteBuffer::deserialize(&recvh, &hint, timeout);
        if (hint != VALUE_HINT)
            throw DragonError(DRAGON_INVALID_OPERATION, "The value hint when deserializing was not correct.");
    }

    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not finalize DDict pop request.");
}

void ndarray_put(dragonDDictDescr_t* ddict, const SerializableString& key, const SerializableByteBuffer& buf, timespec_t* timeout) {
    dragonDDictRequestDescr_t req;
    dragonFLISendHandleDescr_t value_sendh;
    dragonFLISendHandleDescr_t key_sendh;
    dragonError_t err;

    if (ddict == nullptr) {
        string estr = "Cannot pass NULL ddict to ndarray_put.";
        throw DragonError(DRAGON_INVALID_ARGUMENT, estr.c_str());
    }

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not create DDict persistent put request.");

    err = dragon_ddict_request_key_sendh(&req, &key_sendh);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not access the request key send handle.");

    key.serialize(&key_sendh, KEY_HINT, true, timeout);

    err = dragon_ddict_put(&req);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not do ndarray put.");

    err = dragon_ddict_request_value_sendh(&req, &value_sendh);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not access the request send handle.");

    buf.serialize(&value_sendh, VALUE_HINT, false, timeout);

    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not finalize DDict persistent put message.");
}

SerializableByteBuffer ndarray_get(dragonDDictDescr_t* ddict, const SerializableString& key, timespec_t* timeout) {
    dragonDDictRequestDescr_t req;
    dragonError_t err;
    dragonFLISendHandleDescr_t key_sendh;
    dragonFLIRecvHandleDescr_t recvh;
    uint64_t hint;

    if (ddict == nullptr) {
        string estr = "Cannot pass NULL ddict to ndarray_get.";
        throw DragonError(DRAGON_INVALID_ARGUMENT, estr.c_str());
    }

    err = dragon_ddict_create_request(ddict, &req);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not create DDict get request.");

    err = dragon_ddict_request_key_sendh(&req, &key_sendh);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not access the request send handle.");

    key.serialize(&key_sendh, KEY_HINT, true, timeout);

    err = dragon_ddict_get(&req);
    if (err != DRAGON_SUCCESS) {
        std::string msg("Could not send DDict get message.\n");
        msg += dragon_getlasterrstr();
        throw DragonError(err, msg.c_str());
    }

    err = dragon_ddict_request_recvh(&req, &recvh);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not access the request receive handle.");

    SerializableByteBuffer value = SerializableByteBuffer::deserialize(&recvh, &hint, timeout);
    if (hint != VALUE_HINT)
        throw DragonError(DRAGON_INVALID_OPERATION, "The value hint when deserializing was not correct.");

    err = dragon_ddict_finalize_request(&req);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not finalize DDict get request.");

    return value;
}


/**************************************************************/
/*********     Serializable Implementation            *********/
/**************************************************************/

Serializable::Serializable(Serializable&& other): mVal(std::move(other.mVal)) {}

Serializable::Serializable(const Serializable& other) : mVal(nullptr) {
    if (other.mVal == nullptr)
        return;

    switch (other.type_id()) {
        case DragonSerType::SERTYPE_STR:
            mVal = std::make_unique<SerializableString>(other.asSerializableString().val());
            break;
        case DragonSerType::SERTYPE_INT:
            mVal = std::make_unique<SerializableInt>(other.asSerializableInt().val());
            break;
        case DragonSerType::SERTYPE_DOUBLE:
            mVal = std::make_unique<SerializableDouble>(other.asSerializableDouble().val());
            break;
        case DragonSerType::SERTYPE_INTVECTOR:
            mVal = std::make_unique<SerializableIntVector>(other.asSerializableIntVector().val());
            break;
        case DragonSerType::SERTYPE_DOUBLEVECTOR:
            mVal = std::make_unique<SerializableDoubleVector>(other.asSerializableDoubleVector().val());
            break;
        case DragonSerType::SERTYPE_INTMATRIX:
            mVal = std::make_unique<Serializable2DIntMatrix>(other.asSerializable2DIntMatrix().val());
            break;
        case DragonSerType::SERTYPE_DOUBLEMATRIX:
            mVal = std::make_unique<Serializable2DDoubleMatrix>(other.asSerializable2DDoubleMatrix().val());
            break;
        case DragonSerType::SERTYPE_BYTEBUFFER:
            mVal = std::make_unique<SerializableByteBuffer>(other.asSerializableByteBuffer().getSize(), other.asSerializableByteBuffer().getPtr());
            break;
        case DragonSerType::SERTYPE_DOUBLENDARRAY:
            mVal = std::make_unique<SerializableDoubleNDArray>(other.asSerializableDoubleNDArray());
            break;
        case DragonSerType::SERTYPE_FLOATNDARRAY:
            mVal = std::make_unique<SerializableFloatNDArray>(other.asSerializableFloatNDArray());
            break;
        case DragonSerType::SERTYPE_INTNDARRAY:
            mVal = std::make_unique<SerializableIntNDArray>(other.asSerializableIntNDArray());
            break;
        case DragonSerType::SERTYPE_LONGNDARRAY:
            mVal = std::make_unique<SerializableLongNDArray>(other.asSerializableLongNDArray());
            break;
        case DragonSerType::SERTYPE_QUEUE:
            mVal = std::make_unique<SerializableQueue<Serializable>>(other.asSerializableQueue().val());
            break;
        case DragonSerType::SERTYPE_DDICT:
            mVal = std::make_unique<SerializableDDict<Serializable, Serializable>>(other.asSerializableDDict().val());
            break;
        case DragonSerType::SERTYPE_BARRIER:
            mVal = std::make_unique<SerializableBarrier>(other.asSerializableBarrier().val());
            break;
        case DragonSerType::SERTYPE_SEMAPHORE:
            mVal = std::make_unique<SerializableSemaphore>(other.asSerializableSemaphore().val());
            break;

        default:
            throw DragonError(DRAGON_INVALID_ARGUMENT, "Unknown Serializable type_id in copy constructor.");
    }
}

Serializable& Serializable::operator=(const Serializable& other) {
    if (this == &other) return *this;
    Serializable tmp(other);
    std::swap(mVal, tmp.mVal);
    return *this;
}

Serializable& Serializable::operator=(Serializable&& other) {
    if (this == &other) return *this;

    mVal = std::move(other.mVal);
    return *this;
}

Serializable::Serializable(int i): mVal(std::make_unique<SerializableInt>(i)) {}

Serializable::Serializable(double d): mVal(std::make_unique<SerializableDouble>(d)) {}

Serializable::Serializable(std::initializer_list<int> v): mVal(std::make_unique<SerializableIntVector>(std::vector<int>(v))) {}

Serializable::Serializable(std::initializer_list<double> v): mVal(std::make_unique<SerializableDoubleVector>(std::vector<double>(v))) {}

Serializable::Serializable(std::initializer_list<std::initializer_list<double>> m) {
    std::vector<std::vector<double>> vals;
    vals.reserve(m.size());
    for (auto row : m) {
        vals.emplace_back(row);
    }
    mVal = std::make_unique<Serializable2DDoubleMatrix>(vals);
}

Serializable::Serializable(const char* s): mVal(std::make_unique<SerializableString>(s)) {}

Serializable::Serializable(const std::vector<int>& v): mVal(std::make_unique<SerializableIntVector>(v)) {}

Serializable::Serializable(const std::vector<double>& v): mVal(std::make_unique<SerializableDoubleVector>(v)) {}

Serializable::Serializable(const std::vector<std::vector<int>>& m): mVal(std::make_unique<Serializable2DIntMatrix>(m)) {}

Serializable::Serializable(const std::vector<std::vector<double>>& m): mVal(std::make_unique<Serializable2DDoubleMatrix>(m)) {}

Serializable::Serializable(size_t size, uint8_t* ptr): mVal(std::make_unique<SerializableByteBuffer>(size, ptr)) {}

Serializable::Serializable(const SerializableString& s): mVal(std::make_unique<SerializableString>(s.val())) {}

Serializable::Serializable(const SerializableInt& i): mVal(std::make_unique<SerializableInt>(i.val())) {}

Serializable::Serializable(const SerializableDouble& d): mVal(std::make_unique<SerializableDouble>(d.val())) {}

Serializable::Serializable(const SerializableIntVector& v): mVal(std::make_unique<SerializableIntVector>(v.val())) {}

Serializable::Serializable(const SerializableDoubleVector& v): mVal(std::make_unique<SerializableDoubleVector>(v.val())) {}

Serializable::Serializable(const Serializable2DIntMatrix& v): mVal(std::make_unique<Serializable2DIntMatrix>(v.val())) {}

Serializable::Serializable(const Serializable2DDoubleMatrix& v): mVal(std::make_unique<Serializable2DDoubleMatrix>(v.val())) {}

Serializable::Serializable(const SerializableByteBuffer& b): mVal(std::make_unique<SerializableByteBuffer>(b.getSize(), b.getPtr())) {}

Serializable::Serializable(const SerializableDoubleNDArray& v): mVal(std::make_unique<SerializableDoubleNDArray>(v)) {}

Serializable::Serializable(const SerializableFloatNDArray& v): mVal(std::make_unique<SerializableFloatNDArray>(v)) {}

Serializable::Serializable(const SerializableIntNDArray& v): mVal(std::make_unique<SerializableIntNDArray>(v)) {}

Serializable::Serializable(const SerializableLongNDArray& v): mVal(std::make_unique<SerializableLongNDArray>(v)) {}

Serializable::Serializable(const SerializableQueue<Serializable>& q): mVal(std::make_unique<SerializableQueue<Serializable>>(q.val())) {}

Serializable::Serializable(const SerializableDDict<Serializable, Serializable>& d): mVal(std::make_unique<SerializableDDict<Serializable, Serializable>>(d.val())) {}

Serializable::Serializable(const SerializableBarrier& b): mVal(std::make_unique<SerializableBarrier>(b.val())) {}

Serializable::Serializable(const SerializableSemaphore& s): mVal(std::make_unique<SerializableSemaphore>(s.val())) {}

Serializable::~Serializable() = default;

void Serializable::serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const {
    dragonError_t err;

    int ty_val = mVal->type_id();
    err = dragon_fli_send_bytes(sendh, sizeof(int), (uint8_t*)&ty_val, arg, buffer, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "Could not write type id to FLI.");

    mVal->serialize(sendh, arg, buffer, timeout);
}

Serializable Serializable::deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
    dragonError_t err;
    size_t actual_size;
    int ty_val;

    err = dragon_fli_recv_bytes_into(recvh, sizeof(int), &actual_size, (uint8_t*)&ty_val, arg, timeout);
    if (err == DRAGON_TIMEOUT)
        throw TimeoutError(err, "Operation timeout.");

    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not read type of value.");

    if (actual_size != sizeof(int))
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The deserialized value did not have the right size type_id");

    auto it = sDeserializers.find(ty_val);
    if (it != sDeserializers.end()) {
        return it->second(recvh, arg, timeout);
    }

    std::string err_str = "The value to deserialize has an unknown type_id with value " + std::to_string(ty_val);
    throw DragonError(DRAGON_INVALID_ARGUMENT, err_str.c_str());
}

void Serializable::register_deserializer(int ty_val, DeserializeFn fn) {
    sDeserializers[ty_val] = fn;
}

int Serializable::type_id() const {
    return mVal->type_id();
}

SerializableBase* Serializable::val() const {
    return mVal.get();
}

SerializableString Serializable::asSerializableString() const {
    if (type_id() != DragonSerType::SERTYPE_STR)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedString and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableString*>(mVal.get());
    return *subPtr;
}

SerializableInt Serializable::asSerializableInt() const {
    if (type_id() != DragonSerType::SERTYPE_INT)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedInt and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableInt*>(mVal.get());
    return *subPtr;
}

SerializableDouble Serializable::asSerializableDouble() const {
    if (type_id() != DragonSerType::SERTYPE_DOUBLE)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedDouble and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableDouble*>(mVal.get());
    return *subPtr;
}

SerializableIntVector Serializable::asSerializableIntVector() const {
    if (type_id() != DragonSerType::SERTYPE_INTVECTOR)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedIntVector and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableIntVector*>(mVal.get());
    return *subPtr;
}

SerializableDoubleVector Serializable::asSerializableDoubleVector() const {
    if (type_id() != DragonSerType::SERTYPE_DOUBLEVECTOR)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedDoubleVector and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableDoubleVector*>(mVal.get());
    return *subPtr;
}

Serializable2DIntMatrix Serializable::asSerializable2DIntMatrix() const {
    if (type_id() != DragonSerType::SERTYPE_INTMATRIX)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedIntMatrix and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const Serializable2DIntMatrix*>(mVal.get());
    return *subPtr;
}

Serializable2DDoubleMatrix Serializable::asSerializable2DDoubleMatrix() const {
    if (type_id() != DragonSerType::SERTYPE_DOUBLEMATRIX)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedDoubleMatrix and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const Serializable2DDoubleMatrix*>(mVal.get());
    return *subPtr;
}

SerializableByteBuffer Serializable::asSerializableByteBuffer() const {
    if (type_id() != DragonSerType::SERTYPE_BYTEBUFFER)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedByteBuffer and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableByteBuffer*>(mVal.get());
    return *subPtr;
}

SerializableDoubleNDArray Serializable::asSerializableDoubleNDArray() const {
    if (type_id() != DragonSerType::SERTYPE_DOUBLENDARRAY)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedDoubleNDArray and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableDoubleNDArray*>(mVal.get());
    return *subPtr;
}

SerializableFloatNDArray Serializable::asSerializableFloatNDArray() const {
    if (type_id() != DragonSerType::SERTYPE_FLOATNDARRAY)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedFloatNDArray and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableFloatNDArray*>(mVal.get());
    return *subPtr;
}

SerializableIntNDArray Serializable::asSerializableIntNDArray() const {
    if (type_id() != DragonSerType::SERTYPE_INTNDARRAY)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedIntNDArray and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableIntNDArray*>(mVal.get());
    return *subPtr;
}

SerializableLongNDArray Serializable::asSerializableLongNDArray() const {
    if (type_id() != DragonSerType::SERTYPE_LONGNDARRAY)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedLongNDArray and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableLongNDArray*>(mVal.get());
    return *subPtr;
}

SerializableQueue<Serializable> Serializable::asSerializableQueue() const {
    if (type_id() != DragonSerType::SERTYPE_QUEUE)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializableQueue and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableQueue<Serializable>*>(mVal.get());
    return *subPtr;
}

SerializableDDict<Serializable, Serializable> Serializable::asSerializableDDict() const {
    if (type_id() != DragonSerType::SERTYPE_DDICT)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializableDDict and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableDDict<Serializable, Serializable>*>(mVal.get());
    return *subPtr;
}

SerializableBarrier Serializable::asSerializableBarrier() const {
    if (type_id() != DragonSerType::SERTYPE_BARRIER)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializableBarrier and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableBarrier*>(mVal.get());
    return *subPtr;
}

SerializableSemaphore Serializable::asSerializableSemaphore() const {
    if (type_id() != DragonSerType::SERTYPE_SEMAPHORE)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializableSemaphore and cannot be converted to one.");

    const auto* subPtr = dynamic_cast<const SerializableSemaphore*>(mVal.get());
    return *subPtr;
}

bool Serializable::operator==(const Serializable& other) const {
    if (mVal == nullptr || other.mVal == nullptr)
        return mVal == other.mVal;

    if (type_id() != other.type_id())
        return false;

    switch (type_id()) {
        case DragonSerType::SERTYPE_STR:
            return asSerializableString().val() == other.asSerializableString().val();
        case DragonSerType::SERTYPE_INT:
            return asSerializableInt().val() == other.asSerializableInt().val();
        case DragonSerType::SERTYPE_DOUBLE:
            return asSerializableDouble().val() == other.asSerializableDouble().val();
        case DragonSerType::SERTYPE_INTVECTOR:
            return asSerializableIntVector().val() == other.asSerializableIntVector().val();
        case DragonSerType::SERTYPE_DOUBLEVECTOR:
            return asSerializableDoubleVector().val() == other.asSerializableDoubleVector().val();
        case DragonSerType::SERTYPE_INTMATRIX:
            return asSerializable2DIntMatrix().val() == other.asSerializable2DIntMatrix().val();
        case DragonSerType::SERTYPE_DOUBLEMATRIX:
            return asSerializable2DDoubleMatrix().val() == other.asSerializable2DDoubleMatrix().val();
        case DragonSerType::SERTYPE_QUEUE:
            return asSerializableQueue().val() == other.asSerializableQueue().val();
        case DragonSerType::SERTYPE_DDICT:
            return asSerializableDDict().val() == other.asSerializableDDict().val();
        case DragonSerType::SERTYPE_BARRIER:
            return asSerializableBarrier().val() == other.asSerializableBarrier().val();
        case DragonSerType::SERTYPE_SEMAPHORE:
            return asSerializableSemaphore().val() == other.asSerializableSemaphore().val();
        default:
            throw DragonError(DRAGON_INVALID_ARGUMENT, "Unknown Serializable type_id while comparing values.");
    }
}

bool Serializable::operator!=(const Serializable& other) const {
    return !(*this == other);
}

}
