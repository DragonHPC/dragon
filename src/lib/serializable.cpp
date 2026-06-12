#include <cstring>
#include <sstream>
#include <iostream>
#include <utility>
#include <dragon/serializable.hpp>
#include <dragon/exceptions.hpp>
#include <map>
#include <functional>

namespace dragon {

std::map<int, Serializable::DeserializeFn> Serializable::sDeserializers = {
    {DragonSerType::SERTYPE_STR,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) -> Serializable {
            return Serializable(SerializableString::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_INT,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) -> Serializable {
            return Serializable(SerializableInt::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_DOUBLE,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) -> Serializable {
            return Serializable(SerializableDouble::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_INTVECTOR,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) -> Serializable {
            return Serializable(SerializableIntVector::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_DOUBLEVECTOR,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) -> Serializable {
            return Serializable(SerializableDoubleVector::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_INTMATRIX,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) -> Serializable {
            return Serializable(Serializable2DIntMatrix::deserialize(h, a, t));
        }},
    {DragonSerType::SERTYPE_DOUBLEMATRIX,
        [](dragonFLIRecvHandleDescr_t* h, uint64_t* a, const timespec_t* t) -> Serializable {
            return Serializable(Serializable2DDoubleMatrix::deserialize(h, a, t));
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

std::string SerializableString::getVal() const {return mVal;}

/**************************************************************/
/*********     Serializable Implementation      *********/
/**************************************************************/

Serializable::Serializable(Serializable&& other) {
    mVal = other.mVal;
    other.mVal = nullptr;
}

Serializable::Serializable(const Serializable& other) : mVal(nullptr) {
    if (other.mVal == nullptr)
        return;

    switch (other.type_id()) {
        case DragonSerType::SERTYPE_STR:
            mVal = new SerializableString(other.asSerializableString().getVal());
            break;
        case DragonSerType::SERTYPE_INT:
            mVal = new SerializableInt(other.asSerializableInt().getVal());
            break;
        case DragonSerType::SERTYPE_DOUBLE:
            mVal = new SerializableDouble(other.asSerializableDouble().getVal());
            break;
        case DragonSerType::SERTYPE_INTVECTOR:
            mVal = new SerializableIntVector(other.asSerializableIntVector().getVal());
            break;
        case DragonSerType::SERTYPE_DOUBLEVECTOR:
            mVal = new SerializableDoubleVector(other.asSerializableDoubleVector().getVal());
            break;
        case DragonSerType::SERTYPE_INTMATRIX:
            mVal = new Serializable2DIntMatrix(other.asSerializable2DIntMatrix().getVal());
            break;
        case DragonSerType::SERTYPE_DOUBLEMATRIX:
            mVal = new Serializable2DDoubleMatrix(other.asSerializable2DDoubleMatrix().getVal());
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

    delete mVal;
    mVal = other.mVal;
    other.mVal = nullptr;
    return *this;
}

Serializable::Serializable(int i) {
    mVal = new SerializableInt(i);
}

Serializable::Serializable(double d) {
    mVal = new SerializableDouble(d);
}

Serializable::Serializable(std::initializer_list<int> v) {
    mVal = new SerializableIntVector(std::vector<int>(v));
}

Serializable::Serializable(std::initializer_list<double> v) {
    mVal = new SerializableDoubleVector(std::vector<double>(v));
}

Serializable::Serializable(std::initializer_list<std::initializer_list<double>> m) {
    std::vector<std::vector<double>> vals;
    vals.reserve(m.size());
    for (auto row : m) {
        vals.emplace_back(row);
    }
    mVal = new Serializable2DDoubleMatrix(vals);
}

Serializable::Serializable(const char* s) {
    mVal = new SerializableString(s);
}

Serializable::Serializable(const SerializableString& s) {
    mVal = new SerializableString(s.getVal());
}

Serializable::Serializable(const SerializableInt& i) {
    mVal = new SerializableInt(i.getVal());
}

Serializable::Serializable(const SerializableDouble& d) {
    mVal = new SerializableDouble(d.getVal());
}

Serializable::Serializable(const SerializableIntVector& v) {
    mVal = new SerializableIntVector(v.getVal());
}

Serializable::Serializable(const SerializableDoubleVector& v) {
    mVal = new SerializableDoubleVector(v.getVal());
}

Serializable::Serializable(const Serializable2DIntMatrix& v) {
    mVal = new Serializable2DIntMatrix(v.getVal());
}

Serializable::Serializable(const Serializable2DDoubleMatrix& v) {
    mVal = new Serializable2DDoubleMatrix(v.getVal());
}

Serializable::~Serializable() {
    try {
        if (mVal != nullptr)
            delete mVal;
    } catch (...) {}
}

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

    throw DragonError(DRAGON_INVALID_ARGUMENT, "The value to deserialize has an unknown type_id");
}

void Serializable::register_deserializer(int ty_val, DeserializeFn fn) {
    sDeserializers[ty_val] = fn;
}

int Serializable::type_id() const {
    return mVal->type_id();
}

SerializableBase* Serializable::getVal() const {
    return mVal;
}

SerializableString Serializable::asSerializableString() const {
    if (type_id() != DragonSerType::SERTYPE_STR)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedString and cannot be converted to one.");

    SerializableString* subPtr = dynamic_cast<SerializableString*>(mVal);
    return *subPtr;
}

SerializableInt Serializable::asSerializableInt() const {
    if (type_id() != DragonSerType::SERTYPE_INT)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedInt and cannot be converted to one.");

    SerializableInt* subPtr = dynamic_cast<SerializableInt*>(mVal);
    return *subPtr;
}

SerializableDouble Serializable::asSerializableDouble() const {
    if (type_id() != DragonSerType::SERTYPE_DOUBLE)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedDouble and cannot be converted to one.");

    SerializableDouble* subPtr = dynamic_cast<SerializableDouble*>(mVal);
    return *subPtr;
}

SerializableIntVector Serializable::asSerializableIntVector() const {
    if (type_id() != DragonSerType::SERTYPE_INTVECTOR)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedIntVector and cannot be converted to one.");

    SerializableIntVector* subPtr = dynamic_cast<SerializableIntVector*>(mVal);
    return *subPtr;
}

SerializableDoubleVector Serializable::asSerializableDoubleVector() const {
    if (type_id() != DragonSerType::SERTYPE_DOUBLEVECTOR)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedDoubleVector and cannot be converted to one.");

    SerializableDoubleVector* subPtr = dynamic_cast<SerializableDoubleVector*>(mVal);
    return *subPtr;
}

Serializable2DIntMatrix Serializable::asSerializable2DIntMatrix() const {
    if (type_id() != DragonSerType::SERTYPE_INTMATRIX)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedIntMatrix and cannot be converted to one.");

    Serializable2DIntMatrix* subPtr = dynamic_cast<Serializable2DIntMatrix*>(mVal);
    return *subPtr;
}

Serializable2DDoubleMatrix Serializable::asSerializable2DDoubleMatrix() const {
    if (type_id() != DragonSerType::SERTYPE_DOUBLEMATRIX)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The Serializable is not a SerializedDoubleMatrix and cannot be converted to one.");

    Serializable2DDoubleMatrix* subPtr = dynamic_cast<Serializable2DDoubleMatrix*>(mVal);
    return *subPtr;
}

bool Serializable::operator==(const Serializable& other) const {
    if (mVal == nullptr || other.mVal == nullptr)
        return mVal == other.mVal;

    if (type_id() != other.type_id())
        return false;

    switch (type_id()) {
        case DragonSerType::SERTYPE_STR:
            return asSerializableString().getVal() == other.asSerializableString().getVal();
        case DragonSerType::SERTYPE_INT:
            return asSerializableInt().getVal() == other.asSerializableInt().getVal();
        case DragonSerType::SERTYPE_DOUBLE:
            return asSerializableDouble().getVal() == other.asSerializableDouble().getVal();
        case DragonSerType::SERTYPE_INTVECTOR:
            return asSerializableIntVector().getVal() == other.asSerializableIntVector().getVal();
        case DragonSerType::SERTYPE_DOUBLEVECTOR:
            return asSerializableDoubleVector().getVal() == other.asSerializableDoubleVector().getVal();
        case DragonSerType::SERTYPE_INTMATRIX:
            return asSerializable2DIntMatrix().getVal() == other.asSerializable2DIntMatrix().getVal();
        case DragonSerType::SERTYPE_DOUBLEMATRIX:
            return asSerializable2DDoubleMatrix().getVal() == other.asSerializable2DDoubleMatrix().getVal();
        default:
            throw DragonError(DRAGON_INVALID_ARGUMENT, "Unknown Serializable type_id while comparing values.");
    }
}

bool Serializable::operator!=(const Serializable& other) const {
    return !(*this == other);
}

}
