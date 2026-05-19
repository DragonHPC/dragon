#include <cstring>
#include <sstream>
#include <dragon/serializable.hpp>
#include <dragon/exceptions.hpp>

namespace dragon {

/**************************************************************/
/*********       SerializableInt Implementation       *********/
/**************************************************************/

SerializableInt::SerializableInt(int x): mVal(x) {}

void SerializableInt::serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const {
    dragonError_t err;
    err = dragon_fli_send_bytes(sendh, sizeof(int), (uint8_t*)&mVal, arg, buffer, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not serialize the integer.");
}

SerializableInt SerializableInt::deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
    dragonError_t err = DRAGON_SUCCESS;
    size_t actual_size;
    int val;

    err = dragon_fli_recv_bytes_into(recvh, sizeof(int), &actual_size, (uint8_t*)&val, arg, timeout);
    if (err == DRAGON_TIMEOUT)
        throw TimeoutError(err, "Operation Timeout");

    if (err == DRAGON_EOT)
        throw EmptyError(err, "EOT of stream");

    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not deserialize the integer.");

    if (actual_size != sizeof(int)) {
        std::stringstream msg;
        msg << "The expected size of the integer was " << sizeof(int) << " and the received size was " << actual_size <<". The size is not correct.";
        throw DragonError(DRAGON_INVALID_ARGUMENT, msg.str().c_str());
    }

    return SerializableInt(val); //RVO - Return Value Optimization
}

int SerializableInt::getVal() const {return mVal;}

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

std::string SerializableString::getVal() const {return mVal;}

/**************************************************************/
/*********     SerializableDouble Implementation      *********/
/**************************************************************/

SerializableDouble::SerializableDouble(double x): mVal(x) {}

void SerializableDouble::serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const {
    dragonError_t err;
    err = dragon_fli_send_bytes(sendh, sizeof(double), (uint8_t*)&mVal, arg, buffer, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not serialize double.");
}

SerializableDouble SerializableDouble::deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
    dragonError_t err = DRAGON_SUCCESS;
    size_t actual_size;
    double val;

    err = dragon_fli_recv_bytes_into(recvh, sizeof(double), &actual_size, (uint8_t*)&val, arg, timeout);
    if (err == DRAGON_TIMEOUT)
        throw TimeoutError(err, "Operation timeout.");

    if (err == DRAGON_EOT)
        throw EmptyError(err, "EOT of stream");

    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not deserialize double");

    if (actual_size != sizeof(double))
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The size of the double was not correct.");

    return SerializableDouble(val);
}

double SerializableDouble::getVal() const {return mVal;}

/**************************************************************/
/********* SerializableDoubleVector Implementation *********/
/**************************************************************/

SerializableDoubleVector::SerializableDoubleVector(std::vector<double> x): mVal(x) {}
SerializableDoubleVector::SerializableDoubleVector(size_t size): mVal(std::vector<double>(size, 0.0)) {}

void SerializableDoubleVector::serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const {
    dragonError_t err;
    size_t num_items = mVal.size();

    // write the size of the array - needed for efficient deserialization without copies
    err = dragon_fli_send_bytes(sendh, sizeof(size_t), (uint8_t*)&num_items, arg, buffer, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not write vector size to ddict.");

    err = dragon_fli_send_bytes(sendh, sizeof(double)*num_items, (uint8_t*)mVal.data(), arg, buffer, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not write vector to ddict.");
}

SerializableDoubleVector SerializableDoubleVector::deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
    dragonError_t err = DRAGON_SUCCESS;
    size_t received_size = 0;
    size_t expected_size = 0;
    size_t num_items = 0;

    err = dragon_fli_recv_bytes_into(recvh, sizeof(size_t), &received_size, (uint8_t*)&num_items, arg, timeout);
    if (err == DRAGON_TIMEOUT)
        throw TimeoutError(err, "Operation timeout.");

    if (err == DRAGON_EOT)
        throw EmptyError(err, "EOT of stream");

    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not read item count for vector.");

    if (received_size != sizeof(size_t))
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The size of num_items was not correct.");

    SerializableDoubleVector rv(num_items);

    expected_size = sizeof(double) * num_items;

    err = dragon_fli_recv_bytes_into(recvh, expected_size, &received_size, (uint8_t*)rv.mVal.data(), arg, timeout);
    if (err == DRAGON_TIMEOUT)
        throw TimeoutError(err, "Operation timeout.");

    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not read element of vector.");

    if (received_size != expected_size)
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The received data did not match the expected size of the vector.");

    return rv; // Relies on RVO for efficiently returning the vector.
}

const std::vector<double>& SerializableDoubleVector::getVal() const {return mVal;}

/**************************************************************/
/********* SerializableDouble2DVector Implementation *********/
/**************************************************************/

SerializableDouble2DVector::SerializableDouble2DVector(std::vector<std::vector<double>> x): mVal(x) {}

void SerializableDouble2DVector::serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const {
    dragonError_t err;
    size_t nrows = mVal.size();

    // write the number of rows in the vector
    err = dragon_fli_send_bytes(sendh, sizeof(size_t), (uint8_t*)&nrows, arg, buffer, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not write bytes to ddict.");

    for (auto& row: mVal) {
        SerializableDoubleVector sVec(row);
        sVec.serialize(sendh, arg, buffer, timeout);
    }
}

SerializableDouble2DVector SerializableDouble2DVector::deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
    dragonError_t err = DRAGON_SUCCESS;
    size_t actual_size;
    std::vector<std::vector<double>> val;

    size_t nrows = 0;

    err = dragon_fli_recv_bytes_into(recvh, sizeof(size_t), &actual_size, (uint8_t*)&nrows, arg, timeout);
    if (err == DRAGON_TIMEOUT)
        throw TimeoutError(err, "Operation timeout.");

    if (err == DRAGON_EOT)
        throw EmptyError(err, "EOT of stream");

    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not read row count for vector.");

    if (actual_size != sizeof(size_t))
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The size of nrows was not correct.");

    for (size_t i=0; i<nrows; i++) {
        SerializableDoubleVector tmp_vec = SerializableDoubleVector::deserialize(recvh, arg, timeout);
        val.push_back(tmp_vec.getVal());
    }

    return SerializableDouble2DVector(val); // Relies on RVO for efficiently returning the vector.
}

const std::vector<std::vector<double>>& SerializableDouble2DVector::getVal() const {return mVal;}

}
