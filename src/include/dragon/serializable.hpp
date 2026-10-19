#ifndef DRAGON_SERIALIZABLE_HPP
#define DRAGON_SERIALIZABLE_HPP

#include <string>
#include <cstring>
#include <sstream>
#include <vector>
#include <initializer_list>
#include <functional>
#include <memory>
#include <typeindex>
#include <type_traits>
#include <map>
#include <iostream>
#include <dragon/fli.h>
#include <dragon/exceptions.hpp>
#include <dragon/serializable_types.h>
#include <dragon/ddict.h>

namespace dragon {

/**
 * @class SerializableBase
 * @brief An base class for deriving serializable classes
 *
 * Classes derived from this SerializableBase class are used when communicating
 * to/from other distributed objects like Queue and DDict objects. Users who have custom
 * serializable objects to send may implement their own custom serializer/deserializer
 * methods. A few standard serializer/deserializer classes are also provided.
 *
 * Please read the documentation of the DerivedSerializable template. This
 * serves as an outline for how to write a SerializableBase subclass. The
 * DerivedSerializable is never meant to be instantiated. It just serves as a
 * convenience for documenting exactly what should be defined in subclasses of
 * SerializableBase.
 *
 */
class SerializableBase {
    public:

    virtual ~SerializableBase();

    /**
     * @brief Please see the documentation for the DerivedSerializable class.
     *
     * The DerivedSerializable documentation provides a description of what you must write
     * to subclass SerializableBase and use it in your program.
     */
    virtual void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const;



    /**
     * @brief Please see the documentation for the DerivedSerializable class.
     *
     * The DerivedSerializable documentation provides a description of what you must write
     * to subclass Serializable and use it in your program.
     */
    virtual int type_id() const;

    static SerializableBase deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout);
};

/**
 * @class DerivedSerializable
 * @brief This class provides the outline of what a subclass of Serializable should look like.
 *
 * Use this documentation as an outline for writing your own subclasses of Serializable. DO NOT
 * instantiate this class and expect it to do anything.
 */
template<class Type>
class DerivedSerializable: public SerializableBase {
public:
    /**
     * @brief Constructor for DerivedSerializable
     *
     * Write your own subclass of Serializable and a constructor for it. You may pass multiple
     * arguments. The constructor is for your own program's use and is not used by Dragon.
     *
     * @param x A Type of object to wrap.
     */
    DerivedSerializable(Type obj) {
        throw DragonError(DRAGON_INVALID_OPERATION, "This class should not be instantiated. Inherit from SerializableBase instead.");
    }

    /**
     * @brief Serialize a C++ object.
     *
     * This method should be overridden in the implementing derived subclass. It should
     * write the bytes of the serialized object to the FLI send handle using the
     * FLI send_bytes interface defined in fli.h. The arg argument should simply be passed
     * through from the serialize function call to the FLI API call for sending bytes. The
     * buffer argument should typically just be passed through to FLI send_byte operations. It will be
     * determined by the context in which serialize is called. For DDict keys, the writes
     * are buffered. For DDict values, the writes are not. But in some cases you may wish to buffer
     * serialized objects and specify true to cause the writes to be consilidated into one
     * network communication.
     *
     * @param sendh is an FLI send handle used for writing
     * @param arg is a provided hint. You may override this in some circumstances to create your own hint.
     * @param buffer is provided or you can override. A value of true on FLI sends will cause written data to be
     * consolidated into one network transfer. The arg is written through to the receiver only when buffer is false.
     * @param timeout A value of nullptr will wait forever to serialize/transfer data. If value of {0,0} will
     * try once. Otherwise, the timeout specifies how long to wait for the serialization/transfer to be completed.
     */
    virtual void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const {
        throw DragonError(DRAGON_INVALID_OPERATION, "This class should not be instantiated. Inherit from SerializableBase instead.");
    }

    /**
     * @brief Deserialize a serialized C++ object.
     *
     * This method should be written in the implementing derived subclass and should
     * return the derived subtype of SerializableBase. It may throw a
     * DragonError exception when a byte stream is not deserializable. It
     * should throw a EmptyError when EOT is received while reading bytes
     * as it deserializes a value. Assuming that the deserialization
     * succeeds, the deserialize method should return a deserialized value
     * of the derived type after it has read the serialized object's
     * bytes. The arg argument should be passed through to the FLI API
     * calls for reading bytes or pool memory and will be set according to
     * what was sent when it was serialized. This function relies on NRVO
     * in C++17 and above. This optimization means that the object is
     * initialized in the caller's space so when the value is returned, it
     * is already in-place. This means we can return a value without
     * making an extra copy.
     *
     * @param recvh An FLI receive handle. The receive handle is used to read
     * the data of the object. You can read the data using any FLI recvh methods.
     * @param arg A pointer to a variable to hold the received arg value.
     * @param timeout A value of nullptr will wait forever. A value of {0,0} will
     * try once. Otherwise, wait for the specified time to receive the object.
     *
     * @returns A DerivedSerializable instance.
     */
    static DerivedSerializable<Type> deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
        throw DragonError(DRAGON_INVALID_OPERATION, "This class should not be instantiated. Inherit from SerializableBase instead.");
    }

    /**
     * @brief Get the wrapped value for the object.
     *
     * This method may be named whatever you like. It is not part of the SerializableBase class. And you may
     * define more than one accessor method like this to retrieve parts of your object. You will need
     * something like this to access your deserialized object in your program. The wrapped value (i.e.
     * Type) may also be more than one value which would then be passed to the constructor and you
     * would then have multiple accessor methods to get the various pieces out after deserialization.
     *
     * @returns A value.
     */
    Type val() const {
        throw DragonError(DRAGON_INVALID_OPERATION, "This class should not be instantiated. Inherit from SerializableBase instead.");
    }

    /**
     * @brief Return a unique type id for this type
     *
     * This method should return a unique integer to be used to identify this type. The SerializableType enum can provide
     * these values. The return type is left as int to facilitate subclassing and returning your own type values.
     *
     * @returns A unique type id.
     */
    int type_id() const {
        throw DragonError(DRAGON_INVALID_OPERATION, "This class should not be instantiated. Inherit from SerializableBase instead.");
    }

    private:
    Type mVal;
};

/**
 * @class SerializableString
 * @brief A Serializable string class
 *
 * The class provides the Serializable interface for strings.
 */
class SerializableString : public SerializableBase {
    public:
    /**
     * @brief Default Constructor for Serializable Strings
     *
     * Constructs an empty string.
     *
     */
    SerializableString();

    /**
     * @brief Constructor for Serializable Strings
     *
     * This provides a wrapper class for string values that need to be serialized/deserialized in a
     * Dragon program.
     *
     * @param x An string value to wrap.
     */
    SerializableString(std::string x);

    /**
     * @brief See the DerivedSerializable serialize description.
     */
    virtual void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const;

    /**
     * @brief See the DerivedSerializable deserialize description.
     */
    static SerializableString deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout);

    /**
     * @brief Get the wrapped value for the object.
     *
     * @returns The wrapped value.
     */
    std::string val() const;

    /**
     * @brief See the DerivedSerializable type_id description
     */
    int type_id() const;

    private:
    std::string mVal="";
};

/**
 * @class SerializableBuffer
 * @brief A Serializable Byte Buffer
 *
 * The class provides the Serializable interface for strings.
 */
class SerializableByteBuffer : public SerializableBase {
    public:
    /**
     * @brief Default Constructor for Serializable Byte Buffers
     *
     * Default constructor.
     *
     */
    SerializableByteBuffer();

    /**
     * @brief Constructor for Serializable Byte Buffers
     *
     * This provides a wrapper class for byte buffer values that need to be serialized/deserialized in a
     * Dragon program. The byte buffer is not automatically freed by this class. Any data that is
     * Serialized or Deserialized by this class is the responsibility of the application to free.
     *
     * @param size The number of bytes in the buffer
     *
     * @param ptr A blob of bytes to wrap. Note that the buffer is not copied to be as efficient as
     * possible.
     */
    SerializableByteBuffer(size_t size, uint8_t* ptr);

    /**
     * @brief See the DerivedSerializable serialize description.
     */
    virtual void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const;

    /**
     * @brief See the DerivedSerializable deserialize description.
     */
    static SerializableByteBuffer deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout);

    /**
     * @brief Get the number of bytes of the wrapped value for the object.
     *
     * @returns The wrapped value's number of bytes.
     */
    size_t getSize() const;

    /**
     * @brief Get the wrapped value for the object.
     *
     * @returns The wrapped value.
     */
    uint8_t* getPtr() const;

    /**
     * @brief See the DerivedSerializable type_id description
     */
    int type_id() const;

    private:
    size_t mSize;
    uint8_t* mPtr;
};

/**
 * @class SerializableScalar
 * @brief A SerializableScalar class
 *
 * The class provides the Serializable interface for all Scalar types in C++. There are two pre-defined
 * types provided as instances of this template: SerializableInt and SerializableDouble. Users can define
 * additional SerializableScalars by creating additional instances of this template.
 */
template<class Type, int TVal>
class SerializableScalar : public SerializableBase {
    public:
    /**
     * @brief Constructor for SerializableScalar
     *
     * This provides a wrapper class for Type values that need to be serialized/deserialized in a
     * Dragon program.
     *
     * @param x An double value to wrap.
     */
    SerializableScalar(Type x): mVal(x) {}

    /**
     * @brief Constructor for SerializableScalar
     *
     * Default constructor.
     */
    SerializableScalar(): mVal(0) {}

    /**
     * @brief See the DerivedSerializable serialize description.
     */
    virtual void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const {
        dragonError_t err;
        err = dragon_fli_send_bytes(sendh, sizeof(Type), (uint8_t*)&mVal, arg, buffer, timeout);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not serialize the scalar.");
    }

    /**
     * @brief See the DerivedSerializable deserialize description.
     */
    static SerializableScalar deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
        dragonError_t err = DRAGON_SUCCESS;
        size_t actual_size;
        Type val;

        err = dragon_fli_recv_bytes_into(recvh, sizeof(Type), &actual_size, (uint8_t*)&val, arg, timeout);
        if (err == DRAGON_TIMEOUT)
            throw TimeoutError(err, "Operation Timeout");

        if (err == DRAGON_EOT)
            throw EmptyError(err, "EOT of stream");

        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not deserialize the integer.");

        if (actual_size != sizeof(Type)) {
            std::stringstream msg;
            msg << "The expected size of the scalar was " << sizeof(Type) << " and the received size was " << actual_size <<". The size is not correct.";
            throw DragonError(DRAGON_INVALID_ARGUMENT, msg.str().c_str());
        }

        return SerializableScalar<Type, TVal>(val); //RVO - Return Value Optimization
    }

    /**
     * @brief Get the wrapped value for the object.
     *
     * @returns The wrapped value.
     */
    Type val() const {
        return mVal;
    }

    bool operator==(const SerializableScalar<Type, TVal>& other) const {
        return mVal == other.mVal;
    }

    bool operator!=(const SerializableScalar<Type, TVal>& other) const {
        return mVal != other.mVal;
    }

    bool operator==(const Type& other) const {
        return mVal == other;
    }

    bool operator!=(const Type& other) const {
        return mVal != other;
    }

    friend bool operator==(const Type& lhs, const SerializableScalar<Type, TVal>& rhs) {
        return lhs == rhs.mVal;
    }

    friend bool operator!=(const Type& lhs, const SerializableScalar<Type, TVal>& rhs) {
        return lhs != rhs.mVal;
    }

    /**
     * @brief See the DerivedSerializable type_id description
     */
    int type_id() const {
        return TVal;
    }

    private:
    Type mVal=0;
};

/**
 * @class SerializableVector
 * @brief A Serializable Vector of Type
 *
 * The class provides the Serializable interface for all vector types in Dragon C++ code. There are two pre-defined
 * types provided as instances of this template: SerializableIntVector and SerializableDoubleVector. Users can define
 * additional SerializableVectors by creating additional instances of this template.
 */
template<class Type, int TVal>
class SerializableVector: public SerializableBase {
public:
    /**
     * @brief Default Constructor for Serializable Vector of Type
     *
     * Provides an empty vector.
     */
    SerializableVector() = default;

    /**
     * @brief Constructor for Serializable Vector of Type
     *
     * This provides a wrapper class for a vector of Type values that need to be serialized/deserialized in a
     * Dragon program.
     *
     * @param vec A Type vector value to wrap.
     */
    SerializableVector(std::vector<Type> obj): mVal(obj) {}

    /**
     * @brief Constructor for Serializable Vector of Type
     *
     * Contruct an empty serializable Type vector with size elements.
     *
     * @param size The number of elements for the empty vector.
     */
    SerializableVector(size_t size): mVal(std::vector<Type>(size, 0)) {}

    /**
     * @brief See the DerivedSerializable serialize description.
     */
    virtual void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const {
        dragonError_t err;
        size_t num_items = mVal.size();

        // write the size of the array - needed for efficient deserialization without copies
        err = dragon_fli_send_bytes(sendh, sizeof(size_t), (uint8_t*)&num_items, arg, buffer, timeout);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not write vector size.");

        err = dragon_fli_send_bytes(sendh, sizeof(Type)*num_items, (uint8_t*)mVal.data(), arg, buffer, timeout);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not serialize the vector.");
    }

    /**
     * @brief See the DerivedSerializable deserialize description.
    */
    static SerializableVector<Type, TVal> deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
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

        SerializableVector<Type, TVal> rv(num_items);

        expected_size = sizeof(Type) * num_items;

        if (expected_size != 0) {
            err = dragon_fli_recv_bytes_into(recvh, expected_size, &received_size, (uint8_t*)rv.mVal.data(), arg, timeout);
            if (err == DRAGON_TIMEOUT)
                throw TimeoutError(err, "Operation timeout.");

            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not read element of vector.");

            if (received_size != expected_size) {
                char msg[200];
                snprintf(msg, 200, "The received data of size %lu did not match the expected size of %lu for the vector.", received_size, expected_size);
                throw DragonError(DRAGON_INVALID_ARGUMENT, msg);
            }
        }

        return rv; // Relies on RVO for efficiently returning the vector.
    }

    /**
     * @brief Get the wrapped value for the object.
     *
     * @returns The wrapped value.
     */
    std::vector<Type> val() const {
        return mVal;
    }

    /**
     * @brief See the DerivedSerializable type_id description
     */
    int type_id() const {
        return TVal;
    }

    private:
    std::vector<Type> mVal;
};


/**
 * @class Serializable2DMatrix
 * @brief A Serializable 2D Matrix of Type
 *
 * The class provides the Serializable interface for all Matrix types in Dragon C++ code. There are two pre-defined
 * types provided as instances of this template: Serializable2DIntMatrix and Serializable2DDoubleMatrix. Users can define
 * additional Serializable2DMatrices by creating additional instances of this template.
 */
template<class Type, int TVal>
class Serializable2DMatrix: public SerializableBase {
public:
    /**
     * @brief Default Constructor for Serializable 2D Matrix of Type
     *
     * Provides an empty matrix.
     */
    Serializable2DMatrix() = default;

    /**
     * @brief Constructor for Serializable 2D Matrix of Type
     *
     * This provides a wrapper class for a matrix of Type values that need to be serialized/deserialized in a
     * Dragon program.
     *
     * @param vec A Type vector value to wrap.
     */
    Serializable2DMatrix(std::vector<std::vector<Type>> obj): mVal(obj) {}

    /**
     * @brief Constructor for Serializable 2D Matrix of Type
     *
     * Contruct an empty serializable Type Matrix with size elements.
     *
     * @param rows The number of rows for the empty matrix.
     * @param cols The number of columns for the empty matrix.
     */
    Serializable2DMatrix(size_t rows, size_t cols): mVal(std::vector<std::vector<Type>>()) {
        for (size_t i=0; i<rows; i++) {
            std::vector<Type> row;
            for (size_t j=0; j<cols; j++)
                row.push_back(0);

            mVal.push_back(row);
        }
    }

    /**
     * @brief See the DerivedSerializable serialize description.
     */
    virtual void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const {
        dragonError_t err;
        size_t nrows = mVal.size();

        // write the number of rows in the vector
        err = dragon_fli_send_bytes(sendh, sizeof(size_t), (uint8_t*)&nrows, arg, buffer, timeout);
        if (err != DRAGON_SUCCESS)
            throw DragonError(err, "Could not write bytes.");

        for (auto& row: mVal) {
            SerializableVector<Type, TVal> sVec(row);
            sVec.serialize(sendh, arg, buffer, timeout);
        }
    }

    /**
     * @brief See the DerivedSerializable deserialize description.
    */
    static Serializable2DMatrix<Type, TVal> deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
        dragonError_t err = DRAGON_SUCCESS;
        size_t actual_size;
        std::vector<std::vector<Type>> val;

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
            SerializableVector<Type, TVal> tmp_vec = SerializableVector<Type, TVal>::deserialize(recvh, arg, timeout);
            val.push_back(tmp_vec.val());
        }

        return Serializable2DMatrix<Type, TVal>(val); // Relies on RVO for efficiently returning the vector.
    }

    /**
     * @brief Get the wrapped value for the object.
     *
     * @returns The wrapped value.
     */
    std::vector<std::vector<Type>> val() const {
        return mVal;
    }

    /**
     * @brief See the DerivedSerializable type_id description
     */
    int type_id() const {
        return TVal;
    }

    private:
    std::vector<std::vector<Type>> mVal;
};

/* Pre-defined Serializable Types*/

using SerializableInt = SerializableScalar<int, DragonSerType::SERTYPE_INT>;
using SerializableDouble = SerializableScalar<double, DragonSerType::SERTYPE_DOUBLE>;
using SerializableIntVector = SerializableVector<int, DragonSerType::SERTYPE_INTVECTOR>;
using SerializableDoubleVector = SerializableVector<double, DragonSerType::SERTYPE_DOUBLEVECTOR>;
using Serializable2DIntMatrix = Serializable2DMatrix<int, DragonSerType::SERTYPE_INTMATRIX>;
using Serializable2DDoubleMatrix = Serializable2DMatrix<double, DragonSerType::SERTYPE_DOUBLEMATRIX>;

/* NDArray helpers. Private API for SerializableNDArray */
int ndarray_unique_id(dragonDDictDescr_t* ddict, timespec_t* timeout);
void ndarray_attach(dragonDDictDescr_t* ddict, const char* serialized_dict, timespec_t* timeout);
void ndarray_put(dragonDDictDescr_t* ddict, const SerializableString& key, const SerializableByteBuffer& buf, timespec_t* timeout);
SerializableByteBuffer ndarray_get(dragonDDictDescr_t* ddict, const SerializableString& key, timespec_t* timeout);
void ndarray_destroy(dragonDDictDescr_t* ddict, const SerializableString& key, timespec_t* timeout);
void ndarray_detach(dragonDDictDescr_t* ddict, const char* serialized_dict);

/**
 * @class SerializableNDArray
 * @brief A Serializable NDArray
 *
 * The class provides the Serializable interface for generalized matrices
 * of any dimension and size. The Python counterpart is a numpy ndarray. This
 * class makes sharing of data with ndarray objects in Python possible. The
 * C++ version allows the creation of slices of the n-dimensional arrays. All
 * slices within a process share the same cached data to be efficient in storage
 * as possible.
 *
 * The underlying implementation stores all data within a Dragon DDict and it
 * is locally cached lazily when the array is first indexed. If the data is not
 * indexed, then only metadata is communicated between processes to be as efficient
 * in passing an NDArray between processes, through a Queue or DDict, as possible.
 *
 * Since there is locally cached data when working with an NDArray, sync
 * should be called when wishing to flush the cache back so all
 * others can see it and refresh should be called when it is known the
 * cache should be refreshed. Note that calling refresh immediately overwrites
 * any locally cached data with the globally available version without regard to possible
 * modifications of the cache. It is up to the program to provide synchronization
 * around syncing and refreshing NDArray data.
 */
template<class Type, int TVal>
class SerializableNDArray: public SerializableBase {
public:
    /** @brief Constructor for Serializable NDArray
     *
     * Contruct a serializable ndarray with meta data of the ndarray encoded
     * within and the data stored in the provided DDict. When serializing
     * and passing around the ndarray, only the metadata is passed between
     * processes while the ndarray data exists in the DDict and can be
     * retrieved separately when needed.
     *
     * This means you can pass SerializableNDArrays between processes without
     * copying all the data they refer to until you are actually going to
     * use it. Once referenced, all copies of an NDArray object in a
     * process refer to the same cached data so that multiple copies are
     * not kept within a process (or its threads). This makes working on
     * data from multiple threads convenient. However, care must be taken
     * so that multiple threads are not modifying the same cached data at
     * the same time. When data has been updated in a process, sync should
     * be called to copy that data back to the shared DDict so it can be
     * found by other processes. If another process has updated the
     * NDArray, then refresh can be called to load the latest data into
     * cached data for this process.
     *
     * @param dimensions A vector of length matching the dimensions of the ndarray.
     * Each value in dimensions is the size of the ndarray in that dimension.
     * @param data A pointer to the ndarray data. It will be copied into the provided DDict.
     * @param ddict_ser A serialized Distributed Dictionary in which to store the ndarray data.
     * @param timeout A pointer to a timeout structure or NULL. The timeout is used for all interactions
     * with the provided DDict attached from the ddict_ser value.
     */
    SerializableNDArray(const std::vector<int>& dimensions, void* data, const char* ser_ddict, const timespec_t* timeout):
        mElementSize(sizeof(Type)), mDimensions(dimensions), mDDictSer(ser_ddict) {

        if (timeout == nullptr) {
            this->mTimeout = nullptr;
            this->mTimeoutVal = {-1,-1};
        } else {
            this->mTimeoutVal = *timeout;
            this->mTimeout = &this->mTimeoutVal;
        }

        int num_elements = 1;
        for (const auto& dim : dimensions) {
            num_elements *= dim;
        }

        if (dimensions.empty())
            num_elements = 0;



        int total_size = num_elements * mElementSize.val();

        mBuf = SerializableByteBuffer(total_size, (uint8_t*)data);

        ndarray_attach(&mDDict, ser_ddict, mTimeout);
        int unique_int = ndarray_unique_id(&mDDict, mTimeout);

        std::string ndarray_key = "DRAGON_NDARRAY_"+ std::to_string(unique_int);
        mNDArrayKey = ndarray_key;

        ndarray_put(&mDDict, mNDArrayKey, mBuf, mTimeout);
        mCachedData = data;
    }

    ~SerializableNDArray() override = default;

    /** @brief Destroy an ndarray by removing its data from the DDict backing store.
     *
     * An NDArray stores its data in a DDict so only meta data is transferred between
     * processes. This method cleans up that DDict backed data when the ndarray is
     * no longer needed.
     */
    void destroy() {

        try {
            ndarray_destroy(&mDDict, mNDArrayKey, mTimeout);
        } catch (const DragonError&) {
            /* destroy is best effort cleanup; the entry may already be gone. */
        }

        if (mCachedData != nullptr) {
            free(mCachedData);
            mCachedData = nullptr;
        }
    }

    /** @brief Detach from an NDArray DDict
     *
     * Only call detach if you are completely done with the ndarray and no longer need access
     * to the underlying DDict. Call this after calling destroy on the ndarray.
     */
    void ddict_detach() {
        try {
            ndarray_detach(&mDDict, mDDictSer.val().c_str());
        } catch (const DragonError&) {
            /* detach is best effort cleanup; the DDict may already be detached. */
        }
    }

    /** @brief Copy Constructor for Serializable NDArray
     *
     * Construct a copy of an existing SerializableNDArray. The copy shares the same
     * underlying DDict entry and key, meaning both ndarrays refer to the same data.
     */
    SerializableNDArray(const SerializableNDArray<Type, TVal>& other) :
        mElementSize(other.mElementSize),
        mDimensions(other.mDimensions),
        mIndices(other.mIndices),
        mBuf(other.mBuf),
        mDDictSer(other.mDDictSer),
        mNDArrayKey(other.mNDArrayKey),
        mTimeoutVal(other.mTimeoutVal) {

        if (other.mTimeout == nullptr) {
            mTimeout = nullptr;
        } else {
            mTimeout = &mTimeoutVal;
        }

        ndarray_attach(&mDDict, other.mDDictSer.val().c_str(), mTimeout);
        mCachedData = other.mCachedData;
    }

    int type_id() const override {
        return TVal;
    }

    void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const override {
        SerializableInt secs(mTimeoutVal.tv_sec);
        SerializableInt nsecs(mTimeoutVal.tv_nsec);

        mElementSize.serialize(sendh, arg, true, timeout);
        mDimensions.serialize(sendh, arg, true, timeout);
        mIndices.serialize(sendh, arg, true, timeout);
        mDDictSer.serialize(sendh, arg, true, timeout);
        mNDArrayKey.serialize(sendh, arg, true, timeout);
        secs.serialize(sendh, arg, true, timeout);
        nsecs.serialize(sendh, arg, buffer, timeout);
    }

    static SerializableNDArray<Type, TVal> deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
        SerializableInt element_size = SerializableInt::deserialize(recvh, arg, timeout);
        SerializableIntVector dimensions = SerializableIntVector::deserialize(recvh, arg, timeout);
        SerializableIntVector indices = SerializableIntVector::deserialize(recvh, arg, timeout);
        SerializableString ddict_ser = SerializableString::deserialize(recvh, arg, timeout);
        SerializableString ndarray_key = SerializableString::deserialize(recvh, arg, timeout);
        SerializableInt secs = SerializableInt::deserialize(recvh, arg, timeout);
        SerializableInt nsecs = SerializableInt::deserialize(recvh, arg, timeout);

        return SerializableNDArray<Type, TVal>(element_size, dimensions, indices, ddict_ser, ndarray_key, secs, nsecs, nullptr);
    }

    /** @brief Index operator
     *
     * The user can index into a serializable ndarray up to the dimensions of the
     * data. Once all dimensions have been specified, the user can access
     * individual elements of the ndarray. Elements must be of
     * element_size as specified when the ndarray was created. The user
     * can also get the address of any element, including rows within the
     * ndarray. It is assumed that data within the ndarray is stored in
     * row major form abstracted to higher dimensions. The cached data is
     * read lazily when it is actually going to be referenced. This
     * behavior is triggered by calling the index of operator.
     *
     * @param idx The index into the current dimension.
     */
    SerializableNDArray<Type, TVal> operator[](int idx) const {

        if (mCachedData == nullptr)
            refresh();

        std::vector<int> new_indices = mIndices.val();
        if (new_indices.size() >= mDimensions.val().size())
            throw DragonError(DRAGON_INVALID_ARGUMENT, "Too many dimensions indexing into SerializableNDArray.");
        int dim_size = mDimensions.val()[new_indices.size()];
        if (idx >= dim_size)
            throw DragonError(DRAGON_INVALID_ARGUMENT, "Index out of bounds for SerializableNDArray.");
        new_indices.push_back(idx);
        SerializableInt secs(mTimeoutVal.tv_sec);
        SerializableInt nsecs(mTimeoutVal.tv_nsec);
        return SerializableNDArray<Type, TVal>(mElementSize, mDimensions, SerializableIntVector(new_indices), mDDictSer, mNDArrayKey, secs, nsecs, &mBuf, &mDDict);
    }

    /** @brief Address Of
     *
     * Provide the address of a particular row or element within the ndarray. This address is only valid within the current
     * process and should not be shared.
     */
    void* operator&() const {
        if (mCachedData == nullptr)
            refresh();

        const std::vector<int> dims = mDimensions.val();
        const std::vector<int> idxs = mIndices.val();
        const auto elem_sz = (size_t)mElementSize.val();
        const size_t n = dims.size();
        const size_t m = idxs.size();

        // Compute element offset using row-major (C) ordering.
        // For indices [i0, i1, ..., i_{m-1}] into dimensions [d0, d1, ..., d_{n-1}]:
        //   offset = i0*(d1*...*d_{n-1}) + i1*(d2*...*d_{n-1}) + ... + i_{m-1}*(d_m*...*d_{n-1})
        size_t offset_elements = 0;
        for (size_t k = 0; k < m; k++) {
            size_t stride = 1;
            for (size_t j = k + 1; j < n; j++)
                stride *= (size_t)dims[j];
            offset_elements += (size_t)idxs[k] * stride;
        }

        size_t offset = offset_elements * elem_sz;

        if (offset > mBuf.getSize())
            throw DragonError(DRAGON_FAILURE, "The address offset was bigger than the size of the ndarray. This should not happen.");

        return (uint8_t*)mCachedData + offset;
    }

    /** @brief Refresh cached data
     *
     * If accessing the data of the ndarray, data is cached locally. This method will retrieve the shared
     * data from the DDict source and throw away any currently cached data.
     */
    void refresh() const {
        mBuf = ndarray_get(&mDDict, mNDArrayKey, mTimeout);
        mCachedData = mBuf.getPtr();
    }

    /** @brief Update NDArray with Cached data
     *
     * Calling this rewrites the ndarray data with cached data.
     */
    void sync() {
        if (mCachedData == nullptr)
            throw DragonError(DRAGON_INVALID_OPERATION, "Cannot sync SerializableNDArray: no cached data.");

        ndarray_put(&mDDict, mNDArrayKey, mBuf, mTimeout);
    }

    /** @brief Access an element of an NDArray
     *
     * When indices have fully specified the location of an element, that element will be yielded
     * by calling this method.
     */
    Type val() {
        if ((size_t)mElementSize.val() != sizeof(Type))
            throw DragonError(DRAGON_INVALID_ARGUMENT, "The element size does not match size of Type.");
        if (mIndices.val().size() != mDimensions.val().size())
            throw DragonError(DRAGON_INVALID_ARGUMENT, "You must fully specify the indices to an element to use asLong.");

        if (mCachedData == nullptr) {
            refresh();
        }
        void* addr = &(*this);
        return *(Type*)addr;
    }

    /** @brief Return the size of a dimension in the ndarray.
     *
     * Calling this will return the size of a vector in the
     * ndarray. The vector does not really exist. What is returned
     * is the size of the next unspecified dimension of the ndarray.
     */

    int size() const {
        size_t specified_indices = mIndices.val().size();

        if (specified_indices == mDimensions.val().size())
            throw DragonError(DRAGON_INVALID_OPERATION, "You cannot request the size of a primitive ndarray location.");


        return mDimensions.val()[specified_indices];
    }

    /** @brief Conversion operator
     *
     * Convert the SerializableNDArray to its template Type. This is useful when all indices have
     * been provided and the user want to access an element at the specified set of indices.
     */
    operator Type() {
        return val();
    }

    /** @brief Assignment operator
     *
     * Store a value at the indexed location. The region size for the remaining un-indexed
     * dimensions must match the size of a double.
     */
    SerializableNDArray<Type, TVal>& operator=(Type value) {
        // Check that region size matches sizeof(Type)
        if (region_size() != sizeof(Type))
            throw DragonError(DRAGON_INVALID_ARGUMENT, "Region size does not match size of double.");

        // Get address and store value
        void* addr = &(*this);
        *(Type*)addr = value;

        return *this;
    }

    /** @brief Assignment operator for another NDArray
     *
     * Copy data from another ndarray to this ndarray. Both ndarrays must have the same region size.
     */
    SerializableNDArray<Type, TVal>& operator=(const SerializableNDArray<Type, TVal>& other) {
        // Check that region sizes match
        if (region_size() != other.region_size())
            throw DragonError(DRAGON_INVALID_ARGUMENT, "Region sizes do not match for ndarray assignment.");

        // Get addresses and copy data
        void* dest_addr = &(*this);
        void* src_addr = other.operator&();
        memcpy(dest_addr, src_addr, region_size());

        return *this;
    }

    /** @brief Get the size of the specified region
     *
     * Compute and return the size in bytes of the region specified by the current indices.
     * This is the size of the un-indexed sub-region in bytes.
     */
    size_t region_size() const {
        const std::vector<int> dims = mDimensions.val();
        const std::vector<int> idxs = mIndices.val();
        const auto elem_sz = (size_t)mElementSize.val();
        const size_t n = dims.size();
        const size_t m = idxs.size();

        // Compute region size (product of remaining dimensions)
        size_t region_elements = 1;
        for (size_t k = m; k < n; k++)
            region_elements *= (size_t)dims[k];

        return region_elements * elem_sz;
    }

    /** @brief Create a new ndarray from the current region
     *
     * Constructs a new ndarray representing the current indexed region with the remaining
     * un-indexed dimensions. The new ndarray has empty indices and can be indexed further
     * or accessed directly.
     *
     * Calling regionAsNDArray makes a copy of the region. It does not share data with the
     * current NDArray. If you want to share data, you can index into the current
     * array and get the address of any specific region.
     */
    SerializableNDArray<Type, TVal> regionAsNDArray() const {
        // Get the remaining dimensions (those not yet indexed)
        std::vector<int> original_dims = mDimensions.val();
        std::vector<int> current_indices = mIndices.val();
        std::vector<int> remaining_dims(
            original_dims.begin() + current_indices.size(),
            original_dims.end()
        );

        // Get the region data address
        void* region_addr = this->operator&();

        // Create new ndarray with region data
        SerializableNDArray<Type, TVal> region(
            remaining_dims,
            region_addr,
            mDDictSer.val().c_str(),
            mTimeout
        );

        region.refresh();
        return region;
    }

private:
    SerializableNDArray(const SerializableInt& sz, const SerializableIntVector& dim, const SerializableIntVector& idxs,
        const SerializableString& dd_ser, const SerializableString& tdk, const SerializableInt& secs,
        const SerializableInt& ns, const SerializableByteBuffer* buf,
        const dragonDDictDescr_t* ddict = nullptr):
        mElementSize(sz), mDimensions(dim), mIndices(idxs), mDDictSer(dd_ser), mNDArrayKey(tdk) {

        if (secs == -1) {
            mTimeout = nullptr;
            // -1 is the "no timeout" sentinel and must round-trip through serialize/operator[].
            mTimeoutVal = {-1,-1};
        } else {
            mTimeoutVal.tv_sec = secs.val();
            mTimeoutVal.tv_nsec = ns.val();
            mTimeout = &mTimeoutVal;
        }

        if (ddict != nullptr)
            /* Indexing shares the attachment of the array it indexes into. Attaching here
               would create channels for every element access. */
            mDDict = *ddict;
        else
            ndarray_attach(&mDDict, dd_ser.val().c_str(), mTimeout);

        if (buf != nullptr) {
            mBuf = *buf;
            mCachedData = mBuf.getPtr();
        } else {
            mBuf = SerializableByteBuffer(0, nullptr);
            mCachedData = nullptr;
        }
    }

    SerializableInt mElementSize;
    SerializableIntVector mDimensions;
    SerializableIntVector mIndices;
    mutable SerializableByteBuffer mBuf;
    SerializableString mDDictSer;
    SerializableString mNDArrayKey;
    timespec_t mTimeoutVal;
    timespec_t* mTimeout;
    mutable dragonDDictDescr_t mDDict;
    mutable void* mCachedData;
};

/* More Pre-defined Serializable Types*/
using SerializableDoubleNDArray = SerializableNDArray<double, DragonSerType::SERTYPE_DOUBLENDARRAY>;
using SerializableFloatNDArray = SerializableNDArray<float, DragonSerType::SERTYPE_FLOATNDARRAY>;
using SerializableIntNDArray = SerializableNDArray<int, DragonSerType::SERTYPE_INTNDARRAY>;
using SerializableLongNDArray = SerializableNDArray<long, DragonSerType::SERTYPE_LONGNDARRAY>;

/* Declared here so a SerializableQueue can be built from a Queue. */
template <class Serializable> class Queue;

/**
 * @class SerializableQueue
 * @brief A Serializable Dragon Queue
 *
 * The class provides the Serializable interface for Dragon Queues so that a Queue
 * may itself be sent through a Queue or stored in a DDict. Only the base64 encoded
 * descriptor of the Queue travels between processes, exactly as it does when a Queue
 * is serialized in Python and attached to in C++. The lifetime of the Queue is still
 * managed by whoever created it.
 *
 * The template argument mirrors the Queue template argument. It is the type of the
 * values carried by the Queue that this descriptor refers to.
 */
template<class Serializable>
class SerializableQueue: public SerializableBase {
public:
    /** @brief Default constructor providing an empty descriptor. */
    SerializableQueue() = default;

    /** @brief Construct from a base64 encoded serialized Queue descriptor. */
    SerializableQueue(const std::string& serialized): mSerialized(serialized) {}

    /** @brief Construct from a base64 encoded serialized Queue descriptor. */
    SerializableQueue(const char* serialized): mSerialized(std::string(serialized)) {}

    /** @brief Construct from an attached Queue.
     *
     * This is defined in queue.hpp because it requires the complete Queue template.
     */
    SerializableQueue(Queue<Serializable>& queue);

    /**
     * @brief See the DerivedSerializable serialize description.
     */
    void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const override {
        mSerialized.serialize(sendh, arg, buffer, timeout);
    }

    /**
     * @brief See the DerivedSerializable deserialize description.
     */
    static SerializableQueue<Serializable> deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
        return SerializableQueue<Serializable>(SerializableString::deserialize(recvh, arg, timeout).val());
    }

    /**
     * @brief Get the base64 encoded descriptor of the Queue.
     *
     * Pass this to the Queue attach constructor to interact with the Queue. For instance
     *
     *     Queue<Serializable> queue(serializable_queue.val().c_str(), nullptr);
     *
     * @returns The serialized descriptor of the Queue.
     */
    std::string val() const {
        return mSerialized.val();
    }

    /**
     * @brief See the DerivedSerializable type_id description
     */
    int type_id() const override {
        return DragonSerType::SERTYPE_QUEUE;
    }

private:
    SerializableString mSerialized;
};

/* Declared here so a SerializableDDict can be built from a DDict. */
template <class SerializableKey, class SerializableValue> class DDict;

/**
 * @class SerializableDDict
 * @brief A Serializable Distributed Dictionary
 *
 * The class provides the Serializable interface for Dragon Distributed Dictionaries
 * so that a DDict may be sent through a Queue or stored in another DDict. Only the
 * base64 encoded descriptor of the DDict travels between processes, exactly as it does
 * when a DDict is serialized in Python and attached to in C++. The lifetime of the
 * DDict is still managed by whoever created it.
 *
 * The template arguments mirror the DDict template arguments. They are the key and
 * value types of the DDict that this descriptor refers to.
 */
template<class SerializableKey, class SerializableValue>
class SerializableDDict: public SerializableBase {
public:
    /** @brief Default constructor providing an empty descriptor. */
    SerializableDDict() = default;

    /** @brief Construct from a base64 encoded serialized DDict descriptor. */
    SerializableDDict(const std::string& serialized): mSerialized(serialized) {}

    /** @brief Construct from a base64 encoded serialized DDict descriptor. */
    SerializableDDict(const char* serialized): mSerialized(std::string(serialized)) {}

    /** @brief Construct from an attached DDict.
     *
     * This is defined in dictionary.hpp because it requires the complete DDict template.
     */
    SerializableDDict(DDict<SerializableKey, SerializableValue>& dict);

    /**
     * @brief See the DerivedSerializable serialize description.
     */
    void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const override {
        mSerialized.serialize(sendh, arg, buffer, timeout);
    }

    /**
     * @brief See the DerivedSerializable deserialize description.
     */
    static SerializableDDict<SerializableKey, SerializableValue> deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
        return SerializableDDict<SerializableKey, SerializableValue>(SerializableString::deserialize(recvh, arg, timeout).val());
    }

    /**
     * @brief Get the base64 encoded descriptor of the DDict.
     *
     * Assigning a received Serializable to a DDict attaches to it directly, but the
     * descriptor is available here when a timeout is needed on the attach. For instance
     *
     *     DDict<Serializable, Serializable> dict(serializable_ddict.val().c_str(), &timeout);
     *
     * @returns The serialized descriptor of the DDict.
     */
    std::string val() const {
        return mSerialized.val();
    }

    /**
     * @brief See the DerivedSerializable type_id description
     */
    int type_id() const override {
        return DragonSerType::SERTYPE_DDICT;
    }

private:
    SerializableString mSerialized;
};

/* Declared here so a SerializableBarrier and SerializableSemaphore can be built from them. */
class Barrier;
class Semaphore;

/**
 * @class SerializableBarrier
 * @brief A Serializable Dragon Barrier
 *
 * The class provides the Serializable interface for Dragon Barriers so that a Barrier
 * may be sent through a Queue or stored in a DDict. Only the base64 encoded descriptor
 * of the Barrier travels between processes, exactly as it does when a Barrier is
 * serialized in Python and attached to in C++. The lifetime of the Barrier is still
 * managed by whoever created it.
 */
class SerializableBarrier: public SerializableBase {
public:
    /** @brief Default constructor providing an empty descriptor. */
    SerializableBarrier() = default;

    /** @brief Construct from a base64 encoded serialized Barrier descriptor. */
    SerializableBarrier(const std::string& serialized): mSerialized(serialized) {}

    /** @brief Construct from a base64 encoded serialized Barrier descriptor. */
    SerializableBarrier(const char* serialized): mSerialized(std::string(serialized)) {}

    /** @brief Construct from an attached Barrier.
     *
     * This is defined in barrier.hpp because it requires the complete Barrier class.
     */
    SerializableBarrier(Barrier& barrier);

    /**
     * @brief See the DerivedSerializable serialize description.
     */
    void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const override {
        mSerialized.serialize(sendh, arg, buffer, timeout);
    }

    /**
     * @brief See the DerivedSerializable deserialize description.
     */
    static SerializableBarrier deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
        return SerializableBarrier(SerializableString::deserialize(recvh, arg, timeout).val());
    }

    /**
     * @brief Get the base64 encoded descriptor of the Barrier.
     *
     * Assigning a received Serializable to a Barrier attaches to it directly, but the
     * descriptor is available here when an action is needed on the attach. For instance
     *
     *     Barrier barrier(serializable_barrier.val().c_str(), my_action);
     *
     * @returns The serialized descriptor of the Barrier.
     */
    std::string val() const {
        return mSerialized.val();
    }

    /**
     * @brief See the DerivedSerializable type_id description
     */
    int type_id() const override {
        return DragonSerType::SERTYPE_BARRIER;
    }

private:
    SerializableString mSerialized;
};

/**
 * @class SerializableSemaphore
 * @brief A Serializable Dragon Semaphore
 *
 * The class provides the Serializable interface for Dragon Semaphores so that a
 * Semaphore may be sent through a Queue or stored in a DDict. Only the base64 encoded
 * descriptor of the Semaphore travels between processes, exactly as it does when a
 * Semaphore is serialized in Python and attached to in C++. The lifetime of the
 * Semaphore is still managed by whoever created it.
 */
class SerializableSemaphore: public SerializableBase {
public:
    /** @brief Default constructor providing an empty descriptor. */
    SerializableSemaphore() = default;

    /** @brief Construct from a base64 encoded serialized Semaphore descriptor. */
    SerializableSemaphore(const std::string& serialized): mSerialized(serialized) {}

    /** @brief Construct from a base64 encoded serialized Semaphore descriptor. */
    SerializableSemaphore(const char* serialized): mSerialized(std::string(serialized)) {}

    /** @brief Construct from an attached Semaphore.
     *
     * This is defined in semaphore.hpp because it requires the complete Semaphore class.
     */
    SerializableSemaphore(Semaphore& semaphore);

    /**
     * @brief See the DerivedSerializable serialize description.
     */
    void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const override {
        mSerialized.serialize(sendh, arg, buffer, timeout);
    }

    /**
     * @brief See the DerivedSerializable deserialize description.
     */
    static SerializableSemaphore deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout) {
        return SerializableSemaphore(SerializableString::deserialize(recvh, arg, timeout).val());
    }

    /**
     * @brief Get the base64 encoded descriptor of the Semaphore.
     *
     * @returns The serialized descriptor of the Semaphore.
     */
    std::string val() const {
        return mSerialized.val();
    }

    /**
     * @brief See the DerivedSerializable type_id description
     */
    int type_id() const override {
        return DragonSerType::SERTYPE_SEMAPHORE;
    }

private:
    SerializableString mSerialized;
};

/**
 * @class Serializable
 * @brief The Serializable class can be used to encompass any of the pre-defined Serializable
 * types, providing a means to communicate any Serializable over a Dragon FLI, in particular
 * Queues and DDicts.
 *
 * The Serializable class wraps objects of other types allowing for them to be safely shared
 * over and FLI connection and understood at the other end by the receiver when they are deserialized.
 * All the pre-defined types are safely wrapped and unwrapped into/from the Serializable class as
 * needed.
 */
class Serializable: public SerializableBase {
    public:
    using DeserializeFn = Serializable (*)(dragonFLIRecvHandleDescr_t*, uint64_t*, const timespec_t*);

    /* Constructors and Assignment */
    Serializable(Serializable&& other);
    Serializable(const Serializable& other);
    Serializable& operator=(const Serializable& other);
    Serializable& operator=(Serializable&& other);

    /* Type Conversion constructors */
    Serializable(int i);
    Serializable(double d);
    Serializable(std::initializer_list<int> v);
    Serializable(std::initializer_list<double> v);
    Serializable(std::initializer_list<std::initializer_list<double>> m);
    Serializable(const std::vector<int>& v);
    Serializable(const std::vector<double>& v);
    Serializable(const std::vector<std::vector<int>>& m);
    Serializable(const std::vector<std::vector<double>>& m);
    Serializable(size_t size, uint8_t* ptr);
    Serializable(const char* s);
    Serializable(const SerializableString& s);
    Serializable(const SerializableInt& i);
    Serializable(const SerializableDouble& d);
    Serializable(const SerializableIntVector& v);
    Serializable(const SerializableDoubleVector& v);
    Serializable(const Serializable2DIntMatrix& v);
    Serializable(const Serializable2DDoubleMatrix& v);
    Serializable(const SerializableByteBuffer& b);
    Serializable(const SerializableDoubleNDArray& t);
    Serializable(const SerializableFloatNDArray& t);
    Serializable(const SerializableIntNDArray& t);
    Serializable(const SerializableLongNDArray& t);
    Serializable(const SerializableQueue<Serializable>& q);
    Serializable(const SerializableDDict<Serializable, Serializable>& d);
    Serializable(const SerializableBarrier& b);
    Serializable(const SerializableSemaphore& s);


    virtual ~Serializable();

    /**
     * @brief Serialize an object for sending with the given send handle. See the DerivedClass for a description of serialization.
     */
    virtual void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const;

    /**
     * @brief Deserialize an object for by receiving from the given receive handle. See the DerivedClass for a description of deserialization.
     */
    static Serializable deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout);

    /**
     * @brief Register or replace a deserializer function for a Serializable type id.
     */
    static void register_deserializer(int ty_val, DeserializeFn fn);

    /**
     * @brief Return the type identifer for this wrapped serializable. This can be called to determine the wrapped
     * type of the Serializable object before unwrapping it should you need to discover the wrapped type dynamically at
     * run-time. It is not required to check before unwrapping, but unwrapping to an incorrect type will result in a DragonError
     * being thrown.
     */
    int type_id() const;

    SerializableBase* val() const;

    /**
     * @brief Safely unwrap a SerializableString object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableString asSerializableString() const;

    /**
     * @brief Safely unwrap a SerializableInt object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableInt asSerializableInt() const;

    /**
     * @brief Safely unwrap a SerializableDouble object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableDouble asSerializableDouble() const;

    /**
     * @brief Safely unwrap a SerializableIntVector object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableIntVector asSerializableIntVector() const;

    /**
     * @brief Safely unwrap a SerializableDoubleVector object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableDoubleVector asSerializableDoubleVector() const;

    /**
     * @brief Safely unwrap a Serializable2DIntMatrix object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    Serializable2DIntMatrix asSerializable2DIntMatrix() const;

    /**
     * @brief Safely unwrap a Serializable2DDoubleMatrix object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    Serializable2DDoubleMatrix asSerializable2DDoubleMatrix() const;

    /**
     * @brief Safely unwrap a SerializableByteBuffer object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableByteBuffer asSerializableByteBuffer() const;

    /**
     * @brief Safely unwrap a SerializableDoubleNDArray object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableDoubleNDArray asSerializableDoubleNDArray() const;

    /**
     * @brief Safely unwrap a SerializableFloatNDArray object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableFloatNDArray asSerializableFloatNDArray() const;

    /**
     * @brief Safely unwrap a SerializableIntNDArray object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableIntNDArray asSerializableIntNDArray() const;

    /**
     * @brief Safely unwrap a SerializableLongNDArray object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableLongNDArray asSerializableLongNDArray() const;

    /**
     * @brief Safely unwrap a SerializableQueue object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableQueue<Serializable> asSerializableQueue() const;

    /**
     * @brief Safely unwrap a SerializableDDict object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableDDict<Serializable, Serializable> asSerializableDDict() const;

    /**
     * @brief Safely unwrap a SerializableBarrier object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableBarrier asSerializableBarrier() const;

    /**
     * @brief Safely unwrap a SerializableSemaphore object. If the object is not of the correct type a
     * DragonError will be thrown.
     */
    SerializableSemaphore asSerializableSemaphore() const;

    bool operator==(const Serializable& other) const;

    bool operator!=(const Serializable& other) const;

    template <typename T, typename std::enable_if<!std::is_same<typename std::decay<T>::type, Serializable>::value && std::is_constructible<Serializable, T>::value, int>::type = 0>
    bool operator==(const T& other) const {
        return *this == Serializable(other);
    }

    template <typename T, typename std::enable_if<!std::is_same<typename std::decay<T>::type, Serializable>::value && std::is_constructible<Serializable, T>::value, int>::type = 0>
    bool operator!=(const T& other) const {
        return !(*this == other);
    }

    template <typename T, typename std::enable_if<!std::is_same<typename std::decay<T>::type, Serializable>::value && std::is_constructible<Serializable, T>::value, int>::type = 0>
    friend bool operator==(const T& lhs, const Serializable& rhs) {
        return rhs == lhs;
    }

    template <typename T, typename std::enable_if<!std::is_same<typename std::decay<T>::type, Serializable>::value && std::is_constructible<Serializable, T>::value, int>::type = 0>
    friend bool operator!=(const T& lhs, const Serializable& rhs) {
        return !(rhs == lhs);
    }


    /* Automatic Type Assignment conversion operators */
    operator SerializableString() const {
        return asSerializableString();
    }
    operator SerializableInt() const {
        return asSerializableInt();
    }
    operator int() const {
        return asSerializableInt().val();
    }
    operator SerializableDouble() const {
        return asSerializableDouble();
    }
    operator double() const {
        return asSerializableDouble().val();
    }
    operator std::string() const {
        return asSerializableString().val();
    }
    operator SerializableIntVector() const {
        return asSerializableIntVector();
    }
    operator std::vector<int>() const {
        return asSerializableIntVector().val();
    }
    operator SerializableDoubleVector() const {
        return asSerializableDoubleVector();
    }
    operator std::vector<double>() const {
        return asSerializableDoubleVector().val();
    }
    operator Serializable2DIntMatrix() const {
        return asSerializable2DIntMatrix();
    }
    operator std::vector<std::vector<int>>() const {
        return asSerializable2DIntMatrix().val();
    }
    operator Serializable2DDoubleMatrix() const {
        return asSerializable2DDoubleMatrix();
    }
    operator std::vector<std::vector<double>>() const {
        return asSerializable2DDoubleMatrix().val();
    }
    operator SerializableByteBuffer() const {
        return asSerializableByteBuffer();
    }
    operator SerializableDoubleNDArray() const {
        return asSerializableDoubleNDArray();
    }
    operator SerializableFloatNDArray() const {
        return asSerializableFloatNDArray();
    }
    operator SerializableIntNDArray() const {
        return asSerializableIntNDArray();
    }
    operator SerializableLongNDArray() const {
        return asSerializableLongNDArray();
    }
    operator SerializableQueue<Serializable>() const {
        return asSerializableQueue();
    }

    /**
     * @brief Convert a received SerializableQueue directly into an attached Queue.
     *
     * This is defined in queue.hpp because it requires the complete Queue template.
     */
    template<class Value>
    operator Queue<Value>() const;

    operator SerializableDDict<Serializable, Serializable>() const {
        return asSerializableDDict();
    }

    /**
     * @brief Convert a received SerializableDDict directly into an attached DDict.
     *
     * This is defined in dictionary.hpp because it requires the complete DDict template.
     */
    template<class Key, class Value>
    operator DDict<Key, Value>() const;

    operator SerializableBarrier() const {
        return asSerializableBarrier();
    }

    operator SerializableSemaphore() const {
        return asSerializableSemaphore();
    }

    /**
     * @brief Convert a received SerializableBarrier directly into an attached Barrier.
     *
     * This is defined in barrier.hpp because it requires the complete Barrier class.
     */
    operator Barrier() const;

    /**
     * @brief Convert a received SerializableSemaphore directly into an attached Semaphore.
     *
     * This is defined in semaphore.hpp because it requires the complete Semaphore class.
     */
    operator Semaphore() const;

    private:
    static std::map<int, DeserializeFn> sDeserializers;
    std::unique_ptr<SerializableBase> mVal;

};

}

#endif