#ifndef DRAGON_SERIALIZABLE_HPP
#define DRAGON_SERIALIZABLE_HPP

#include <string>
#include <cstring>
#include <sstream>
#include <vector>
#include <initializer_list>
#include <functional>
#include <typeindex>
#include <type_traits>
#include <map>
#include <dragon/fli.h>
#include <dragon/exceptions.hpp>
#include <dragon/serializable_types.h>

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
    Type getVal() const {
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
    std::string getVal() const;

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
    size_t size;
    uint8_t* ptr;
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
    Type getVal() const {
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
            throw DragonError(err, "Could not write vector size to ddict.");

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

        return rv; // Relies on RVO for efficiently returning the vector.
    }

    /**
     * @brief Get the wrapped value for the object.
     *
     * @returns The wrapped value.
     */
    std::vector<Type> getVal() const {
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
     * @brief Constructor for Serializable Vector of Type
     *
     * This provides a wrapper class for a vector of Type values that need to be serialized/deserialized in a
     * Dragon program.
     *
     * @param vec A Type vector value to wrap.
     */
    Serializable2DMatrix(std::vector<std::vector<Type>> obj): mVal(obj) {}

    /**
     * @brief Constructor for Serializable Vector of Type
     *
     * Contruct an empty serializable Type vector with size elements.
     *
     * @param rows The number of rows for the empty matrix.
     * @param cols The number of columns for the empty matrix.
     */
    Serializable2DMatrix(size_t rows, size_t cols): mVal(std::vector<std::vector<Type>>()) {
        for (size_t i; i<rows; i++) {
            std::vector<Type> row;
            for (size_t j; j<cols; j++)
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
            throw DragonError(err, "Could not write bytes to ddict.");

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
            val.push_back(tmp_vec.getVal());
        }

        return Serializable2DMatrix<Type, TVal>(val); // Relies on RVO for efficiently returning the vector.
    }

    /**
     * @brief Get the wrapped value for the object.
     *
     * @returns The wrapped value.
     */
    std::vector<std::vector<Type>> getVal() const {
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

    SerializableBase* getVal() const;

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
        return asSerializableInt().getVal();
    }
    operator SerializableDouble() const {
        return asSerializableDouble();
    }
    operator double() const {
        return asSerializableDouble().getVal();
    }
    operator std::string() const {
        return asSerializableString().getVal();
    }
    operator SerializableIntVector() const {
        return asSerializableIntVector();
    }
    operator std::vector<int>() const {
        return asSerializableIntVector().getVal();
    }
    operator SerializableDoubleVector() const {
        return asSerializableDoubleVector();
    }
    operator std::vector<double>() const {
        return asSerializableDoubleVector().getVal();
    }
    operator Serializable2DIntMatrix() const {
        return asSerializable2DIntMatrix();
    }
    operator std::vector<std::vector<int>>() const {
        return asSerializable2DIntMatrix().getVal();
    }
    operator Serializable2DDoubleMatrix() const {
        return asSerializable2DDoubleMatrix();
    }
    operator std::vector<std::vector<double>>() const {
        return asSerializable2DDoubleMatrix().getVal();
    }
    operator SerializableByteBuffer() const {
        return asSerializableByteBuffer();
    }

    private:
    static std::map<int, DeserializeFn> sDeserializers;
    SerializableBase* mVal;

};

}

#endif