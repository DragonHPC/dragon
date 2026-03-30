#ifndef DRAGON_SERIALIZABLE_HPP
#define DRAGON_SERIALIZABLE_HPP

#include <string>
#include <vector>
#include <dragon/fli.h>
#include <dragon/exceptions.hpp>

namespace dragon {


/**
 * @class Serializable
 * @brief An abstract base class for deriving serializable classes
 *
 * Classes derived from this Serializable abstract base class are used when communicating
 * to/from other distributed objects like Queue and DDict objects. Users who have custom
 * serializable objects to send may implement their own custom serializer/deserializer
 * methods. A few standard serializer/deserializer classes are also provided.
 *
 * Please read the documentation of the DerivedSerializable template below. This
 * serves as an outline for how to write a Serializable subclass. The
 * DerivedSerializable is never meant to be instantiated. It just serves as a
 * convenience for documenting exactly what should be defined in subclasses of
 * Serializable.
 *
 */
class Serializable {
    public:

    /**
     * @brief Please see the documentation for the DerivedSerializable class.
     *
     * The DerivedSerializable documentation provides a description of what you must write
     * to subclass Serializable and use it in your program.
     */
    virtual void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const = 0;

    //static Serializable deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout);
};

/**
 * @class DerivedSerializable
 * @brief This class provides the outline of what a subclass of Serializable should look like.
 *
 * Use this documentation as an outline for writing your own subclasses of Serializable. DO NOT
 * instantiate this class and expect it to do anything.
 */
template<class Type>
class DerivedSerializable: public Serializable {
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
        throw DragonError(DRAGON_INVALID_OPERATION, "This class should not be instantiated. Inherit from Serializable instead.");
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
     * consolidated into one network transfer.
     * @param timeout A value of nullptr will wait forever to serialize/transfer data. If value of {0,0} will
     * try once. Otherwise, the timeout specifies how long to wait for the serialization/transfer to be completed.
     */
    virtual void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const {
        throw DragonError(DRAGON_INVALID_OPERATION, "This class should not be instantiated. Inherit from Serializable instead.");
    }

    /**
     * @brief Deserialize a serialized C++ object.
     *
     * This method should be written in the implementing derived subclass and should
     * return the derived subtype of Serializable. It may throw a
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
        throw DragonError(DRAGON_INVALID_OPERATION, "This class should not be instantiated. Inherit from Serializable instead.");
    }

    /**
     * @brief Get the wrapped value for the object.
     *
     * This method may be named whatever you like. It is not part of the Serializable class. And you may
     * define more than one accessor method like this to retrieve parts of your object. You will need
     * something like this to access your deserialized object in your program. The wrapped value (i.e.
     * Type) may also be more than one value which would then be passed to the constructor and you
     * would then have multiple accessor methods to get the various pieces out after deserialization.
     *
     * @returns A value.
     */
    Type getVal() const {
        throw DragonError(DRAGON_INVALID_OPERATION, "This class should not be instantiated. Inherit from Serializable instead.");
    }

    private:
    Type mVal;
};

/**
 * @class SerializableInt
 * @brief A Serializable integer class
 *
 * The class provides the Serializable interface for integers.
 */
class SerializableInt : public Serializable {
    public:
    /**
     * @brief Constructor for Serializable Ints
     *
     * This provides a wrapper class for integer values that need to be serialized/deserialized in a
     * Dragon program.
     *
     * @param x An integer value to wrap.
     */
    SerializableInt(int x);

    /**
     * @brief See the DerivedSerializable serialize description.
     */
    virtual void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const;

    /**
     * @brief See the DerivedSerializable deserialize description.
     */
    static SerializableInt deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout);

    /**
     * @brief Get the wrapped value for the object.
     *
     * @returns The wrapped value.
     */
    int getVal() const;

    private:
    int mVal=0;
};


/**
 * @class SerializableString
 * @brief A Serializable string class
 *
 * The class provides the Serializable interface for strings.
 */
class SerializableString : public Serializable {
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

    private:
    std::string mVal="";
};


/**
 * @class SerializableDouble
 * @brief A Serializable double class
 *
 * The class provides the Serializable interface for doubles.
 */
class SerializableDouble : public Serializable {
    public:
    /**
     * @brief Constructor for Serializable Doubles
     *
     * This provides a wrapper class for double values that need to be serialized/deserialized in a
     * Dragon program.
     *
     * @param x An double value to wrap.
     */
    SerializableDouble(double x);

    /**
     * @brief See the DerivedSerializable serialize description.
     */
    virtual void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const;

    /**
     * @brief See the DerivedSerializable deserialize description.
     */
    static SerializableDouble deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout);

    /**
     * @brief Get the wrapped value for the object.
     *
     * @returns The wrapped value.
     */
    double getVal() const;

    private:
    double mVal=0.0;
};


/**
 * @class SerializableDouble2DVector
 * @brief A Serializable 2D Vector of Doubles
 *
 * The class provides the Serializable interface for a 2D vector of doubles.
 */
class SerializableDouble2DVector : public Serializable {
    public:
    /**
     * @brief Constructor for Serializable 2D Doubles
     *
     * This provides a wrapper class for a 2D vector of double values that need to be serialized/deserialized in a
     * Dragon program.
     *
     * @param vec An double vector value to wrap.
     */
    SerializableDouble2DVector(std::vector<std::vector<double>> vec);

    /**
     * @brief See the DerivedSerializable serialize description.
     */
    virtual void serialize(dragonFLISendHandleDescr_t* sendh, uint64_t arg, const bool buffer, const timespec_t* timeout) const;

    /**
     * @brief See the DerivedSerializable deserialize description.
    */
    static SerializableDouble2DVector deserialize(dragonFLIRecvHandleDescr_t* recvh, uint64_t* arg, const timespec_t* timeout);

    /**
     * @brief Get the wrapped value for the object.
     *
     * @returns The wrapped value.
     */
    const std::vector<std::vector<double>>& getVal() const;

    private:
    std::vector<std::vector<double>> mVal;
};

}

#endif