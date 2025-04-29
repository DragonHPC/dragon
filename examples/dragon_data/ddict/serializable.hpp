#include <dragon/dictionary.hpp>


// serializable int
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

// serializable double
class SerializableDouble : public DDictSerializable {
    public:
    SerializableDouble();
    SerializableDouble(double x);
    virtual void serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout);
    virtual void deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout);
    static SerializableDouble* create(size_t num_bytes, uint8_t* data);
    double getVal() const;

    private:
    double val=0.0;
};

// serializable string
class SerializableString : public DDictSerializable {
    public:
    SerializableString();
    SerializableString(std::string x);
    virtual void serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout);
    virtual void deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout);
    static SerializableString* create(size_t num_bytes, uint8_t* data);
    std::string getVal() const;

    private:
    std::string val="";
};