#include <dragon/dictionary.hpp>
#include <dragon/return_codes.h>

class SerializableInt : public DDictSerializable {
    public:
    SerializableInt();
    SerializableInt(int x);
    virtual void serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout);
    virtual void deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout);
    int getVal() const;
    private:
    int val;
};

SerializableInt::SerializableInt(): val(0) {}
SerializableInt::SerializableInt(int x): val(x) {}

void SerializableInt::serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout)
{
    dragonError_t err;
    err = dragon_ddict_write_bytes(req, sizeof(int), (uint8_t*)&val, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, dragon_getlasterrstr());
}

void SerializableInt::deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout)
{
    dragonError_t err;
    size_t actual_size;

    err = dragon_ddict_read_bytes_into(req, sizeof(int), &actual_size, (uint8_t*)&val, timeout);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, dragon_getlasterrstr());
    if (actual_size != sizeof(int))
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The size of the integer was not correct.");
}

int SerializableInt::getVal() const {return val;}

void testit(char* ser) {
    SerializableInt x(6);
    SerializableInt y(42);
    DDict<SerializableInt, SerializableInt> test(ser, NULL);
    test[x] = y;
    SerializableInt z = test[x];
    printf("%d\n", z.getVal());
}