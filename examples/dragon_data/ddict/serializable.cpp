#include <serializable.hpp>


// serializable int
SerializableInt::SerializableInt(): val(0) {}
SerializableInt::SerializableInt(int x): val(x) {}

SerializableInt* SerializableInt::create(size_t num_bytes, uint8_t* data) {
    auto val = new SerializableInt((int)*data);
    free(data);
    return val;
}

void SerializableInt::serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) {
    dragonError_t err;
    err = dragon_ddict_write_bytes(req, sizeof(int), (uint8_t*)&val);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, dragon_getlasterrstr());
}

void SerializableInt::deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) {
    dragonError_t err = DRAGON_SUCCESS;
    size_t actual_size;
    uint8_t * received_val = nullptr;
    size_t num_val_expected = 1;


    err = dragon_ddict_read_bytes(req, sizeof(int), &actual_size, &received_val);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, dragon_getlasterrstr());

    if (actual_size != sizeof(int))
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The size of the integer was not correct.");

    val = *reinterpret_cast<int*>(received_val);

    free(received_val);

    err = dragon_ddict_read_bytes(req, sizeof(int), &actual_size, &received_val);

    if (err != DRAGON_EOT) {
        fprintf(stderr, "Did not received expected EOT, ec: %s\ntraceback: %s\n", dragon_get_rc_string(err), dragon_getlasterrstr());
        fflush(stderr);
    }
}

int SerializableInt::getVal() const {return val;}


// serializable double
SerializableDouble::SerializableDouble(): val(0) {}
SerializableDouble::SerializableDouble(double x): val(x) {}

SerializableDouble* SerializableDouble::create(size_t num_bytes, uint8_t* data) {
    auto val = new SerializableDouble((double)*data);
    free(data);
    return val;
}

void SerializableDouble::serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) {
    dragonError_t err;
    uint8_t* val_bytes = (uint8_t*)&val;
    err = dragon_ddict_write_bytes(req, sizeof(double), (uint8_t*)&val);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, dragon_getlasterrstr());
}

void SerializableDouble::deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) {
    dragonError_t err = DRAGON_SUCCESS;
    size_t actual_size;
    uint8_t * received_val = nullptr;
    size_t num_val_expected = 1;


    err = dragon_ddict_read_bytes(req, sizeof(double), &actual_size, &received_val);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, dragon_getlasterrstr());

    if (actual_size != sizeof(double))
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The size of the double was not correct.");

    val = *reinterpret_cast<double*>(received_val);

    free(received_val);

    err = dragon_ddict_read_bytes(req, sizeof(double), &actual_size, &received_val);

    if (err != DRAGON_EOT) {
        fprintf(stderr, "Did not received expected EOT, ec: %s\ntraceback: %s\n", dragon_get_rc_string(err), dragon_getlasterrstr());
        fflush(stderr);
    }
}

double SerializableDouble::getVal() const {return val;}


// serializable string
SerializableString::SerializableString(): val("") {}
SerializableString::SerializableString(std::string x): val(x) {}

SerializableString* SerializableString::create(size_t num_bytes, uint8_t* data) {
    char* data_chars = reinterpret_cast<char*>(data);
    std::string data_str = std::string(*data_chars, strlen(data_chars));
    auto val = new SerializableString(data_str);
    free(data);
    return val;
}

void SerializableString::serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) {
    dragonError_t err;
    uint8_t* val_bytes = (uint8_t*)&val;
    err = dragon_ddict_write_bytes(req, strlen(val.c_str()), (uint8_t*)&val);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, dragon_getlasterrstr());
}

void SerializableString::deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) {
    dragonError_t err = DRAGON_SUCCESS;
    size_t actual_size;
    uint8_t * received_val = nullptr;
    size_t num_val_expected = 1;
    size_t max_size = 512;


    err = dragon_ddict_read_bytes(req, max_size, &actual_size, &received_val);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, dragon_getlasterrstr());

    val = std::string(reinterpret_cast<const char*>(received_val), actual_size);

    free(received_val);

    err = dragon_ddict_read_bytes(req, sizeof(double), &actual_size, &received_val);

    if (err != DRAGON_EOT) {
        fprintf(stderr, "Did not received expected EOT, ec: %s\ntraceback: %s\n", dragon_get_rc_string(err), dragon_getlasterrstr());
        fflush(stderr);
    }
}

std::string SerializableString::getVal() const {return val;}