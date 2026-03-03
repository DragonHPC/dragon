#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <cstring>
#include <dragon/exceptions.hpp>

namespace dragon {

using namespace std;

DragonError::DragonError(const dragonError_t err, const char* err_str, const char* tb):
    mErr(err), mErrStr(err_str), mTraceback(tb)
{}

DragonError::DragonError(const dragonError_t err, const char* err_str):
    mErr(err), mErrStr(err_str), mTraceback(dragon_getlasterrstr())
{}

DragonError::~DragonError() {}

dragonError_t DragonError::rc() const {
    return mErr;
}

const char* DragonError::err_str() const {
    return mErrStr.c_str();
}

const char* DragonError::tb() const {
    return mTraceback.c_str();
}

std::ostream& operator<<(std::ostream& os, const DragonError& obj) {
    os << "DragonError(" << dragon_get_rc_string(obj.rc()) << ", \"" << obj.err_str() << "\")";
    if (strlen(obj.tb()) > 0)
        os << "\n" << obj.tb();
    return os;
}

EmptyError::EmptyError(const dragonError_t err, const char* err_str): DragonError(err, err_str) {}

FullError::FullError(const dragonError_t err, const char* err_str): DragonError(err, err_str) {}

TimeoutError::TimeoutError(const dragonError_t err, const char* err_str): DragonError(err, err_str) {}

} // end dragon namespace
