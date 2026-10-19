#ifndef exceptions_hpp
#define exceptions_hpp

#include <dragon/return_codes.h>
#include <string>

namespace dragon {

class DragonError {
    public:
    DragonError(const dragonError_t err, const char* err_str, const char* tb);
    DragonError(const dragonError_t err, const char* err_str);
    virtual ~DragonError();
    virtual dragonError_t rc() const;
    virtual const char* err_str() const;
    virtual const char* tb() const;

    private:
    dragonError_t mErr;
    std::string mErrStr;
    std::string mTraceback;
};

std::ostream& operator<<(std::ostream& os, const DragonError& obj);

class EmptyError: public DragonError {
    public:
    EmptyError(const dragonError_t err, const char* err_str);
};

class FullError: public DragonError {
    public:
    FullError(const dragonError_t err, const char* err_str);
};

class TimeoutError: public DragonError {
    public:
    TimeoutError(const dragonError_t err, const char* err_str);
};

} // end dragon namespace

#endif