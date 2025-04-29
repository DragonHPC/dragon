#ifndef HAVE_DRAGON_ERR_H
#define HAVE_DRAGON_ERR_H

#include <string.h>
#include <stdio.h>
#include <dragon/utils.h>

#ifdef __cplusplus

#include <thread>

extern "C" {
#endif

extern bool dg_enable_errstr;

char * dragon_getlasterrstr();
char * dragon_getrawerrstr();
void dragon_setrawerrstr(char* err_str);
void dragon_enable_errstr(bool enable_errstr);


#define DRAGON_MAX_ERRSTR_REC_LEN 4096

#ifdef DRAGON_DEBUG

/* This can be modified during debugging if desired */
#define no_err_return(err) ({\
    if (dg_enable_errstr) {\
        _set_errstr(NULL);\
    }\
    return err;\
})

#define err_return(err, str) ({\
    if (dg_enable_errstr) {\
        char * head = (char*) malloc(sizeof(char) * (snprintf(NULL, 0, "  %s: %s() (line %i) :: %s", __FILE__, __func__, __LINE__, dragon_get_rc_string(err)) + 1));\
        sprintf(head, "  %s: %s() (line %i) :: ", __FILE__, __func__, __LINE__);\
        _set_errstr(head);\
        free(head);\
        _append_errstr((char*)str);\
    }\
    return err;\
})

#define err_noreturn(str) ({\
    if (dg_enable_errstr) {\
        char * head = (char*) malloc(sizeof(char) * (snprintf(NULL, 0, "  %s: %s() (line %i) :: ", __FILE__, __func__, __LINE__) + 1));\
        sprintf(head, "  %s: %s() (line %i) :: ", __FILE__, __func__, __LINE__);\
        _set_errstr(head);\
        free(head);\
        _append_errstr((char*)str);\
    }\
})

#define append_err_return(err, str) ({\
    if (dg_enable_errstr) {\
        char * head = (char*) malloc(sizeof(char) * (snprintf(NULL, 0, "\n  %s: %s() (line %i) :: ", __FILE__, __func__, __LINE__) + 1));\
        sprintf(head, "\n  %s: %s() (line %i) :: ", __FILE__, __func__, __LINE__);\
        _append_errstr(head);\
        free(head);\
        _append_errstr((char*)str);\
    }\
    return err;\
})

#define append_err_noreturn(str) ({\
    if (dg_enable_errstr) {\
        char * head = (char*) malloc(sizeof(char) * (snprintf(NULL, 0, "\n  %s: %s() (line %i) :: ", __FILE__, __func__, __LINE__) + 1));\
        sprintf(head, "\n  %s: %s() (line %i) :: ", __FILE__, __func__, __LINE__);\
        _append_errstr(head);\
        free(head);\
        _append_errstr((char*)str);\
    }\
})
#else

#define no_err_return(err) ({\
    return err;\
})

#define err_return(err, str) ({\
    return err;\
})

#define err_noreturn(str) do {} while(0)

#define append_err_return(err, str) ({\
    return err;\
})

#define append_err_noreturn(str) do {} while(0)
#endif // DRAGON_DEBUG

// internal use only, use the macro defines above to interface with the
// error handling traceback utility.

#ifdef __cplusplus
extern thread_local char * errstr;
#else
extern _Thread_local char * errstr;
#endif

void _set_errstr(char * new_errstr);

void _append_errstr(char * more_errstr);

char * _errstr_with_code(char * str, int code);

#ifdef __cplusplus
}
#endif

#endif
