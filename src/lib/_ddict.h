#ifndef _DRAGON_INTERNAL_DDICT_H_
#define _DRAGON_INTERNAL_DDICT_H_


#include <dragon/ddict.h>


#ifdef __cplusplus
extern "C" {
#endif
dragonError_t _dragon_ddict_attach(const char * dd_ser, dragonDDictDescr_t * dd, timespec_t * default_timeout,
                                   dragonChannelSerial_t *str_chser, dragonChannelSerial_t *resp_chser,
                                   dragonChannelSerial_t *buffered_resp_chser, char * mgr_ser);
#ifdef __cplusplus
}
#endif

#endif