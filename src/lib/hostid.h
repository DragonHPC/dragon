#include <dragon/global_types.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

dragonULInt
dragon_host_id();

dragonError_t
dragon_set_host_id(dragonULInt id);

#ifdef __cplusplus
}
#endif