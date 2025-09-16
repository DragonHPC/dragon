#ifndef HAVE_DRAGON_UTILS_INTERNAL_H
#define HAVE_DRAGON_UTILS_INTERNAL_H

#include <dragon/return_codes.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__i386) || defined(__x86_64)
static inline void
rep_nop(void)
{
    __asm__ __volatile__("rep;nop": : :"memory");
}
#define PAUSE() rep_nop()
#elif defined(__aarch64__)
static inline void
rep_nop(void)
{
    __asm__ __volatile__("YIELD": : :"memory");
}
#define PAUSE() rep_nop()
#else
#define PAUSE()
#endif

void
_set_thread_local_mode_channels(bool set_thread_local);

void
_set_thread_local_mode_channelsets(bool set_thread_local);

void
_set_thread_local_mode_managed_memory(bool set_thread_local);

void
_set_thread_local_mode_bcast(bool set_thread_local);

void
_set_thread_local_mode_ddict(bool set_thread_local);

void
_set_thread_local_mode_fli(bool set_thread_local);


#ifdef __cplusplus
}
#endif

#define DRAGON_UUID_OFFSET_HID 0
#define DRAGON_UUID_OFFSET_PID 8
#define DRAGON_UUID_OFFSET_CTR 12

#ifdef __cplusplus
#define THREAD_LOCAL thread_local
#else
#define THREAD_LOCAL _Thread_local
#endif

#define DRAGON_GLOBAL_LIST(name)\
static dragonList_t *_dg_proc_##name = NULL;\
static THREAD_LOCAL dragonList_t *_dg_thread_##name = NULL;\
static THREAD_LOCAL dragonList_t **dg_##name = &_dg_proc_##name;

#define DRAGON_GLOBAL_MAP(name)\
static dragonMap_t *_dg_proc_##name = NULL;\
static THREAD_LOCAL dragonMap_t *_dg_thread_##name = NULL;\
static THREAD_LOCAL dragonMap_t **dg_##name = &_dg_proc_##name;

#define DRAGON_GLOBAL_BOOL(name)\
static bool _dg_proc_##name = false;\
static THREAD_LOCAL bool _dg_thread_##name = false;\
static THREAD_LOCAL bool *dg_##name = &_dg_proc_##name;

#endif
