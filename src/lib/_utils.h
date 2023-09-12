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

#ifdef __cplusplus
}
#endif

#define DRAGON_UUID_OFFSET_HID 0
#define DRAGON_UUID_OFFSET_PID 8
#define DRAGON_UUID_OFFSET_CTR 12

#endif
