#ifndef HAVE_DRAGON_LOCK_INTERNAL_H
#define HAVE_DRAGON_LOCK_INTERNAL_H

#include <pthread.h>
#include <errno.h>
#include <stdint.h>
#include <dragon/shared_lock.h>
#include "shared_lock.h"

#ifdef __cplusplus
#include <atomic>
using namespace std;
#else
#include <stdatomic.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define DRAGON_LOCK_POLL_PRE_SLEEP_ITERS 10000000UL
#define DRAGON_LOCK_POLL_SLEEP_USEC 2
#define DRAGON_LOCK_MEM_ORDER memory_order_acq_rel
#define DRAGON_LOCK_MEM_ORDER_FAIL memory_order_relaxed
#define DRAGON_LOCK_MEM_ORDER_READ memory_order_acquire
#define DRAGON_LOCK_CL_PADDING 64
#define DRAGON_LOCK_NODE_FANOUT 16

#ifdef __cplusplus
}
#endif

#endif