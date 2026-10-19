/*
  Copyright 2020, 2022 Hewlett Packard Enterprise Development LP
*/
#ifndef HAVE_DRAGON_SHAREDLOCK_H
#define HAVE_DRAGON_SHAREDLOCK_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum dragonLockKind_st {
    DRAGON_LOCK_FIFO,
    DRAGON_LOCK_FIFO_LITE,
    DRAGON_LOCK_GREEDY
} dragonLockKind_t;

#ifdef __cplusplus
}
#endif

#endif