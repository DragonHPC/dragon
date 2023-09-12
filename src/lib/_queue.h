#ifndef HAVE_DRAGON_QUEUE_INTERNAL_H
#define HAVE_DRAGON_QUEUE_INTERNAL_H

#include <dragon/managed_memory.h>
#include <dragon/channels.h>

#define DRAGON_QUEUE_UMAP_SEED 12

typedef struct dragonQueue_st {
    dragonMemoryPoolDescr_t pool;
    dragonChannelDescr_t ch;
    dragonChannelSendh_t csend;
    dragonChannelRecvh_t crecv;
    dragonQ_UID_t q_uid;
} dragonQueue_t;

#endif
