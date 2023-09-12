#ifndef HAVE_DRAGON_CHANNELSETS_INTERNAL_H
#define HAVE_DRAGON_CHANNELSETS_INTERNAL_H

#include <dragon/channels.h>
#include <dragon/channelsets.h>
#include <dragon/bcast.h>
#include <assert.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DRAGON_CHANNELSET_DEFAULT_ALLOWED_SPIN_WAITERS 5
#define DRAGON_CHANNELSET_UMAP_SEED 1984
#define DRAGON_CHANNELSET_DEFAULT_LOCK_TYPE DRAGON_LOCK_FIFO_LITE

typedef struct dragonChannelSetChannel_st {
    dragonULInt token;
    dragonChannelDescr_t descr;
} dragonChannelSetChannel_t;

/* seated ChannelSet structure */
typedef struct dragonChannelSet_st {
    dragonChannelSetChannel_t * channels;
    dragonChannelSetAttrs_t attrs;
    dragonMemoryPoolDescr_t pool;
    dragonBCastDescr_t bcast;
    int num_channels;
    bool first_poll_call;
    uint8_t event_mask;
} dragonChannelSet_t;

typedef struct dragonChannelSetCallbackArg_st {
    dragonChannelSetDescr_t chset_descr;
    void * user_def_ptr;
    dragonChannelSetNotifyCallback callback;
} dragonChannelSetCallbackArg_t;

/* This is here to check that the ChannelSet Event
   Notification is simply a renaming of the fields
   from the Channel Event Notification. An alert
   is generated if they are not the same size. */

static_assert(sizeof(dragonChannelEventNotification_t) == sizeof(dragonChannelSetEventNotification_t), "ChannelSet and Channel Event Notifications must be a renaming of fields and are not.");

#ifdef __cplusplus
}
#endif

#endif