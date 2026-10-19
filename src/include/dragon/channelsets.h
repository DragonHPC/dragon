/*
 * Dragon ChannelSets communication API
 * Copyright Cray/HPE 2020
*/

#ifndef HAVE_DRAGON_CHANNELSETS_H
#define HAVE_DRAGON_CHANNELSETS_H

#include <dragon/shared_lock.h>
#include <dragon/return_codes.h>
#include <dragon/channels.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * ChannelSet Attributes
 *
 * This structure defines user selectable attributes of the ChannelSet.
 * The num_allowed_spin_waiters controls how many spin waiters a ChannelSet
 * can support. The lock_type is user selectable. The sync_type is either
 * DRAGON_NO_SYNC (the default) or DRAGON_SYNC. No synchronization means
 * that there is a window where poll events could be missed. In this case,
 * it is up to the application to call poll and then scan every channel in the
 * channel set to see if any existing messages are already in those channels.
 * If DRAGON_SYNC is called, then the application can be guaranteed there will
 * be no missed poll events. This is at the expense of possibly delaying messages
 * being deposited into channels. If DRAGON_SYNC is chosen, it is performance
 * critical, that the application maintain a polling process/thread that is
 * constantly monitoring the ChannelSet.
 */
typedef struct dragonChannelSetAttrs_st {
    int num_allowed_spin_waiters; /**< Number of allowed spin waiters */
    dragonLockKind_t lock_type; /**< User selectable lock */
    dragonSyncType_t sync_type; /**< DRAGON_SYNC or DRAGON_NO_SYNC (the default) */
} dragonChannelSetAttrs_t;

typedef struct dragonChannelSetDescr_st {
    uint64_t _idx;
} dragonChannelSetDescr_t;

/*
* channel_idx is the index in the channel list that was originally provided
* during the construction of the channelset. It is not the channel cuid.
*/
typedef struct dragonChannelSetEventNotification_st {
    int channel_idx;
    short revent;
} dragonChannelSetEventNotification_t;

typedef void
(*dragonChannelSetNotifyCallback)(void * user_def_ptr, dragonChannelSetEventNotification_t * event,
                                  dragonError_t err, char* err_str);

dragonError_t
dragon_channelset_attr_init(dragonChannelSetAttrs_t* attrs);

dragonError_t
dragon_channelset_create(dragonChannelDescr_t * descr_list[], int num_channels, const short event_mask,
                         dragonMemoryPoolDescr_t * pool_descr, dragonChannelSetAttrs_t * attrs,
                         dragonChannelSetDescr_t * chset_descr);

dragonError_t
dragon_channelset_destroy(dragonChannelSetDescr_t * chset_descr);

dragonError_t
dragon_channelset_get_channels(dragonChannelSetDescr_t * chset_descr, dragonChannelDescr_t ** descr_list,
                               int* num_channels);

dragonError_t
dragon_channelset_get_event_mask(dragonChannelSetDescr_t * chset_descr, short * event_mask);

dragonError_t
dragon_channelset_set_event_mask(dragonChannelSetDescr_t * chset_descr, short event_mask);

dragonError_t
dragon_channelset_poll(dragonChannelSetDescr_t * chset_descr, dragonWaitMode_t wait_mode,
                       timespec_t * timeout, dragonReleaseFun release_fun, void* release_arg,
                       dragonChannelSetEventNotification_t ** event);

dragonError_t
dragon_channelset_notify_callback(dragonChannelSetDescr_t * chset_descr, void * user_def_ptr,
                                  dragonWaitMode_t wait_mode, timespec_t * timeout,
                                  dragonReleaseFun release_fun, void* release_arg,
                                  dragonChannelSetNotifyCallback cb);

dragonError_t
dragon_channelset_notify_signal(dragonChannelSetDescr_t * chset_descr, dragonWaitMode_t wait_mode,
                                timespec_t * timeout,  dragonReleaseFun release_fun, void* release_arg,
                                int sig, dragonChannelSetEventNotification_t** event_ptr_ptr,
                                dragonError_t* drc, char** err_string);


dragonError_t
dragon_channelset_reset(dragonChannelSetDescr_t* chset_descr);

#ifdef __cplusplus
}
#endif

#endif