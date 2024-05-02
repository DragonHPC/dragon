#include "_channelsets.h"
#include <dragon/channels.h>
#include "umap.h"
#include "err.h"
#include <stdlib.h>
#include <pthread.h>

dragonMap_t * dg_channelsets = NULL;
static size_t throw_away_payload_sz;

// CHANNELSET INTERNAL FUNCTIONS

/* obtain a channel structure from a given channel descriptor */
static dragonError_t
_channelset_from_descr(const dragonChannelSetDescr_t * chset_descr, dragonChannelSet_t ** chset)
{
    if (chset_descr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ChannelSet descriptor");

    /* find the entry in our pool map for this descriptor */
    dragonError_t err = dragon_umap_getitem(dg_channelsets, chset_descr->_idx, (void *)chset);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to find item in channelset umap");

    no_err_return(DRAGON_SUCCESS);
}


static dragonError_t
_validate_channelset_attrs(dragonChannelSetAttrs_t * attrs)
{
    if (attrs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ChannelSet Attributes pointer. Must be non-null.");

    if (attrs->sync_type != DRAGON_SYNC && attrs->sync_type != DRAGON_NO_SYNC)
        err_return(DRAGON_INVALID_ARGUMENT, "sync_type must be either DRAGON_SYNC or DRAGON_NO_SYNC.");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_copy_channelset_attrs(dragonChannelSetAttrs_t* attrs, dragonChannelSetAttrs_t* new_attrs)
{
    if (attrs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ChannelSet Attributes pointer. Must be non-null.");

    if (new_attrs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Invalid ChannelSet Attributes pointer. Must be non-null.");

    new_attrs->num_allowed_spin_waiters = attrs->num_allowed_spin_waiters;
    new_attrs->lock_type = attrs->lock_type;
    new_attrs->sync_type = attrs->sync_type;

    no_err_return(DRAGON_SUCCESS);
}

/* insert a channelset structure into the unordered map with a generated key */
static dragonError_t
_add_umap_channelset_entry(dragonChannelSetDescr_t * chset, const dragonChannelSet_t * newchset)
{
    dragonError_t err;

    /* register this channel in our umap */
    if (dg_channelsets == NULL) {
        /* this is a process-global variable and has no specific call to be destroyed */
        dg_channelsets = malloc(sizeof(dragonMap_t));
        if (dg_channelsets == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate umap for channel sets.");

        err = dragon_umap_create(dg_channelsets, DRAGON_CHANNELSET_UMAP_SEED);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "failed to create umap for channel sets");
    }

    err = dragon_umap_additem_genkey(dg_channelsets, newchset, &chset->_idx);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to insert item into channelset umap");

    no_err_return(DRAGON_SUCCESS);
}

/* This is some setup code. It is called by the bcast callback (for a trigger),
   but then in turn does the call of the user-defined callback which was provided
   in the my_ptr arg. This is used to implement the channelset callback when required. */

static void
_channelset_callback(void* my_ptr, void* payload, size_t payload_sz, dragonError_t err, char* err_str)
{

    dragonChannelSetCallbackArg_t* arg = (dragonChannelSetCallbackArg_t*) my_ptr;
    dragonChannelEventNotification_t* event = (dragonChannelEventNotification_t*) payload;

    /* Call the callback */
    (arg->callback)(arg->user_def_ptr, (dragonChannelSetEventNotification_t*)event, err, err_str);
    free(arg);
}

/* This must run in a pthread when the bcast is synchronized, because the thread will block
   as the bcast is triggered. */
static void*
_channelset_sync(void * ptr)
{
    dragonChannelSet_t* chset = (dragonChannelSet_t*) ptr;
    timespec_t no_wait = {0, 0};
    dragonChannelSetEventNotification_t event;

    for (int i = 0; i < chset->num_channels; i++) {
        dragonError_t err;
        err = dragon_channel_poll(&chset->channels[i].descr, DRAGON_SPIN_WAIT, chset->event_mask, &no_wait, NULL);

        if (err == DRAGON_SUCCESS) {
            event.channel_idx = i;
            event.revent = chset->event_mask;
            dragon_bcast_trigger_all(&chset->bcast, NULL, &event, sizeof(dragonChannelEventNotification_t));
        }
    }

    return NULL;
}

// USER API

/** @brief Initialize a ChannelSet attributes structure
 *
 *  When overriding default ChannelSet attributes, call this first to initialize the
 *  attributes structure before overriding individual attributes. This must be called
 *  before creating a ChannelSet with overridden attributes. The number of spin waiters,
 *  lock type, and synchronization type are all user specifiable. See the
 *  dragon_channelset_poll function for a description of synchronization.
 *
 *  @param attrs A ChannelSet attributes structure
 *
 *  @return
 *      DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_channelset_attr_init(dragonChannelSetAttrs_t* attrs)
{
    if (attrs == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The attrs pointer cannot be null.");

    attrs->num_allowed_spin_waiters = DRAGON_CHANNELSET_DEFAULT_ALLOWED_SPIN_WAITERS;
    attrs->lock_type = DRAGON_CHANNELSET_DEFAULT_LOCK_TYPE;
    attrs->sync_type = DRAGON_NO_SYNC;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Create a ChannelSet from a list of channels with possible attributes.
 *
 * Creating a ChannelSet makes it possible to poll across a set of channels. The
 * details of how this is accomplished is handled by the channel set API. A
 * list of channels must be provided to poll across.
 *
 * @param descr_list An array of channel descriptors to be included in this
 * ChannelSet.
 *
 * @param num_channels The size of the channel descriptor list.
 *
 * @param event_mask The event mask to use in monitoring for events on the
 * channels.
 *
 * @param pool The memory pool to use for the ChannelSet.
 *
 * @param attrs The ChanneSet attributes. NULL may be specified to get the
 * default attributes.
 *
 * @param chset_descr The ChannelSet descriptor which is initialized when
 * DRAGON_SUCCESS is returned. The user provides the space for the ChannelSet
 * descriptor.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_channelset_create(dragonChannelDescr_t * descr_list[], int num_channels, const short event_mask, dragonMemoryPoolDescr_t * pool, dragonChannelSetAttrs_t * attrs, dragonChannelSetDescr_t * chset_descr)
{
    dragonError_t err;

    if (descr_list == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid channel descriptor list. It must be non-null.");

    if (num_channels <= 0)
        err_return(DRAGON_INVALID_ARGUMENT, "The number of Channel descriptors must be greater than 0.");

    if (pool == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "invalid memory pool descriptor");

    if (event_mask > DRAGON_CHANNEL_POLLFULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Only DRAGON_CHANNEL_POLLIN, DRAGON_CHANNEL_POLLOUT, DRAGON_CHANNEL_POLLINOUT, DRAGON_CHANNEL_POLLEMPTY, and DRAGON_CHANNEL_POLLFULL are allowed in the event_mask.");

    /* the memory pool must be locally addressable */
    if (!dragon_memory_pool_is_local(pool))
        append_err_return(DRAGON_INVALID_ARGUMENT, "Memory Pool must be local to create a ChannelSet.");

    /* if the attrs are NULL populate a default one */
    dragonChannelSetAttrs_t def_attrs;
    if (attrs == NULL) {
        err = dragon_channelset_attr_init(&def_attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not intialize ChannelSet attributes.");

        attrs = &def_attrs;

    } else {

        err = _validate_channelset_attrs(attrs);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "ChannelSet Attribute(s) are invalid.");

    }

    dragonChannelSet_t * newchset = malloc(sizeof(dragonChannelSet_t));
    if (newchset == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "cannot allocate new ChannelSet object");

    newchset->channels = malloc(sizeof(dragonChannelSetChannel_t) * num_channels);

    if (newchset->channels == NULL) {
        err = DRAGON_INTERNAL_MALLOC_FAIL;
        err_noreturn("Could not allocated new channel set object");
        goto chset_create_cleanup_1;
    }

    newchset->num_channels = num_channels;

    err = _copy_channelset_attrs(attrs, &newchset->attrs);

    if (err != DRAGON_SUCCESS)
        goto chset_create_cleanup_2;

    newchset->event_mask = event_mask;
    newchset->first_poll_call = true; /* Has not had poll called yet. */

    newchset->pool = *pool; /* copies the entire descriptor into the handle, but is not used after create */

    dragonBCastAttr_t battr;

    err = dragon_bcast_attr_init(&battr);
    if (err != DRAGON_SUCCESS)
        goto chset_create_cleanup_2;

    battr.sync_type = attrs->sync_type;
    if (battr.sync_type == DRAGON_SYNC)
        battr.sync_num = 1;
    else
        battr.sync_num = 0;

    err = dragon_bcast_create(pool,sizeof(dragonChannelEventNotification_t), attrs->num_allowed_spin_waiters, &battr, &newchset->bcast);
    if (err != DRAGON_SUCCESS)
        goto chset_create_cleanup_2;


    /* serialize bcast object and place serialized bcast into each channel of channel set */
    dragonBCastSerial_t bserial;

    err = dragon_bcast_serialize(&newchset->bcast, &bserial);
    if (err != DRAGON_SUCCESS)
        goto chset_create_cleanup_3;

    for (int k=0;k<num_channels;k++) {
        newchset->channels[k].descr = *descr_list[k];
        err = dragon_channel_add_event_bcast(&newchset->channels[k].descr, &bserial, event_mask, k, &newchset->channels[k].token);
        if (err != DRAGON_SUCCESS)
            goto chset_create_cleanup_3;
    }

    err = _add_umap_channelset_entry(chset_descr, newchset);
    if (err != DRAGON_SUCCESS)
        goto chset_create_cleanup_3;

    no_err_return(DRAGON_SUCCESS);

chset_create_cleanup_3:
    dragon_bcast_destroy(&newchset->bcast);

chset_create_cleanup_2:
    free(newchset->channels);

chset_create_cleanup_1:
    free(newchset);
    append_err_return(err, "Failed to create channel set.");
}

/** @brief Destroy a ChannelSet
 *
 *  A ChannelSet that is no longer needed should be destroyed. If it is not destroyed
 *  when no longer needed, it will leave its member channels in an unknown state.
 *
 *  @param chset_descr An ChannelSet descriptor that was successfully created.
 *
 *  @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_channelset_destroy(dragonChannelSetDescr_t * chset_descr)
{

    dragonChannelSet_t * chset;
    dragonError_t err = _channelset_from_descr(chset_descr, &chset);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Cannot get channelset from descriptor.");

    if (!chset->first_poll_call) {
        int perr = pthread_join(chset->tid, NULL);
        if (perr != 0) {
            char err_str[80];
            snprintf(err_str, 80, "There was an error on the pthread_join call. ERR=%d", perr);
            err_return(DRAGON_FAILURE, err_str);
        }
    }

    for (int k=0;k<chset->num_channels;k++) {
        dragon_channel_remove_event_bcast(&chset->channels[k].descr, chset->channels[k].token);
    }
    /* We aren't checking the result here because either way we should do the rest of the
       destroy and if this destroy fails, it might be because a channel in the set destroyed
       it first when the channel was destroyed. */
    dragon_bcast_destroy(&chset->bcast);

    /* remove the item from the umap */
    err = dragon_umap_delitem(dg_channelsets, chset_descr->_idx);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "failed to delete item from channelset umap");

    free(chset->channels);
    free(chset);

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Get the channel descriptors from a ChannelSet.
 *
 *  The descr_list will be a list of channel descriptors from this ChannelSet.
 *  Upon successful completion of the call, the num_channels will be set to the
 *  number of channels in the descr_list. The list must be freed via free, when
 *  it is no longer needed. ChannelSets are immutable, so once called, it will not
 *  change.
 *
 *  @param chset_descr An ChannelSet descriptor that was successfully created.
 *  @param descr_list An array of Channel descriptors.
 *  @param num_channels A pointer to an integer which on successful completion will
 *  contain the number of channels in descr_list.
 *
 *  @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_channelset_get_channels(dragonChannelSetDescr_t * chset_descr, dragonChannelDescr_t ** descr_list, int* num_channels)
{

    dragonChannelSet_t * chset;
    dragonError_t err = _channelset_from_descr(chset_descr, &chset);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Cannot get channelset from descriptor.");

    if (descr_list == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot pass NULL for descr_list");

    if (num_channels == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot pass NULL for num_channels");

    *descr_list = malloc(sizeof(dragonChannelDescr_t) * chset->num_channels);

    if (*descr_list == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocated new channel set object");

    memcpy(*descr_list, chset->channels, sizeof(dragonChannelDescr_t) * chset->num_channels);
    *num_channels = chset->num_channels;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Get the event mask from a ChannelSet.
 *
 *  When created an event mask is provided for a ChannelSet. This call is provided
 *  for completeness to get the current event mask.
 *
 *  @param chset_descr A ChannelSet descriptor that was successfully created.
 *  @param event_mask A pointer to a short which will be initialized to the event
 *  mask upon successful completion of the call.
 *
 *  @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_channelset_get_event_mask(dragonChannelSetDescr_t * chset_descr, short* event_mask)
{
    dragonChannelSet_t * chset;
    dragonError_t err = _channelset_from_descr(chset_descr, &chset);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Cannot get channelset from descriptor.");

    *event_mask = chset->event_mask;

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Set the event mask for a ChannelSet.
 *
 *  The event mask may be changed during the course of a ChannelSet's lifetime.
 *  This call allows you to do that.
 *
 *  @param chset_descr A ChannelSet descriptor that was successfully created.
 *  @param event_mask A short which will be used to change to the event mask
 *  upon successful completion of the call.
 *
 *  @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channelset_set_event_mask(dragonChannelSetDescr_t * chset_descr, short event_mask)
{
    dragonChannelSet_t * chset;
    dragonError_t err = _channelset_from_descr(chset_descr, &chset);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Cannot get channelset from descriptor.");

    chset->event_mask = event_mask;

    for (int k=0;k<chset->num_channels;k++) {
        err = dragon_channel_update_event_mask(&chset->channels[k].descr, chset->channels[k].token, event_mask);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Cannot set event mask in channel of channel set.");
    }

    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Poll for a channel event on a ChannelSet
 *
 * This call polls a set of channels in an efficient, scalable manner for events
 * satisfying the event mask specified when the ChannelSet was created or
 * modified using the dragon_channelset_set_event_mask call.
 *
 * The ChannelSet attribute sync_type, specified when the ChannelSet is created,
 * may be set to DRAGON_SYNC in which case there must be a process polling on
 * the ChannelSet for a channel to accept a message or send a message. In this
 * way the ChannelSet will guarantee that no channel events are missed at the
 * possible expense of slowing down the channels (depending on activity and
 * the client implementation). The default sync_type is DRAGON_NO_SYNC. In
 * this case it is possible for there to be missed poll events on channels.
 * While a new message will always generate an event if there is a poller,
 * there is a window where no polling is currently being executed then new
 * messages may arrive and no poll event will be generated when the message
 * arrives. In the case of specifying the default DRAGON_NO_SYNC behavior, the
 * client application should initiate a poll in a thread or process and then
 * scan all channels in the ChannelSet for any missed events.
 *
 * @param chset_descr A ChannelSet descriptor that was successfully created.
 *
 * @param wait_mode Either DRAGON_IDLE_WAIT, DRAGON_SPIN_WAIT, or
 * DRAGON_ADAPTIVE_WAIT. Spin waiting requires more CPU time, but may be
 * faster in some applications, while idle waiting conserves resources at the
 * possible expense of more required time in the application. If the maximum
 * spin waiters is reached, the channel set will automatically switch over to
 * idle waiting.
 *
 * @param timeout If NULL, the application will wait indefinitely. Otherwise, it
 * will return after a successful poll or time out. @param release_fun For
 * some applications it may be desirable to release a resource once the
 * application has sucessfully become a waiter on the poll. This functionality
 * allows an application to close a small but possible window in
 * synchronization in an application specific way. @param release_arg The arg
 * to provide to release_fun when the resource is released. Only one arg is
 * allowed, but that arg could be a composite struture if desired. @param
 * event A pointer to a pointer. This must be a pointer, pointer to an event
 * notification structure. The poll function will return the event by pointing
 * the pointer, pointer to a new event structure. The user application must
 * free the event space via a call to free once it is no longer needed.
 *
 * @param release_fun If non-NULL this function will be called when the
 * process has become a waiter on this channelset. This is useful when there
 * is a race condition between something else and waiting that must be closed.
 * The function must take a void* argument, which is supplied here. The argument
 * may be NULL. The function should return a dragonError_t return code of
 * DRAGON_SUCCESS to indicate it was successful.
 *
 * @param release_arg This is the argument to the release_fun. It can be a pointer
 * to any user-defined structure.
 *
 * @param event The event argument is a pointer to a pointer which will receive the
 * result event. The space for the event return value must be freed by the caller. It
 * contains both the event that occurred and the index into the list of channels in the
 * channel set. If POLLNOTHING is returned as the event, then the channel
 * that was indicated was destroyed while waiting on the channel set.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */

dragonError_t
dragon_channelset_poll(dragonChannelSetDescr_t * chset_descr, dragonWaitMode_t wait_mode, timespec_t * timeout,
                       dragonReleaseFun release_fun, void* release_arg, dragonChannelSetEventNotification_t ** event)
{
    pthread_attr_t attr;

    pthread_attr_init(&attr);

    size_t payload_sz;
    dragonChannelSet_t * chset;
    dragonError_t err = _channelset_from_descr(chset_descr, &chset);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Cannot get channelset from descriptor.");

    if (event == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The event parameter cannot be null on poll request.");

    if (chset->first_poll_call) {
        chset->first_poll_call = false;

        int perr = pthread_create(&chset->tid, &attr, _channelset_sync, (void*)chset);
        pthread_attr_destroy(&attr);

        if (perr != 0) {
            char err_str[80];
            snprintf(err_str, 80, "There was an error on the pthread_create call. ERR=%d", err);
            err_return(DRAGON_FAILURE, err_str);
        }
    }

    /* Assuming the bcast is synchronized (the default), any new events will not
       be missed after creating the channelset. The code above checks for
       pre-existing events before the channelset was created. The
       synchronized bcast means there is no window for losing events
       between the code above and this wait. If a user chooses a
       non-synchronized bcast, then they are OK with missing events. */

    err = dragon_bcast_wait(&chset->bcast, wait_mode, timeout, (void**)event, &payload_sz, release_fun, release_arg);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "BCast wait returned an error while waiting for channel set poll.");

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Poll for a channel event on a ChannelSet via a callback
 *
 *  This call polls a set of channels in an efficient, scalable manner for
 *  events satisfying the event mask specified when the ChannelSet was created or
 *  modified using the dragon_channelset_set_event_mask call. A user-defined callback
 *  function is called in a thread once the event occurs.
 *
 *  See the poll function for a description of the synchronization behavior that
 *  also applies to this function call.
 *
 *  @param chset_descr A ChannelSet descriptor that was successfully created.
 *  @param user_def_ptr A user-defined pointer to an argument that must be in thread
 *  shareable space with the thread of execution that makes this call.
 *  @param wait_mode Either DRAGON_IDLE_WAIT or DRAGON_SPIN_WAIT. Spin waiting requires
 *  more CPU time, but may be faster in some applications, while idle waiting conserves
 *  resources at the possible expense of more required time in the application. If the
 *  maximum spin waiters is reached, the channel set will automatically switch over to
 *  idle waiting.
 *  @param timeout If NULL, the application will wait indefinitely. Otherwise, it will
 *  return after a successful poll or time out.
 *  @param release_fun For some applications it may be desirable to release a resource
 *  once the application has sucessfully become a waiter on the poll. This functionality
 *  allows an application to close a small but possible window in synchronization in an
 *  application specific way.
 *  @param release_arg The arg to provide to release_fun when the resource is released. Only
 *  one arg is allowed, but that arg could be a composite struture if desired.
 *  @param cb A pointer to a function that must have the signature defined in
 *  dragonChannelSetNotifyCallback consisting of four arguments as defined in channelsets.h.
 *  The event pointer will point at the discovered event information. It must be freed via
 *  free once the event information is no longer needed. The err argument will indicate if
 *  the callback is successfully reporting an event or if an error occurred while waiting for
 *  an event. In the case of an error, the err_str will point to an error string with additional
 *  information. When an error occurs, the err_str must be freed once it is no longer needed.
 *
 *  @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channelset_notify_callback(dragonChannelSetDescr_t * chset_descr, void* user_def_ptr, dragonWaitMode_t wait_mode, timespec_t * timeout,
                                  dragonReleaseFun release_fun, void* release_arg, dragonChannelSetNotifyCallback cb)
{
    dragonChannelSet_t * chset;
    dragonError_t err = _channelset_from_descr(chset_descr, &chset);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Cannot get channelset from descriptor.");

    dragonChannelSetCallbackArg_t* arg = malloc(sizeof(dragonChannelSetCallbackArg_t));
    if (arg == NULL)
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Cannot allocate internal callback argument.");

    arg->chset_descr = *chset_descr;
    arg->callback = cb;
    arg->user_def_ptr = user_def_ptr;

    err = dragon_bcast_notify_callback(&chset->bcast, arg, wait_mode, timeout, release_fun, release_arg, _channelset_callback);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not set up callback for channelset.");

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Poll for a channel event on a ChannelSet via a signal interface.
 *
 *  This call polls a set of channels in an efficient, scalable manner for
 *  events satisfying the event mask specified when the ChannelSet was created or
 *  modified using the dragon_channelset_set_event_mask call. A user-defined signal
 *  is sent to the process via the signal interface when a poll event occurs.
 *
 *  See the poll function for a description of the synchronization behavior that
 *  also applies to this function call.
 *
 *  @param chset_descr A ChannelSet descriptor that was successfully created.
 *  @param wait_mode Either DRAGON_IDLE_WAIT or DRAGON_SPIN_WAIT. Spin waiting requires
 *  more CPU time, but may be faster in some applications, while idle waiting conserves
 *  resources at the possible expense of more required time in the application. If the
 *  maximum spin waiters is reached, the channel set will automatically switch over to
 *  idle waiting.
 *  @param timeout If NULL, the application will wait indefinitely. Otherwise, it will
 *  return after a successful poll or time out.
 *  @param release_fun For some applications it may be desirable to release a resource
 *  once the application has sucessfully become a waiter on the poll. This functionality
 *  allows an application to close a small but possible window in synchronization in an
 *  application specific way.
 *  @param release_arg The arg to provide to release_fun when the resource is released. Only
 *  one arg is allowed, but that arg could be a composite struture if desired.
 *  @param sig The user-defined signal to send to the process.
 *  @param event_ptr_ptr  The event pointer, pointer will point at the discovered event
 *  information when no error has occurred. It must be freed via free once the event
 *  information is no longer needed.
 *  @param drc The argument will indicate if
 *  the signal is successfully reporting an event or if an error occurred while waiting for
 *  an event.
 *  @param err_string In the case of an error, *err_str will point to an error string with additional
 *  information. When an error occurs, the *err_str must be freed once it is no longer needed.
 *
 *  @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channelset_notify_signal(dragonChannelSetDescr_t * chset_descr, dragonWaitMode_t wait_mode, timespec_t * timeout,
                                dragonReleaseFun release_fun, void* release_arg, int sig, dragonChannelSetEventNotification_t** event_ptr_ptr,
                                dragonError_t* drc, char** err_string)
{
    dragonChannelSet_t * chset;
    dragonError_t err = _channelset_from_descr(chset_descr, &chset);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Cannot get channelset from descriptor.");

    err = dragon_bcast_notify_signal(&chset->bcast, wait_mode, timeout, release_fun, release_arg, sig,
                                     (void**)event_ptr_ptr, &throw_away_payload_sz, drc, err_string);

    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not set up signal for channelset.");

    no_err_return(DRAGON_SUCCESS);
}

/** @brief Reset the channel set event monitor.
 *
 *  This function should be called only when there is a guarantee of no activity
 *  on all channels in the channel set. It resets the event monitor to the state
 *  that it was in immediately after creating it. It should only be needed to
 *  recover from an error condition.
 *
 *  @param chset_descr A ChannelSet descriptor that was successfully created.
 *
 *  @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_channelset_reset(dragonChannelSetDescr_t* chset_descr)
{
    dragonChannelSet_t * chset;
    dragonError_t err = _channelset_from_descr(chset_descr, &chset);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Cannot get channelset from descriptor.");

    err = dragon_bcast_reset(&chset->bcast);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Cannot reset event monitor bcast.");

    return DRAGON_SUCCESS;
}
