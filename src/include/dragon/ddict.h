/*
  Copyright 2020, 2022 Hewlett Packard Enterprise Development LP
*/
#ifndef HAVE_DRAGON_DDICT_H
#define HAVE_DRAGON_DDICT_H

#include <dragon/channels.h>
#include <dragon/global_types.h>
#include <dragon/managed_memory.h>
#include <dragon/return_codes.h>

#ifdef __cplusplus
extern "C" {
#endif

/** @defgroup ddict_structs API Structures
 *
 *  The ddict API structures.
 *  @{
 */

/**
 * @brief A Key Structure
 *
 * When retrieving keys from a dictionary, this structure is used
 * to provide a pointer to the serialized data and its length.
*/
typedef struct dragonDDictKey_st {
    size_t num_bytes;
    uint8_t* data;
} dragonDDictKey_t;


/**
 * @brief An opaque DDict descriptor object
 *
 * When a using a distributed dictionary from C, this serves
 * as the handle to the dictionary. Attaching to a distributed
 * dictionary intializes a dragonDDictDescr_t.
*/
typedef struct dragonDDictDescr_st {
    uint64_t _idx;
} dragonDDictDescr_t;


/**
 * @brief An opaque handle to a request object
 *
 * This is used when doing any interaction with the distributed dictionary.
 * Operations on the dictionary may involve multiple call, such as a put or a
 * get operation, and this request descriptor helps maintain the state of the
 * request and response to this request.
*/
typedef struct dragonDDictRequestDescr_st {
    uint64_t _idx;
} dragonDDictRequestDescr_t;

 /** @} */ // end of ddict_structs group.


/** @defgroup ddict_lifecycle
 *  DDict Life Cycle Functions
 *  @{
 */
/**
 * @brief Serialize a DDict object for sharing with another process. When sharing
 * a DDict object with another process you may use this function to create a
 * shareable serialized descriptor. This creates an ASCII compliant string
 * that may be shared. This call mallocs the space occupied by the serial
 * pointer that is returned.
 *
 * NOTE: You must call free to free the serialized descriptor after calling this
 * function to free the space allocated by this function once you are done
 * with the serialized descriptor.
 *
 * @param obj is a valid DDict descriptor that has previously been created or attached.
 *
 * @param serial is a serialized descriptor that will be initialized with the correct
 * byte count and serialized bytes for so it can be passed to another process.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
**/
dragonError_t
dragon_ddict_serialize(const dragonDDictDescr_t* obj, const char ** serial);


/**
 * @brief Attach to a DDict object
 *
 * Calling this attaches to a DDict object by using a serialized DDict descriptor
 * that was passed to this process. The serialized DDict descriptor must have
 * been created using the dragon_ddict_serialize function.
 *
 * @param serial is a pointer to the serialized DDict descriptor.
 *
 * @param obj is a pointer to a DDict descriptor that will be initialized by
 * this call.
 *
 * @param timeout is a timeout in every request and operation.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_ddict_attach(const char* serial, dragonDDictDescr_t* obj, timespec_t* timeout);

/**
 * @brief Detach from a DDict object.
 *
 * All internal, process local resources are freed by making this call.
 *
 * @param obj is a descriptor and opaque handle to the DDict object.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_ddict_detach(dragonDDictDescr_t* obj);

/** @} */ // end of ddict_lifecycle group.

/** @defgroup ddict_requests
 *  DDict Request Functions
 *  @{
 */

/**
 * @brief Create a request descriptor for making a request to the distributed
 * dictionary.
 *
 * All internal state of the connection to the distributed dictionary is
 * maintained by this request object. Details of particular operations may be
 * stored in the private data structure for this object but are not accessible
 * directly by the user. The user uses associated API calls that use this
 * object. Not every request requires a request object. Requests that are
 * accomplished by one API call are not dependent on a request object. When a
 * request object is required it will be evident in the API.
 *
 * @param obj is a valid DDict descriptor that has previously been created or attached.
 *
 * @param req is a pointer to a request object that will be initialized by this call.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_ddict_create_request(const dragonDDictDescr_t* obj, const dragonDDictRequestDescr_t* req);

/**
 * @brief Return the value free memory variable from the request.
 *
 * In some cases, we don't want to free the memory after receiving as other processes are still
 * using it. This API provides a guide to user if the memory should be cleaned up or not.
 *
 * @param req is an initialized request object.
 *
 * @param free_mem is a boolean flag that determines if the memory should be released.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_ddict_free_required(const dragonDDictRequestDescr_t* req, bool * free_mem);

/**
 * @brief This finalizes a request by completing any operation that was still
 * pending for this request. When a request object is required it will be
 * indicated in the API.
 *
 * @param req is a valid request object.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_finalize_request(const dragonDDictRequestDescr_t* req);

/** @} */ // end of ddict_requests group.

/** @defgroup ddict_sendrecv
 *  DDict Send/Receive Functions
 *  @{
 */

/**
 * @brief Writes either key or value data to the distributed dictionary.
 *
 * The client may call this multiple times to put the parts of a key or value to
 * the distributed dictionary. Internally, all key writes are buffered so the
 * key can then be used to determine where the data is to be placed in the
 * distributed dictionary. All value writes are streamed immediately to the
 * distributed dictionary. All Key writes must come first for a request,
 * followed by value writes. Key writes are terminated by an API call to the
 * actual operation that requires a key as part of its request. Value writes,
 * for a put, follow the API call for the operation until the request is
 * finalized. All clients use the same selection algorithm for data placement
 * so data put by one client can be found by all other clients.
 *
 * @param req is an initialized request object.
 *
 * @param num_bytes is the number of bytes on this put request. There may be
 * additional bytes sent using this call as well.
 *
 * @param bytes is a pointer to a byte array (continguous bytes) with num_bytes size.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
dragonError_t
dragon_ddict_write_bytes(const dragonDDictRequestDescr_t* req, size_t num_bytes, uint8_t* bytes);

/**
 * @brief Waits to receive streamed data from a distributed dictionary manager.
 *
 * If all data has been read, then DRAGON_EOT will be returned as the return
 * code. This should be called after a get operation has been performed by
 * calling dragon_ddict_get. Note that before calling the get operation, the
 * key should have been written using the dragon_ddict_write_bytes operation.
 *
 * @param req is a valid request object that has been used to initiate reading
 * data from the distributed dictionary. For example, a key should have been
 * written and dragon_ddict_get should have been called.
 *
 * @param requested_size is the number of requested bytes. The actual size will
 * be equal to or less than the requested_size.
 *
 * @param received_size is a pointer to the number of bytes that have been read on
 * the call (assuming DRAGON_SUCCESS was returned).
 *
 * @param bytes is a pointer pointer that will be initialized to the bytes that
 * were read. The space is malloc'ed and should be freed by the user once the
 * data has been processed.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_read_bytes(const dragonDDictRequestDescr_t* req, size_t requested_size,
                         size_t* received_size, uint8_t** bytes);

/**
 * @brief Receive streamed data.
 *
 * Calling this waits to receive streamed data from a distributed
 * dictionary manager. If all data has been read, then DRAGON_EOT will be
 * returned as the return code. This should be called after a get operation
 * has been performed by calling dragon_ddict_get. Note that before calling
 * the get operation, the key should have been written using the
 * dragon_ddict_write_bytes operation.
 *
 * @param req is a valid request object that has been used to initiate reading
 * data from the distributed dictionary. For example, a key should have been
 * written and dragon_ddict_get should have been called.
 *
 * @param requested_size is the number of requested bytes. The actual size will
 * be equal to or less than the requested_size.
 *
 * @param received_size is a pointer to the number of bytes that have been read on
 * the call (assuming DRAGON_SUCCESS was returned).
 *
 * @param bytes is a pointer to valid space where the data should be placed. It must
 * be at least requested_size in size.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_read_bytes_into(const dragonDDictRequestDescr_t* req, size_t requested_size,
                              size_t* received_size, uint8_t* bytes);

/**
 * @brief Receive streamed data.
 *
 * Calling this waits to receive streamed data from a distributed
 * dictionary manager but instead of copying it into malloced memory, returns
 * the underlying managed memory object to the user. If all data has been
 * read, then DRAGON_EOT will be returned as the return code. This should be
 * called after a get operation has been performed by calling
 * dragon_ddict_get. Note that before calling the get operation, the key
 * should have been written using the dragon_ddict_write_bytes operation.
 *
 * @param req is a valid request object that has been used to initiate reading
 * data from the distributed dictionary. For example, a key should have been
 * written and dragon_ddict_get should have been called.
 *
 * @param mem is a managed memory allocation containing the packet of streamed
 * data. The size of the memory allocation is available as part of the object
 * and the managed memory API provides a means to get a pointer to the data.
 * The managed memory allocation should be freed using the managed memory API
 * once it is no longer needed.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_read_mem(const dragonDDictRequestDescr_t* req, dragonMemoryDescr_t* mem);


/** @} */ // end of fli_sendrecv group.

/** @defgroup ddict_ops
 *  Distributed Dictionary Operations
 *  @{
 */

/**
 * @brief Calling this tells the ddict client to take the key already written via
 * the dragon_ddict_write_bytes call(s) to be posted to the correct manager
 * and wait for a response.
 *
 * @param req is a valid created request object. It must have already had a key
 * written to it via the dragon_ddict_write_bytes call.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_contains(const dragonDDictRequestDescr_t* req);

/**
 * @brief Calling this tells the ddict client to take the key already written via
 * the dragon_ddict_write_bytes call(s) to be posted to the correct manager
 * and wait for a response.
 *
 * @param req is a valid created request object. It must have already had a key
 * written to it via the dragon_ddict_write_bytes call.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_get(const dragonDDictRequestDescr_t* req);

/**
 * @brief Calling this tells the ddict client to take the key already written via
 * the dragon_ddict_write_bytes call(s) to be posted to the correct manager.
 * The key must be written before calling put. All writes to this request,
 * following the call to this function are written to the correct manager as
 * the value for the put on the distributed dictionary manager.
 *
 * @param req is a valid created request object. It must have already had a key
 * written to it via the dragon_ddict_write_bytes call.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_put(const dragonDDictRequestDescr_t* req);

/**
 * @brief Calling this tells the ddict client to take the key already written via
 * the dragon_ddict_write_bytes call(s) to be posted to the correct manager.
 * The key must be written before calling put. All writes to this request,
 * following the call to this function are written to the correct manager as
 * the value for the put on the distributed dictionary manager.
 *
 * @param req is a valid created request object. It must have already had a key
 * written to it via the dragon_ddict_write_bytes call.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_pput(const dragonDDictRequestDescr_t* req);

/**
 * @brief Calling this retrieves the number of keys in the distributed dictionary.
 *
 * @param dd_descr is a serialized descriptor of the dictionary.
 *
 * @param length is a pointer to an uint64_t that will hold the number of keys up
 * return from the function call when DRAGON_SUCCESS is returned as the return code.
 * Otherwise, the field will not be initialized.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_length(const dragonDDictDescr_t * dd_descr, uint64_t* length);

/**
 * @brief This removes all key/value pairs from the distributed dictionary.
 *
 * @param dd_descr is a serialized descriptor of the dictionary.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_clear(const dragonDDictDescr_t * dd_descr);

/**
 * @brief Calling this tells the ddict client to take the key already written via
 * the dragon_ddict_write_bytes call(s) to be posted to the correct manager.
 * The key must be written before calling put. All writes to this request,
 * following the call to this function are written to the correct manager as
 * the value for the put on the distributed dictionary manager.
 *
 * @param req is a valid created request object. It must have already had a key
 * written to it via the dragon_ddict_write_bytes call.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_pop(const dragonDDictRequestDescr_t* req);

/**
 * @brief Calling this tells the ddict client to send request to all managers
 * to get all keys.
 *
 * @param dd_descr is a serialized descriptor of the dictionary.
 *
 * @param keys is a pointer to an byte array that store an array of
 * keys following the response from managers.
 *
 * @param num_keys is a pointer to the number of keys received.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_keys(const dragonDDictDescr_t * dd_descr, dragonDDictKey_t*** keys, size_t * num_keys);

/**
 * @brief Calling this to get the stats of all managers.
 *
 * @param dd_descr is a serialized descriptor of the dictionary.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_stats(const dragonDDictDescr_t * dd_descr);

 /**
 * @brief Calling this to move to the next checkpoint.
 *
 * @param dd_descr is a valid DDict descriptor that has previously been created or attached.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_checkpoint(const dragonDDictDescr_t* dd_descr);

 /**
 * @brief Calling this to rollback to one earlier checkpoint.
 *
 * @param dd_descr is a valid DDict descriptor that has previously been created or attached.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_rollback(const dragonDDictDescr_t * dd_descr);

 /**
 * @brief Calling this to get latest checkpoints from all managers and set current checkpoint
 * to the newest among all managers.
 *
 * @param dd_descr is a valid DDict descriptor that has previously been created or attached.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_sync_to_newest_checkpoint(const dragonDDictDescr_t * dd_descr);

 /**
 * @brief Calling this to get client's current checkpoint ID
 *
 * @param dd_descr is a valid DDict descriptor that has previously been created or attached.
 *
 * @param chkpt_id is a pointer to the checkpoint ID
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_checkpoint_id(const dragonDDictDescr_t * dd_descr, uint64_t * chkpt_id);

  /**
 * @brief Set the client's current checkpoint ID
 *
 * @param dd_descr is a valid DDict descriptor that has previously been created or attached.
 *
 * @param chkpt_id a non-negative checkpoint ID.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
dragonError_t
dragon_ddict_set_checkpoint_id(const dragonDDictDescr_t * dd_descr, uint64_t chkpt_id);


 /**
 * @brief Get client's local manager if there is one.
 *
 * @param dd_descr is a valid DDict descriptor that has previously been created or attached.
 *
 * @param local_manager_id is a pointer to the local manager ID.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_local_manager(const dragonDDictDescr_t * dd_descr, uint64_t * local_manager_id);

 /**
 * @brief Get client's main manager.
 *
 * @param dd_descr is a valid DDict descriptor that has previously been created or attached.
 *
 * @param main_manager is a pointer to the main manager ID.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_main_manager(const dragonDDictDescr_t * dd_descr, uint64_t * main_manager);

/**
 * @brief Create a copy of original client object and assign a chosen manager. The client will only
 * interact with the chosen manager.
 *
 * @param dd_descr is a valid DDict descriptor that has previously been created or attached.
 *
 * @param dd_new_descr is a DDict descriptor that holds a copy of client from dd_descr with the chosen manager.
 *
 * @param id is a pointer to the chosen manager ID.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_manager(const dragonDDictDescr_t * dd_descr, dragonDDictDescr_t * dd_new_descr, uint64_t id);

/**
 * @brief Return a list of empty managers during restart
 *
 * @param dd_descr is a valid DDict descriptor that has previously been created or attached.
 *
 * @param manager_ids is the address of an array of empty managers' IDs.
 *
 * @param num_empty_managers is the number of empty managers.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_empty_managers(const dragonDDictDescr_t * dd_descr, uint64_t ** manager_ids, size_t * num_empty_managers);


 /**
 * @brief Return client ID. This API is for test purpose only.
 *
 * @param dd_descr is a valid DDict descriptor that has previously been created or attached.
 *
 * @param client_id is the address of the client ID.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 _dragon_ddict_client_ID(const dragonDDictDescr_t * dd_descr, uint64_t * client_id);


 /**
 * @brief Return local manager IDs.
 *
 * @param dd_descr is a valid DDict descriptor that has previously been created or attached.
 *
 * @param local_manager_ids is an array of local manager IDs.
 *
 * @param num_local_managers is the number of local managers.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
dragonError_t
dragon_ddict_local_managers(const dragonDDictDescr_t * dd_descr, uint64_t ** local_manager_ids, size_t * num_local_managers);


/**
 * @brief Return the DDict keys that are located on the same node as the caller.
 *
 * @param dd_descr is a serialized descriptor of the dictionary.
 *
 * @param keys is a pointer to an byte array that store an array of
 * local keys following the response from local managers.
 *
 * @param num_keys is a pointer to the number of local keys received.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_local_keys(const dragonDDictDescr_t * dd_descr, dragonDDictKey_t*** local_keys, size_t * num_local_keys);

/**
 * @brief Calling this tells the ddict client to synchronize dictionaries.
 *
 * @param serialized_ddicts is a list of serialized descriptor of the dictionaries to synchronize.
 *
 * @param num_serialized_ddicts is the number of dictionaries to synchronize.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_synchronize(const char ** serialized_ddicts, const size_t num_serialized_ddicts, timespec_t * timeout);

 /**
 * @brief Calling this tells the ddict client to clone dictionaries.
 *
 * @param dd_descr is a serialized descriptor of the source dictionary.
 *
 * @param serialized_ddicts is a list of serialized descriptor of the destination dictionaries.
 *
 * @param num_serialized_ddicts is the number of destination dictionaries.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_clone(const dragonDDictDescr_t * dd_descr, const char ** serialized_ddicts, const size_t num_serialized_ddicts);
 /** Other Dictionary operations will follow. **/

/** @} */ // end of ddict_ops group.


#ifdef __cplusplus
}
#endif

#endif
