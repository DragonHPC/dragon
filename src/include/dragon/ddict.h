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


/**
 * @brief A serialized DDict object
 *
 * A serialized DDict object can be passed to other processes which can then
 * attach to the object. Attaching initializes a dragonDDictDescr_t handle to
 * the distributed dictionary.
*/
typedef struct dragonDDictSerial_st {
    size_t len; /*!< The length of the serialized descriptor in bytes. */
    uint8_t * data; /* !<  The serialized descriptor data to be shared. */
} dragonDDictSerial_t;

 /** @} */ // end of ddict_structs group.


/** @defgroup ddict_lifecycle
 *  DDict Life Cycle Functions
 *  @{
 */
/**
 * @brief Serialize a DDict object for sharing with another process. When sharing
 * an DDict object with another process you may use this function to create a
 * shareable serialized descriptor. This creates a binary string which may not
 * be ASCII compliant. Before sharing, if ASCII compliance is required, call a
 * base64 encoder like the dragon_base64_encode found in dragon/utils.h before
 * sharing and dragon_base64_decode before attaching from the other process.
 *
 * NOTE: You must call dragon_ddict_serial_free to free a serialized descriptor
 * after calling this function to free the extra space allocated by this
 * function once you are done with the serialized descriptor.
 *
 * @param obj is a valid DDict descriptor that has previously been created or attached.
 *
 * @param serial is a serialized descriptor that will be initialized with the correct
 * byte count and serialized bytes for so it can be passed to another process.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
**/
dragonError_t
dragon_ddict_serialize(const dragonDDictDescr_t* obj, dragonDDictSerial_t* serial);


/**
 * @brief Free the internal resources of a serialized DDict descriptor
 *
 * This frees internal structures of a serialized DDict descriptor. It does not
 * destroy the DDict object itself.
 *
 * @param serial is a serialized DDict descriptor.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_ddict_serial_free(dragonDDictSerial_t* serial);

/**
 * @brief Attach to an DDict object using a base 64 encoded string
 *
 * Calling this attaches to a DDict object by using a base 64 encoded serialized
 * DDict descriptor that was passed to this process. The serialized DDict
 * descriptor must have been created by base 64 encoding a serialized DDict
 * descriptor.
 *
 * @param b64_str is a pointer to the serialized base 64 encoded string.
 *
 * @param obj is a pointer to an DDict descriptor that will be initialized by
 * this call.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait
 * indefinitely. Otherwise, wait for the specified amount of time for the operation
 * to complete.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 *
 * NOTES: This does the base64 decode and then calls the normal attach function.
 */
dragonError_t
dragon_ddict_attach_b64(char* b64_str, dragonDDictDescr_t* obj, const timespec_t* timeout);

/**
 * @brief Attach to an DDict object
 *
 * Calling this attaches to a DDict object by using a serialized DDict descriptor
 * that was passed to this process. The serialized DDict descriptor must have
 * been created using the dragon_ddict_serialize function.
 *
 * @param serial is a pointer to the serialized DDict descriptor.
 *
 * @param obj is a pointer to an DDict descriptor that will be initialized by
 * this call.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait
 * indefinitely. Otherwise, wait for the specified amount of time for the operation
 * to complete.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_ddict_attach(const dragonDDictSerial_t* serial, dragonDDictDescr_t* obj, const timespec_t* timeout);

/**
 * @brief Detach from a DDict object.
 *
 * All internal, process local resources are freed by making this call.
 *
 * @param obj is a descriptor and opaque handle to the DDict object.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait
 * indefinitely. Otherwise, wait for the specified amount of time for the operation
 * to complete.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_ddict_detach(dragonDDictDescr_t* obj, const timespec_t* timeout);

/**
 * @brief Destroy a DDict object.
 *
 * The distributed dictionary is destroyed including the orchestrator, all
 * managers, and their associated flis and channels.
 *
 * @param obj is a descriptor and opaque handle to the DDict object.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait
 * indefinitely. Otherwise, wait for the specified amount of time for the
 * operation to complete.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_ddict_destroy(dragonDDictDescr_t* obj, const timespec_t* timeout);

/** @} */ // end of ddict_lifecycle group.

/** @defgroup ddict_requests
 *  DDict Request Functions
 *  @{
 */

/**
 * @brief Create a request descriptor for sending a request to the distributed
 * dictionary and waiting for a response.
 *
 * All internal state of the connection to the distributed dictionary is
 * maintained by this request object. Details of particular operations may be
 * stored in the private data structure for this object but are not accessible
 * directly by the user. The user uses associated API calls that use this
 * object. Not every request requires a request object. Requests that are
 * accomplished by one API call are not dependent on a request object. When a
 * request object is required it will be evident in the API.
 *
 * @param obj is a pointer to an initialized distributed dicationary descriptor.
 *
 * @param req is a pointer to a request object that will be initialized by this call.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 */
dragonError_t
dragon_ddict_create_request(dragonDDictDescr_t* obj, dragonDDictRequestDescr_t* req);

/**
 * @brief This finalizes a request by completing any operation that was still
 * pending for this request. When a request object is required it will be
 * indicated in the API.
 *
 * @param req is a valid request object.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait
 * indefinitely. Otherwise, wait for the specified amount of time for the
 * operation to complete.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_finalize_request(dragonDDictRequestDescr_t* req, const timespec_t* timeout);

/** @} */ // end of ddict_requests group.

/** @defgroup ddict_sendrecv
 *  DDict Send/Receive Functions
 *  @{
 */

/**
 * @brief Use this to write either key or value data to the distributed dictionary.
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
 * @param timeout is a pointer to a timeout structure. If NULL, then wait
 * indefinitely. Otherwise, wait for the specified amount of time for the
 * operation to complete.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
dragonError_t
dragon_ddict_write_bytes(dragonDDictRequestDescr_t* req, size_t num_bytes,
                         uint8_t* bytes, const timespec_t* timeout);

/**
 * @brief Calling this waits to receive streamed data from a distributed
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
 * @param bytes is a pointer pointer that will be initialized to the bytes that
 * were read. The space is malloc'ed and should be freed by the user once the
 * data has been processed.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait
 * indefinitely. Otherwise, wait for the specified amount of time for the
 * operation to complete.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_read_bytes(dragonDDictRequestDescr_t* req, size_t requested_size,
                         size_t* received_size, uint8_t** bytes, const timespec_t* timeout);

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
 * @param timeout is a pointer to a timeout structure. If NULL, then wait
 * indefinitely. Otherwise, wait for the specified amount of time for the
 * operation to complete.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_read_bytes_into(dragonDDictRequestDescr_t* req, size_t requested_size,
                              size_t* received_size, uint8_t* bytes, const timespec_t* timeout);

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
 dragon_ddict_read_mem(dragonDDictRequestDescr_t* req, dragonMemoryDescr_t* mem);


/** @} */ // end of fli_sendrecv group.

/** @defgroup ddict_ops
 *  Distributed Dictionary Operations
 *  @{
 */

/**
 * @brief Check to see if key exists in ddict
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
 * @param bytes must be a valid pointer to space that will be initialized to the
 * bytes that were read.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait
 * indefinitely. Otherwise, wait for the specified amount of time for the
 * operation to complete.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_contains(dragonDDictRequestDescr_t* req);

/**
 * @brief Calling this tells the ddict client to take the key already written via
 * the dragon_ddict_write_bytes call(s) to be posted to the correct manager
 * and wait for a response.
 *
 * @param req is a valid created request object. It must have already had a key
 * written to it via the dragon_ddict_write_bytes call.
 *
 * @param timeout is a pointer to a timeout structure. If NULL, then wait
 * indefinitely. Otherwise, wait for the specified amount of time for the
 * operation to complete.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_get(dragonDDictRequestDescr_t* req, const timespec_t* timeout);

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
 * @param timeout is a pointer to a timeout structure. If NULL, then wait
 * indefinitely. Otherwise, wait for the specified amount of time for the
 * operation to complete.
 *
 * @return DRAGON_SUCCESS or a return code to indicate what problem occurred.
 **/
 dragonError_t
 dragon_ddict_put(dragonDDictRequestDescr_t* req, const timespec_t* timeout);

 /** Other Dictionary operations will follow. **/

/** @} */ // end of ddict_ops group.


#ifdef __cplusplus
}
#endif

#endif
