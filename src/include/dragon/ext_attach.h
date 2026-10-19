#ifndef HAVE_DRAGON_EXT_ATTACH_H
#define HAVE_DRAGON_EXT_ATTACH_H

#include <dragon/global_types.h>
#include <dragon/return_codes.h>
#include <dragon/managed_memory.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct dragonExtAttachDescr_st {
    uint64_t _idx;
} dragonExtAttachDescr_t;

/* Mirrors dragon.infrastructure.messages.LSExternalResourcesReady.Errors.
   NOTE: these values are NOT dragonError_t values and must never be compared
   against DRAGON_* return codes except DRAGON_SUCCESS==0 which is shared. */
typedef enum dragonExtRegisterErr_st {
    DRAGON_EXT_REGISTER_SUCCESS       = 0,
    DRAGON_EXT_REGISTER_FAIL          = 1,
    DRAGON_EXT_REGISTER_AUTH_FAILURE  = 2,
    DRAGON_EXT_REGISTER_ALREADY       = 3,
    DRAGON_EXT_REGISTER_PUID_EXHAUSTED = 5,
    DRAGON_EXT_REGISTER_CONFLICTING_ATTEMPT = 6
} dragonExtRegisterErr_t;

/* Mirrors dragon.infrastructure.messages.GSProcessDetachResponse.Errors. */
typedef enum dragonExtDetachErr_st {
    DRAGON_EXT_DETACH_SUCCESS = 0,
    DRAGON_EXT_DETACH_FAIL    = 1,
    DRAGON_EXT_DETACH_UNKNOWN = 2
} dragonExtDetachErr_t;

/**
 * @brief Attach to an already-running, --attachable Dragon runtime.
 *
 * Mirrors dragon.workflows.runtime._local_attach(). Reads attach_info.txt
 * (a Base64-encoded AttachableRuntimeDescr capnp message, honoring
 * DRAGON_ATTACH_DIR), attaches the default pool, creates a
 * temporary reply channel/FLI, sends LSExternalRegister to the node-local LS
 * input queue, installs the LS-provided gateway channels, waits for direct GS
 * completion, and on success stores the process's GS input/return FLIs and
 * assigned p_uid in *handle. Also sets the DRAGON_DEFAULT_PD (and, if present,
 * DRAGON_INF_PD, DRAGON_RT_UID) environment variables from the resolved
 * attach data / response, so that the caller can subsequently use the
 * existing dragon_memory_pool_attach_default() (and equivalent inf-pool)
 * APIs directly without this library needing to expose a pool-descriptor
 * accessor of its own. Also sets DRAGON_LOCAL_LS_QD and DRAGON_LS_RET_QD
 * from the same attach data / response, so that dragon_ls_set_kv() and
 * dragon_ls_get_kv() (src/include/dragon/utils.h) become usable
 * afterward without the caller needing to do anything else. Does not set up
 * the four general-purpose infrastructure queues used by
 * dragon.globalservices.api_setup.connect_as_external() -- intentionally out
 * of scope.
 *
 * @param auth_token Explicit token; if NULL, falls back to the
 *        DRAGON_ATTACH_TOKEN environment variable.
 * @param timeout One timeout budget for LS provisioning and GS completion. NULL = no timeout.
 * @param remote_err Optional out-param (may be NULL). Only meaningful when the
 *        round-trip itself succeeded but the *message* reported failure --
 *        callers should primarily check the dragonError_t return value.
 * @param handle Out-param, opaque attach handle used by all other calls.
 * @return DRAGON_SUCCESS, or a dragonError_t describing a *local* C-layer
 *         failure (bad JSON, pool attach failure, timeout, etc.) or
 *         DRAGON_INVALID_OPERATION if the round-trip succeeded but
 *         *remote_err != DRAGON_EXT_REGISTER_SUCCESS (check remote_err for detail).
 */
dragonError_t dragon_ext_attach(const char* auth_token, const timespec_t* timeout,
                                 dragonExtRegisterErr_t* remote_err,
                                 dragonExtAttachDescr_t* handle);

/**
 * @brief Detach from the Dragon runtime. Mirrors LocalRuntime.detach().
 *
 * Sends GSProcessDetach directly to GS's input queue (attach_info.txt's
 * gs_qd), waits for GSProcessDetachResponse on this process's own GS-return
 * FLI (the one obtained during attach), acknowledges response consumption to
 * GS, then detaches gs_input_fli, gs_return_fli, and the default pool handle
 * this library itself attached
 * during dragon_ext_attach(). Does NOT call anything resembling
 * disconnect_infrastructure() -- there is no full infra connection to tear
 * down since dragon_ext_attach() never created one. Server-side, GS/LS
 * automatically clean up the ls_ret_qd channel and the gs_ret channel as
 * part of processing GSProcessDetach -- no client action needed for those.
 *
 * After a successful call, *handle is invalidated and must not be reused.
 * Calling this twice on the same handle returns DRAGON_INVALID_ARGUMENT
 * the second time.
 */
dragonError_t dragon_ext_detach(dragonExtAttachDescr_t* handle, const timespec_t* timeout,
                                 dragonExtDetachErr_t* remote_err);

/** @brief Returns the p_uid assigned to this process by GS during attach. */
dragonError_t dragon_ext_get_puid(const dragonExtAttachDescr_t* handle, dragonP_UID_t* puid);

#ifdef __cplusplus
}
#endif

#endif // HAVE_DRAGON_EXT_ATTACH_H
