#include <dragon/ext_attach.h>
#include <dragon/messages.hpp>
#include <dragon/messages_api.h>
#include <dragon/managed_memory.h>
#include <dragon/channels.h>
#include <dragon/fli.h>
#include <dragon/utils.h>
#include "err.h"

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <unistd.h>
#include <sys/types.h>

using namespace dragon;

/* ----------------------------------------------------------------------
 * Constants duplicated from src/dragon/infrastructure/facts.py.
 *
 * IMPORTANT: these five constants MUST stay in sync, by hand, forever, with
 * facts.py's ATTACH_INFO_FILENAME / ATTACH_RUNTIME_DIR_NAME_TEMPLATE /
 * ATTACH_DIR_ENV / DRAGON_ATTACH_TOKEN_ENV / BASE_ATTACH_TEMP_CUID /
 * RANGE_ATTACH_TEMP_CUID.
 * There is no automated enforcement of this -- this is a known, accepted
 * limitation, the same pattern already used by global_types.h for a
 * different, smaller set of facts.py constants (DRAGON_DEFAULT_PD_VAR,
 * DRAGON_INF_PD_VAR).
 * ------------------------------------------------------------------- */
#define DRAGON_ATTACH_INFO_FILENAME "attach_info.txt"
#define DRAGON_ATTACH_RUNTIME_DIR_PREFIX "dragon_"
#define DRAGON_ATTACH_DIR_ENV "DRAGON_ATTACH_DIR"
#define DRAGON_ATTACH_TOKEN_ENV "DRAGON_ATTACH_TOKEN"
#define DRAGON_ATTACH_ENV_PREFIX "DRAGON_"
#define DRAGON_BASE_ATTACH_TEMP_CUID (1ULL << 53)
#define DRAGON_RANGE_ATTACH_TEMP_CUID (1ULL << 20)

static inline uint64_t
attach_temp_cuid_from_pid(pid_t pid)
{
    return DRAGON_BASE_ATTACH_TEMP_CUID + ((uint64_t)pid % DRAGON_RANGE_ATTACH_TEMP_CUID);
}

/* ----------------------------------------------------------------------
 * Internal per-handle state.
 * ------------------------------------------------------------------- */
struct ExtAttachState {
    dragonMemoryPoolDescr_t pool;    /* library's OWN handle, for its own detach bookkeeping only */
    dragonFLIDescr_t gs_input_fli;   /* attach_info.txt's gs_qd -- GS's INPUT queue */
    dragonFLIDescr_t gs_return_fli;  /* LSExternalResourcesReady.gsReturnQd -- our GS-return queue */
    dragonC_UID_t gs_return_cuid;    /* cached dragon_fli_main_channel_cuid(gs_return_fli) == r_c_uid */
    dragonP_UID_t p_uid;
    uint64_t pending_detach_ref;
    bool detach_response_received;
    bool detached;
};

static std::unordered_map<uint64_t, ExtAttachState*> g_ext_attach_table;
static uint64_t g_next_idx = 0;
static uint64_t g_ext_tag = 0; /* fresh local monotonic tag counter, not shared with messages_api.cpp */

static inline uint64_t
next_ext_tag()
{
    return g_ext_tag++;
}

/* ----------------------------------------------------------------------
 * attach_info.txt holds the Base64 encoding of a serialized capnp
 * AttachableRuntimeDescr message written by Local Services.
 * ------------------------------------------------------------------- */
static dragonError_t
resolve_attach_info_path(std::string& path)
{
    const char* attach_dir = getenv(DRAGON_ATTACH_DIR_ENV);

    if (attach_dir != nullptr && strlen(attach_dir) > 0) {
        path = std::string(attach_dir) + "/" + DRAGON_ATTACH_INFO_FILENAME;
    } else {
        char* tmpdir = nullptr;
        dragonError_t err = dragon_gettempdir(&tmpdir);
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not determine the temporary directory.");

        path = tmpdir;
        free(tmpdir);
        if (path.empty() || path.back() != '/')
            path += "/";
        path += DRAGON_ATTACH_RUNTIME_DIR_PREFIX + std::to_string(getuid()) + "/" + DRAGON_ATTACH_INFO_FILENAME;
    }

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
read_attach_info(uint64_t& node_index, std::string& gs_qd, std::string& ls_qd,
                 std::string& default_pd, std::string& inf_pd, std::string& rt_uid)
{
    std::string path;
    dragonError_t err = resolve_attach_info_path(path);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not resolve the attach-info file path.");

    std::ifstream fh(path.c_str());
    if (!fh.good())
        err_return(DRAGON_NOT_FOUND,
                   "No running Dragon runtime found: attach-info file does not exist. Start a Dragon "
                   "runtime with 'dragon -s' (or the equivalent --attachable launch) first.");

    std::stringstream buf;
    buf << fh.rdbuf();
    std::string encoded = buf.str();
    size_t last = encoded.find_last_not_of(" \t\r\n");
    encoded.erase(last == std::string::npos ? 0 : last + 1);
    if (encoded.empty())
        err_return(DRAGON_INVALID_MESSAGE, "The attach-info file is empty.");

    size_t raw_len = 0;
    uint8_t* raw = dragon_base64_decode(encoded.c_str(), &raw_len);
    if (raw == nullptr)
        err_return(DRAGON_INVALID_MESSAGE, "The attach-info file does not contain valid Base64.");

    DragonMsg* msg = nullptr;
    err = dragon_deserialize_msg(raw, raw_len, &msg);
    free(raw);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "The attach-info file does not contain a valid Dragon message.");

    if (msg->tc() != ATTACHABLE_RUNTIME_DESCR) {
        delete msg;
        err_return(DRAGON_INVALID_MESSAGE, "The attach-info file does not contain an AttachableRuntimeDescr.");
    }

    AttachableRuntimeDescrMsg* descr = static_cast<AttachableRuntimeDescrMsg*>(msg);
    node_index = descr->nodeIndex();
    gs_qd = descr->gsQd();
    ls_qd = descr->lsQd();
    default_pd = descr->defaultPd();
    inf_pd = descr->infPd();
    rt_uid = descr->rtUid();
    delete descr;

    if (gs_qd.empty() || ls_qd.empty() || default_pd.empty() || inf_pd.empty() || rt_uid.empty())
        err_return(DRAGON_INVALID_MESSAGE, "The attach-info descriptor is missing one or more required fields.");

    no_err_return(DRAGON_SUCCESS);
}

static bool
is_gateway_env_name(const std::string& name)
{
    const std::string prefix = DRAGON_CHANNEL_DEFAULT_GW_ENV_PREFIX;
    if (name.compare(0, prefix.size(), prefix) != 0 || name.size() == prefix.size())
        return false;

    for (size_t idx = prefix.size(); idx < name.size(); ++idx) {
        if (!isdigit((unsigned char)name[idx]))
            return false;
    }

    return true;
}

class BootstrapEnvironmentGuard {
    public:
    BootstrapEnvironmentGuard(): mCommitted(false)
    {
        extern char** environ;
        for (char** item = environ; item != nullptr && *item != nullptr; ++item) {
            std::string entry(*item);
            size_t separator = entry.find('=');
            std::string key = entry.substr(0, separator);
            if (key.compare(0, strlen(DRAGON_ATTACH_ENV_PREFIX), DRAGON_ATTACH_ENV_PREFIX) == 0)
                mEnvironment.emplace(key, entry.substr(separator + 1));
        }
    }

    ~BootstrapEnvironmentGuard()
    {
        if (!mCommitted)
            restore();
    }

    void commit() { mCommitted = true; }

    private:
    void restore() noexcept
    {
        // Best-effort: the destructor is noexcept, so an allocation failure must not escape.
        try {
            extern char** environ;
            std::vector<std::string> dragon_keys;
            for (char** item = environ; item != nullptr && *item != nullptr; ++item) {
                std::string entry(*item);
                size_t separator = entry.find('=');
                std::string key = entry.substr(0, separator);
                if (key.compare(0, strlen(DRAGON_ATTACH_ENV_PREFIX), DRAGON_ATTACH_ENV_PREFIX) == 0)
                    dragon_keys.push_back(key);
            }

            for (const auto& key : dragon_keys)
                unsetenv(key.c_str());
            for (const auto& entry : mEnvironment)
                setenv(entry.first.c_str(), entry.second.c_str(), 1);
        } catch (...) {
        }
    }

    std::unordered_map<std::string, std::string> mEnvironment;
    bool mCommitted;
};

static dragonError_t
install_gateway_environment(uint64_t num_gateways,
                            const std::vector<std::pair<std::string, std::string>>& gateway_env)
{
    if (gateway_env.size() != num_gateways)
        err_return(DRAGON_INVALID_OPERATION, "The external attach gateway environment has the wrong size.");

    std::unordered_map<std::string, std::string> gateways;
    for (const auto& entry : gateway_env) {
        if (!is_gateway_env_name(entry.first) || !gateways.emplace(entry.first, entry.second).second)
            err_return(DRAGON_INVALID_OPERATION, "The external attach gateway environment is invalid.");
    }

    for (uint64_t idx = 1; idx <= num_gateways; ++idx) {
        std::string key = std::string(DRAGON_CHANNEL_DEFAULT_GW_ENV_PREFIX) + std::to_string(idx);
        if (gateways.find(key) == gateways.end())
            err_return(DRAGON_INVALID_OPERATION, "The external attach gateway environment is incomplete.");
    }

    extern char** environ;
    std::vector<std::string> stale_gateway_keys;
    for (char** item = environ; item != nullptr && *item != nullptr; ++item) {
        std::string entry(*item);
        size_t separator = entry.find('=');
        std::string key = entry.substr(0, separator);
        if (is_gateway_env_name(key))
            stale_gateway_keys.push_back(key);
    }
    for (const auto& key : stale_gateway_keys)
        unsetenv(key.c_str());

    std::string num_gateways_str = std::to_string(num_gateways);
    if (setenv(DRAGON_NUM_GW_ENV_VAR, num_gateways_str.c_str(), 1) != 0)
        err_return(DRAGON_FAILURE, "Could not set the Dragon gateway count environment variable.");
    for (const auto& entry : gateways) {
        if (setenv(entry.first.c_str(), entry.second.c_str(), 1) != 0)
            err_return(DRAGON_FAILURE, "Could not set a Dragon gateway environment variable.");
    }

    no_err_return(DRAGON_SUCCESS);
}

class ExtAttachTimeoutBudget {
    public:
    explicit ExtAttachTimeoutBudget(const timespec_t* timeout): mLimited(timeout != nullptr)
    {
        mInitErr = mLimited ? dragon_timespec_deadline(timeout, &mDeadline) : DRAGON_SUCCESS;
    }

    dragonError_t initErr() const { return mInitErr; }

    const timespec_t* next()
    {
        if (!mLimited)
            return nullptr;
        if (dragon_timespec_remaining(&mDeadline, &mRemaining) != DRAGON_SUCCESS) {
            mRemaining.tv_sec = 0;
            mRemaining.tv_nsec = 0;
        }
        return &mRemaining;
    }

    private:
    bool mLimited;
    dragonError_t mInitErr;
    timespec_t mDeadline;
    timespec_t mRemaining;
};

static void
cancel_external_registration(const std::string& ls_qd,
                             const std::string& runtime_uid, const std::string& attempt_id,
                             uint64_t p_uid, ExtAttachTimeoutBudget& timeout_budget)
{
    dragonFLISerial_t ls_serial;
    ls_serial.data = dragon_base64_decode(ls_qd.c_str(), &ls_serial.len);
    if (ls_serial.data == nullptr)
        return;

    dragonFLIDescr_t ls_input;
    dragonError_t err = dragon_fli_attach(&ls_serial, nullptr, &ls_input);
    dragon_fli_serial_free(&ls_serial);
    if (err != DRAGON_SUCCESS)
        return;

    LSExternalRegisterCancelMsg cancel(
        next_ext_tag(), runtime_uid.c_str(), attempt_id.c_str(), p_uid);
    dragonFLISendHandleDescr_t send_handle;
    err = dragon_fli_open_send_handle(&ls_input, &send_handle, nullptr, nullptr, timeout_budget.next());
    if (err == DRAGON_SUCCESS) {
        err = cancel.send(&send_handle, timeout_budget.next());
        dragon_fli_close_send_handle(&send_handle, timeout_budget.next());
    }
    dragon_fli_detach(&ls_input);
}

dragonError_t
dragon_ext_attach(const char* auth_token, const timespec_t* timeout, dragonExtRegisterErr_t* remote_err,
                   dragonExtAttachDescr_t* handle)
{
    dragonError_t err;
    ExtAttachTimeoutBudget timeout_budget(timeout);
    if (timeout_budget.initErr() != DRAGON_SUCCESS)
        append_err_return(timeout_budget.initErr(), "Could not initialize the external attach timeout.");

    if (handle == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "The handle argument cannot be NULL.");

    if (remote_err != nullptr)
        *remote_err = DRAGON_EXT_REGISTER_SUCCESS;

    /* Step 1/2/3: resolve and read attach_info.txt */
    uint64_t node_index = 0;
    std::string gs_qd, ls_qd, default_pd, inf_pd, rt_uid_str;
    err = read_attach_info(node_index, gs_qd, ls_qd, default_pd, inf_pd, rt_uid_str);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not read the attach-info file.");

    BootstrapEnvironmentGuard environment_guard;

    /* Step 4: DRAGON_RT_UID must be set before the pool attach so the C
       memory-pool layer recognizes the default pool as local. */
    if (setenv("DRAGON_RT_UID", rt_uid_str.c_str(), 1) != 0)
        err_return(DRAGON_FAILURE, "Could not set the Dragon runtime UID environment variable.");

    /* Step 5: resolve auth token */
    const char* token = auth_token;
    if (token == nullptr || strlen(token) == 0)
        token = getenv(DRAGON_ATTACH_TOKEN_ENV);

    if (token == nullptr || strlen(token) == 0)
        err_return(DRAGON_INVALID_ARGUMENT,
                   "No attach auth token provided. Pass a non-NULL auth_token or set the "
                   "DRAGON_ATTACH_TOKEN environment variable.");

    ExtAttachState* state = new ExtAttachState();
    state->detach_response_received = false;
    state->detached = false;

    /* Step 6: attach the default pool (library's own bookkeeping handle) */
    dragonMemoryPoolSerial_t pool_ser;
    pool_ser.data = dragon_base64_decode(default_pd.c_str(), &pool_ser.len);

    err = dragon_memory_pool_attach(&state->pool, &pool_ser);
    dragon_memory_pool_serial_free(&pool_ser);
    if (err != DRAGON_SUCCESS) {
        delete state;
        append_err_return(err, "Could not attach to the default memory pool.");
    }

    /* Step 7/8/9/10: create a temp reply channel/FLI and serialize it */
    dragonChannelDescr_t temp_ch;
    uint64_t temp_cuid = attach_temp_cuid_from_pid(getpid());

    err = dragon_channel_create(&temp_ch, temp_cuid, &state->pool, nullptr);
    if (err != DRAGON_SUCCESS) {
        dragon_memory_pool_detach(&state->pool);
        delete state;
        append_err_return(err, "Could not create the temporary reply channel.");
    }

    dragonFLIDescr_t temp_fli;
    err = dragon_fli_create(&temp_fli, &temp_ch, nullptr, &state->pool, 0, nullptr, true, nullptr);
    if (err != DRAGON_SUCCESS) {
        dragon_channel_destroy(&temp_ch);
        dragon_memory_pool_detach(&state->pool);
        delete state;
        append_err_return(err, "Could not create the temporary reply FLI.");
    }

    dragonFLISerial_t temp_fli_ser;
    err = dragon_fli_serialize(&temp_fli, &temp_fli_ser);
    if (err != DRAGON_SUCCESS) {
        dragon_fli_destroy(&temp_fli);
        dragon_channel_destroy(&temp_ch);
        dragon_memory_pool_detach(&state->pool);
        delete state;
        append_err_return(err, "Could not serialize the temporary reply FLI.");
    }

    char* proc_resp_sdesc = dragon_base64_encode(temp_fli_ser.data, temp_fli_ser.len);
    dragon_fli_serial_free(&temp_fli_ser);

    /* Step 11: attach to the node-local LS input queue */
    dragonFLISerial_t ls_ser;
    ls_ser.data = dragon_base64_decode(ls_qd.c_str(), &ls_ser.len);

    dragonFLIDescr_t ls_input_fli;
    err = dragon_fli_attach(&ls_ser, nullptr, &ls_input_fli);
    dragon_fli_serial_free(&ls_ser);
    if (err != DRAGON_SUCCESS) {
        free(proc_resp_sdesc);
        dragon_fli_destroy(&temp_fli);
        dragon_channel_destroy(&temp_ch);
        dragon_memory_pool_detach(&state->pool);
        delete state;
        append_err_return(err, "Could not attach to the node-local Local Services input queue.");
    }

    /* Step 12/13: send LSExternalRegister to Local Services */
    uint64_t tag = next_ext_tag();
    dragonUUID attempt_uuid;
    dragon_generate_uuid(attempt_uuid);
    char* attempt_id = dragon_uuid_to_hex_str(attempt_uuid);
    if (attempt_id == nullptr) {
        free(proc_resp_sdesc);
        dragon_fli_detach(&ls_input_fli);
        dragon_fli_destroy(&temp_fli);
        dragon_channel_destroy(&temp_ch);
        dragon_memory_pool_detach(&state->pool);
        delete state;
        err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate the external attach attempt ID.");
    }
    LSExternalRegisterMsg req(tag, token, (uint64_t)getpid(), proc_resp_sdesc,
                              rt_uid_str.c_str(), attempt_id);
    free(proc_resp_sdesc);
    std::string attempt_id_str(attempt_id);
    free(attempt_id);

    dragonFLISendHandleDescr_t sendh;
    err = dragon_fli_open_send_handle(&ls_input_fli, &sendh, nullptr, nullptr, timeout_budget.next());
    if (err == DRAGON_SUCCESS) {
        err = req.send(&sendh, timeout_budget.next());
        dragonError_t close_err = dragon_fli_close_send_handle(&sendh, timeout_budget.next());
        if (err == DRAGON_SUCCESS)
            err = close_err;
    }

    dragon_fli_detach(&ls_input_fli);

    if (err != DRAGON_SUCCESS) {
        cancel_external_registration(
            ls_qd, rt_uid_str, attempt_id_str, 0, timeout_budget);
        dragon_fli_destroy(&temp_fli);
        dragon_channel_destroy(&temp_ch);
        dragon_memory_pool_detach(&state->pool);
        delete state;
        append_err_return(err, "Could not send LSExternalRegister to Local Services.");
    }

    /* Step 14/15: wait for LSExternalResourcesReady on the temp reply queue */
    dragonFLIRecvHandleDescr_t recvh;
    err = dragon_fli_open_recv_handle(&temp_fli, &recvh, nullptr, nullptr, timeout_budget.next());
    if (err != DRAGON_SUCCESS) {
        cancel_external_registration(
            ls_qd, rt_uid_str, attempt_id_str, 0, timeout_budget);
        dragon_fli_destroy(&temp_fli);
        dragon_channel_destroy(&temp_ch);
        dragon_memory_pool_detach(&state->pool);
        delete state;
        append_err_return(err, "Could not open a receive handle on the temporary reply FLI.");
    }

    LSExternalResourcesReadyMsg* resources = nullptr;
    while (resources == nullptr) {
        DragonMsg* raw_msg = nullptr;
        err = recv_fli_msg(&recvh, &raw_msg, timeout_budget.next());
        if (err != DRAGON_SUCCESS) {
            dragon_fli_close_recv_handle(&recvh, timeout_budget.next());
            cancel_external_registration(
                ls_qd, rt_uid_str, attempt_id_str, 0, timeout_budget);
            dragon_fli_destroy(&temp_fli);
            dragon_channel_destroy(&temp_ch);
            dragon_memory_pool_detach(&state->pool);
            delete state;
            append_err_return(err, "Could not receive LSExternalResourcesReady.");
        }

        if (raw_msg->tc() != LS_EXTERNAL_RESOURCES_READY) {
            delete raw_msg;
            continue;
        }

        LSExternalResourcesReadyMsg* candidate = static_cast<LSExternalResourcesReadyMsg*>(raw_msg);
        if (candidate->ref() != tag) {
            delete candidate;
            continue;
        }

        resources = candidate;
    }

    err = dragon_fli_close_recv_handle(&recvh, timeout_budget.next());

    /* Step 16: single-use temp reply channel/FLI -- destroy it now. */
    dragon_fli_destroy(&temp_fli);
    dragon_channel_destroy(&temp_ch);

    if (err != DRAGON_SUCCESS) {
        cancel_external_registration(
            ls_qd, rt_uid_str, attempt_id_str, resources->pUid(), timeout_budget);
        delete resources;
        dragon_memory_pool_detach(&state->pool);
        delete state;
        append_err_return(err, "Could not close the temporary reply FLI receive handle.");
    }

    dragonExtRegisterErr_t resp_err = (dragonExtRegisterErr_t)resources->err();
    if (remote_err != nullptr)
        *remote_err = resp_err;

    if (resp_err != DRAGON_EXT_REGISTER_SUCCESS) {
        delete resources;
        dragon_memory_pool_detach(&state->pool);
        delete state;
        err_return(DRAGON_INVALID_OPERATION, "Dragon attach provisioning was rejected by Local Services.");
    }

    if (resources->runtimeUid() != rt_uid_str ||
        resources->attemptId() != attempt_id_str || resources->nodeIndex() != node_index) {
        cancel_external_registration(
            ls_qd, rt_uid_str, attempt_id_str, resources->pUid(), timeout_budget);
        delete resources;
        dragon_memory_pool_detach(&state->pool);
        delete state;
        err_return(DRAGON_INVALID_OPERATION, "Dragon attach provisioning identity mismatch.");
    }

    /* Step 16: set DRAGON_DEFAULT_PD / DRAGON_INF_PD env vars for
       dragon_memory_pool_attach_default() and friends to work afterward. */
    std::string default_pd_final = (strlen(resources->defaultPd()) > 0) ? resources->defaultPd() : default_pd;
    std::string inf_pd_final = (strlen(resources->infPd()) > 0) ? resources->infPd() : inf_pd;
    if (setenv(DRAGON_DEFAULT_PD_VAR, default_pd_final.c_str(), 1) != 0 ||
        setenv(DRAGON_INF_PD_VAR, inf_pd_final.c_str(), 1) != 0) {
        cancel_external_registration(
            ls_qd, rt_uid_str, attempt_id_str, resources->pUid(), timeout_budget);
        delete resources;
        dragon_memory_pool_detach(&state->pool);
        delete state;
        err_return(DRAGON_FAILURE, "Could not set the Dragon pool environment variables.");
    }

    /* Set DRAGON_LOCAL_LS_QD / DRAGON_LS_RET_QD so that the existing
       dragon_ls_set_kv()/dragon_ls_get_kv() helpers (src/lib/messages_api.cpp,
       declared in src/include/dragon/utils.h) work transparently for
       callers, mirroring the DRAGON_DEFAULT_PD pattern above.
       DRAGON_LOCAL_LS_QD is the same node-local LS input queue descriptor
       already read from attach_info.txt above (variable `ls_qd`) and used to
       send LSExternalRegister. DRAGON_LS_RET_QD is the per-process LS-return
    channel descriptor LS just assigned us in the response. LS/GS destroy
    this channel automatically on detach, so no client-side cleanup is needed. */
    if (setenv("DRAGON_LOCAL_LS_QD", ls_qd.c_str(), 1) != 0 ||
        (strlen(resources->lsReturnQd()) > 0 && setenv("DRAGON_LS_RET_QD", resources->lsReturnQd(), 1) != 0)) {
        cancel_external_registration(
            ls_qd, rt_uid_str, attempt_id_str, resources->pUid(), timeout_budget);
        delete resources;
        dragon_memory_pool_detach(&state->pool);
        delete state;
        err_return(DRAGON_FAILURE, "Could not set the Dragon Local Services environment variables.");
    }

    err = install_gateway_environment(resources->numGwChannelsPerNode(), resources->gwEnv());
    if (err != DRAGON_SUCCESS) {
        cancel_external_registration(
            ls_qd, rt_uid_str, attempt_id_str, resources->pUid(), timeout_budget);
        delete resources;
        dragon_memory_pool_detach(&state->pool);
        delete state;
        append_err_return(err, "Could not install the provisioned gateway environment.");
    }

    /* Step 17: attach our own GS-return queue */
    dragonFLISerial_t gs_ret_ser;
    gs_ret_ser.data = dragon_base64_decode(resources->gsReturnQd(), &gs_ret_ser.len);
    err = dragon_fli_attach(&gs_ret_ser, nullptr, &state->gs_return_fli);
    dragon_fli_serial_free(&gs_ret_ser);
    if (err != DRAGON_SUCCESS) {
        cancel_external_registration(
            ls_qd, rt_uid_str, attempt_id_str, resources->pUid(), timeout_budget);
        delete resources;
        dragon_memory_pool_detach(&state->pool);
        delete state;
        append_err_return(err, "Could not attach to the process's own GS-return queue.");
    }

    /* Step 18: wait for direct GS registration completion. */
    err = dragon_fli_open_recv_handle(&state->gs_return_fli, &recvh, nullptr, nullptr, timeout_budget.next());
    if (err != DRAGON_SUCCESS) {
        cancel_external_registration(
            ls_qd, rt_uid_str, attempt_id_str, resources->pUid(), timeout_budget);
        dragon_fli_detach(&state->gs_return_fli);
        delete resources;
        dragon_memory_pool_detach(&state->pool);
        delete state;
        append_err_return(err, "Could not open a receive handle on the GS-return queue.");
    }

    GSExternalRegisterCompleteMsg* completion = nullptr;
    while (completion == nullptr) {
        DragonMsg* raw_msg = nullptr;
        err = recv_fli_msg(&recvh, &raw_msg, timeout_budget.next());
        if (err != DRAGON_SUCCESS) {
            dragon_fli_close_recv_handle(&recvh, timeout_budget.next());
            cancel_external_registration(
                ls_qd, rt_uid_str, attempt_id_str, resources->pUid(), timeout_budget);
            dragon_fli_detach(&state->gs_return_fli);
            delete resources;
            dragon_memory_pool_detach(&state->pool);
            delete state;
            append_err_return(err, "Could not receive GSExternalRegisterComplete.");
        }

        if (raw_msg->tc() != GS_EXTERNAL_REGISTER_COMPLETE) {
            delete raw_msg;
            continue;
        }
        completion = static_cast<GSExternalRegisterCompleteMsg*>(raw_msg);
    }

    err = dragon_fli_close_recv_handle(&recvh, timeout_budget.next());
    if (err != DRAGON_SUCCESS) {
        cancel_external_registration(
            ls_qd, rt_uid_str, attempt_id_str, resources->pUid(), timeout_budget);
        delete completion;
        dragon_fli_detach(&state->gs_return_fli);
        delete resources;
        dragon_memory_pool_detach(&state->pool);
        delete state;
        append_err_return(err, "Could not close the GS registration completion receive handle.");
    }

    if (completion->err() != DRAGON_SUCCESS || completion->pUid() != resources->pUid()) {
        if (remote_err != nullptr)
            *remote_err = DRAGON_EXT_REGISTER_FAIL;
        cancel_external_registration(
            ls_qd, rt_uid_str, attempt_id_str, resources->pUid(), timeout_budget);
        delete completion;
        dragon_fli_detach(&state->gs_return_fli);
        delete resources;
        dragon_memory_pool_detach(&state->pool);
        delete state;
        err_return(DRAGON_INVALID_OPERATION, "Global Services did not commit the external registration.");
    }

    /* Step 19: eagerly attach GS's input queue */
    dragonFLISerial_t gs_in_ser;
    gs_in_ser.data = dragon_base64_decode(gs_qd.c_str(), &gs_in_ser.len);
    err = dragon_fli_attach(&gs_in_ser, nullptr, &state->gs_input_fli);
    dragon_fli_serial_free(&gs_in_ser);
    if (err != DRAGON_SUCCESS) {
        cancel_external_registration(
            ls_qd, rt_uid_str, attempt_id_str, resources->pUid(), timeout_budget);
        dragon_fli_detach(&state->gs_return_fli);
        delete completion;
        delete resources;
        dragon_memory_pool_detach(&state->pool);
        delete state;
        append_err_return(err, "Could not attach to the Global Services input queue.");
    }

    /* Step 22 */
    err = dragon_fli_main_channel_cuid(&state->gs_return_fli, &state->gs_return_cuid);
    if (err != DRAGON_SUCCESS) {
        cancel_external_registration(
            ls_qd, rt_uid_str, attempt_id_str, resources->pUid(), timeout_budget);
        dragon_fli_detach(&state->gs_input_fli);
        dragon_fli_detach(&state->gs_return_fli);
        delete completion;
        delete resources;
        dragon_memory_pool_detach(&state->pool);
        delete state;
        append_err_return(err, "Could not get the main channel cuid of the GS-return queue.");
    }

    /* Step 23 */
    state->p_uid = resources->pUid();
    state->detached = false;

    /* Step 24 */
    delete completion;
    delete resources;

    /* Step 25 */
    uint64_t idx = g_next_idx++;
    g_ext_attach_table[idx] = state;
    handle->_idx = idx;
    environment_guard.commit();

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_ext_detach(dragonExtAttachDescr_t* handle, const timespec_t* timeout, dragonExtDetachErr_t* remote_err)
{
    if (handle == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "The handle argument cannot be NULL.");

    auto it = g_ext_attach_table.find(handle->_idx);
    if (it == g_ext_attach_table.end() || it->second->detached)
        err_return(DRAGON_INVALID_ARGUMENT, "This handle is not currently attached.");

    ExtAttachState* state = it->second;

    dragonError_t err;
    ExtAttachTimeoutBudget timeout_budget(timeout);
    if (timeout_budget.initErr() != DRAGON_SUCCESS)
        append_err_return(timeout_budget.initErr(), "Could not initialize the external detach timeout.");

    if (remote_err != nullptr)
        *remote_err = DRAGON_EXT_DETACH_SUCCESS;

    dragonFLISendHandleDescr_t sendh;
    if (!state->detach_response_received) {
        uint64_t tag = next_ext_tag();
        GSProcessDetachMsg req(tag, state->p_uid, state->gs_return_cuid);

        err = dragon_fli_open_send_handle(&state->gs_input_fli, &sendh, nullptr, nullptr, timeout_budget.next());
        if (err == DRAGON_SUCCESS) {
            err = req.send(&sendh, timeout_budget.next());
            dragonError_t close_err = dragon_fli_close_send_handle(&sendh, timeout_budget.next());
            if (err == DRAGON_SUCCESS)
                err = close_err;
        }

        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not send GSProcessDetach to Global Services.");

        dragonFLIRecvHandleDescr_t recvh;
        err = dragon_fli_open_recv_handle(&state->gs_return_fli, &recvh, nullptr, nullptr, timeout_budget.next());
        if (err != DRAGON_SUCCESS)
            append_err_return(err, "Could not open a receive handle on the GS-return queue.");

        GSProcessDetachResponseMsg* resp = nullptr;
        while (resp == nullptr) {
            DragonMsg* raw_msg = nullptr;
            err = recv_fli_msg(&recvh, &raw_msg, timeout_budget.next());
            if (err != DRAGON_SUCCESS) {
                dragon_fli_close_recv_handle(&recvh, timeout_budget.next());
                append_err_return(err, "Could not receive GSProcessDetachResponse.");
            }

            if (raw_msg->tc() != GS_PROCESS_DETACH_RESPONSE) {
                delete raw_msg;
                continue;
            }

            GSProcessDetachResponseMsg* candidate = static_cast<GSProcessDetachResponseMsg*>(raw_msg);
            if (candidate->ref() != tag) {
                delete candidate;
                continue;
            }

            resp = candidate;
        }

        err = dragon_fli_close_recv_handle(&recvh, timeout_budget.next());
        if (err != DRAGON_SUCCESS) {
            delete resp;
            append_err_return(err, "Could not close the GS detach response receive handle.");
        }

        dragonExtDetachErr_t response_error = (dragonExtDetachErr_t)resp->err();
        if (remote_err != nullptr)
            *remote_err = response_error;

        if (response_error != DRAGON_EXT_DETACH_SUCCESS) {
            delete resp;
            err_return(DRAGON_INVALID_OPERATION, "Global Services rejected the external detach request.");
        }

        state->pending_detach_ref = resp->ref();
        state->detach_response_received = true;
        delete resp;
    }

    GSProcessDetachAckMsg ack(next_ext_tag(), state->p_uid, state->pending_detach_ref);
    err = dragon_fli_open_send_handle(&state->gs_input_fli, &sendh, nullptr, nullptr, timeout_budget.next());
    if (err == DRAGON_SUCCESS) {
        err = ack.send(&sendh, timeout_budget.next());
        dragonError_t close_err = dragon_fli_close_send_handle(&sendh, timeout_budget.next());
        if (err == DRAGON_SUCCESS)
            err = close_err;
    }
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "Could not acknowledge GSProcessDetachResponse to Global Services.");

    dragon_fli_detach(&state->gs_input_fli);
    dragon_fli_detach(&state->gs_return_fli);
    dragon_memory_pool_detach(&state->pool);

    g_ext_attach_table.erase(it);
    delete state;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_ext_get_puid(const dragonExtAttachDescr_t* handle, dragonP_UID_t* puid)
{
    if (handle == nullptr || puid == nullptr)
        err_return(DRAGON_INVALID_ARGUMENT, "The handle and puid arguments cannot be NULL.");

    auto it = g_ext_attach_table.find(handle->_idx);
    if (it == g_ext_attach_table.end() || it->second->detached)
        err_return(DRAGON_INVALID_ARGUMENT, "This handle is not currently attached.");

    *puid = it->second->p_uid;

    no_err_return(DRAGON_SUCCESS);
}
