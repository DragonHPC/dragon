#include <dragon/utils.h>
#include "_utils.h"
#include <dragon/return_codes_map.h>
#include "hostid.h"
#include "err.h"
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdatomic.h>
#include <ctype.h>
#include <math.h>
#include <fcntl.h>
#include <unistd.h>

#define ONE_BILLION 1000000000
#define ONE_MILLION 1000000
#define NSEC_PER_SECOND 1000000000
#define HUGEPAGE_MOUNT_DIR_MAX_LEN 512

bool dg_enable_errstr = true;
_Thread_local char * errstr = NULL;
static _Thread_local bool dg_thread_local_mode = false;
static timespec_t NO_TIMEOUT = {157680000000, 0}; // 5000 years, we'll all be dead.

const char*
dragon_get_rc_string(const dragonError_t rc)
{
    if (rc > dragon_max_rc_value)
        return dragon_rc_map[dragon_max_rc_value];

    return dragon_rc_map[rc];
}


void
_set_errstr(char * new_errstr)
{
    if (errstr != NULL)
        free(errstr);

    if (new_errstr == NULL)
        errstr = NULL;
    else
        errstr = strndup(new_errstr, DRAGON_MAX_ERRSTR_REC_LEN+1);
}


void
_append_errstr(char * more_errstr)
{
    if (errstr == NULL) {
        _set_errstr(more_errstr);
    } else {
        char * new_errstr = malloc(sizeof(char) * (strlen(errstr) +
                                        strnlen(more_errstr, DRAGON_MAX_ERRSTR_REC_LEN) + 1));
        if (new_errstr != NULL) {
            strcpy(new_errstr, errstr);
            strncat(new_errstr, more_errstr, DRAGON_MAX_ERRSTR_REC_LEN+1);
            free(errstr);
            errstr = new_errstr;
        }
    }
}


char *
_errstr_with_code(char * str, int code)
{
    char * new_str = malloc(sizeof(char) * (strnlen(str, DRAGON_MAX_ERRSTR_REC_LEN) +
                                            snprintf(NULL, 0, " %s", dragon_get_rc_string(code)) + 1));
    sprintf(new_str, "%s %s", str, dragon_get_rc_string(code));
    return new_str;
}


char *
dragon_getlasterrstr()
{
    char * str;
    if (errstr == NULL) {
        str = strdup("");
    } else {
        char* message = "Traceback (most recent call first):\n";
        str = malloc(sizeof(char) * (strlen(errstr) + strlen(message) + 1));
        if (str != NULL) {
            strcpy(str, message);
            strcat(str, errstr);
        } else
            str = strdup(errstr);
    }
    return str;
}

char *
dragon_getrawerrstr()
{
    char * str;
    if (errstr == NULL) {
        str = strdup("");
    } else {
        str = strdup(errstr);
    }
    return str;
}

void
dragon_setrawerrstr(char* err_str)
{
    _set_errstr(err_str);
}

void
dragon_enable_errstr(bool enable_errstr)
{
    dg_enable_errstr = enable_errstr;
}

dragonError_t
_lower_id(char *boot_id)
{
    while (*boot_id != '\0') {
        *boot_id = tolower(*boot_id);
        boot_id++;
    }
    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
_sanitize_id(char *boot_id)
{
    // make everything lower
    if (_lower_id(boot_id) != DRAGON_SUCCESS)
        err_return(DRAGON_FAILURE, "Unable to lower boot ID hex");

    // Remove all non-hex characters
    char *pr = boot_id;
    char *pw = boot_id;
    while (*pr) {
        *pw = *pr++;
        if (isxdigit(*pw)) pw++;
    }
    *pw = '\0';

    no_err_return(DRAGON_SUCCESS);
}

int
_get_dec_from_hex(char hex) {

    /* This only works for lowercase hex letters and digits. Don't use
       for anything else! */

    if (isdigit(hex))
        return hex - '0';
    else
        return hex - 'a';
}

dragonError_t
_hex_to_dec(char *hex, uint64_t *dec)
{
    *dec = 0UL;
    int i, len = strlen(hex);
    int start = len - 16;

    if (start < 0)
        err_return(DRAGON_INVALID_ARGUMENT, "Hex string less than 8 bytes");

    // Read the last 16 digits and convert
    for (i = start;  i < len; i++) {
        *dec += *dec * 16 + _get_dec_from_hex(hex[i]);
    }

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
_get_hostid_from_bootid(uint64_t *host_id)
{
    int fd;
    size_t n, bsize = 512;
    char boot_id[bsize];
    char *filename = "/proc/sys/kernel/random/boot_id";

    // Read hex boot id
    if ((fd = open(filename, O_RDONLY|O_CLOEXEC|O_NOCTTY)) == -1)
        err_return(DRAGON_FAILURE, "Unable to open /proc/sys/kernel/random/boot_id for host ID generation");

    if ((n = read(fd, boot_id, bsize)) == -1)
        err_return(DRAGON_FAILURE, "Unable to read /proc/sys/kernel/random/boot_id for host ID generation");

    boot_id[n] = '\0';
    close(fd);

    // Clean out any non-hex charactars and convert to dec
    if (_sanitize_id(boot_id) != DRAGON_SUCCESS)
        err_return(DRAGON_FAILURE, "Unable to sanitize boot ID");

    if (_hex_to_dec(boot_id, host_id) != DRAGON_SUCCESS)
        err_return(DRAGON_FAILURE, "Unable to convert boot ID from hex to dec");

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
_get_hostid_from_k8s_podid(char *pod_uid, uint64_t *host_id)
{
    // Clean out any non-hex charactars and convert to dec
    if (_sanitize_id(pod_uid) != DRAGON_SUCCESS)
        err_return(DRAGON_FAILURE, "Unable to sanitize boot ID");

    if (_hex_to_dec(pod_uid, host_id) != DRAGON_SUCCESS)
        err_return(DRAGON_FAILURE, "Unable to convert boot ID from hex to dec");

    no_err_return(DRAGON_SUCCESS);
}

dragonULInt dg_hostid;
dragonUInt dg_pid;
atomic_uint dg_ctr;
int dg_hostid_called = 0;

dragonULInt
dragon_host_id()
{
    char *k8s_pod_uid = getenv("POD_UID");

    if (dg_hostid_called == 0) {

        uint64_t lg_hostid;

        if (k8s_pod_uid != NULL) { // if we are within a Kubernetes Pod
            char *pod_uid = strdup(k8s_pod_uid);

            if (pod_uid == NULL) {
                err_return(DRAGON_FAILURE, "Unable to copy the POD_UID environment variable.");
            }
            if (_get_hostid_from_k8s_podid(pod_uid, &lg_hostid) != DRAGON_SUCCESS)
                err_return(DRAGON_FAILURE, "Unable to generate host ID from Kubernetes pod UUID.");

            free(pod_uid);
        }
        else {
            if (_get_hostid_from_bootid(&lg_hostid) != DRAGON_SUCCESS)
                err_return(DRAGON_FAILURE, "Unable to generate host ID from boot ID");
        }
        pid_t pid = getpid();
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);

        dg_ctr = (uint32_t)(1.0e-9 * (ONE_BILLION * now.tv_sec + now.tv_nsec));
        dg_hostid = (dragonULInt)lg_hostid;
        dg_pid = (dragonUInt)pid;

        dg_hostid_called = 1;
    }

    return dg_hostid;
}

dragonULInt
dragon_host_id_from_k8s_uuid(char *pod_uid)
{
    uint64_t lg_hostid;
    if (_get_hostid_from_k8s_podid(pod_uid, &lg_hostid) != DRAGON_SUCCESS)
        err_return(DRAGON_FAILURE, "Unable to generate host ID from Kubernetes pod UUID.");

    return (dragonULInt)lg_hostid;
}

dragonError_t
dragon_set_host_id(dragonULInt id)
{
    if (dg_hostid_called == 1) {
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot set host ID after it has been previously set");
    }
    else {
        pid_t pid = getpid();
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);

        dg_ctr = (uint32_t)(1.0e-9 * (ONE_BILLION * now.tv_sec + now.tv_nsec));
        dg_hostid = id;
        dg_pid = (dragonUInt)pid;

        dg_hostid_called = 1;
    }
    no_err_return(DRAGON_SUCCESS);
}

/* get the front end's external IP address, along with the IP address
 * for the head node, which are used to identify this Dragon runtime */
dragonULInt
dragon_get_local_rt_uid()
{
    static dragonULInt rt_uid = 0UL;

    if (rt_uid == 0UL) {
        char *rt_uid_str = getenv("DRAGON_RT_UID");

        /* Return 0 to indicate failure */
        if (rt_uid_str == NULL)
            return 0UL;

        rt_uid = (dragonULInt) strtoul(rt_uid_str, NULL, 10);
    }

    return rt_uid;
}

dragonULInt
dragon_get_my_puid()
{
    static dragonULInt local_get_puid = 0UL;
    static bool get_puid_called = false;

    if (get_puid_called)
        return local_get_puid;

    get_puid_called = true;

    char* puid_str = getenv("DRAGON_MY_PUID");

    if (puid_str != NULL)
        local_get_puid = (dragonULInt) strtoul(puid_str, NULL, 10);

    return local_get_puid;
}

dragonULInt
dragon_get_env_var_as_ulint(char* env_key)
{
    dragonULInt ret_val = 0UL;

    if (env_key == NULL)
        return 0UL;

    char* env_val = getenv(env_key);

    if (env_val != NULL)
        ret_val = (dragonULInt) strtoul(env_val, NULL, 10);

    return ret_val;
}

dragonError_t
dragon_set_env_var_as_ulint(char* env_key, dragonULInt val)
{
    if (env_key == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot set NULL key");

    char env_val[200];
    snprintf(env_val, 199, "%lu", val);

    int rc = setenv(env_key, env_val, 1);

    if (rc != 0) {
        char err_str[200];
        snprintf(err_str, 199, "Error on setting env var with EC=%d", rc);
        err_return(DRAGON_INVALID_OPERATION, err_str);
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_unset_env_var(char* env_key)
{
    if (env_key == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot unset NULL key");

    int rc = unsetenv(env_key);
    if (rc != 0) {
        char err_str[200];
        snprintf(err_str, 199, "Error on unsetting env var with EC=%d", rc);
        err_return(DRAGON_INVALID_OPERATION, err_str);
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_set_procname(char * name)
{
    if (name == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The name argument cannot be NULL.");
    prctl(PR_SET_NAME, (unsigned long)name, 0uL, 0uL, 0uL);
    no_err_return(DRAGON_SUCCESS);
}

void
dragon_zero_uuid(dragonUUID uuid)
{
    dragonULInt * zptr = (dragonULInt *)&uuid[0];
    *zptr = 0UL;

    zptr++;
    *zptr = 0UL;
}

char* dragon_uuid_to_hex_str(dragonUUID uuid)
{
    dragonULInt * uuid_word = (dragonULInt *)uuid;
    char val[40];
    char* ret_str;
    snprintf(val, 39, "%lx%lx", uuid_word[0], uuid_word[1]);
    ret_str = malloc(strlen(val)+1);
    if (ret_str == NULL)
        return NULL;
    strcpy(ret_str, val);
    return ret_str;
}

void
dragon_generate_uuid(dragonUUID uuid)
{
    dragonULInt hid = dragon_host_id();
    uint32_t ctr = atomic_fetch_add(&dg_ctr, 1UL);
    uint32_t pid = (uint32_t)dg_pid;

    dragonULInt * huid_ptr = (dragonULInt *)&uuid[DRAGON_UUID_OFFSET_HID];
    *huid_ptr = hid;

    dragonUInt * pid_ptr = (dragonUInt *)&uuid[DRAGON_UUID_OFFSET_PID];
    *pid_ptr = pid;

    dragonUInt * ctr_ptr = (dragonUInt *)&uuid[DRAGON_UUID_OFFSET_CTR];
    *ctr_ptr = ctr;
}

void
dragon_copy_uuid(dragonUUID dest, const dragonUUID src)
{
    dragonULInt * dest_word = (dragonULInt *)dest;
    dragonULInt * src_word = (dragonULInt *)src;

    dest_word[0] = src_word[0];
    dest_word[1] = src_word[1];
}

int
dragon_compare_uuid(const dragonUUID u1, const dragonUUID u2)
{
    dragonULInt * u1_word = (dragonULInt *)u1;
    dragonULInt * u2_word = (dragonULInt *)u2;

    if (u1_word[0] != u2_word[0])
        return 1;

    if (u1_word[1] != u2_word[1])
        return 1;

    return 0;
}

dragonError_t
dragon_encode_uuid(const dragonUUID uuid, void * ptr)
{
    if (ptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "destination pointer is invalid");

    memcpy(ptr, (void *)uuid, sizeof(dragonUUID));
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_decode_uuid(const void * ptr, dragonUUID uuid)
{
    if (ptr == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "source pointer is invalid");

    memcpy((void *)uuid, ptr, sizeof(dragonUUID));
    no_err_return(DRAGON_SUCCESS);
}

dragonULInt
dragon_get_host_id_from_uuid(dragonUUID uuid)
{
    return *(dragonULInt *)&uuid[DRAGON_UUID_OFFSET_HID];
}

pid_t
dragon_get_pid_from_uuid(dragonUUID uuid)
{
    return *(pid_t *)&uuid[DRAGON_UUID_OFFSET_PID];
}

uint32_t
dragon_get_ctr_from_uuid(dragonUUID uuid)
{
    return *(uint32_t *)&uuid[DRAGON_UUID_OFFSET_CTR];
}

// The while loop below ensures the timespec result is normalized.
dragonError_t
dragon_timespec_add(timespec_t* result, const timespec_t* first, const timespec_t* second)
{
    if (result == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The result argument must be non-NULL\n");

    if (first == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The first argument must be non-NULL\n");

    if (second == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The second argument must be non-NULL\n");

    result->tv_sec = first->tv_sec + second->tv_sec;
    result->tv_nsec = first->tv_nsec + second->tv_nsec;
    while (result->tv_nsec >= ONE_BILLION) {
        result->tv_sec += 1;
        result->tv_nsec -= ONE_BILLION;
    }

    no_err_return(DRAGON_SUCCESS);
}

// The while loop below ensures the timespec result is normalized.
dragonError_t
dragon_timespec_diff(timespec_t* result, const timespec_t* first, const timespec_t* second)
{
    if (result == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The result argument must be non-NULL\n");

    if (first == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The first argument must be non-NULL\n");

    if (second == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The second argument must be non-NULL\n");

    result->tv_sec = first->tv_sec - second->tv_sec;
    result->tv_nsec = first->tv_nsec - second->tv_nsec;

    while (result->tv_nsec < 0) {
        result->tv_sec -= 1;
        result->tv_nsec += ONE_BILLION;
    }

    no_err_return(DRAGON_SUCCESS);
}

// This comparison assumes the two timespecs are normalized.
bool
dragon_timespec_le(const timespec_t* first, const timespec_t* second)
{
    return ((first->tv_sec < second->tv_sec) ||
            ((first->tv_sec == second->tv_sec) && (first->tv_nsec <= second->tv_nsec)));
}


/***************************************************************************************
 * Find the deadline for a given timespec timeout.
 *
 * This function initializes a deadline based on the current time and the value of timer.
 *
 * @param timer A pointer to a timespec structure or NULL. If not null, then it has
 * the timeout value to be used in the computation of the deadline.
 * @param deadline A pointer to a timespec structure that holds the time when the timer
 * has expired.
 * @returns DRAGON_SUCCESS or DRAGON_INVALID_ARGUMENT
 **********************************************************************************/

dragonError_t
dragon_timespec_deadline(const timespec_t* timeout, timespec_t* deadline)
{
    timespec_t current;
    clock_gettime(CLOCK_MONOTONIC, &current);

    if (deadline == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The deadline argument cannot be NULL.");

    if (timeout == NULL) {
        dragon_timespec_add(deadline, &current, &NO_TIMEOUT);
        no_err_return(DRAGON_SUCCESS);
    }

    if (timeout->tv_nsec == 0 && timeout->tv_sec == 0) {
        /* A zero timeout corresponds to a try-once attempt */
        deadline->tv_nsec = 0;
        deadline->tv_sec = 0;
        no_err_return(DRAGON_SUCCESS);
    }

    dragon_timespec_add(deadline, &current, timeout);

    no_err_return(DRAGON_SUCCESS);
}

/***************************************************************************************
 * Check whether the current time has past the end of a timer and compute remaining time.
 *
 * This function no_err_return(DRAGON_SUCCESS) if no timeout has occurred and computes the
 * remaining time. If deadline is in the past, then this function returns DRAGON_TIMEOUT.
 *
 * @param deadline A pointer to a timespec structure that holds the time when the timer
 * will expire.
 * @param remaining_timeout The computed remaining time for the given deadline.
 * @returns DRAGON_SUCCESS or DRAGON_TIMEOUT or an undetermined error code.
 **********************************************************************************/

dragonError_t
dragon_timespec_remaining(const timespec_t * deadline, timespec_t * remaining_timeout)
{
    timespec_t now_time;

    if (remaining_timeout == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "Cannot pass NULL as remaining_timeout argument.");

    if (deadline == NULL) {
        *remaining_timeout = NO_TIMEOUT;
        no_err_return(DRAGON_SUCCESS);
    }

    if (deadline->tv_nsec == 0 && deadline->tv_sec == 0) {
        /* A zero timeout corresponds to a try-once attempt */
        remaining_timeout->tv_nsec = 0;
        remaining_timeout->tv_sec = 0;
        no_err_return(DRAGON_SUCCESS);
    }

    clock_gettime(CLOCK_MONOTONIC, &now_time);

    if (dragon_timespec_le(deadline, &now_time)) {
        remaining_timeout->tv_sec = 0;
        remaining_timeout->tv_nsec = 0;
        no_err_return(DRAGON_TIMEOUT);
    }

    dragonError_t err = dragon_timespec_diff(remaining_timeout, deadline, &now_time);
    if (err != DRAGON_SUCCESS)
        append_err_return(err, "This shouldn't happen.");

    no_err_return(DRAGON_SUCCESS);
}

double dragon_get_current_time_as_double() {
    timespec_t the_time;
    clock_gettime(CLOCK_MONOTONIC, &the_time);
    double time_val = the_time.tv_sec + ((double)the_time.tv_nsec) / NSEC_PER_SECOND;
    return time_val;
}

void strip_newlines(const char* inout_str, size_t* input_length) {
    size_t idx = *input_length-1;

    while (inout_str[idx] == '\n')
        idx--;

    *input_length = idx+1;
}

static const char encoding_table[] = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
            'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
            'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
            'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
            'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
            'w', 'x', 'y', 'z', '0', '1', '2', '3',
            '4', '5', '6', '7', '8', '9', '+', '/' };

static const unsigned char decoding_table[256] = {
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3e, 0x00, 0x00, 0x00, 0x3f,
    0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
    0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
    0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

char*
dragon_base64_encode(uint8_t *data, size_t input_length)
{

    const int mod_table[] = { 0, 2, 1 };

    size_t output_length = 4 * ((input_length + 2) / 3);

    char *encoded_data = (char*)malloc(1 + output_length);

    if (encoded_data == NULL)
        return NULL;

    for (int i = 0, j = 0; i < input_length;) {

        uint32_t octet_a = i < input_length ? (unsigned char)data[i++] : 0;
        uint32_t octet_b = i < input_length ? (unsigned char)data[i++] : 0;
        uint32_t octet_c = i < input_length ? (unsigned char)data[i++] : 0;

        uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

        encoded_data[j++] = encoding_table[(triple >> 3 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 2 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 1 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 0 * 6) & 0x3F];
    }

    for (int i = 0; i < mod_table[input_length % 3]; i++)
        encoded_data[output_length - 1 - i] = '=';

    encoded_data[output_length] = '\0';

    return encoded_data;
}

uint8_t*
dragon_base64_decode(const char *data, size_t *output_length)
{
    size_t input_length = strlen(data);

    strip_newlines(data, &input_length);

    if (input_length % 4 != 0)
        return NULL;


    *output_length = input_length / 4 * 3;

    if (data[input_length - 1] == '=') (*output_length)--;
    if (data[input_length - 2] == '=') (*output_length)--;

    uint8_t* decoded_data = (unsigned char*)malloc(*output_length);

    if (decoded_data == NULL)
        return NULL;

    for (int i = 0, j = 0; i < input_length;) {

        uint32_t sextet_a = data[i] == '=' ? 0 & i++ : decoding_table[(unsigned char)data[i++]];
        uint32_t sextet_b = data[i] == '=' ? 0 & i++ : decoding_table[(unsigned char)data[i++]];
        uint32_t sextet_c = data[i] == '=' ? 0 & i++ : decoding_table[(unsigned char)data[i++]];
        uint32_t sextet_d = data[i] == '=' ? 0 & i++ : decoding_table[(unsigned char)data[i++]];

        uint32_t triple = (sextet_a << 3 * 6)
            + (sextet_b << 2 * 6)
            + (sextet_c << 1 * 6)
            + (sextet_d << 0 * 6);

        if (j < *output_length) decoded_data[j++] = (triple >> 2 * 8) & 0xFF;
        if (j < *output_length) decoded_data[j++] = (triple >> 1 * 8) & 0xFF;
        if (j < *output_length) decoded_data[j++] = (triple >> 0 * 8) & 0xFF;

    }

    return decoded_data;

}

/* this is hash function based on splitmix64 from
http://xorshift.di.unimi.it/splitmix64.c */
dragonULInt
dragon_hash_ulint(dragonULInt x)
{
    dragonULInt z = (x += 0x9e3779b97f4a7c15);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
    return z ^ (z >> 31);
}

/* murmur3_32 comes from Wikipedia at
   https://en.wikipedia.org/wiki/MurmurHash
   and has been released into the public domain. */

static inline uint32_t murmur_32_scramble(uint32_t k) {
    k *= 0xcc9e2d51;
    k = (k << 15) | (k >> 17);
    k *= 0x1b873593;
    return k;
}

uint32_t murmur3_32(const uint8_t* key, size_t len, uint32_t seed)
{
    uint32_t h = seed;
    uint32_t k;
    /* Read in groups of 4. */
    for (size_t i = len >> 2; i; i--) {
        // Here is a source of differing results across endiannesses.
        // A swap here has no effects on hash properties though.
        memcpy(&k, key, sizeof(uint32_t));
        key += sizeof(uint32_t);
        h ^= murmur_32_scramble(k);
        h = (h << 13) | (h >> 19);
        h = h * 5 + 0xe6546b64;
    }
    /* Read the rest. */
    k = 0;
    for (size_t i = len & 3; i; i--) {
        k <<= 8;
        k |= key[i - 1];
    }
    // A swap is *not* necessary here because the preceding loop already
    // places the low bytes in the low places according to whatever endianness
    // we use. Swaps only apply when the memory is copied in a chunk.
    h ^= murmur_32_scramble(k);
    /* Finalize. */
    h ^= len;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

dragonULInt
dragon_hash(void* ptr, size_t num_bytes)
{
    //1164799 is a large prime number.
    dragonULInt hash_val = murmur3_32((const uint8_t*)ptr, num_bytes, 1164799);
    return hash_val;
}

bool
dragon_bytes_equal(void* ptr1, void* ptr2, size_t ptr1_numbytes, size_t ptr2_numbytes)
{
    /* It is assumed that each pointer points to a word boundary since memory allocations
       in Dragon always start on word boundaries. */

    if (ptr1_numbytes != ptr2_numbytes)
        return false;

    if (ptr1 == ptr2)
        return true;

    size_t num_words = ptr1_numbytes/sizeof(dragonULInt);
    size_t rem = ptr1_numbytes%sizeof(dragonULInt);
    dragonULInt* first = (dragonULInt*) ptr1;
    dragonULInt* second = (dragonULInt*) ptr2;

    for (size_t i=0;i<num_words;i++)
        if (first[i] != second[i])
            return false;

    uint8_t* first_bytes = (uint8_t*)&first[num_words];
    uint8_t* second_bytes = (uint8_t*)&second[num_words];

    for (size_t i=0;i<rem;i++)
        if (first_bytes[i] != second_bytes[i])
            return false;

    return true;
}

uint64_t
dragon_sec_to_nsec(uint64_t sec)
{
    return sec * 1e9;
}

void
dragon_set_thread_local_mode(bool set_thread_local)
{
    _set_thread_local_mode_channels(set_thread_local);
    _set_thread_local_mode_channelsets(set_thread_local);
    _set_thread_local_mode_managed_memory(set_thread_local);
    _set_thread_local_mode_bcast(set_thread_local);
    _set_thread_local_mode_ddict(set_thread_local);
    _set_thread_local_mode_fli(set_thread_local);
    _set_thread_local_mode_queues(set_thread_local);

    dg_thread_local_mode = set_thread_local;
}

bool
dragon_get_thread_local_mode()
{
    return dg_thread_local_mode;
}

bool
dragon_check_dir_rw_permissions(char *dir)
{
    struct stat st;

    if (stat(dir, &st) == 0) {
        return (bool) (st.st_mode & S_IWOTH) && (bool) (st.st_mode & S_IROTH);
    }
    else {
        return false;
    }
}

dragonError_t
dragon_get_hugepage_mount(char **mount_dir_out)
{
    const size_t buf_size = 1024;
    char buf[buf_size];
    char *maybe_buf = NULL;
    FILE *mounts_file = NULL;

    static char mount_dir[HUGEPAGE_MOUNT_DIR_MAX_LEN];
    static bool found_mount_dir = false;

    if (found_mount_dir) {
        *mount_dir_out = mount_dir;
    }

    static bool check_envar = true;
    static bool enable_huge_pages = true;

    if (check_envar) {
        char *enable_hugepages_envar = getenv("_DRAGON_ENABLE_HUGEPAGES");
        if (enable_hugepages_envar != NULL) {
            enable_huge_pages = (bool) atoi(enable_hugepages_envar);
        }
        check_envar = false;
    }

    if (!enable_huge_pages) {
        no_err_return(DRAGON_NOT_FOUND);
    }

    mounts_file = fopen("/proc/mounts", "r");
    if (!mounts_file) {
        no_err_return(DRAGON_NOT_FOUND);
    }

    while ((maybe_buf = fgets(buf, buf_size, mounts_file))) {
        if (strstr(buf, "hugetlbfs") != NULL && strstr(buf, "pagesize=2M") != NULL) {
            // get device (not needed)
            char *tmp_tok = strtok(maybe_buf, " ");
            // get mount point
            tmp_tok = strtok(NULL, " ");
            if (dragon_check_dir_rw_permissions(tmp_tok)) {
                snprintf(mount_dir, HUGEPAGE_MOUNT_DIR_MAX_LEN, "%s", tmp_tok);
                *mount_dir_out = mount_dir;
                found_mount_dir = true;
                no_err_return(DRAGON_SUCCESS);
            }
        }
    }

    no_err_return(DRAGON_NOT_FOUND);
}
