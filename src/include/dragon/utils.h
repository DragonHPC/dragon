#ifndef HAVE_DRAGON_UTILS_H
#define HAVE_DRAGON_UTILS_H

#include <stdint.h>
#include <time.h>
#include <stdbool.h>
#include <sys/prctl.h>
#include <dragon/return_codes.h>
#include <dragon/global_types.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DRAGON_TWO_MEG (1UL << 21)

dragonULInt
dragon_get_local_rt_uid();

dragonError_t
dragon_set_procname(char * name);

void
dragon_zero_uuid(dragonUUID uuid);

char*
dragon_uuid_to_hex_str(dragonUUID uuid);

void
dragon_generate_uuid(dragonUUID uuid);

void
dragon_copy_uuid(dragonUUID dest, const dragonUUID src);

int
dragon_compare_uuid(const dragonUUID u1, const dragonUUID u2);

dragonError_t
dragon_encode_uuid(const dragonUUID uuid, void * ptr);

dragonError_t
dragon_decode_uuid(const void * ptr, dragonUUID uuid);

dragonULInt
dragon_get_host_id_from_uuid(dragonUUID uuid);

pid_t
dragon_get_pid_from_uuid(dragonUUID uuid);

uint32_t
dragon_get_ctr_from_uuid(dragonUUID uuid);

dragonError_t
dragon_timespec_add(timespec_t* result, const timespec_t* first, const timespec_t* second);

dragonError_t
dragon_timespec_diff(timespec_t* result, const timespec_t* first, const timespec_t* second);

bool
dragon_timespec_le(const timespec_t* first, const timespec_t* second);

dragonError_t
dragon_timespec_deadline(const timespec_t* timer, timespec_t* deadline);

dragonError_t
dragon_timespec_remaining(const timespec_t * deadline, timespec_t * remaining_timeout);

double dragon_get_current_time_as_double();

char*
dragon_base64_encode(uint8_t *data, size_t input_length);

uint8_t*
dragon_base64_decode(const char *data, size_t *output_length);

dragonULInt
dragon_hash_ulint(dragonULInt x);

dragonULInt
dragon_hash(void* ptr, size_t num_bytes);

bool
dragon_bytes_equal(void* ptr1, void* ptr2, size_t ptr1_numbytes, size_t ptr2_numbytes);

dragonError_t
dragon_ls_set_kv(const unsigned char* key, const unsigned char* value, const timespec_t* timeout);

dragonError_t
dragon_ls_get_kv(const unsigned char* key, char** value, const timespec_t* timeout);

uint64_t
dragon_sec_to_nsec(uint64_t sec);

dragonULInt
dragon_get_my_puid();

dragonError_t
dragon_set_env_var_as_ulint(char* env_key, dragonULInt val);

dragonULInt
dragon_get_env_var_as_ulint(char* env_key);

dragonError_t
dragon_unset_env_var(char* env_key);

void
dragon_set_thread_local_mode(bool set_thread_local);

bool
dragon_get_thread_local_mode(void);

bool
dragon_check_dir_rw_permissions(char *dir);

dragonError_t
dragon_get_hugepage_mount(char **mount_dir);

#ifdef __cplusplus
}
#endif

#endif
